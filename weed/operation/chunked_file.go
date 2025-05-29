package operation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	// when the remote server does not allow range requests (Accept-Ranges was not set)
	ErrRangeRequestsNotSupported = errors.New("Range requests are not supported by the remote server")
	// ErrInvalidRange is returned by Read when trying to read past the end of the file
	ErrInvalidRange = errors.New("Invalid range")
)

type ChunkInfo struct {
	Fid    string `json:"fid"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

type ChunkList []*ChunkInfo

type ChunkManifest struct {
	Name   string    `json:"name,omitempty"`
	Mime   string    `json:"mime,omitempty"`
	Size   int64     `json:"size,omitempty"`
	Chunks ChunkList `json:"chunks,omitempty"`
}

// seekable chunked file reader
type ChunkedFileReader struct {
	totalSize      int64
	chunkList      []*ChunkInfo
	master         pb.ServerAddress
	pos            int64
	pr             *io.PipeReader
	pw             *io.PipeWriter
	mutex          sync.Mutex
	grpcDialOption grpc.DialOption
	authHeader     string
}

func (s ChunkList) Len() int           { return len(s) }
func (s ChunkList) Less(i, j int) bool { return s[i].Offset < s[j].Offset }
func (s ChunkList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func LoadChunkManifest(buffer []byte, isCompressed bool) (*ChunkManifest, error) {
	if isCompressed {
		var err error
		if buffer, err = util.DecompressData(buffer); err != nil {
			glog.V(0).Infof("fail to decompress chunk manifest: %v", err)
		}
	}
	cm := ChunkManifest{}
	if e := json.Unmarshal(buffer, &cm); e != nil {
		return nil, e
	}
	sort.Sort(cm.Chunks)
	return &cm, nil
}

func (cm *ChunkManifest) Marshal() ([]byte, error) {
	return json.Marshal(cm)
}

func (cm *ChunkManifest) DeleteChunks(masterFn GetMasterFn, usePublicUrl bool, grpcDialOption grpc.DialOption) error {
	startTime := time.Now()
	var fileIds []string
	for _, ci := range cm.Chunks {
		fileIds = append(fileIds, ci.Fid)
	}
	glog.V(4).Infof("开始删除分块文件，总数量: %d", len(fileIds))
	results, err := DeleteFiles(masterFn, usePublicUrl, grpcDialOption, fileIds)
	if err != nil {
		glog.V(0).Infof("删除分块文件失败 %+v: %v, 耗时: %v", fileIds, err, time.Since(startTime))
		return fmt.Errorf("chunk delete: %v", err)
	}
	var errorCount int
	for _, result := range results {
		if result.Error != "" {
			errorCount++
			glog.V(0).Infof("删除文件失败 %+v: %v", result.FileId, result.Error)
			return fmt.Errorf("chunk delete %v: %v", result.FileId, result.Error)
		}
	}

	// 删除成功后，异步触发EC卷的垃圾回收
	go func() {
		// 从fileIds中提取所有volumeId
		volumeIdMap := make(map[uint32]bool)
		for _, fileId := range fileIds {
			vid, _, parseErr := ParseFileId(fileId)
			if parseErr == nil {
				if volumeId, convErr := strconv.ParseUint(vid, 10, 32); convErr == nil {
					volumeIdMap[uint32(volumeId)] = true
				}
			}
		}

		// 并发调用master的vacuum方法处理每个volumeId
		var wg sync.WaitGroup
		semaphore := make(chan struct{}, 50) // 限制并发数量为50
		for volumeId := range volumeIdMap {
			wg.Add(1)
			go func(vid uint32) {
				defer wg.Done()
				semaphore <- struct{}{}        // 获取信号量
				defer func() { <-semaphore }() // 释放信号量
				// 通过gRPC调用master的VacuumVolume方法
				masterAddress := masterFn(context.Background())
				if masterAddress == "" {
					glog.V(0).Infof("无法获取master地址，跳过EC卷 %d 的垃圾回收", vid)
					return
				}
				err := pb.WithMasterClient(false, masterAddress, grpcDialOption, false, func(client master_pb.SeaweedClient) error {
					_, vacuumErr := client.VacuumEcVolume(context.Background(), &master_pb.VacuumEcVolumeRequest{
						VolumeId: vid,
					})
					return vacuumErr
				})
				if err != nil {
					glog.V(0).Infof("EC卷 %d 垃圾回收失败: %v", vid, err)
				} else {
					glog.V(1).Infof("EC卷 %d 垃圾回收已触发", vid)
				}
			}(volumeId)
		}
		wg.Wait()
		glog.V(1).Infof("chunks文件删除后的EC卷垃圾回收处理完成，处理的卷数量: %d", len(volumeIdMap))
	}()

	glog.V(4).Infof("分块文件删除完成，总数量: %d，成功: %d，失败: %d，总耗时: %v",
		len(fileIds), len(results)-errorCount, errorCount, time.Since(startTime))
	return nil
}

func readChunkNeedle(fileUrl string, w io.Writer, offset int64, jwt, authHeader string) (written int64, e error) {
	req, err := http.NewRequest(http.MethodGet, fileUrl, nil)
	if err != nil {
		return written, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

	resp, err := util.Do(req)
	if err != nil {
		return written, err
	}
	defer util.CloseResponse(resp)

	switch resp.StatusCode {
	case http.StatusRequestedRangeNotSatisfiable:
		return written, ErrInvalidRange
	case http.StatusOK:
		if offset > 0 {
			return written, ErrRangeRequestsNotSupported
		}
	case http.StatusPartialContent:
		break
	default:
		return written, fmt.Errorf("Read chunk needle error: [%d] %s", resp.StatusCode, fileUrl)

	}
	return io.Copy(w, resp.Body)
}

func NewChunkedFileReader(chunkList []*ChunkInfo, master pb.ServerAddress, grpcDialOption grpc.DialOption, authHeader string) *ChunkedFileReader {
	var totalSize int64
	for _, chunk := range chunkList {
		totalSize += chunk.Size
	}
	sort.Sort(ChunkList(chunkList))
	return &ChunkedFileReader{
		totalSize:      totalSize,
		chunkList:      chunkList,
		master:         master,
		grpcDialOption: grpcDialOption,
		authHeader:     authHeader,
	}
}

func (cf *ChunkedFileReader) Seek(offset int64, whence int) (int64, error) {
	var err error
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += cf.pos
	case io.SeekEnd:
		offset = cf.totalSize + offset
	}
	if offset > cf.totalSize {
		err = ErrInvalidRange
	}
	if cf.pos != offset {
		cf.Close()
	}
	cf.pos = offset
	return cf.pos, err
}

func (cf *ChunkedFileReader) WriteTo(w io.Writer) (n int64, err error) {
	chunkIndex := -1
	chunkStartOffset := int64(0)
	for i, ci := range cf.chunkList {
		if cf.pos >= ci.Offset && cf.pos < ci.Offset+ci.Size {
			chunkIndex = i
			chunkStartOffset = cf.pos - ci.Offset
			break
		}
	}
	if chunkIndex < 0 {
		return n, ErrInvalidRange
	}
	for ; chunkIndex < len(cf.chunkList); chunkIndex++ {
		ci := cf.chunkList[chunkIndex]
		// if we need read date from local volume server first?
		fileUrl, jwt, lookupError := LookupFileId(func(_ context.Context) pb.ServerAddress {
			return cf.master
		}, cf.grpcDialOption, ci.Fid)
		if lookupError != nil {
			return n, lookupError
		}
		if wn, e := readChunkNeedle(fileUrl, w, chunkStartOffset, jwt, cf.authHeader); e != nil {
			return n, e
		} else {
			n += wn
			cf.pos += wn
		}

		chunkStartOffset = 0
	}
	return n, nil
}

func (cf *ChunkedFileReader) ReadAt(p []byte, off int64) (n int, err error) {
	cf.Seek(off, 0)
	return cf.Read(p)
}

func (cf *ChunkedFileReader) Read(p []byte) (int, error) {
	return cf.getPipeReader().Read(p)
}

func (cf *ChunkedFileReader) Close() (e error) {
	cf.mutex.Lock()
	defer cf.mutex.Unlock()
	return cf.closePipe()
}

func (cf *ChunkedFileReader) closePipe() (e error) {
	if cf.pr != nil {
		if err := cf.pr.Close(); err != nil {
			e = err
		}
	}
	cf.pr = nil
	if cf.pw != nil {
		if err := cf.pw.Close(); err != nil {
			e = err
		}
	}
	cf.pw = nil
	return e
}

func (cf *ChunkedFileReader) getPipeReader() io.Reader {
	cf.mutex.Lock()
	defer cf.mutex.Unlock()
	if cf.pr != nil && cf.pw != nil {
		return cf.pr
	}
	cf.closePipe()
	cf.pr, cf.pw = io.Pipe()
	go func(pw *io.PipeWriter) {
		_, e := cf.WriteTo(pw)
		pw.CloseWithError(e)
	}(cf.pw)
	return cf.pr
}
