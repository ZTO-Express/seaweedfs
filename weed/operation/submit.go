package operation

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"mime"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/shirou/gopsutil/v4/mem"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

type FilePart struct {
	Reader      io.Reader
	FileName    string
	FileSize    int64
	MimeType    string
	ModTime     int64 //in seconds
	Replication string
	Collection  string
	DataCenter  string
	Ttl         string
	DiskType    string
	Server      string //this comes from assign result
	Fid         string //this comes from assign result, but customizable
	Fsync       bool
}

type SubmitResult struct {
	FileName string `json:"fileName,omitempty"`
	FileUrl  string `json:"url,omitempty"`
	Fid      string `json:"fid,omitempty"`
	Size     uint64 `json:"size,omitempty"`
	Error    string `json:"error,omitempty"`
}

type AsyncChunkUploadResult struct {
	index int64
	fid   string
	count uint32
	err   error
}

type GetMasterFn func(ctx context.Context) pb.ServerAddress

func SubmitFiles(masterFn GetMasterFn, grpcDialOption grpc.DialOption, files []FilePart, replication string, collection string,
	dataCenter string, ttl string, diskType string, maxMB int, usePublicUrl bool, username, password string, chunkConcurrent int) ([]SubmitResult, error) {
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		results[index].FileName = file.FileName
	}

	var basicAuth string
	if username != "" {
		basicAuth = genBasicAuth(username, password)
	}

	// 为每个文件单独处理，在Upload内部进行assign
	for index, file := range files {
		file.Replication = replication
		file.Collection = collection
		file.DataCenter = dataCenter
		file.Ttl = ttl
		file.DiskType = diskType

		// 在Upload方法内部进行assign，确保assign和upload之间时间间隔最小
		size, fid, fileUrl, err := file.UploadWithAssign(maxMB, masterFn, usePublicUrl, basicAuth, grpcDialOption, chunkConcurrent)
		if err != nil {
			results[index].Error = err.Error()
		} else {
			results[index].Size = size
			results[index].Fid = fid
			results[index].FileUrl = fileUrl
		}
	}
	return results, nil
}

func NewFileParts(fullPathFilenames []string) (ret []FilePart, err error) {
	ret = make([]FilePart, len(fullPathFilenames))
	for index, file := range fullPathFilenames {
		if ret[index], err = newFilePart(file); err != nil {
			return
		}
	}
	return
}
func newFilePart(fullPathFilename string) (ret FilePart, err error) {
	fh, openErr := os.Open(fullPathFilename)
	if openErr != nil {
		glog.V(0).Info("Failed to open file: ", fullPathFilename)
		return ret, openErr
	}
	ret.Reader = fh

	fi, fiErr := fh.Stat()
	if fiErr != nil {
		glog.V(0).Info("Failed to stat file:", fullPathFilename)
		return ret, fiErr
	}
	ret.ModTime = fi.ModTime().UTC().Unix()
	ret.FileSize = fi.Size()
	ext := strings.ToLower(path.Ext(fullPathFilename))
	ret.FileName = fi.Name()
	if ext != "" {
		ret.MimeType = mime.TypeByExtension(ext)
	}

	return ret, nil
}

// buildUrlWithParams 构建带参数的URL
func buildUrlWithParams(baseUrl string, modTime int64, fsync bool) string {
	url := baseUrl
	params := make([]string, 0, 2)

	if modTime != 0 {
		params = append(params, "ts="+strconv.Itoa(int(modTime)))
	}
	if fsync {
		params = append(params, "fsync=true")
	}

	if len(params) > 0 {
		url += "?" + strings.Join(params, "&")
	}
	return url
}

func (fi FilePart) UploadWithAssign(maxMB int, masterFn GetMasterFn, usePublicUrl bool, authHeader string, grpcDialOption grpc.DialOption, chunkConcurrent int) (retSize uint64, fid string, url string, err error) {
	if closer, ok := fi.Reader.(io.Closer); ok {
		defer closer.Close()
	}
	baseName := path.Base(fi.FileName)
	if maxMB > 0 && fi.FileSize > int64(maxMB*1024*1024) {
		chunkSize := int64(maxMB * 1024 * 1024)
		chunks := fi.FileSize/chunkSize + 1
		cm := ChunkManifest{
			Name:   baseName,
			Size:   fi.FileSize,
			Mime:   fi.MimeType,
			Chunks: make([]*ChunkInfo, chunks),
		}

		// 不再预先批量assign，每个chunk在上传前单独assign
		var offset int64
		var concurrent = chunkConcurrent
		var response = make(chan *AsyncChunkUploadResult, chunks)
		var sem = util.NewSemaphore(concurrent)

		fmt.Println("Upload file, chunks ", chunks, "concurrent", concurrent, "baseName", baseName, "maxMB", maxMB, "collection", fi.Collection, "ttl", fi.Ttl, "diskType", fi.DiskType)

		for i := int64(0); i < chunks; i++ {
			filename := baseName + "-" + strconv.FormatInt(i+1, 10)
			//reader := io.LimitReader(fi.Reader, chunkSize)
			ar := &VolumeAssignRequest{
				Count:       1,
				Replication: fi.Replication,
				Collection:  fi.Collection,
				Ttl:         fi.Ttl,
				DiskType:    fi.DiskType,
			}
			newReader := io.NewSectionReader(fi.Reader.(*os.File), offset, chunkSize)
			offset += chunkSize

			sem.Acquire()
			go uploadAsync(i, filename, fi.DataCenter, newReader, masterFn, usePublicUrl, authHeader, grpcDialOption, ar, response, sem)
		}

		glog.V(1).Infof("[监控] UploadWithAssign: 等待接收 %d 个chunk的结果", chunks)

		for i := 0; i < int(chunks); i++ {
			r := <-response
			glog.V(1).Infof("[监控] UploadWithAssign: 收到chunk %d 结果 - fid: %s, count: %d, err: %v", r.index, r.fid, r.count, r.err)

			if r.err != nil {
				err = r.err
				glog.Errorf("[监控] UploadWithAssign: chunk %d 失败，设置全局错误: %v", r.index, r.err)
				continue
			}

			// 检查 count 是否为 0
			if r.count == 0 {
				err = fmt.Errorf("chunk %d uploaded 0 bytes", r.index)
				glog.Errorf("[监控] UploadWithAssign: chunk %d 上传了0字节，设置全局错误: %v", r.index, err)
				continue
			}

			glog.V(1).Infof("[监控] UploadWithAssign: chunk %d 成功，创建ChunkInfo", r.index)
			cm.Chunks[r.index] = &ChunkInfo{
				Offset: r.index * chunkSize,
				Size:   int64(r.count),
				Fid:    r.fid,
			}
			retSize += uint64(r.count)
		}
		close(response)

		glog.V(1).Infof("[监控] UploadWithAssign: 所有chunk处理完成 - 全局错误: %v, 总大小: %d", err, retSize)

		// 验证chunk完整性
		var missingChunks []int64
		var successChunks []int64
		for i := int64(0); i < chunks; i++ {
			if cm.Chunks[i] == nil {
				missingChunks = append(missingChunks, i)
			} else {
				successChunks = append(successChunks, i)
			}
		}
		glog.V(1).Infof("[监控] UploadWithAssign: 成功的chunk: %v, 缺失的chunk: %v", successChunks, missingChunks)

		if err != nil {
			// delete all uploaded chunks
			glog.Errorf("[监控] UploadWithAssign: 因为错误退出，删除所有已上传的chunk: %v", err)
			cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption, false)
			return 0, "", "", err
		}

		// 所有chunks上传完成后，为文件清单重新assign
		glog.V(1).Infof("[监控] UploadWithAssign: 开始为文件清单分配volume")
		ar := &VolumeAssignRequest{
			Count:       1,
			Replication: fi.Replication,
			Collection:  fi.Collection,
			DataCenter:  fi.DataCenter,
			Ttl:         fi.Ttl,
			DiskType:    fi.DiskType,
		}
		ret, assignErr := Assign(masterFn, grpcDialOption, ar)
		if assignErr != nil {
			// delete all uploaded chunks
			glog.Errorf("[监控] UploadWithAssign: 文件清单volume分配失败: %v", assignErr)
			cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption, false)
			return 0, "", "", assignErr
		}

		glog.V(1).Infof("[监控] UploadWithAssign: 文件清单volume分配成功 - fid: %s", ret.Fid)

		// 构建文件清单的URL
		var manifestBaseUrl string
		if usePublicUrl {
			manifestBaseUrl = "http://" + ret.PublicUrl + "/" + ret.Fid
		} else {
			manifestBaseUrl = "http://" + ret.Url + "/" + ret.Fid
		}
		manifestUrl := buildUrlWithParams(manifestBaseUrl, fi.ModTime, fi.Fsync)

		glog.V(1).Infof("[监控] UploadWithAssign: 开始上传文件清单到: %s", manifestUrl)
		err = upload_chunked_file_manifest(manifestUrl, &cm, ret.Auth, authHeader)
		if err != nil {
			// delete all uploaded chunks
			glog.Errorf("[监控] UploadWithAssign: 文件清单上传失败: %v", err)
			cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption, false)
			return 0, "", "", err
		}

		glog.V(1).Infof("[监控] UploadWithAssign: 文件清单上传成功，返回结果 - fid: %s, url: %s, size: %d",
			ret.Fid, ret.PublicUrl+"/"+ret.Fid, retSize)

		// 返回文件清单的fid
		return retSize, ret.Fid, ret.PublicUrl + "/" + ret.Fid, nil
	} else {
		// 在实际上传前进行assign
		ar := &VolumeAssignRequest{
			Count:       1,
			Replication: fi.Replication,
			Collection:  fi.Collection,
			DataCenter:  fi.DataCenter,
			Ttl:         fi.Ttl,
			DiskType:    fi.DiskType,
		}
		assignRep, assignError := Assign(masterFn, grpcDialOption, ar)
		if assignError != nil {
			return 0, "", "", assignError
		}
		var baseUrl string
		if usePublicUrl {
			baseUrl = "http://" + assignRep.PublicUrl + "/" + assignRep.Fid
		} else {
			baseUrl = "http://" + assignRep.Url + "/" + assignRep.Fid
		}
		fileUrl := buildUrlWithParams(baseUrl, fi.ModTime, fi.Fsync)

		uploadOption := &UploadOption{
			UploadUrl:         fileUrl,
			Filename:          baseName,
			Cipher:            false,
			IsInputCompressed: false,
			MimeType:          fi.MimeType,
			PairMap:           nil,
			Jwt:               assignRep.Auth,
			AuthHeader:        authHeader,
		}
		ret, e, _ := Upload(fi.Reader, uploadOption)
		if e != nil {
			return 0, "", "", e
		}
		// 对于非分块上传，返回原始fid
		return uint64(ret.Size), assignRep.Fid, assignRep.PublicUrl + "/" + assignRep.Fid, nil
	}
}

func (fi FilePart) Upload(maxMB int, masterFn GetMasterFn, usePublicUrl bool, jwt security.EncodedJwt, authHeader string, grpcDialOption grpc.DialOption, chunkConcurrent int) (retSize uint32, err error) {
	fileUrl := "http://" + fi.Server + "/" + fi.Fid
	if fi.ModTime != 0 {
		fileUrl += "?ts=" + strconv.Itoa(int(fi.ModTime))
	}
	if fi.Fsync {
		fileUrl += "?fsync=true"
	}
	if closer, ok := fi.Reader.(io.Closer); ok {
		defer closer.Close()
	}
	baseName := path.Base(fi.FileName)
	if maxMB > 0 && fi.FileSize > int64(maxMB*1024*1024) {
		chunkSize := int64(maxMB * 1024 * 1024)
		chunks := fi.FileSize/chunkSize + 1
		cm := ChunkManifest{
			Name:   baseName,
			Size:   fi.FileSize,
			Mime:   fi.MimeType,
			Chunks: make([]*ChunkInfo, chunks),
		}

		var offset int64
		var concurrent = chunkConcurrent
		var response = make(chan *AsyncChunkUploadResult, chunks)
		var sem = util.NewSemaphore(concurrent)

		fmt.Println("Upload file, chunks ", chunks, "concurrent", concurrent, "baseName", baseName, "maxMB", maxMB, "collection", fi.Collection, "ttl", fi.Ttl, "diskType", fi.DiskType)

		for i := int64(0); i < chunks; i++ {
			filename := baseName + "-" + strconv.FormatInt(i+1, 10)
			//reader := io.LimitReader(fi.Reader, chunkSize)
			ar := &VolumeAssignRequest{
				Count:       1,
				Replication: fi.Replication,
				Collection:  fi.Collection,
				Ttl:         fi.Ttl,
				DiskType:    fi.DiskType,
			}
			newReader := io.NewSectionReader(fi.Reader.(*os.File), offset, chunkSize)
			offset += chunkSize

			sem.Acquire()
			go uploadAsync(i, filename, fi.DataCenter, newReader, masterFn, usePublicUrl, authHeader, grpcDialOption, ar, response, sem)
		}

		for i := 0; i < int(chunks); i++ {
			r := <-response
			if r.err != nil {
				err = r.err
				break
			}
			cm.Chunks[r.index] = &ChunkInfo{
				Offset: r.index * chunkSize,
				Size:   int64(r.count),
				Fid:    r.fid,
			}
			retSize += r.count
		}
		close(response)
		if err != nil {
			// delete all uploaded chunks
			cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption, false)
			return
		}

		err = upload_chunked_file_manifest(fileUrl, &cm, jwt, authHeader)
		if err != nil {
			// delete all uploaded chunks
			cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption, false)
		}
	} else {
		uploadOption := &UploadOption{
			UploadUrl:         fileUrl,
			Filename:          baseName,
			Cipher:            false,
			IsInputCompressed: false,
			MimeType:          fi.MimeType,
			PairMap:           nil,
			Jwt:               jwt,
			AuthHeader:        authHeader,
		}
		ret, e, _ := Upload(fi.Reader, uploadOption)
		if e != nil {
			return 0, e
		}
		return ret.Size, e
	}
	return
}

// 计算多大的并发数同时上传chunks
func calculateConcurrent(chunkSize int64, chunks int64) int {
	v, err := mem.VirtualMemory()
	if err != nil {
		return int(math.Min(float64(chunks), 10))
	}
	avail := (float64(v.Available) * 0.7) / float64(chunkSize) //基于70%的可用内存计算
	if avail < 10 {
		return int(math.Min(float64(chunks), 10))
	}
	return int(math.Min(float64(chunks), avail))
}

func uploadAsync(index int64, filename, dataCenter string, reader io.Reader, masterFn GetMasterFn, usePublicUrl bool, authHeader string,
	grpcDialOption grpc.DialOption, ar *VolumeAssignRequest, resp chan<- *AsyncChunkUploadResult, sem *util.Semaphore) {
	defer func() {
		sem.Release()
	}()

	startTime := time.Now()
	glog.V(1).Infof("[监控] uploadAsync: 开始处理chunk %d (%s)", index, filename)

	fmt.Println("Uploading chunks async,index ", index, "filename ", filename, " dataCenter ", dataCenter, "usePublicUrl", usePublicUrl, "currentTime", time.Now().Format("2006-01-02 15:04:05.010101"))

	var res = AsyncChunkUploadResult{}
	var id string
	var err error

	// 在实际上传chunk前进行assign，避免EC编码问题
	glog.V(2).Infof("[监控] uploadAsync: chunk %d 开始分配volume", index)
	ret, err := Assign(masterFn, grpcDialOption, ar)
	if err != nil {
		glog.Errorf("[监控] uploadAsync: chunk %d volume分配失败: %v", index, err)
		res.err = err
		resp <- &res
		return
	}
	id = ret.Fid
	fileUrl := "http://" + ret.Url + "/" + id
	if usePublicUrl {
		fileUrl = "http://" + ret.PublicUrl + "/" + id
	}

	count, e := upload_one_chunk(filename, reader, masterFn, fileUrl, ret.Auth, authHeader)
	res.err = e
	res.count = count
	res.fid = id
	res.index = index
	resp <- &res

	//if e != nil {
	//	// delete all uploaded chunks
	//	cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption)
	//	return 0, e
	//}
}

func upload_one_chunk(filename string, reader io.Reader, masterFn GetMasterFn,
	fileUrl string, jwt security.EncodedJwt, basicAuth string,
) (size uint32, e error) {
	start := time.Now()
	uploadOption := &UploadOption{
		UploadUrl:         fileUrl,
		Filename:          filename,
		Cipher:            false,
		IsInputCompressed: false,
		MimeType:          "",
		PairMap:           nil,
		Jwt:               jwt,
		AuthHeader:        basicAuth,
		StreamMode:        true,
	}
	uploadResult, uploadError, _ := Upload(reader, uploadOption)

	if uploadError != nil {
		glog.Errorf("Uploading chunk finish, filename:%s,to%s, err:%v", filename, fileUrl, uploadError)
		return 0, uploadError
	}
	glog.V(1).Infof("Uploading chunk success, filename:%s, to%s, resultSize:%d, cost:%f s", filename, fileUrl, uploadResult.Size, time.Since(start).Seconds())
	return uploadResult.Size, nil
}

func upload_chunked_file_manifest(fileUrl string, manifest *ChunkManifest, jwt security.EncodedJwt, basicAuth string) error {
	buf, e := manifest.Marshal()
	if e != nil {
		return e
	}
	glog.V(4).Info("Uploading chunks manifest ", manifest.Name, " to ", fileUrl, "...")
	u, _ := url.Parse(fileUrl)
	q := u.Query()
	q.Set("cm", "true")
	u.RawQuery = q.Encode()
	uploadOption := &UploadOption{
		UploadUrl:         u.String(),
		Filename:          manifest.Name,
		Cipher:            false,
		IsInputCompressed: false,
		MimeType:          "application/json",
		PairMap:           nil,
		Jwt:               jwt,
		AuthHeader:        basicAuth,
	}
	_, e = UploadData(buf, uploadOption)
	return e
}

func genBasicAuth(username, password string) string {
	auth := username + ":" + password
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
}
