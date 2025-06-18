package weed_server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/images"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var fileNameEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

func NotFound(w http.ResponseWriter) {
	stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorGetNotFound).Inc()
	w.WriteHeader(http.StatusNotFound)
}

func InternalError(w http.ResponseWriter) {
	stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorGetInternal).Inc()
	w.WriteHeader(http.StatusInternalServerError)
}

func (vs *VolumeServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request) {
	n := new(needle.Needle)
	vid, fid, filename, ext, _ := parseURLPath(r.URL.Path)

	if !vs.maybeCheckJwtAuthorization(r, vid, fid, false) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infof("parsing vid %s: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = n.ParsePath(fid)
	if err != nil {
		glog.V(2).Infof("parsing fid %s: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// glog.V(4).Infoln("volume", volumeId, "reading", n)
	hasVolume := vs.store.HasVolume(volumeId)
	_, hasEcVolume := vs.store.FindEcVolume(volumeId)
	if !hasVolume && !hasEcVolume {
		if vs.ReadMode == "local" {
			glog.V(0).Infoln("volume is not local:", err, r.URL.Path)
			NotFound(w)
			return
		}
		lookupResult, err := operation.LookupVolumeId(vs.GetMaster, vs.grpcDialOption, volumeId.String())
		glog.V(2).Infoln("volume", volumeId, "found on", lookupResult, "error", err)
		if err != nil || len(lookupResult.Locations) <= 0 {
			glog.V(0).Infoln("lookup error:", err, r.URL.Path)
			NotFound(w)
			return
		}
		if vs.ReadMode == "proxy" {
			// proxy client request to target server
			localUrl := fmt.Sprintf("%s:%d", vs.store.Ip, vs.store.Port)
			var proxyIp string
			for _, loc := range lookupResult.Locations {
				if localUrl == loc.Url {
					continue
				}
				proxyIp = loc.Url
				break
			}
			if proxyIp == "" {
				glog.V(0).Infof("failed to instance http request of location: %v", lookupResult.Locations)
				InternalError(w)
				return
			}
			u, _ := url.Parse(util.NormalizeUrl(proxyIp))
			r.URL.Host = u.Host
			r.URL.Scheme = u.Scheme
			request, err := http.NewRequest(http.MethodGet, r.URL.String(), nil)
			if err != nil {
				glog.V(0).Infof("failed to instance http request of url %s: %v", r.URL.String(), err)
				InternalError(w)
				return
			}
			for k, vv := range r.Header {
				for _, v := range vv {
					request.Header.Add(k, v)
				}
			}

			response, err := client.Do(request)
			if err != nil {
				glog.V(0).Infof("request remote url %s: %v", r.URL.String(), err)
				InternalError(w)
				return
			}
			defer util.CloseResponse(response)
			// proxy target response to client
			for k, vv := range response.Header {
				for _, v := range vv {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(response.StatusCode)
			buf := mem.Allocate(128 * 1024)
			defer mem.Free(buf)
			io.CopyBuffer(w, response.Body, buf)
			return
		} else {
			// redirect
			u, _ := url.Parse(util.NormalizeUrl(lookupResult.Locations[0].PublicUrl))
			u.Path = fmt.Sprintf("%s/%s,%s", u.Path, vid, fid)
			arg := url.Values{}
			if c := r.FormValue("collection"); c != "" {
				arg.Set("collection", c)
			}
			u.RawQuery = arg.Encode()
			http.Redirect(w, r, u.String(), http.StatusMovedPermanently)
			return
		}
	}
	cookie := n.Cookie

	readOption := &storage.ReadOption{
		ReadDeleted:    r.FormValue("readDeleted") == "true",
		HasSlowRead:    vs.hasSlowRead,
		ReadBufferSize: vs.readBufferSizeMB * 1024 * 1024,
	}

	var count int
	var memoryCost types.Size
	readOption.AttemptMetaOnly, readOption.MustMetaOnly = shouldAttemptStreamWrite(hasVolume, ext, r)
	onReadSizeFn := func(size types.Size) {
		memoryCost = size
		atomic.AddInt64(&vs.inFlightDownloadDataSize, int64(memoryCost))
	}
	if hasVolume {
		count, err = vs.store.ReadVolumeNeedle(volumeId, n, readOption, onReadSizeFn)
	} else if hasEcVolume {
		count, err = vs.store.ReadEcShardNeedle(volumeId, n, onReadSizeFn)
	}
	defer func() {
		atomic.AddInt64(&vs.inFlightDownloadDataSize, -int64(memoryCost))
		vs.inFlightDownloadDataLimitCond.Signal()
	}()

	if err != nil && err != storage.ErrorDeleted && hasVolume {
		glog.V(4).Infof("read needle: %v", err)
		// start to fix it from other replicas, if not deleted and hasVolume and is not a replicated request
	}
	// glog.V(4).Infoln("read bytes", count, "error", err)
	if err != nil || count < 0 {
		glog.V(3).Infof("read %s isNormalVolume %v error: %v", r.URL.Path, hasVolume, err)
		if err == storage.ErrorNotFound || err == storage.ErrorDeleted {
			NotFound(w)
		} else {
			InternalError(w)
		}
		return
	}
	if n.Cookie != cookie {
		glog.V(0).Infof("request %s with cookie:%x expected:%x from %s agent %s", r.URL.Path, cookie, n.Cookie, r.RemoteAddr, r.UserAgent())
		NotFound(w)
		return
	}
	if n.LastModified != 0 {
		w.Header().Set("Last-Modified", time.Unix(int64(n.LastModified), 0).UTC().Format(http.TimeFormat))
		if r.Header.Get("If-Modified-Since") != "" {
			if t, parseError := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since")); parseError == nil {
				if t.Unix() >= int64(n.LastModified) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
	}
	if inm := r.Header.Get("If-None-Match"); inm == "\""+n.Etag()+"\"" {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	SetEtag(w, n.Etag())

	if n.HasPairs() {
		pairMap := make(map[string]string)
		err = json.Unmarshal(n.Pairs, &pairMap)
		if err != nil {
			glog.V(0).Infoln("Unmarshal pairs error:", err)
		}
		for k, v := range pairMap {
			w.Header().Set(k, v)
		}
	}

	if vs.tryHandleChunkedFile(n, filename, ext, w, r) {
		return
	}

	if n.NameSize > 0 && filename == "" {
		filename = string(n.Name)
		if ext == "" {
			ext = filepath.Ext(filename)
		}
	}
	mtype := ""
	if n.MimeSize > 0 {
		mt := string(n.Mime)
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mtype = mt
		}
	}

	if n.IsCompressed() {
		_, _, _, shouldResize := shouldResizeImages(ext, r)
		_, _, _, _, shouldCrop := shouldCropImages(ext, r)
		if shouldResize || shouldCrop {
			if n.Data, err = util.DecompressData(n.Data); err != nil {
				glog.V(0).Infoln("ungzip error:", err, r.URL.Path)
			}
			// } else if strings.Contains(r.Header.Get("Accept-Encoding"), "zstd") && util.IsZstdContent(n.Data) {
			//	w.Header().Set("Content-Encoding", "zstd")
		} else if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") && util.IsGzippedContent(n.Data) {
			w.Header().Set("Content-Encoding", "gzip")
		} else {
			if n.Data, err = util.DecompressData(n.Data); err != nil {
				glog.V(0).Infoln("uncompress error:", err, r.URL.Path)
			}
		}
	}

	if !readOption.IsMetaOnly {
		rs := conditionallyCropImages(bytes.NewReader(n.Data), ext, r)
		rs = conditionallyResizeImages(rs, ext, r)
		if e := writeResponseContent(filename, mtype, rs, w, r); e != nil {
			glog.V(2).Infoln("response write error:", e)
		}
	} else {
		vs.streamWriteResponseContent(filename, mtype, volumeId, n, w, r, readOption)
	}
}

func shouldAttemptStreamWrite(hasLocalVolume bool, ext string, r *http.Request) (shouldAttempt bool, mustMetaOnly bool) {
	if !hasLocalVolume {
		return false, false
	}
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	if r.Method == http.MethodHead {
		return true, true
	}
	_, _, _, shouldResize := shouldResizeImages(ext, r)
	_, _, _, _, shouldCrop := shouldCropImages(ext, r)
	if shouldResize || shouldCrop {
		return false, false
	}
	return true, false
}

func (vs *VolumeServer) tryHandleChunkedFile(n *needle.Needle, fileName string, ext string, w http.ResponseWriter, r *http.Request) (processed bool) {
	if !n.IsChunkedManifest() || r.URL.Query().Get("cm") == "false" {
		return false
	}

	chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsCompressed())
	if e != nil {
		glog.V(0).Infof("load chunked manifest (%s) error: %v", r.URL.Path, e)
		return false
	}

	// 在严格模式的HEAD请求时,验证所有chunks的存在性
	strictMode := r.URL.Query().Get("strict") == "true"
	if r.Method == http.MethodHead && strictMode && !vs.validateChunksExistence(chunkManifest.Chunks, r.Header.Get("Authorization")) {
		glog.V(0).Infof("chunks validation failed for manifest (%s)", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return true
	}

	if fileName == "" && chunkManifest.Name != "" {
		fileName = chunkManifest.Name
	}

	if ext == "" {
		ext = filepath.Ext(fileName)
	}

	mType := ""
	if chunkManifest.Mime != "" {
		mt := chunkManifest.Mime
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mType = mt
		}
	}

	w.Header().Set("X-File-Store", "chunked")

	chunkedFileReader := operation.NewChunkedFileReader(chunkManifest.Chunks, vs.GetMaster(context.Background()), vs.grpcDialOption, r.Header.Get("Authorization"))
	defer chunkedFileReader.Close()

	rs := conditionallyCropImages(chunkedFileReader, ext, r)
	rs = conditionallyResizeImages(rs, ext, r)

	if e := writeResponseContent(fileName, mType, rs, w, r); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
	return true
}

func conditionallyResizeImages(originalDataReaderSeeker io.ReadSeeker, ext string, r *http.Request) io.ReadSeeker {
	rs := originalDataReaderSeeker
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	width, height, mode, shouldResize := shouldResizeImages(ext, r)
	if shouldResize {
		rs, _, _ = images.Resized(ext, originalDataReaderSeeker, width, height, mode)
	}
	return rs
}

func shouldResizeImages(ext string, r *http.Request) (width, height int, mode string, shouldResize bool) {
	if ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" || ext == ".webp" {
		if r.FormValue("width") != "" {
			width, _ = strconv.Atoi(r.FormValue("width"))
		}
		if r.FormValue("height") != "" {
			height, _ = strconv.Atoi(r.FormValue("height"))
		}
	}
	mode = r.FormValue("mode")
	shouldResize = width > 0 || height > 0
	return
}

func conditionallyCropImages(originalDataReaderSeeker io.ReadSeeker, ext string, r *http.Request) io.ReadSeeker {
	rs := originalDataReaderSeeker
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	x1, y1, x2, y2, shouldCrop := shouldCropImages(ext, r)
	if shouldCrop {
		var err error
		rs, err = images.Cropped(ext, rs, x1, y1, x2, y2)
		if err != nil {
			glog.Errorf("Cropping images error: %s", err)
		}
	}
	return rs
}

func shouldCropImages(ext string, r *http.Request) (x1, y1, x2, y2 int, shouldCrop bool) {
	if ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" {
		if r.FormValue("crop_x1") != "" {
			x1, _ = strconv.Atoi(r.FormValue("crop_x1"))
		}
		if r.FormValue("crop_y1") != "" {
			y1, _ = strconv.Atoi(r.FormValue("crop_y1"))
		}
		if r.FormValue("crop_x2") != "" {
			x2, _ = strconv.Atoi(r.FormValue("crop_x2"))
		}
		if r.FormValue("crop_y2") != "" {
			y2, _ = strconv.Atoi(r.FormValue("crop_y2"))
		}
	}
	shouldCrop = x1 >= 0 && y1 >= 0 && x2 > x1 && y2 > y1
	return
}

func writeResponseContent(filename, mimeType string, rs io.ReadSeeker, w http.ResponseWriter, r *http.Request) error {
	totalSize, e := rs.Seek(0, 2)
	if mimeType == "" {
		if ext := filepath.Ext(filename); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	w.Header().Set("Accept-Ranges", "bytes")

	AdjustPassthroughHeaders(w, r, filename)

	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return nil
	}

	return ProcessRangeRequest(r, w, totalSize, mimeType, func(offset int64, size int64) (filer.DoStreamContent, error) {
		return func(writer io.Writer) error {
			if _, e = rs.Seek(offset, 0); e != nil {
				return e
			}
			_, e = io.CopyN(writer, rs, size)
			return e
		}, nil
	})
}

func (vs *VolumeServer) streamWriteResponseContent(filename string, mimeType string, volumeId needle.VolumeId, n *needle.Needle, w http.ResponseWriter, r *http.Request, readOption *storage.ReadOption) {
	totalSize := int64(n.DataSize)
	if mimeType == "" {
		if ext := filepath.Ext(filename); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	AdjustPassthroughHeaders(w, r, filename)

	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return
	}

	ProcessRangeRequest(r, w, totalSize, mimeType, func(offset int64, size int64) (filer.DoStreamContent, error) {
		return func(writer io.Writer) error {
			return vs.store.ReadVolumeNeedleDataInto(volumeId, n, readOption, writer, offset, size)
		}, nil
	})

}

// validateChunksExistence 验证所有chunks的存在性
func (vs *VolumeServer) validateChunksExistence(chunks []*operation.ChunkInfo, authHeader string) bool {
	// 严格模式下，先校验所有涉及的volume的可用性
	if !vs.validateVolumesAvailability(chunks) {
		return false
	}
	startTime := time.Now()
	glog.V(1).Infof("[Performance] Starting concurrent chunks validation for %d chunks with concurrency 20", len(chunks))

	// 并发验证chunk，并发数限制为30
	const maxConcurrency = 30
	semaphore := make(chan struct{}, maxConcurrency)
	resultChan := make(chan bool, len(chunks))
	errorChan := make(chan error, len(chunks))

	// 启动goroutine验证每个chunk
	for i, chunk := range chunks {
		go func(index int, c *operation.ChunkInfo) {
			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			chunkStartTime := time.Now()
			glog.V(2).Infof("[Performance] Validating chunk %d/%d: %s", index+1, len(chunks), c.Fid)

			// 通过LookupFileId获取chunk的URL
			lookupStartTime := time.Now()
			fullUrl, jwt, err := operation.LookupFileId(func(_ context.Context) pb.ServerAddress {
				return vs.GetMaster(context.Background())
			}, vs.grpcDialOption, c.Fid)
			lookupDuration := time.Since(lookupStartTime)
			glog.V(2).Infof("[Performance] Lookup chunk %s took %v, url: %s", c.Fid, lookupDuration, fullUrl)

			if err != nil {
				glog.V(0).Infof("lookup chunk %s failed after %v: %v", c.Fid, lookupDuration, err)
				errorChan <- err
				return
			}

			// 发送HEAD请求验证chunk存在性
			reqStartTime := time.Now()
			req, err := http.NewRequest(http.MethodHead, fullUrl, nil)
			if err != nil {
				glog.V(0).Infof("create HEAD request for chunk %s failed: %v", c.Fid, err)
				errorChan <- err
				return
			}

			// 添加认证头
			if jwt != "" {
				req.Header.Set("Authorization", "BEARER "+string(jwt))
			} else if authHeader != "" {
				req.Header.Set("Authorization", authHeader)
			}

			// 执行HEAD请求
			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			httpDuration := time.Since(reqStartTime)
			glog.V(2).Infof("[Performance] HTTP HEAD request for chunk %s took %v", c.Fid, httpDuration)

			if err != nil {
				glog.V(0).Infof("HEAD request for chunk %s failed after %v: %v", c.Fid, httpDuration, err)
				errorChan <- err
				return
			}
			resp.Body.Close()

			// 检查响应状态码
			if resp.StatusCode != http.StatusOK {
				glog.V(0).Infof("chunk %s not found, status: %d, took %v", c.Fid, resp.StatusCode, httpDuration)
				errorChan <- fmt.Errorf("chunk %s not found, status: %d", c.Fid, resp.StatusCode)
				return
			}

			chunkDuration := time.Since(chunkStartTime)
			glog.V(2).Infof("[Performance] Chunk %s validation completed in %v (lookup: %v, http: %v)", c.Fid, chunkDuration, lookupDuration, httpDuration)
			resultChan <- true
		}(i, chunk)
	}

	// 等待所有goroutine完成
	successCount := 0
	for i := 0; i < len(chunks); i++ {
		select {
		case <-resultChan:
			successCount++
		case err := <-errorChan:
			glog.V(0).Infof("chunk validation failed: %v", err)
			// 等待剩余的goroutine完成
			for j := i + 1; j < len(chunks); j++ {
				select {
				case <-resultChan:
				case <-errorChan:
				}
			}
			totalDuration := time.Since(startTime)
			glog.V(1).Infof("[Performance] Chunks validation failed after %v, validated %d/%d chunks", totalDuration, successCount, len(chunks))
			return false
		}
	}

	totalDuration := time.Since(startTime)
	glog.V(1).Infof("[Performance] All %d chunks validation completed concurrently in %v (avg: %v per chunk)", len(chunks), totalDuration, totalDuration/time.Duration(len(chunks)))
	return true
}

// validateVolumesAvailability 严格模式下校验所有涉及volume的可用性
// 对于EC volume，检查数据分片数量是否大于等于DataShardsCount
// 对于普通volume，检查是否有足够的副本
func (vs *VolumeServer) validateVolumesAvailability(chunks []*operation.ChunkInfo) bool {
	// 收集所有涉及的volumeId
	volumeIds := make(map[needle.VolumeId]bool)
	for _, chunk := range chunks {
		parts := strings.Split(chunk.Fid, ",")
		if len(parts) != 2 {
			continue
		}
		if volumeId, err := needle.NewVolumeId(parts[0]); err == nil {
			volumeIds[volumeId] = true
		}
	}

	glog.V(1).Infof("[Strict Mode] Validating availability for %d volumes", len(volumeIds))

	// 分离本地volume和远程volume
	localVolumeIds := make([]needle.VolumeId, 0)
	remoteVolumeIds := make([]needle.VolumeId, 0)

	for volumeId := range volumeIds {
		// 检查是否为本地volume（EC或普通volume）
		if _, hasEcVolume := vs.store.FindEcVolume(volumeId); hasEcVolume {
			localVolumeIds = append(localVolumeIds, volumeId)
		} else if vs.store.HasVolume(volumeId) {
			localVolumeIds = append(localVolumeIds, volumeId)
		} else {
			remoteVolumeIds = append(remoteVolumeIds, volumeId)
		}
	}

	// 验证本地volumes
	for _, volumeId := range localVolumeIds {
		if !vs.validateSingleVolumeAvailability(volumeId) {
			return false
		}
	}

	// 批量验证远程volumes
	if len(remoteVolumeIds) > 0 {
		if !vs.validateRemoteVolumesAvailability(remoteVolumeIds, "", false) {
			return false
		}
	}

	return true
}

// validateSingleVolumeAvailability 校验单个volume的可用性
func (vs *VolumeServer) validateSingleVolumeAvailability(volumeId needle.VolumeId) bool {
	// 首先检查是否为EC volume
	ecVolume, hasEcVolume := vs.store.FindEcVolume(volumeId)
	if hasEcVolume {
		return vs.validateEcVolumeAvailability(volumeId, ecVolume)
	}

	// 检查是否为普通volume
	hasVolume := vs.store.HasVolume(volumeId)
	if hasVolume {
		return vs.validateNormalVolumeAvailability(volumeId)
	}

	// 如果本地没有volume，通过master查询副本信息
	return vs.validateRemoteVolumeAvailability(volumeId)
}

// validateEcVolumeAvailability 校验EC volume的可用性
func (vs *VolumeServer) validateEcVolumeAvailability(volumeId needle.VolumeId, ecVolume *erasure_coding.EcVolume) bool {
	// 统计本地可用的数据分片数量
	localDataShards := 0
	for _, shard := range ecVolume.Shards {
		if shard != nil && shard.ShardId < erasure_coding.DataShardsCount {
			localDataShards++
		}
	}

	glog.V(2).Infof("[Strict Mode] EC volume %d has %d local data shards", volumeId, localDataShards)

	// 如果本地数据分片足够，直接返回true
	if localDataShards >= erasure_coding.DataShardsCount {
		return true
	}

	// 否则需要查询远程分片信息
	lookupResult, err := operation.LookupVolumeId(vs.GetMaster, vs.grpcDialOption, volumeId.String())
	if err != nil {
		glog.V(0).Infof("[Strict Mode] Failed to lookup EC volume %d: %v", volumeId, err)
		return false
	}

	// 检查总的可用位置数量是否足够
	if len(lookupResult.Locations) < erasure_coding.DataShardsCount {
		glog.V(0).Infof("[Strict Mode] EC volume %d has insufficient locations: %d < %d", volumeId, len(lookupResult.Locations), erasure_coding.DataShardsCount)
		return false
	}

	glog.V(2).Infof("[Strict Mode] EC volume %d validation passed with %d locations", volumeId, len(lookupResult.Locations))
	return true
}

// validateNormalVolumeAvailability 校验普通volume的可用性
func (vs *VolumeServer) validateNormalVolumeAvailability(volumeId needle.VolumeId) bool {
	// 获取本地volume信息
	volume := vs.store.GetVolume(volumeId)
	if volume == nil {
		glog.V(0).Infof("[Strict Mode] Local volume %d not found", volumeId)
		return false
	}

	// 获取副本配置
	requiredCopies := volume.ReplicaPlacement.GetCopyCount()
	glog.V(2).Infof("[Strict Mode] Volume %d requires %d copies", volumeId, requiredCopies)

	// 如果只需要1个副本，本地有就足够了
	if requiredCopies <= 1 {
		return true
	}

	// 查询所有副本位置
	lookupResult, err := operation.LookupVolumeId(vs.GetMaster, vs.grpcDialOption, volumeId.String())
	if err != nil {
		glog.V(0).Infof("[Strict Mode] Failed to lookup volume %d: %v", volumeId, err)
		return false
	}

	// 检查可用副本数量
	if len(lookupResult.Locations) < requiredCopies {
		glog.V(0).Infof("[Strict Mode] Volume %d has insufficient replicas: %d < %d", volumeId, len(lookupResult.Locations), requiredCopies)
		return false
	}

	glog.V(2).Infof("[Strict Mode] Volume %d validation passed with %d/%d replicas", volumeId, len(lookupResult.Locations), requiredCopies)
	return true
}

// validateRemoteVolumeAvailability 校验远程volume的可用性
func (vs *VolumeServer) validateRemoteVolumeAvailability(volumeId needle.VolumeId) bool {
	// 调用master端的ValidateVolumesAvailability接口进行验证
	return vs.validateRemoteVolumesAvailability([]needle.VolumeId{volumeId}, "", true)
}

// validateRemoteVolumesAvailability 批量验证远程volume的可用性
func (vs *VolumeServer) validateRemoteVolumesAvailability(volumeIds []needle.VolumeId, collection string, strictMode bool) bool {
	// 转换volumeIds为uint32数组
	volumeIdInts := make([]uint32, len(volumeIds))
	for i, vid := range volumeIds {
		volumeIdInts[i] = uint32(vid)
	}

	// 调用master的ValidateVolumesAvailability gRPC接口
	err := operation.WithMasterServerClient(false, vs.GetMaster(context.Background()), vs.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
		req := &master_pb.ValidateVolumesAvailabilityRequest{
			VolumeIds:  volumeIdInts,
			Collection: collection,
			StrictMode: strictMode,
		}
		resp, err := masterClient.ValidateVolumesAvailability(context.Background(), req)
		if err != nil {
			glog.V(0).Infof("[Strict Mode] Failed to validate volumes availability: %v", err)
			return err
		}

		if !resp.AllAvailable {
			glog.V(0).Infof("[Strict Mode] Some volumes are not available:")
			for _, volInfo := range resp.VolumeInfo {
				if !volInfo.IsAvailable {
					glog.V(0).Infof("[Strict Mode] Volume %d: %s", volInfo.VolumeId, volInfo.ErrorMessage)
				}
			}
			return fmt.Errorf("volumes validation failed")
		}

		glog.V(2).Infof("[Strict Mode] All %d volumes validation passed", len(volumeIds))
		return nil
	})

	return err == nil
}
