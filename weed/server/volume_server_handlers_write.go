package weed_server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/buffer_pool"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
)

func (vs *VolumeServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	if e := r.ParseForm(); e != nil {
		glog.V(0).Infoln("form parse error:", e)
		writeJsonError(w, r, http.StatusBadRequest, e)
		return
	}

	vid, fid, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, ve := needle.NewVolumeId(vid)
	if ve != nil {
		glog.V(0).Infoln("NewVolumeId error:", ve)
		writeJsonError(w, r, http.StatusBadRequest, ve)
		return
	}

	if !vs.maybeCheckJwtAuthorization(r, vid, fid, true) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	bytesBuffer := buffer_pool.SyncPoolGetBuffer()
	defer buffer_pool.SyncPoolPutBuffer(bytesBuffer)

	reqNeedle, originalSize, contentMd5, ne := needle.CreateNeedleFromRequest(r, vs.FixJpgOrientation, vs.fileSizeLimitBytes, bytesBuffer)
	if ne != nil {
		writeJsonError(w, r, http.StatusBadRequest, ne)
		return
	}

	ret := operation.UploadResult{}
	isUnchanged, writeError := topology.ReplicatedWrite(vs.GetMaster, vs.grpcDialOption, vs.store, volumeId, reqNeedle, r, contentMd5)
	if writeError != nil {
		writeJsonError(w, r, http.StatusInternalServerError, writeError)
		return
	}

	// http 204 status code does not allow body
	if writeError == nil && isUnchanged {
		SetEtag(w, reqNeedle.Etag())
		w.WriteHeader(http.StatusNoContent)
		return
	}

	httpStatus := http.StatusCreated
	if reqNeedle.HasName() {
		ret.Name = string(reqNeedle.Name)
	}
	ret.Size = uint32(originalSize)
	ret.ETag = reqNeedle.Etag()
	ret.Mime = string(reqNeedle.Mime)
	SetEtag(w, ret.ETag)
	w.Header().Set("Content-MD5", contentMd5)
	writeJsonQuiet(w, r, httpStatus, ret)

}

func (vs *VolumeServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	n := new(needle.Needle)
	vid, fid, _, _, _ := parseURLPath(r.URL.Path)
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

	if !vs.maybeCheckJwtAuthorization(r, vid, fid, true) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	// glog.V(2).Infof("volume %s deleting %s", vid, n)

	cookie := n.Cookie

	hasVolume := vs.store.HasVolume(volumeId)
	ecVolume, hasEcVolume := vs.store.FindEcVolume(volumeId)

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
			request, err := http.NewRequest(http.MethodDelete, r.URL.String(), nil)
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

	if hasEcVolume {
		// 先读取EC分片的needle内容，以便检查是否为分块清单
		_, err := vs.store.ReadEcShardNeedle(volumeId, n, nil)
		if err != nil {
			m := make(map[string]uint32)
			m["size"] = 0
			writeJsonQuiet(w, r, http.StatusNotFound, m)
			return
		}

		if n.Cookie != cookie {
			glog.V(0).Infoln("delete", r.URL.Path, "with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
			writeJsonError(w, r, http.StatusBadRequest, errors.New("File Random Cookie does not match."))
			return
		}

		count := int64(n.Size)

		// 检查是否为分块清单，如果是，需要先删除所有块
		if n.IsChunkedManifest() {
			chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsCompressed())
			if e != nil {
				writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Load chunks manifest error: %v", e))
				return
			}
			// 确保在删除清单前删除所有块
			if e := chunkManifest.DeleteChunks(vs.GetMaster, false, vs.grpcDialOption); e != nil {
				writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Delete chunks error: %v", e))
				return
			}
			count = chunkManifest.Size
		}

		// 删除EC分片
		_, err = vs.store.DeleteEcShardNeedle(ecVolume, n, cookie)
		if err == nil {
			// 删除成功后，异步触发EC卷的垃圾回收
			go func() {
				masterAddress := vs.GetMaster(context.Background())
				if masterAddress == "" {
					glog.V(0).Infof("无法获取master地址，跳过EC卷 %d 的垃圾回收", volumeId)
					return
				}
				vacuumErr := pb.WithMasterClient(false, masterAddress, vs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
					_, vacuumErr := client.VacuumEcVolume(context.Background(), &master_pb.VacuumEcVolumeRequest{
						VolumeId: uint32(volumeId),
					})
					return vacuumErr
				})
				if vacuumErr != nil {
					glog.V(0).Infof("EC卷 %d 垃圾回收失败: %v", volumeId, vacuumErr)
				} else {
					glog.V(1).Infof("EC卷 %d 垃圾回收已触发", volumeId)
				}
			}()
		}
		writeDeleteResult(err, count, w, r)
		return
	}

	_, ok := vs.store.ReadVolumeNeedle(volumeId, n, nil, nil)
	if ok != nil {
		m := make(map[string]uint32)
		m["size"] = 0
		writeJsonQuiet(w, r, http.StatusNotFound, m)
		return
	}

	if n.Cookie != cookie {
		glog.V(0).Infoln("delete", r.URL.Path, "with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
		writeJsonError(w, r, http.StatusBadRequest, errors.New("File Random Cookie does not match."))
		return
	}

	count := int64(n.Size)

	if n.IsChunkedManifest() {
		chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsCompressed())
		if e != nil {
			writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Load chunks manifest error: %v", e))
			return
		}
		// make sure all chunks had deleted before delete manifest
		if e := chunkManifest.DeleteChunks(vs.GetMaster, false, vs.grpcDialOption); e != nil {
			writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Delete chunks error: %v", e))
			return
		}
		count = chunkManifest.Size
	}

	n.LastModified = uint64(time.Now().Unix())
	if len(r.FormValue("ts")) > 0 {
		modifiedTime, err := strconv.ParseInt(r.FormValue("ts"), 10, 64)
		if err == nil {
			n.LastModified = uint64(modifiedTime)
		}
	}

	_, err = topology.ReplicatedDelete(vs.GetMaster, vs.grpcDialOption, vs.store, volumeId, n, r)

	writeDeleteResult(err, count, w, r)

}

func writeDeleteResult(err error, count int64, w http.ResponseWriter, r *http.Request) {
	if err == nil {
		m := make(map[string]int64)
		m["size"] = count
		writeJsonQuiet(w, r, http.StatusAccepted, m)
	} else {
		// 提取URL路径中的卷ID和文件ID信息
		vid, fid, _, _, _ := parseURLPath(r.URL.Path)
		glog.V(4).Infof("Delete failed for volume:%s file:%s from %s method:%s error:%v", vid, fid, r.RemoteAddr, r.Method, err)
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Deletion Failed: %v", err))
	}
}

func SetEtag(w http.ResponseWriter, etag string) {
	if etag != "" {
		if strings.HasPrefix(etag, "\"") {
			w.Header().Set("ETag", etag)
		} else {
			w.Header().Set("ETag", "\""+etag+"\"")
		}
	}
}

func getEtag(resp *http.Response) (etag string) {
	etag = resp.Header.Get("ETag")
	if strings.HasPrefix(etag, "\"") && strings.HasSuffix(etag, "\"") {
		return etag[1 : len(etag)-1]
	}
	return
}
