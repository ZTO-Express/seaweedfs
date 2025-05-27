package operation

import (
	"context"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

type DeleteResult struct {
	Fid    string `json:"fid"`
	Size   int    `json:"size"`
	Status int    `json:"status"`
	Error  string `json:"error,omitempty"`
}

func ParseFileId(fid string) (vid string, key_cookie string, err error) {
	commaIndex := strings.Index(fid, ",")
	if commaIndex <= 0 {
		return "", "", errors.New("Wrong fid format.")
	}
	return fid[:commaIndex], fid[commaIndex+1:], nil
}

// DeleteFiles batch deletes a list of fileIds
func DeleteFiles(masterFn GetMasterFn, usePublicUrl bool, grpcDialOption grpc.DialOption, fileIds []string) ([]*volume_server_pb.DeleteResult, error) {
	startTime := time.Now()
	glog.V(4).Infof("DeleteFiles: 开始批量删除文件，数量: %d", len(fileIds))

	lookupFunc := func(vids []string) (results map[string]*LookupResult, err error) {
		lookupStartTime := time.Now()
		glog.V(4).Infof("DeleteFiles: 开始查找卷ID位置，数量: %d", len(vids))
		results, err = LookupVolumeIds(masterFn, grpcDialOption, vids)
		if err != nil {
			glog.V(0).Infof("DeleteFiles: 查找卷ID位置失败: %v", err)
			return
		}
		if usePublicUrl {
			for _, result := range results {
				for _, loc := range result.Locations {
					loc.Url = loc.PublicUrl
				}
			}
		}
		glog.V(4).Infof("DeleteFiles: 查找卷ID位置完成，数量: %d，耗时: %v", len(results), time.Since(lookupStartTime))
		return
	}

	results, err := DeleteFilesWithLookupVolumeId(grpcDialOption, fileIds, lookupFunc)
	glog.V(4).Infof("DeleteFiles: 批量删除文件完成，数量: %d，耗时: %v", len(fileIds), time.Since(startTime))
	return results, err

}

func DeleteFilesWithLookupVolumeId(grpcDialOption grpc.DialOption, fileIds []string, lookupFunc func(vid []string) (map[string]*LookupResult, error)) ([]*volume_server_pb.DeleteResult, error) {
	startTime := time.Now()
	glog.V(4).Infof("开始删除文件，总数量: %d", len(fileIds))

	var ret []*volume_server_pb.DeleteResult

	vid_to_fileIds := make(map[string][]string)
	var vids []string
	for i, fileId := range fileIds {
		if i > 0 && i%1000 == 0 {
			glog.V(4).Infof("正在处理文件ID分组: %d/%d, 耗时: %v", i, len(fileIds), time.Since(startTime))
		}
		vid, _, err := ParseFileId(fileId)
		if err != nil {
			glog.V(0).Infof("解析文件ID失败: %s, 错误: %v", fileId, err)
			ret = append(ret, &volume_server_pb.DeleteResult{
				FileId: fileId,
				Status: http.StatusBadRequest,
				Error:  err.Error()},
			)
			continue
		}
		if _, ok := vid_to_fileIds[vid]; !ok {
			vid_to_fileIds[vid] = make([]string, 0)
			vids = append(vids, vid)
		}
		vid_to_fileIds[vid] = append(vid_to_fileIds[vid], fileId)
	}

	glog.V(4).Infof("文件ID分组完成，共有%d个卷ID，耗时: %v", len(vids), time.Since(startTime))

	lookupStartTime := time.Now()
	glog.V(4).Infof("开始查找卷服务器位置，卷ID数量: %d", len(vids))
	lookupResults, err := lookupFunc(vids)
	if err != nil {
		glog.V(0).Infof("查找卷服务器位置失败: %v", err)
		return ret, err
	}
	glog.V(4).Infof("查找卷服务器位置完成，耗时: %v", time.Since(lookupStartTime))

	groupStartTime := time.Now()
	glog.V(4).Infof("开始按服务器分组文件ID")
	server_to_fileIds := make(map[pb.ServerAddress][]string)
	// 用于跟踪每个服务器已经处理过的文件ID，避免重复
	server_to_processed_fileIds := make(map[pb.ServerAddress]map[string]bool)

	for vid, result := range lookupResults {
		if result.Error != "" {
			glog.V(0).Infof("卷ID %s 查找失败: %s", vid, result.Error)
			ret = append(ret, &volume_server_pb.DeleteResult{
				FileId: vid,
				Status: http.StatusBadRequest,
				Error:  result.Error},
			)
			continue
		}
		for _, location := range result.Locations {
			serverAddress := location.ServerAddress()
			if _, ok := server_to_fileIds[serverAddress]; !ok {
				server_to_fileIds[serverAddress] = make([]string, 0)
				server_to_processed_fileIds[serverAddress] = make(map[string]bool)
			}

			// 对每个文件ID进行去重处理
			for _, fileId := range vid_to_fileIds[vid] {
				if !server_to_processed_fileIds[serverAddress][fileId] {
					server_to_fileIds[serverAddress] = append(server_to_fileIds[serverAddress], fileId)
					server_to_processed_fileIds[serverAddress][fileId] = true
				}
			}
			break
		}
	}
	glog.V(4).Infof("按服务器分组完成，共有%d个服务器，耗时: %v", len(server_to_fileIds), time.Since(groupStartTime))

	deleteStartTime := time.Now()
	glog.V(4).Infof("开始并行删除文件")
	resultChan := make(chan []*volume_server_pb.DeleteResult, len(server_to_fileIds))
	var wg sync.WaitGroup
	serverCount := 0
	for server, fidList := range server_to_fileIds {
		serverCount++
		glog.V(4).Infof("准备删除服务器 %s 上的文件，数量: %d (%d/%d)", server, len(fidList), serverCount, len(server_to_fileIds))
		wg.Add(1)
		go func(server pb.ServerAddress, fidList []string) {
			defer wg.Done()
			serverStartTime := time.Now()
			glog.V(4).Infof("开始删除服务器 %s 上的文件，数量: %d", server, len(fidList))

			if deleteResults, deleteErr := DeleteFilesAtOneVolumeServer(server, grpcDialOption, fidList, false); deleteErr != nil {
				glog.V(0).Infof("删除服务器 %s 上的文件失败: %v", server, deleteErr)
				err = deleteErr
			} else if deleteResults != nil {
				glog.V(4).Infof("删除服务器 %s 上的文件完成，数量: %d，耗时: %v", server, len(deleteResults), time.Since(serverStartTime))
				resultChan <- deleteResults
			}

		}(server, fidList)
	}
	wg.Wait()
	close(resultChan)
	glog.V(4).Infof("所有服务器的删除操作已完成，耗时: %v", time.Since(deleteStartTime))

	glog.V(4).Infof("开始合并删除结果")
	for result := range resultChan {
		ret = append(ret, result...)
	}

	glog.V(4).Infof("删除操作全部完成，总耗时: %v，结果数量: %d", time.Since(startTime), len(ret))
	return ret, err
}

// DeleteFilesAtOneVolumeServer deletes a list of files that is on one volume server via gRpc
func DeleteFilesAtOneVolumeServer(volumeServer pb.ServerAddress, grpcDialOption grpc.DialOption, fileIds []string, includeCookie bool) (ret []*volume_server_pb.DeleteResult, err error) {
	startTime := time.Now()
	glog.V(4).Infof("DeleteFilesAtOneVolumeServer: 开始删除服务器 %s 上的文件，数量: %d", volumeServer, len(fileIds))

	err = WithVolumeServerClient(false, volumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		glog.V(4).Infof("DeleteFilesAtOneVolumeServer: 已连接到服务器 %s，准备发送删除请求，连接耗时: %v", volumeServer, time.Since(startTime))

		req := &volume_server_pb.BatchDeleteRequest{
			FileIds:         fileIds,
			SkipCookieCheck: !includeCookie,
		}

		rpcStartTime := time.Now()
		resp, err := volumeServerClient.BatchDelete(context.Background(), req)
		rpcTime := time.Since(rpcStartTime)

		// fmt.Printf("deleted %v %v: %v\n", fileIds, err, resp)

		if err != nil {
			glog.V(0).Infof("DeleteFilesAtOneVolumeServer: 删除请求失败，服务器: %s，错误: %v，RPC耗时: %v", volumeServer, err, rpcTime)
			return err
		}

		glog.V(4).Infof("DeleteFilesAtOneVolumeServer: 删除请求成功，服务器: %s，结果数量: %d，RPC耗时: %v", volumeServer, len(resp.Results), rpcTime)
		ret = append(ret, resp.Results...)

		return nil
	})

	if err != nil {
		glog.V(0).Infof("DeleteFilesAtOneVolumeServer: 与服务器 %s 通信失败: %v，总耗时: %v", volumeServer, err, time.Since(startTime))
		return
	}

	var errorCount int
	for _, result := range ret {
		if result.Error != "" && result.Error != "not found" {
			errorCount++
			glog.V(0).Infof("DeleteFilesAtOneVolumeServer: 删除文件 %s 失败: %v", result.FileId, result.Error)
			return nil, fmt.Errorf("delete fileId %s: %v", result.FileId, result.Error)
		}
	}

	glog.V(4).Infof("DeleteFilesAtOneVolumeServer: 服务器 %s 上的文件删除完成，总数: %d，成功: %d，总耗时: %v",
		volumeServer, len(fileIds), len(ret), time.Since(startTime))

	return

}
