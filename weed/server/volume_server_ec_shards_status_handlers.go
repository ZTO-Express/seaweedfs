package weed_server

import (
	"net/http"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// ecShardsStatusHandler 处理HTTP请求，检查EC卷中的所有文件是否已被删除
func (vs *VolumeServer) ecShardsStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 从URL参数中获取卷ID
	volumeIdStr := r.FormValue("volumeId")
	if volumeIdStr == "" {
		http.Error(w, "缺少volumeId参数", http.StatusBadRequest)
		return
	}

	// 转换卷ID为数字
	volumeId, err := strconv.ParseUint(volumeIdStr, 10, 64)
	if err != nil {
		http.Error(w, "volumeId参数无效: "+err.Error(), http.StatusBadRequest)
		return
	}

	// 记录开始检查的日志
	glog.V(0).Infof("开始检查EC卷 %d 中的文件删除状态", volumeId)

	// 创建请求对象
	req := &volume_server_pb.VolumeEcShardsStatusRequest{
		VolumeId: uint32(volumeId),
	}

	// 调用VolumeEcShardsStatus方法
	resp, err := vs.VolumeEcShardsStatus(r.Context(), req)
	if err != nil {
		glog.Errorf("检查EC卷 %d 文件删除状态失败: %v", volumeId, err)
		http.Error(w, "检查失败: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 记录检查结果的日志
	glog.V(0).Infof("EC卷 %d 中的所有文件是否已删除: %v", volumeId, resp.IsAllNeedlesDeleted)

	// 返回检查结果
	if resp.IsAllNeedlesDeleted {
		w.Write([]byte("true"))
	} else {
		w.Write([]byte("false"))
	}
}
