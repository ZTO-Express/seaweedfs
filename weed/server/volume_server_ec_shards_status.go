package weed_server

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeEcShardsStatus 检查EC卷中的所有文件是否已被删除
func (vs *VolumeServer) VolumeEcShardsStatus(ctx context.Context, req *volume_server_pb.VolumeEcShardsStatusRequest) (*volume_server_pb.VolumeEcShardsStatusResponse, error) {
	vid := needle.VolumeId(req.VolumeId)

	// 查找EC卷
	ecVolume, found := vs.store.FindEcVolume(vid)
	if !found {
		glog.V(4).Infof("EC卷 %d 未找到", vid)
		return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: false}, nil
	}

	// 先获取EC卷中的所有文件列表
	needleIds, err := ecVolume.GetAllNeedleIds()
	if err != nil {
		glog.Errorf("获取EC卷 %d 文件列表失败: %v", vid, err)
		return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: false}, nil
	}

	// 如果没有文件，则认为所有文件都已删除
	if len(needleIds) == 0 {
		glog.V(4).Infof("EC卷 %d 中没有文件，视为所有文件已删除", vid)
		return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: true}, nil
	}

	glog.V(4).Infof("EC卷 %d 中共有 %d 个文件需要检查删除状态", vid, len(needleIds))

	// 遍历每个文件，检查是否已被删除
	deletedCount := 0
	for i, needleId := range needleIds {
		// 获取文件的位置信息
		_, size, intervals, err := ecVolume.LocateEcShardNeedle(needleId, ecVolume.Version)
		if err != nil {
			glog.Errorf("定位EC分片文件 %d 失败: %v", needleId, err)
			return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: false}, nil
		}

		// 如果文件标记为已删除，则继续检查下一个文件
		if size.IsDeleted() {
			glog.V(4).Infof("EC卷 %d 中的文件 %d 已标记为删除", vid, needleId)
			deletedCount++
			continue
		}

		// 检查文件的每个分片是否已删除
		fileIsDeleted := false
		for j, interval := range intervals {
			// 参考store_ec.go中的ReadOneEcShardInterval方法
			if err = vs.store.CachedLookupEcShardLocations(ecVolume); err != nil {
				glog.Errorf("通过master grpc %s 定位分片失败: %v", vs.store.MasterAddress, err)
			}
			_, isDeleted, _ := vs.store.ReadOneEcShardInterval(needleId, ecVolume, interval)
			if isDeleted {
				glog.V(4).Infof("EC卷 %d 中的文件 %d 的分片 %d 已删除", vid, needleId, j)
				fileIsDeleted = true
				break
			}
		}

		// 如果有任何一个文件未被删除，则返回false
		if !fileIsDeleted && !size.IsDeleted() {
			glog.V(3).Infof("EC卷 %d 中的文件 %d 未被删除，已检查 %d/%d 个文件，其中 %d 个已删除",
				vid, needleId, i+1, len(needleIds), deletedCount)
			return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: false}, nil
		}

		deletedCount++
		if (i+1)%100 == 0 || i+1 == len(needleIds) {
			glog.V(4).Infof("EC卷 %d 检查进度: %d/%d 个文件，其中 %d 个已删除",
				vid, i+1, len(needleIds), deletedCount)
		}
	}

	// 所有文件都已删除
	glog.V(3).Infof("EC卷 %d 中的所有 %d 个文件都已删除", vid, len(needleIds))
	return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: true}, nil
}
