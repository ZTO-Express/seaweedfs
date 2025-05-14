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
		return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: false}, nil
	}

	// 先获取EC卷中的所有文件列表
	needleIds, err := ecVolume.GetAllNeedleIds()
	if err != nil {
		glog.Errorf("get ec volume %d needle ids: %v", vid, err)
		return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: false}, nil
	}

	// 如果没有文件，则认为所有文件都已删除
	if len(needleIds) == 0 {
		return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: true}, nil
	}

	// 遍历每个文件，检查是否已被删除
	for _, needleId := range needleIds {
		// 获取文件的位置信息
		_, size, intervals, err := ecVolume.LocateEcShardNeedle(needleId, ecVolume.Version)
		if err != nil {
			glog.Errorf("locate ec shard needle %d: %v", needleId, err)
			continue
		}

		// 如果文件标记为已删除，则继续检查下一个文件
		if size.IsDeleted() {
			continue
		}

		// 检查文件的每个分片是否已删除
		fileIsDeleted := false
		for _, interval := range intervals {
			// 参考store_ec.go中的ReadOneEcShardInterval方法
			if err = vs.store.CachedLookupEcShardLocations(ecVolume); err != nil {
				glog.Errorf("failed to locate shard via master grpc %s: %v", vs.store.MasterAddress, err)
			}
			_, isDeleted, _ := vs.store.ReadOneEcShardInterval(needleId, ecVolume, interval)
			if isDeleted {
				fileIsDeleted = true
				break
			}
		}

		// 如果有任何一个文件未被删除，则返回false
		if !fileIsDeleted && !size.IsDeleted() {
			return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: false}, nil
		}
	}

	// 所有文件都已删除
	return &volume_server_pb.VolumeEcShardsStatusResponse{IsAllNeedlesDeleted: true}, nil
}
