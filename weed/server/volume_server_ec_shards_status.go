package weed_server

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
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
		//var fileData []byte

		for j, interval := range intervals {
			// 参考store_ec.go中的ReadOneEcShardInterval方法
			if err = vs.store.CachedLookupEcShardLocations(ecVolume); err != nil {
				glog.Errorf("通过master grpc %s 定位分片失败: %v", vs.store.MasterAddress, err)
			}
			_, isDeleted, _ := vs.store.ReadOneEcShardInterval(needleId, ecVolume, interval)
			if isDeleted {
				glog.V(4).Infof("EC卷 %d 中的文件 %d 的分片 %d 已删除", vid, needleId, j)
				fileIsDeleted = true
				continue
			} else {
				glog.V(4).Infof("EC卷 %d 中的文件 %d 的分片 %d 未被删除", vid, needleId, j)
				// 仿照store_ec.go中的逻辑，收集未删除文件的数据
				//if j == 0 {
				//	fileData = data
				//} else {
				//	fileData = append(fileData, data...)
				//}
			}
		}
		// 如果有任何一个文件未被删除，则收集该文件ID
		if !fileIsDeleted && !size.IsDeleted() {
			//n := new(needle.Needle)
			//err = n.ReadBytes(fileData, offset.ToActualOffset(), size, ecVolume.Version)
			//key := needle.NewFileIdFromNeedle(vid, n).String()
			glog.V(3).Infof("EC卷 %d 中的文件 %s 未被删除，已检查 %d/%d 个文件，其中 %d 个已删除",
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

// GetAllNeedleIds 获取EC卷中的所有文件ID
func (vs *VolumeServer) GetAllNeedleIds(ctx context.Context, req *volume_server_pb.GetAllNeedleIdsRequest) (*volume_server_pb.GetAllNeedleIdsResponse, error) {
	vid := needle.VolumeId(req.VolumeId)

	// 查找EC卷
	ecVolume, found := vs.store.FindEcVolume(vid)
	if !found {
		glog.V(4).Infof("GetAllNeedleIds EC卷 %d 未找到", vid)
		return &volume_server_pb.GetAllNeedleIdsResponse{NeedleIdMap: make(map[uint64]*volume_server_pb.Shards)}, nil
	}

	// 获取EC卷中的所有文件列表
	needleIds, err := ecVolume.GetAllNeedleIds()
	if err != nil {
		glog.Errorf("获取EC卷 %d 文件列表失败: %v", vid, err)
		return &volume_server_pb.GetAllNeedleIdsResponse{NeedleIdMap: make(map[uint64]*volume_server_pb.Shards)}, err
	}

	// 转换为Shards结构体映射
	needleIdMap := make(map[uint64]*volume_server_pb.Shards, len(needleIds))
	for _, needleId := range needleIds {
		// 为每个needle创建独立的shardIds数组
		shardIds := make([]uint32, 0, erasure_coding.TotalShardsCount-erasure_coding.DataShardsCount+1)
		_, _, intervals, err := ecVolume.LocateEcShardNeedle(needleId, ecVolume.Version)
		if err == nil && len(intervals) > 0 {
			shardId, _ := intervals[0].ToShardIdAndOffset(erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize)
			// 添加当前needle所在的数据分片
			shardIds = append(shardIds, uint32(shardId))
		}
		// 添加所有奇偶校验分片（用于检查needleId是否被删除的信息）
		for parityShardId := erasure_coding.DataShardsCount; parityShardId < erasure_coding.TotalShardsCount; parityShardId++ {
			shardIds = append(shardIds, uint32(parityShardId))
		}
		// 创建Shards结构体实例
		needleIdMap[types.NeedleIdToUint64(needleId)] = &volume_server_pb.Shards{
			ShardIds: shardIds,
		}
	}
	glog.V(4).Infof("EC卷 %d 中共有 %d 个文件", vid, len(needleIds))
	return &volume_server_pb.GetAllNeedleIdsResponse{NeedleIdMap: needleIdMap}, nil
}

// FastNeedleIdStatus 快速检查EC卷中的单个文件是否已被删除
func (vs *VolumeServer) FastNeedleIdStatus(ctx context.Context, req *volume_server_pb.FastNeedleIdStatusRequest) (*volume_server_pb.FastNeedleIdStatusResponse, error) {
	vid := needle.VolumeId(req.VolumeId)

	// 查找EC卷
	ecVolume, found := vs.store.FindEcVolume(vid)

	if !found {
		glog.V(4).Infof("FastNeedleIdStatus EC卷 %d 未找到", vid)
		return &volume_server_pb.FastNeedleIdStatusResponse{IsDeleted: false}, nil
	}

	needleId := types.Uint64ToNeedleId(req.NeedleId)
	_, size, err := ecVolume.LocateEcShardNeedleByNeedleIdStatus(needleId)

	if err != nil {
		glog.Errorf("定位EC分片文件 %d 失败: %v", needleId, err)
		return &volume_server_pb.FastNeedleIdStatusResponse{IsDeleted: false}, nil
	}

	if size.IsDeleted() {
		glog.V(4).Infof("EC卷 %d 中的文件 %d 已标记为删除", vid, needleId)
		return &volume_server_pb.FastNeedleIdStatusResponse{IsDeleted: true}, nil
	}
	glog.V(4).Infof("EC卷 %d 中的文件 %d 未删除", vid, needleId)
	return &volume_server_pb.FastNeedleIdStatusResponse{IsDeleted: false}, nil
}
