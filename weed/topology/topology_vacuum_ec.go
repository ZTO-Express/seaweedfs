package topology

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

// VacuumEcVolumes 处理EC卷的垃圾回收
func (t *Topology) VacuumEcVolumes(grpcDialOption grpc.DialOption, collection string, volumeId uint32) {
	glog.V(1).Infof("Start vacuum EC volumes for collection: %s volumeId: %d", collection, volumeId)

	// 如果指定了特定的卷ID，只处理该卷
	if volumeId > 0 {
		vid := needle.VolumeId(volumeId)
		ecLocations, found := t.LookupEcShards(vid)
		if found {
			t.vacuumOneEcVolumeId(grpcDialOption, ecLocations, vid, collection)
		}
		return
	}

	// 处理所有EC卷 - 使用无锁设计，复制ecShardMap后处理
	// 短暂加锁复制ecShardMap
	// todo 暂时使用深copy来处理
	t.ecShardMapLock.RLock()
	ecShardMapCopy := make(map[needle.VolumeId]*EcShardLocations, len(t.ecShardMap))
	for vid, ecLocations := range t.ecShardMap {
		// 深拷贝EcShardLocations
		locationsCopy := &EcShardLocations{
			Collection: ecLocations.Collection,
			Locations:  ecLocations.Locations, // 这里也需要深拷贝数组
		}
		// 深拷贝Locations数组
		for i := range ecLocations.Locations {
			locationsCopy.Locations[i] = make([]*DataNode, len(ecLocations.Locations[i]))
			copy(locationsCopy.Locations[i], ecLocations.Locations[i])
		}
		ecShardMapCopy[vid] = locationsCopy
	}
	t.ecShardMapLock.RUnlock()
	// 在无锁状态下处理复制的数据
	for vid, ecLocations := range ecShardMapCopy {
		// 如果指定了集合名称，只处理该集合的卷
		if collection != "" && collection != ecLocations.Collection {
			continue
		}
		t.vacuumOneEcVolumeId(grpcDialOption, ecLocations, vid, ecLocations.Collection)
	}
}

// 处理单个EC卷的垃圾回收
func (t *Topology) vacuumOneEcVolumeId(grpcDialOption grpc.DialOption, ecLocations *EcShardLocations, vid needle.VolumeId, collection string) {
	// 记录开始时间，用于统计耗时
	startTime := time.Now()
	glog.V(1).Infof("Check vacuum on EC volume:%d", vid)

	// 检查是否有足够的EC分片
	hasEnoughShards := false
	shardCount := 0
	for _, shardLocations := range ecLocations.Locations {
		if len(shardLocations) > 0 {
			shardCount++
		}
	}
	hasEnoughShards = shardCount >= erasure_coding.DataShardsCount

	if !hasEnoughShards {
		glog.V(0).Infof("Skip vacuum EC volume:%d, not enough shards: %d < %d", vid, shardCount, erasure_coding.DataShardsCount)
		return
	}

	// 检查EC卷是否需要垃圾回收
	if needVacuum := t.checkEcVolumeNeedVacuum(grpcDialOption, vid, ecLocations); needVacuum {
		glog.V(0).Infof("EC volume:%d needs vacuum, all files are deleted", vid)
		// 执行EC卷的垃圾回收
		if t.cleanupEcVolume(grpcDialOption, vid, collection, ecLocations) {
			glog.V(0).Infof("Successfully vacuumed EC volume:%d", vid)
		} else {
			glog.V(0).Infof("Failed to vacuum EC volume:%d", vid)
		}
	}
	// 计算并记录耗时
	elapsedTime := time.Since(startTime)
	glog.V(0).Infof("Vacuum EC volume:%d completed, time cost: %v", vid, elapsedTime)
}

// 检查EC卷是否需要垃圾回收
func (t *Topology) checkEcVolumeNeedVacuum(grpcDialOption grpc.DialOption, vid needle.VolumeId, ecLocations *EcShardLocations) bool {
	// 分别从三个节点获取所有needleIds
	var allNeedleIdMap map[uint64]*volume_server_pb.Shards
	for _, dataNodes := range ecLocations.Locations {
		// 检查该shard的第一个节点
		dn := dataNodes[0]
		err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			resp, err := volumeServerClient.GetAllNeedleIds(context.Background(), &volume_server_pb.GetAllNeedleIdsRequest{
				VolumeId: uint32(vid),
			})
			if err != nil {
				glog.V(0).Infof("Failed to get all needle ids for EC volume:%d on %s: %v", vid, dataNodes[0].ServerAddress(), err)
				return nil
			}
			allNeedleIdMap = resp.NeedleIdMap
			glog.V(4).Infof("Got %d needle ids for EC volume:%d from %s", len(allNeedleIdMap), vid, dataNodes[0].ServerAddress())
			return nil
		})
		if err != nil {
			glog.V(0).Infof("Failed to get needle ids for EC volume:%d: %v", vid, err)
			continue
		}
		// 如果成功获取到needleIdMap，跳出所有循环
		if len(allNeedleIdMap) > 0 {
			break
		}
	}

	// 如果没有文件，则不垃圾回收 防止因为节点问题导致删除数据
	if len(allNeedleIdMap) == 0 {
		glog.V(4).Infof("EC volume:%d has no files, can be vacuumed", vid)
		return false
	}

	// 检查每个文件的删除状态
	for needleId, shards := range allNeedleIdMap {
		fileDeleted := false
		// 先遍历所有shardIds去重并获取所有节点
		var dataNodes []*DataNode
		for _, shardId := range shards.ShardIds {
			shardNodes := ecLocations.Locations[shardId]
			for _, dn := range shardNodes {
				// 检查数据节点是否已经在列表中
				found := false
				for _, existingDn := range dataNodes {
					if existingDn.Id() == dn.Id() {
						found = true
						break
					}
				}
				if !found {
					dataNodes = append(dataNodes, dn)
				}
			}
		}
		// 循环调用所有节点的FastNeedleIdStatus
		for _, dn := range dataNodes {
			err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				resp, err := volumeServerClient.FastNeedleIdStatus(context.Background(), &volume_server_pb.FastNeedleIdStatusRequest{
					VolumeId: uint32(vid),
					NeedleId: needleId,
				})
				if err != nil {
					glog.V(0).Infof("Failed to check needle %d status for EC volume:%d on %s: %v", needleId, vid, dn.ServerAddress(), err)
					return err
				}
				if resp.IsDeleted {
					glog.V(3).Infof("Needle %d in EC volume:%d is deleted on %s", needleId, vid, dn.ServerAddress())
					fileDeleted = true
				}
				return nil
			})
			if err != nil {
				glog.V(0).Infof("Failed to check needle %d status for EC volume:%d on %s: %v", needleId, vid, dn.ServerAddress(), err)
				continue
			}
			// 如果有一个节点返回被删除，则认为文件被删除
			if fileDeleted {
				break
			}
		}
		// 如果文件未被删除，则不需要垃圾回收
		if !fileDeleted {
			glog.V(3).Infof("Needle %d in EC volume:%d is not deleted, cannot vacuum", needleId, vid)
			return false
		}
	}

	// 所有文件都已删除，可以垃圾回收
	glog.V(3).Infof("All needles in EC volume:%d are deleted, can be vacuumed", vid)
	return true
}

// 清理EC卷
func (t *Topology) cleanupEcVolume(grpcDialOption grpc.DialOption, vid needle.VolumeId, collection string, ecLocations *EcShardLocations) bool {
	// 收集包含该卷分片的所有数据节点
	var dataNodes []*DataNode
	for _, shardLocations := range ecLocations.Locations {
		for _, dn := range shardLocations {
			// 检查数据节点是否已经在列表中
			found := false
			for _, existingDn := range dataNodes {
				if existingDn.Id() == dn.Id() {
					found = true
					break
				}
			}
			if !found {
				dataNodes = append(dataNodes, dn)
			}
		}
	}
	// 对每个数据节点执行清理操作
	ch := make(chan bool, len(dataNodes))
	for _, dn := range dataNodes {
		go func(url pb.ServerAddress, vid needle.VolumeId, dn *DataNode) {
			// 直接删除所有EC分片，不需要获取具体的分片ID
			err := operation.WithVolumeServerClient(false, url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				// 删除EC卷分片，传递空的shardIds让服务端自动处理所有分片
				_, deleteErr := volumeServerClient.EcVolumeDelete(context.Background(), &volume_server_pb.EcVolumeDeleteRequest{
					VolumeId:   uint32(vid),
					Collection: collection,
					ShardIds:   nil,   // 传递nil，服务端会自动处理所有分片
					Soft:       false, //软删除
				})
				return deleteErr
			})
			if err != nil {
				glog.Errorf("Error when cleaning up EC volume %d on %s: %v", vid, url, err)
				ch <- false
			} else {
				glog.V(0).Infof("Complete cleaning up EC volume %d on %s", vid, url)
				ch <- true
			}
		}(dn.ServerAddress(), vid, dn)
	}

	// 等待所有清理操作完成
	waitTimeout := time.NewTimer(5 * time.Minute)
	defer waitTimeout.Stop()

	isCleanupSuccess := true
	for i := 0; i < len(dataNodes); i++ {
		select {
		case success := <-ch:
			isCleanupSuccess = isCleanupSuccess && success
		case <-waitTimeout.C:
			return false
		}
	}

	// 如果清理成功，从拓扑中移除EC卷
	if isCleanupSuccess {
		t.ecShardMapLock.Lock()
		delete(t.ecShardMap, vid)
		t.ecShardMapLock.Unlock()
	}

	return isCleanupSuccess
}
