package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func (ms *MasterServer) ProcessGrowRequest() {
	go func() {
		filter := sync.Map{}
		for {
			req, ok := <-ms.volumeGrowthRequestChan
			if !ok {
				break
			}
			glog.V(3).Infoln("enter automatic volume grow: ", req)

			option := req.Option
			vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)

			if !ms.Topo.IsLeader() {
				glog.V(3).Infoln("current ip is not leader, skip request: ", req)
				//discard buffered requests
				time.Sleep(time.Second * 1)
				vl.DoneGrowRequest()
				continue
			}

			// filter out identical requests being processed
			found := false
			filter.Range(func(k, v interface{}) bool {
				if reflect.DeepEqual(k, req) {
					found = true
				}
				return !found
			})

			// not atomic but it's okay
			if !found && vl.ShouldGrowVolumes(option) {
				filter.Store(req, nil)
				// we have lock called inside vg
				go func() {
					glog.V(1).Infof("ProcessGrowRequest: starting automatic volume grow for option: %+v, count: %d", req.Option, req.Count)
					start := time.Now()
					newVidLocations, err := ms.vg.AutomaticGrowByType(req.Option, ms.grpcDialOption, ms.Topo, req.Count)
					elapsed := time.Now().Sub(start)
					glog.V(1).Infof("ProcessGrowRequest: finished automatic volume grow, cost: %v, created volumes: %d, err: %v", elapsed, len(newVidLocations), err)
					vl.DoneGrowRequest()
					if err == nil {
						glog.V(1).Infof("ProcessGrowRequest: successfully created %d volumes, broadcasting to clients", len(newVidLocations))
						for _, newVidLocation := range newVidLocations {
							glog.V(2).Infof("ProcessGrowRequest: broadcasting new volume location: %+v", newVidLocation)
							ms.broadcastToClients(&master_pb.KeepConnectedResponse{VolumeLocation: newVidLocation})
						}
						glog.V(1).Infof("ProcessGrowRequest: volume grow completed successfully")
					} else {
						glog.Errorf("ProcessGrowRequest: automatic volume grow failed for option %+v: %v", req.Option, err)
					}
					filter.Delete(req)
				}()

			} else {
				if found {
					glog.V(3).Infof("ProcessGrowRequest: discarding duplicate volume grow request: %+v", req)
				} else {
					glog.V(3).Infof("ProcessGrowRequest: discarding volume grow request (should not grow): %+v", req)
				}
				time.Sleep(time.Millisecond * 211)
				vl.DoneGrowRequest()
			}
		}
	}()
}

func (ms *MasterServer) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {

	resp := &master_pb.LookupVolumeResponse{}
	volumeLocations := ms.lookupVolumeId(req.VolumeOrFileIds, req.Collection)

	for _, volumeOrFileId := range req.VolumeOrFileIds {
		vid := volumeOrFileId
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
		if result, found := volumeLocations[vid]; found {
			var locations []*master_pb.Location
			for _, loc := range result.Locations {
				locations = append(locations, &master_pb.Location{
					Url:        loc.Url,
					PublicUrl:  loc.PublicUrl,
					DataCenter: loc.DataCenter,
					GrpcPort:   uint32(loc.GrpcPort),
				})
			}
			var auth string
			if commaSep > 0 { // this is a file id
				auth = string(security.GenJwtForVolumeServer(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, result.VolumeOrFileId))
			}
			resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
				VolumeOrFileId: result.VolumeOrFileId,
				Locations:      locations,
				Error:          result.Error,
				Auth:           auth,
			})
		}
	}

	return resp, nil
}

func (ms *MasterServer) ValidateVolumesAvailability(ctx context.Context, req *master_pb.ValidateVolumesAvailabilityRequest) (*master_pb.ValidateVolumesAvailabilityResponse, error) {
	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.ValidateVolumesAvailabilityResponse{
		AllAvailable: true,
	}

	glog.V(2).Infof("[Strict Mode] Validating availability for %d volumes in collection %s", len(req.VolumeIds), req.Collection)

	for _, volumeId := range req.VolumeIds {
		volumeInfo := ms.validateSingleVolumeAvailabilityMaster(needle.VolumeId(volumeId), req.Collection, req.StrictMode)
		resp.VolumeInfo = append(resp.VolumeInfo, volumeInfo)
		if !volumeInfo.IsAvailable {
			resp.AllAvailable = false
		}
	}

	glog.V(2).Infof("[Strict Mode] Volume validation completed, all available: %v", resp.AllAvailable)
	return resp, nil
}

// validateSingleVolumeAvailabilityMaster 在master端校验单个volume的可用性
func (ms *MasterServer) validateSingleVolumeAvailabilityMaster(volumeId needle.VolumeId, collection string, strictMode bool) *master_pb.VolumeAvailabilityInfo {
	volumeInfo := &master_pb.VolumeAvailabilityInfo{
		VolumeId:    uint32(volumeId),
		IsAvailable: false,
	}

	// 查找volume位置
	dataNodes := ms.Topo.Lookup(collection, volumeId)
	if len(dataNodes) == 0 {
		volumeInfo.ErrorMessage = fmt.Sprintf("volume %d not found in collection %s", volumeId, collection)
		return volumeInfo
	}

	// 检查是否为EC volume
	ecLocations, found := ms.Topo.LookupEcShards(volumeId)
	if found {
		return ms.validateEcVolumeAvailabilityMaster(volumeId, ecLocations, strictMode)
	}

	// 检查普通volume
	return ms.validateNormalVolumeAvailabilityMaster(volumeId, dataNodes, strictMode)
}

// validateEcVolumeAvailabilityMaster 校验EC volume的可用性
func (ms *MasterServer) validateEcVolumeAvailabilityMaster(volumeId needle.VolumeId, ecLocations *topology.EcShardLocations, strictMode bool) *master_pb.VolumeAvailabilityInfo {
	volumeInfo := &master_pb.VolumeAvailabilityInfo{
		VolumeId:         uint32(volumeId),
		IsEcVolume:       true,
		RequiredEcShards: erasure_coding.TotalShardsCount,
	}

	// 统计可用的数据分片数量
	availableDataShards := 0
	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if len(ecLocations.Locations[shardId]) > 0 {
			availableDataShards++
		}
	}

	volumeInfo.AvailableEcShards = int32(availableDataShards)

	if strictMode {
		// 严格模式下需要至少16个数据分片
		volumeInfo.IsAvailable = availableDataShards == erasure_coding.TotalShardsCount
		if !volumeInfo.IsAvailable {
			volumeInfo.ErrorMessage = fmt.Sprintf("EC volume %d has insufficient data shards: %d < %d", volumeId, availableDataShards, erasure_coding.TotalShardsCount)
		}
	} else {
		// 非严格模式下至少需要14个分片
		volumeInfo.IsAvailable = availableDataShards >= erasure_coding.DataShardsCount
		if !volumeInfo.IsAvailable {
			volumeInfo.ErrorMessage = fmt.Sprintf("EC volume %d has insufficient data shards: %d < %d", volumeId, availableDataShards, erasure_coding.DataShardsCount)
		}
	}

	glog.V(3).Infof("[Strict Mode] EC volume %d validation: available_shards=%d, required=%d, available=%v",
		volumeId, availableDataShards, erasure_coding.TotalShardsCount, volumeInfo.IsAvailable)
	return volumeInfo
}

// validateNormalVolumeAvailabilityMaster 校验普通volume的可用性
func (ms *MasterServer) validateNormalVolumeAvailabilityMaster(volumeId needle.VolumeId, dataNodes []*topology.DataNode, strictMode bool) *master_pb.VolumeAvailabilityInfo {
	volumeInfo := &master_pb.VolumeAvailabilityInfo{
		VolumeId:          uint32(volumeId),
		IsEcVolume:        false,
		AvailableReplicas: int32(len(dataNodes)),
	}

	// 获取副本配置要求
	if len(dataNodes) > 0 {
		// 从第一个节点获取volume信息来确定副本要求
		for _, volInfo := range dataNodes[0].GetVolumes() {
			if volInfo.Id == volumeId {
				volumeInfo.RequiredReplicas = int32(volInfo.ReplicaPlacement.GetCopyCount())
				break
			}
		}
	}

	// 如果无法获取副本要求，使用默认值1
	if volumeInfo.RequiredReplicas == 0 {
		volumeInfo.RequiredReplicas = 1
	}

	if strictMode {
		// 严格模式下检查副本数量是否满足要求
		volumeInfo.IsAvailable = volumeInfo.AvailableReplicas >= volumeInfo.RequiredReplicas
		if !volumeInfo.IsAvailable {
			volumeInfo.ErrorMessage = fmt.Sprintf("volume %d has insufficient replicas: %d < %d",
				volumeId, volumeInfo.AvailableReplicas, volumeInfo.RequiredReplicas)
		}
	} else {
		// 非严格模式下至少需要1个副本
		volumeInfo.IsAvailable = volumeInfo.AvailableReplicas > 0
		if !volumeInfo.IsAvailable {
			volumeInfo.ErrorMessage = fmt.Sprintf("volume %d has no available replicas", volumeId)
		}
	}

	glog.V(3).Infof("[Strict Mode] Normal volume %d validation: available_replicas=%d, required=%d, available=%v",
		volumeId, volumeInfo.AvailableReplicas, volumeInfo.RequiredReplicas, volumeInfo.IsAvailable)
	return volumeInfo
}

func (ms *MasterServer) Statistics(ctx context.Context, req *master_pb.StatisticsRequest) (*master_pb.StatisticsResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	if req.Replication == "" {
		req.Replication = ms.option.DefaultReplicaPlacement
	}
	replicaPlacement, err := super_block.NewReplicaPlacementFromString(req.Replication)
	if err != nil {
		return nil, err
	}
	ttl, err := needle.ReadTTL(req.Ttl)
	if err != nil {
		return nil, err
	}

	volumeLayout := ms.Topo.GetVolumeLayout(req.Collection, replicaPlacement, ttl, types.ToDiskType(req.DiskType))
	stats := volumeLayout.Stats()
	totalSize := ms.Topo.GetDiskUsages().GetMaxVolumeCount() * int64(ms.option.VolumeSizeLimitMB) * 1024 * 1024
	resp := &master_pb.StatisticsResponse{
		TotalSize: uint64(totalSize),
		UsedSize:  stats.UsedSize,
		FileCount: stats.FileCount,
	}
	return resp, nil
}

func (ms *MasterServer) VolumeList(ctx context.Context, req *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.VolumeListResponse{
		TopologyInfo:      ms.Topo.ToTopologyInfo(),
		VolumeSizeLimitMb: uint64(ms.option.VolumeSizeLimitMB),
	}

	return resp, nil
}

func (ms *MasterServer) VolumeListWithoutECVolume(ctx context.Context, req *master_pb.VolumeListWithoutECVolumeRequest) (*master_pb.VolumeListResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.VolumeListResponse{
		TopologyInfo:      ms.Topo.ToTopologyInfoByQuery(req),
		VolumeSizeLimitMb: uint64(ms.option.VolumeSizeLimitMB),
	}

	return resp, nil
}

func (ms *MasterServer) EcCollectList(ctx context.Context, req *master_pb.EcCollectRequest) (*master_pb.EcCollectResponse, error) {
	volumeListResponse, err := ms.VolumeList(ctx, &master_pb.VolumeListRequest{})
	if err != nil {
		return nil, err
	}
	resp := &master_pb.EcCollectResponse{
		EcNodeInfo:        ms.Topo.ToEcCollectInfo(volumeListResponse.TopologyInfo, req),
		VolumeSizeLimitMb: uint64(ms.option.VolumeSizeLimitMB),
	}
	return resp, nil
}

func (ms *MasterServer) LookupEcVolume(ctx context.Context, req *master_pb.LookupEcVolumeRequest) (*master_pb.LookupEcVolumeResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.LookupEcVolumeResponse{}

	ecLocations, found := ms.Topo.LookupEcShards(needle.VolumeId(req.VolumeId))

	if !found {
		return resp, fmt.Errorf("ec volume %d not found", req.VolumeId)
	}

	resp.VolumeId = req.VolumeId

	for shardId, shardLocations := range ecLocations.Locations {
		var locations []*master_pb.Location
		for _, dn := range shardLocations {
			locations = append(locations, &master_pb.Location{
				Url:        string(dn.Id()),
				PublicUrl:  dn.PublicUrl,
				DataCenter: dn.GetDataCenterId(),
			})
		}
		resp.ShardIdLocations = append(resp.ShardIdLocations, &master_pb.LookupEcVolumeResponse_EcShardIdLocation{
			ShardId:   uint32(shardId),
			Locations: locations,
		})
	}

	return resp, nil
}

func (ms *MasterServer) VacuumVolume(ctx context.Context, req *master_pb.VacuumVolumeRequest) (*master_pb.VacuumVolumeResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.VacuumVolumeResponse{}

	ms.Topo.Vacuum(ms.grpcDialOption, float64(req.GarbageThreshold), req.VolumeId, req.Collection, ms.preallocateSize)

	return resp, nil
}

func (ms *MasterServer) VacuumEcVolume(ctx context.Context, req *master_pb.VacuumEcVolumeRequest) (*master_pb.VacuumEcVolumeResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.VacuumEcVolumeResponse{}

	// 使用去重队列机制
	ms.addToVacuumEcQueue(req.VolumeId)

	return resp, nil
}

func (ms *MasterServer) DisableVacuum(ctx context.Context, req *master_pb.DisableVacuumRequest) (*master_pb.DisableVacuumResponse, error) {

	ms.Topo.DisableVacuum()
	resp := &master_pb.DisableVacuumResponse{}
	return resp, nil
}

// addToVacuumEcQueue 添加卷ID到去重队列，并确保协程池正在运行
func (ms *MasterServer) addToVacuumEcQueue(volumeId uint32) {
	ms.vacuumEcQueueLock.Lock()
	defer ms.vacuumEcQueueLock.Unlock()

	// 检查是否已经在队列中
	if _, exists := ms.vacuumEcQueue[volumeId]; exists {
		return // 已存在，无需重复添加
	}

	// 添加到队列中
	ms.vacuumEcQueue[volumeId] = true

	// 启动协程池（如果还没启动）
	if !ms.vacuumEcStarted {
		ms.vacuumEcStarted = true
		ms.startVacuumEcWorkerPool()
	}

	// 发送任务到通道
	select {
	case ms.vacuumEcTaskChan <- volumeId:
		// 任务已发送
	default:
		// 通道满了，任务会在队列中等待
	}
}

// startVacuumEcWorkerPool 启动EC卷垃圾回收协程池
func (ms *MasterServer) startVacuumEcWorkerPool() {
	for i := 0; i < ms.vacuumEcMaxWorkers; i++ {
		go ms.vacuumEcWorker()
	}
}

// vacuumEcWorker EC卷垃圾回收工作协程
func (ms *MasterServer) vacuumEcWorker() {
	ms.vacuumEcQueueLock.Lock()
	ms.vacuumEcWorkerCount++
	ms.vacuumEcQueueLock.Unlock()

	defer func() {
		ms.vacuumEcQueueLock.Lock()
		ms.vacuumEcWorkerCount--
		if ms.vacuumEcWorkerCount == 0 {
			ms.vacuumEcStarted = false
		}
		ms.vacuumEcQueueLock.Unlock()
	}()

	for {
		select {
		case volumeId := <-ms.vacuumEcTaskChan:
			// 从队列中移除该卷ID
			ms.vacuumEcQueueLock.Lock()
			delete(ms.vacuumEcQueue, volumeId)
			ms.vacuumEcQueueLock.Unlock()

			// 执行垃圾回收
			ms.Topo.VacuumEcVolumes(ms.grpcDialOption, "", volumeId)

		case <-ms.vacuumEcStopChan:
			return
		}
	}
}

// SetVacuumEcMaxWorkers 设置EC卷垃圾回收最大工作协程数
func (ms *MasterServer) SetVacuumEcMaxWorkers(maxWorkers int) {
	ms.vacuumEcQueueLock.Lock()
	defer ms.vacuumEcQueueLock.Unlock()
	ms.vacuumEcMaxWorkers = maxWorkers
}

// StopVacuumEcWorkers 停止所有EC卷垃圾回收工作协程
func (ms *MasterServer) StopVacuumEcWorkers() {
	ms.vacuumEcQueueLock.Lock()
	defer ms.vacuumEcQueueLock.Unlock()

	if ms.vacuumEcStarted {
		close(ms.vacuumEcStopChan)
		ms.vacuumEcStarted = false
	}
}

func (ms *MasterServer) EnableVacuum(ctx context.Context, req *master_pb.EnableVacuumRequest) (*master_pb.EnableVacuumResponse, error) {

	ms.Topo.EnableVacuum()
	resp := &master_pb.EnableVacuumResponse{}
	return resp, nil
}

func (ms *MasterServer) VolumeMarkReadonly(ctx context.Context, req *master_pb.VolumeMarkReadonlyRequest) (*master_pb.VolumeMarkReadonlyResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	resp := &master_pb.VolumeMarkReadonlyResponse{}

	replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(req.ReplicaPlacement))
	vl := ms.Topo.GetVolumeLayout(req.Collection, replicaPlacement, needle.LoadTTLFromUint32(req.Ttl), types.ToDiskType(req.DiskType))
	dataNodes := ms.Topo.Lookup(req.Collection, needle.VolumeId(req.VolumeId))

	for _, dn := range dataNodes {
		if dn.Ip == req.Ip && dn.Port == int(req.Port) {
			if req.IsReadonly {
				vl.SetVolumeReadOnly(dn, needle.VolumeId(req.VolumeId))
			} else {
				vl.SetVolumeWritable(dn, needle.VolumeId(req.VolumeId))
			}
		}
	}

	return resp, nil
}
