package topology

import (
	"math/rand"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

func (t *Topology) StartRefreshWritableVolumes(grpcDialOption grpc.DialOption, garbageThreshold float64, growThreshold float64, preallocate int64) {
	go func() {
		for {
			if t.IsLeader() {
				start := time.Now()
				freshThreshHold := time.Now().Unix() - 3*t.pulse //3 times of sleep interval
				glog.V(1).Infof("[RefreshWritableVolumes] Starting periodic volume check as Leader, freshThreshold=%d, volumeSizeLimit=%d, growThreshold=%.2f",
					freshThreshHold, t.volumeSizeLimit, growThreshold)
				t.CollectDeadNodeAndFullVolumes(freshThreshHold, t.volumeSizeLimit, growThreshold)
				glog.V(1).Infof("[RefreshWritableVolumes] Completed periodic volume check in %v", time.Since(start))
			} else {
				glog.V(2).Infof("[RefreshWritableVolumes] Skipping volume check - not Leader")
			}
			sleepDuration := time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond
			glog.V(2).Infof("[RefreshWritableVolumes] Sleeping for %v before next check", sleepDuration)
			time.Sleep(sleepDuration)
		}
	}()
	go func(garbageThreshold float64) {
		for {
			if t.IsLeader() {
				if !t.isDisableVacuum {
					t.Vacuum(grpcDialOption, garbageThreshold, 0, "", preallocate)
				}
			} else {
				stats.MasterReplicaPlacementMismatch.Reset()
			}
			time.Sleep(14*time.Minute + time.Duration(120*rand.Float32())*time.Second)
		}
	}(garbageThreshold)
	go func() {
		for {
			select {
			case fv := <-t.chanFullVolumes:
				start := time.Now()
				glog.V(0).Infof("[VolumeFullEvent] Processing full volume event for volume %d, collection=%s, size=%d, diskType=%s at %v",
					fv.Id, fv.Collection, fv.Size, fv.DiskType, start)
				success := t.SetVolumeCapacityFull(fv)
				duration := time.Since(start)
				if success {
					glog.V(0).Infof("[VolumeFullEvent] Successfully processed full volume %d in %v, removed from writable list", fv.Id, duration)
				} else {
					glog.V(0).Infof("[VolumeFullEvent] Failed to process full volume %d in %v, volume may already be non-writable", fv.Id, duration)
				}
			case cv := <-t.chanCrowdedVolumes:
				start := time.Now()
				glog.V(1).Infof("[VolumeCrowdedEvent] Processing crowded volume event for volume %d, collection=%s, size=%d at %v",
					cv.Id, cv.Collection, cv.Size, start)
				t.SetVolumeCrowded(cv)
				glog.V(1).Infof("[VolumeCrowdedEvent] Processed crowded volume %d in %v", cv.Id, time.Since(start))
			}
		}
	}()
	// 被删除的ecVolumes回收
	go func() {
		for {
			if t.IsLeader() {
				if !t.isDisableEcVacuum {
					t.VacuumEcVolumes(grpcDialOption, "", 0)
				}
			} else {
				stats.MasterReplicaPlacementMismatch.Reset()
			}
			time.Sleep(14*time.Minute + time.Duration(120*rand.Float32())*time.Second)
		}
	}()
}
func (t *Topology) SetVolumeCapacityFull(volumeInfo storage.VolumeInfo) bool {
	start := time.Now()
	diskType := types.ToDiskType(volumeInfo.DiskType)
	glog.V(0).Infof("[SetVolumeCapacityFull] Starting to set volume %d as full, collection=%s, replication=%s, ttl=%s, diskType=%s",
		volumeInfo.Id, volumeInfo.Collection, volumeInfo.ReplicaPlacement, volumeInfo.Ttl, diskType)

	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.ReplicaPlacement, volumeInfo.Ttl, diskType)
	if !vl.SetVolumeCapacityFull(volumeInfo.Id) {
		glog.V(0).Infof("[SetVolumeCapacityFull] Volume %d was already non-writable, operation completed in %v",
			volumeInfo.Id, time.Since(start))
		return false
	}

	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	vidLocations, found := vl.vid2location[volumeInfo.Id]
	if !found {
		glog.V(0).Infof("[SetVolumeCapacityFull] Volume %d not found in vid2location, operation completed in %v",
			volumeInfo.Id, time.Since(start))
		return false
	}

	glog.V(0).Infof("[SetVolumeCapacityFull] Volume %d found on %d data nodes, updating disk usage",
		volumeInfo.Id, len(vidLocations.list))

	for _, dn := range vidLocations.list {
		if !volumeInfo.ReadOnly {
			glog.V(1).Infof("[SetVolumeCapacityFull] Updating disk usage for volume %d on node %s",
				volumeInfo.Id, dn.Id())

			disk := dn.getOrCreateDisk(volumeInfo.DiskType)
			deltaDiskUsages := newDiskUsages()
			deltaDiskUsage := deltaDiskUsages.getOrCreateDisk(types.ToDiskType(volumeInfo.DiskType))
			deltaDiskUsage.activeVolumeCount = -1
			disk.UpAdjustDiskUsageDelta(deltaDiskUsages)
		} else {
			glog.V(1).Infof("[SetVolumeCapacityFull] Skipping disk usage update for readonly volume %d on node %s",
				volumeInfo.Id, dn.Id())
		}
	}

	glog.V(0).Infof("[SetVolumeCapacityFull] Successfully set volume %d as full and updated disk usage, total operation time: %v",
		volumeInfo.Id, time.Since(start))
	return true
}

func (t *Topology) SetVolumeCrowded(volumeInfo storage.VolumeInfo) {
	diskType := types.ToDiskType(volumeInfo.DiskType)
	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.ReplicaPlacement, volumeInfo.Ttl, diskType)
	vl.SetVolumeCrowded(volumeInfo.Id)
}

func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	dn.IsTerminating = true
	for _, v := range dn.GetVolumes() {
		glog.V(0).Infoln("Removing Volume", v.Id, "from the dead volume server", dn.Id())
		diskType := types.ToDiskType(v.DiskType)
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
		vl.SetVolumeUnavailable(dn, v.Id)
	}

	// unregister ec shards when volume server disconnected
	for _, s := range dn.GetEcShards() {
		t.UnRegisterEcShards(s, dn)
	}

	negativeUsages := dn.GetDiskUsages().negative()
	dn.UpAdjustDiskUsageDelta(negativeUsages)
	dn.DeltaUpdateVolumes([]storage.VolumeInfo{}, dn.GetVolumes())
	dn.DeltaUpdateEcShards([]*erasure_coding.EcVolumeInfo{}, dn.GetEcShards())
	if dn.Parent() != nil {
		dn.Parent().UnlinkChildNode(dn.Id())
	}
}
