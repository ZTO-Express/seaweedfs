package topology

import (
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

type copyState int

const (
	noCopies copyState = 0 + iota
	insufficientCopies
	enoughCopies
)

type volumeState string

const (
	readOnlyState  volumeState = "ReadOnly"
	oversizedState             = "Oversized"
	crowdedState               = "Crowded"
)

type stateIndicator func(copyState) bool

func ExistCopies() stateIndicator {
	return func(state copyState) bool { return state != noCopies }
}

func NoCopies() stateIndicator {
	return func(state copyState) bool { return state == noCopies }
}

type volumesBinaryState struct {
	rp        *super_block.ReplicaPlacement
	name      volumeState    // the name for volume state (eg. "Readonly", "Oversized")
	indicator stateIndicator // indicate whether the volumes should be marked as `name`
	copyMap   map[needle.VolumeId]*VolumeLocationList
}

func NewVolumesBinaryState(name volumeState, rp *super_block.ReplicaPlacement, indicator stateIndicator) *volumesBinaryState {
	return &volumesBinaryState{
		rp:        rp,
		name:      name,
		indicator: indicator,
		copyMap:   make(map[needle.VolumeId]*VolumeLocationList),
	}
}

func (v *volumesBinaryState) Dump() (res []uint32) {
	for vid, list := range v.copyMap {
		if v.indicator(v.copyState(list)) {
			res = append(res, uint32(vid))
		}
	}
	return
}

func (v *volumesBinaryState) IsTrue(vid needle.VolumeId) bool {
	list, _ := v.copyMap[vid]
	return v.indicator(v.copyState(list))
}

func (v *volumesBinaryState) Add(vid needle.VolumeId, dn *DataNode) {
	list, _ := v.copyMap[vid]
	if list != nil {
		list.Set(dn)
		return
	}
	list = NewVolumeLocationList()
	list.Set(dn)
	v.copyMap[vid] = list
}

func (v *volumesBinaryState) Remove(vid needle.VolumeId, dn *DataNode) {
	list, _ := v.copyMap[vid]
	if list != nil {
		list.Remove(dn)
		if list.Length() == 0 {
			delete(v.copyMap, vid)
		}
	}
}

func (v *volumesBinaryState) copyState(list *VolumeLocationList) copyState {
	if list == nil {
		return noCopies
	}
	if list.Length() < v.rp.GetCopyCount() {
		return insufficientCopies
	}
	return enoughCopies
}

// mapping from volume to its locations, inverted from server to volume
type VolumeLayout struct {
	growRequestCount  int32
	rp                *super_block.ReplicaPlacement
	ttl               *needle.TTL
	diskType          types.DiskType
	vid2location      map[needle.VolumeId]*VolumeLocationList
	writables         []needle.VolumeId // transient array of writable volume id
	crowded           map[needle.VolumeId]struct{}
	crowdedAccessLock sync.RWMutex
	readonlyVolumes   *volumesBinaryState // readonly volumes
	oversizedVolumes  *volumesBinaryState // oversized volumes
	vacuumedVolumes   map[needle.VolumeId]time.Time
	volumeSizeLimit   uint64
	replicationAsMin  bool
	accessLock        sync.RWMutex
}

type VolumeLayoutStats struct {
	TotalSize uint64
	UsedSize  uint64
	FileCount uint64
}

func NewVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType, volumeSizeLimit uint64, replicationAsMin bool) *VolumeLayout {
	return &VolumeLayout{
		rp:               rp,
		ttl:              ttl,
		diskType:         diskType,
		vid2location:     make(map[needle.VolumeId]*VolumeLocationList),
		writables:        *new([]needle.VolumeId),
		crowded:          make(map[needle.VolumeId]struct{}),
		readonlyVolumes:  NewVolumesBinaryState(readOnlyState, rp, ExistCopies()),
		oversizedVolumes: NewVolumesBinaryState(oversizedState, rp, ExistCopies()),
		vacuumedVolumes:  make(map[needle.VolumeId]time.Time),
		volumeSizeLimit:  volumeSizeLimit,
		replicationAsMin: replicationAsMin,
	}
}

func (vl *VolumeLayout) String() string {
	return fmt.Sprintf("rp:%v, ttl:%v, writables:%v, volumeSizeLimit:%v", vl.rp, vl.ttl, vl.writables, vl.volumeSizeLimit)
}

func (vl *VolumeLayout) RegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	defer vl.rememberOversizedVolume(v, dn)

	if _, ok := vl.vid2location[v.Id]; !ok {
		vl.vid2location[v.Id] = NewVolumeLocationList()
	}
	vl.vid2location[v.Id].Set(dn)
	// glog.V(4).Infof("volume %d added to %s len %d copy %d", v.Id, dn.Id(), vl.vid2location[v.Id].Length(), v.ReplicaPlacement.GetCopyCount())
	for _, dn := range vl.vid2location[v.Id].list {
		if vInfo, err := dn.GetVolumesById(v.Id); err == nil {
			if vInfo.ReadOnly {
				glog.V(1).Infof("vid %d removed from writable", v.Id)
				vl.removeFromWritable(v.Id)
				vl.readonlyVolumes.Add(v.Id, dn)
				return
			} else {
				vl.readonlyVolumes.Remove(v.Id, dn)
			}
		} else {
			glog.V(1).Infof("vid %d removed from writable", v.Id)
			vl.removeFromWritable(v.Id)
			vl.readonlyVolumes.Remove(v.Id, dn)
			return
		}
	}

}

func (vl *VolumeLayout) rememberOversizedVolume(v *storage.VolumeInfo, dn *DataNode) {
	if vl.isOversized(v) {
		vl.oversizedVolumes.Add(v.Id, dn)
	} else {
		vl.oversizedVolumes.Remove(v.Id, dn)
	}
}

func (vl *VolumeLayout) UnRegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	// remove from vid2location map
	location, ok := vl.vid2location[v.Id]
	if !ok {
		return
	}

	if location.Remove(dn) {

		vl.readonlyVolumes.Remove(v.Id, dn)
		vl.oversizedVolumes.Remove(v.Id, dn)
		vl.ensureCorrectWritables(v.Id)

		if location.Length() == 0 {
			delete(vl.vid2location, v.Id)
		}

	}
}

func (vl *VolumeLayout) EnsureCorrectWritables(v *storage.VolumeInfo) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vl.ensureCorrectWritables(v.Id)
}

func (vl *VolumeLayout) ensureCorrectWritables(vid needle.VolumeId) {
	if vl.enoughCopies(vid) && vl.isAllWritable(vid) {
		if !vl.oversizedVolumes.IsTrue(vid) {
			vl.setVolumeWritable(vid)
		}
	} else {
		if !vl.enoughCopies(vid) {
			glog.V(0).Infof("volume %d does not have enough copies", vid)
		}
		if !vl.isAllWritable(vid) {
			glog.V(0).Infof("volume %d are not all writable", vid)
		}
		glog.V(0).Infof("volume %d remove from writable", vid)
		vl.removeFromWritable(vid)
	}
}

func (vl *VolumeLayout) isAllWritable(vid needle.VolumeId) bool {
	if location, ok := vl.vid2location[vid]; ok {
		for _, dn := range location.list {
			if v, getError := dn.GetVolumesById(vid); getError == nil {
				if v.ReadOnly {
					return false
				}
			}
		}
	} else {
		return false
	}

	return true
}

func (vl *VolumeLayout) isOversized(v *storage.VolumeInfo) bool {
	return uint64(v.Size) >= vl.volumeSizeLimit
}

func (vl *VolumeLayout) isWritable(v *storage.VolumeInfo) bool {
	return !vl.isOversized(v) &&
		v.Version == needle.CurrentVersion &&
		!v.ReadOnly
}

func (vl *VolumeLayout) isEmpty() bool {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	return len(vl.vid2location) == 0
}

func (vl *VolumeLayout) Lookup(vid needle.VolumeId) []*DataNode {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	if location := vl.vid2location[vid]; location != nil {
		return location.list
	}
	return nil
}

func (vl *VolumeLayout) ListVolumeServers() (nodes []*DataNode) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	for _, location := range vl.vid2location {
		nodes = append(nodes, location.list...)
	}
	return
}

func (vl *VolumeLayout) PickForWrite(count uint64, option *VolumeGrowOption) (vid needle.VolumeId, counter uint64, locationList *VolumeLocationList, shouldGrow bool, err error) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	lenWriters := len(vl.writables)
	if lenWriters <= 0 {
		//glog.V(0).Infoln("No more writable volumes!")
		shouldGrow = true
		return 0, 0, nil, shouldGrow, errors.New("No more writable volumes!")
	}
	if option.DataCenter == "" && option.Rack == "" && option.DataNode == "" {
		vid := vl.writables[rand.Intn(lenWriters)]
		locationList = vl.vid2location[vid]
		if locationList != nil && locationList.Length() > 0 {
			// check whether picked file is close to full
			dn := locationList.Head()
			info, _ := dn.GetVolumesById(vid)
			if float64(info.Size) > float64(vl.volumeSizeLimit)*VolumeGrowStrategy.Threshold {
				shouldGrow = true
				vl.setVolumeCrowded(vid)
			}
			return vid, count, locationList.Copy(), shouldGrow, nil
		}
		return 0, 0, nil, shouldGrow, errors.New("Strangely vid " + vid.String() + " is on no machine!")
	}

	// clone vl.writables
	writables := make([]needle.VolumeId, len(vl.writables))
	copy(writables, vl.writables)
	// randomize the writables
	rand.Shuffle(len(writables), func(i, j int) {
		writables[i], writables[j] = writables[j], writables[i]
	})

	for _, writableVolumeId := range writables {
		volumeLocationList := vl.vid2location[writableVolumeId]
		for _, dn := range volumeLocationList.list {
			if option.DataCenter != "" && dn.GetDataCenter().Id() != NodeId(option.DataCenter) {
				continue
			}
			if option.Rack != "" && dn.GetRack().Id() != NodeId(option.Rack) {
				continue
			}
			if option.DataNode != "" && dn.Id() != NodeId(option.DataNode) {
				continue
			}
			vid, locationList = writableVolumeId, volumeLocationList.Copy()
			// check whether picked file is close to full
			info, _ := dn.GetVolumesById(writableVolumeId)
			if float64(info.Size) > float64(vl.volumeSizeLimit)*VolumeGrowStrategy.Threshold {
				shouldGrow = true
			}
			counter = count
			return
		}
	}
	return vid, count, locationList, true, fmt.Errorf("No writable volumes in DataCenter:%v Rack:%v DataNode:%v", option.DataCenter, option.Rack, option.DataNode)
}

func (vl *VolumeLayout) HasGrowRequest() bool {
	return atomic.LoadInt32(&vl.growRequestCount) > 0
}
func (vl *VolumeLayout) AddGrowRequest() {
	atomic.AddInt32(&vl.growRequestCount, 1)
}
func (vl *VolumeLayout) DoneGrowRequest() {
	atomic.AddInt32(&vl.growRequestCount, -1)
}

func (vl *VolumeLayout) ShouldGrowVolumes(option *VolumeGrowOption) bool {
	total, active, crowded := vl.GetActiveVolumeCount(option)
	stats.MasterVolumeLayout.WithLabelValues(option.Collection, option.DataCenter, "total").Set(float64(total))
	stats.MasterVolumeLayout.WithLabelValues(option.Collection, option.DataCenter, "active").Set(float64(active))
	stats.MasterVolumeLayout.WithLabelValues(option.Collection, option.DataCenter, "crowded").Set(float64(crowded))
	glog.V(0).Infof("active volume: %d, high usage volume: %d\n", active, crowded)
	return (float64(active) * VolumeGrowStrategy.CrowdedThreshold) <= float64(crowded)
}

func (vl *VolumeLayout) GetActiveVolumeCount(option *VolumeGrowOption) (total, active, crowded int) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()
	if option.DataCenter == "" {
		return len(vl.writables), len(vl.writables), len(vl.getVolumeCrowded())
	}
	total = len(vl.writables)
	for _, v := range vl.writables {
		for _, dn := range vl.vid2location[v].list {
			if dn.GetDataCenter().Id() == NodeId(option.DataCenter) {
				if option.Rack != "" && dn.GetRack().Id() != NodeId(option.Rack) {
					continue
				}
				if option.DataNode != "" && dn.Id() != NodeId(option.DataNode) {
					continue
				}
				active++
				info, _ := dn.GetVolumesById(v)
				if float64(info.Size) > float64(vl.volumeSizeLimit)*VolumeGrowStrategy.Threshold {
					crowded++
				}
			}
		}
	}
	return
}

func (vl *VolumeLayout) removeFromWritable(vid needle.VolumeId) bool {
	toDeleteIndex := -1
	for k, id := range vl.writables {
		if id == vid {
			toDeleteIndex = k
			break
		}
	}
	if toDeleteIndex >= 0 {
		glog.V(0).Infoln("Volume", vid, "becomes unwritable")
		vl.writables = append(vl.writables[0:toDeleteIndex], vl.writables[toDeleteIndex+1:]...)
		vl.removeFromCrowded(vid)
		return true
	}
	return false
}
func (vl *VolumeLayout) setVolumeWritable(vid needle.VolumeId) bool {
	for _, v := range vl.writables {
		if v == vid {
			return false
		}
	}
	glog.V(0).Infoln("Volume", vid, "becomes writable")
	vl.writables = append(vl.writables, vid)
	return true
}

func (vl *VolumeLayout) SetVolumeReadOnly(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[vid]; ok {
		vl.readonlyVolumes.Add(vid, dn)
		return vl.removeFromWritable(vid)
	}
	return true
}

func (vl *VolumeLayout) SetVolumeWritable(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[vid]; ok {
		vl.readonlyVolumes.Remove(vid, dn)
	}

	if vl.enoughCopies(vid) {
		return vl.setVolumeWritable(vid)
	}
	return false
}

func (vl *VolumeLayout) SetVolumeUnavailable(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if location, ok := vl.vid2location[vid]; ok {
		if location.Remove(dn) {
			vl.readonlyVolumes.Remove(vid, dn)
			vl.oversizedVolumes.Remove(vid, dn)
			if location.Length() < vl.rp.GetCopyCount() {
				glog.V(0).Infoln("Volume", vid, "has", location.Length(), "replica, less than required", vl.rp.GetCopyCount())
				return vl.removeFromWritable(vid)
			}
		}
	}
	return false
}
func (vl *VolumeLayout) SetVolumeAvailable(dn *DataNode, vid needle.VolumeId, isReadOnly, isFullCapacity bool) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vInfo, err := dn.GetVolumesById(vid)
	if err != nil {
		return false
	}

	vl.vid2location[vid].Set(dn)

	if vInfo.ReadOnly || isReadOnly || isFullCapacity {
		return false
	}

	if vl.enoughCopies(vid) {
		return vl.setVolumeWritable(vid)
	}
	return false
}

func (vl *VolumeLayout) enoughCopies(vid needle.VolumeId) bool {
	locations := vl.vid2location[vid].Length()
	desired := vl.rp.GetCopyCount()
	return locations == desired || (vl.replicationAsMin && locations > desired)
}

func (vl *VolumeLayout) SetVolumeCapacityFull(vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	wasWritable := vl.removeFromWritable(vid)
	if wasWritable {
		glog.V(0).Infof("Volume %d reaches full capacity.", vid)
	}
	return wasWritable
}

func (vl *VolumeLayout) removeFromCrowded(vid needle.VolumeId) {
	vl.crowdedAccessLock.Lock()
	defer vl.crowdedAccessLock.Unlock()
	delete(vl.crowded, vid)
}

func (vl *VolumeLayout) setVolumeCrowded(vid needle.VolumeId) {
	vl.crowdedAccessLock.Lock()
	defer vl.crowdedAccessLock.Unlock()
	if _, ok := vl.crowded[vid]; !ok {
		vl.crowded[vid] = struct{}{}
		glog.V(0).Infoln("Volume", vid, "becomes crowded")
	}
}

func (vl *VolumeLayout) getVolumeCrowded() map[needle.VolumeId]struct{} {
	vl.crowdedAccessLock.RLock()
	defer vl.crowdedAccessLock.RUnlock()
	return vl.crowded
}

func (vl *VolumeLayout) SetVolumeCrowded(vid needle.VolumeId) {
	// since delete is guarded by accessLock.Lock(),
	// and is always called in sequential order,
	// RLock() should be safe enough
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	for _, v := range vl.writables {
		if v == vid {
			vl.setVolumeCrowded(vid)
			break
		}
	}
}

type VolumeLayoutInfo struct {
	Replication string            `json:"replication"`
	TTL         string            `json:"ttl"`
	Writables   []needle.VolumeId `json:"writables"`
	Collection  string            `json:"collection"`
	DiskType    string            `json:"diskType"`
}

func (vl *VolumeLayout) ToInfo() (info VolumeLayoutInfo) {
	info.Replication = vl.rp.String()
	info.TTL = vl.ttl.String()
	info.Writables = vl.writables
	info.DiskType = vl.diskType.ReadableString()
	//m["locations"] = vl.vid2location
	return
}

func (vl *VolumeLayout) Stats() *VolumeLayoutStats {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	ret := &VolumeLayoutStats{}

	freshThreshold := time.Now().Unix() - 60

	for vid, vll := range vl.vid2location {
		size, fileCount := vll.Stats(vid, freshThreshold)
		ret.FileCount += uint64(fileCount)
		ret.UsedSize += size * uint64(vll.Length())
		if vl.readonlyVolumes.IsTrue(vid) {
			ret.TotalSize += size * uint64(vll.Length())
		} else {
			ret.TotalSize += vl.volumeSizeLimit * uint64(vll.Length())
		}
	}

	return ret
}
