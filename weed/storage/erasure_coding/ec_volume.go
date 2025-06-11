package erasure_coding

import (
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/slices"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

var (
	NotFoundError             = errors.New("needle not found")
	destroyDelaySeconds int64 = 0
)

type EcVolume struct {
	VolumeId                  needle.VolumeId
	Collection                string
	dir                       string
	dirIdx                    string
	ecxFile                   *os.File
	ecxFileAccessLock         sync.RWMutex
	ecxFileSize               int64
	ecxCreatedAt              time.Time
	Shards                    []*EcVolumeShard
	ShardLocations            map[ShardId][]pb.ServerAddress
	ShardLocationsRefreshTime time.Time
	ShardLocationsLock        sync.RWMutex
	Version                   needle.Version
	ecjFile                   *os.File
	ecjFileAccessLock         sync.Mutex
	diskType                  types.DiskType
	lastReadAt                time.Time
	expireTime                int64
	DestroyTime               uint64 //ec volume删除时间
	ecxFileRefCount           int64  // 原子计数器，记录ecx文件的引用次数
}

func NewEcVolume(diskType types.DiskType, dir string, dirIdx string, collection string, vid needle.VolumeId, ecVolumeExpireClose int64) (ev *EcVolume, err error) {
	ev = &EcVolume{
		dir:             dir,
		dirIdx:          dirIdx,
		Collection:      collection,
		VolumeId:        vid,
		diskType:        diskType,
		ecxFileRefCount: 0, // 显式初始化引用计数为0
	}

	dataBaseFileName := EcShardFileName(collection, dir, int(vid))
	indexBaseFileName := EcShardFileName(collection, dirIdx, int(vid))

	// open ecx file
	if ev.ecxFile, err = os.OpenFile(indexBaseFileName+".ecx", os.O_RDWR, 0644); err != nil {
		return nil, fmt.Errorf("cannot open ec volume index %s.ecx: %v", indexBaseFileName, err)
	}
	ecxFi, statErr := ev.ecxFile.Stat()
	if statErr != nil {
		_ = ev.ecxFile.Close()
		return nil, fmt.Errorf("can not stat ec volume index %s.ecx: %v", indexBaseFileName, statErr)
	}
	ev.ecxFileSize = ecxFi.Size()
	ev.ecxCreatedAt = ecxFi.ModTime()

	// open ecj file
	if ev.ecjFile, err = os.OpenFile(indexBaseFileName+".ecj", os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, fmt.Errorf("cannot open ec volume journal %s.ecj: %v", indexBaseFileName, err)
	}

	// read volume info
	ev.Version = needle.Version3
	if volumeInfo, _, found, _ := volume_info.MaybeLoadVolumeInfo(dataBaseFileName + ".vif"); found {
		ev.Version = needle.Version(volumeInfo.Version)
		ev.DestroyTime = volumeInfo.DestroyTime
	} else {
		glog.Warningf("vif file not found,volumeId:%d, filename:%s", vid, dataBaseFileName)
		volume_info.SaveVolumeInfo(dataBaseFileName+".vif", &volume_server_pb.VolumeInfo{Version: uint32(ev.Version)})
	}

	ev.ShardLocations = make(map[ShardId][]pb.ServerAddress)
	ev.lastReadAt = time.Now()
	ev.expireTime = ecVolumeExpireClose
	return
}

func (ev *EcVolume) AddEcVolumeShard(ecVolumeShard *EcVolumeShard) bool {
	for _, s := range ev.Shards {
		if s.ShardId == ecVolumeShard.ShardId {
			return false
		}
	}
	ev.Shards = append(ev.Shards, ecVolumeShard)
	slices.SortFunc(ev.Shards, func(a, b *EcVolumeShard) int {
		if a.VolumeId != b.VolumeId {
			return int(a.VolumeId - b.VolumeId)
		}
		return int(a.ShardId - b.ShardId)
	})

	ev.lastReadAt = time.Now()
	return true
}

func (ev *EcVolume) DeleteEcVolumeShard(shardId ShardId) (ecVolumeShard *EcVolumeShard, deleted bool) {
	foundPosition := -1
	for i, s := range ev.Shards {
		if s.ShardId == shardId {
			foundPosition = i
		}
	}
	if foundPosition < 0 {
		return nil, false
	}

	ecVolumeShard = ev.Shards[foundPosition]

	ev.Shards = append(ev.Shards[:foundPosition], ev.Shards[foundPosition+1:]...)
	ev.lastReadAt = time.Now()
	return ecVolumeShard, true
}

func (ev *EcVolume) FindEcVolumeShard(shardId ShardId) (ecVolumeShard *EcVolumeShard, found bool) {
	for _, s := range ev.Shards {
		if s.ShardId == shardId {
			return s, true
		}
	}
	return nil, false
}

func (ev *EcVolume) Close() {
	for _, s := range ev.Shards {
		s.Close()
	}
	if ev.ecjFile != nil {
		ev.ecjFileAccessLock.Lock()
		_ = ev.ecjFile.Close()
		ev.ecjFile = nil
		ev.ecjFileAccessLock.Unlock()
	}
	if ev.ecxFile != nil {
		ev.ecxFileAccessLock.Lock()
		_ = ev.ecxFile.Sync()
		_ = ev.ecxFile.Close()
		ev.ecxFile = nil
		ev.ecxFileAccessLock.Unlock()
	}
}

func (ev *EcVolume) CloseAndNotEcxLock() {
	for _, s := range ev.Shards {
		s.Close()
	}
	if ev.ecjFile != nil {
		ev.ecjFileAccessLock.Lock()
		_ = ev.ecjFile.Close()
		ev.ecjFile = nil
		ev.ecjFileAccessLock.Unlock()
	}
	if ev.ecxFile != nil {
		_ = ev.ecxFile.Sync()
		_ = ev.ecxFile.Close()
		ev.ecxFile = nil
	}
}

func (ev *EcVolume) DestroyShards(shards []uint32, soft bool) []ShardId {
	var deletedShards []ShardId
	for _, s := range shards {
		shard, deleted := ev.DeleteEcVolumeShard(ShardId(s))
		if deleted {
			shard.Close()
			err := shard.Destroy(soft)
			if err != nil {
				glog.Errorf("destory ec shard %s_%d[%d] err:%v", ev.Collection, ev.VolumeId, shard.ShardId, err)
				return deletedShards
			}
			deletedShards = append(deletedShards, shard.ShardId)
		}
	}
	return deletedShards
}

func (ev *EcVolume) Destroy(soft bool) []ShardId {

	ev.Close()

	var shards []ShardId
	var err error
	for _, s := range ev.Shards {
		err = s.Destroy(soft)
		if err != nil {
			glog.Errorf("destory ec shard %s_%d[%d] err:%v", ev.Collection, ev.VolumeId, s.ShardId, err)
			return shards
		}
		shards = append(shards, s.ShardId)
	}
	if soft {
		err = MoveFile(ev.FileName(".ecx"), GetSoftDeleteDir(ev.FileName(".ecx")))
		if err != nil {
			glog.Errorf("destory ec shard %s_%d move ecx err:%v", ev.Collection, ev.VolumeId, err)
		}
		err = MoveFile(ev.FileName(".ecj"), GetSoftDeleteDir(ev.FileName(".ecj")))
		if err != nil {
			glog.Errorf("destory ec shard %s_%d move ecj err:%v", ev.Collection, ev.VolumeId, err)
		}
		err = MoveFile(ev.FileName(".vif"), GetSoftDeleteDir(ev.FileName(".vif")))
		if err != nil {
			glog.Errorf("destory ec shard %s_%d move vif err:%v", ev.Collection, ev.VolumeId, err)
		}
		return shards
	}
	if err = os.Remove(ev.FileName(".ecx")); err != nil {
		glog.Errorf("failed to remove ec volume %s_%d ecx file %s: %v", ev.Collection, ev.VolumeId, ev.FileName(".ecx"), err)
	}
	if err = os.Remove(ev.FileName(".ecj")); err != nil {
		glog.Errorf("failed to remove ec volume %s_%d ecj file %s: %v", ev.Collection, ev.VolumeId, ev.FileName(".ecj"), err)
	}
	if err = os.Remove(ev.FileName(".vif")); err != nil {
		glog.Errorf("failed to remove ec volume %s_%d vif file %s: %v", ev.Collection, ev.VolumeId, ev.FileName(".vif"), err)
	}
	return shards
}

func (ev *EcVolume) FileName(ext string) string {
	switch ext {
	case ".ecx", ".ecj":
		return ev.IndexBaseFileName() + ext
	}
	// .vif
	return ev.DataBaseFileName() + ext
}

func (ev *EcVolume) DataBaseFileName() string {
	return EcShardFileName(ev.Collection, ev.dir, int(ev.VolumeId))
}

func (ev *EcVolume) IndexBaseFileName() string {
	return EcShardFileName(ev.Collection, ev.dirIdx, int(ev.VolumeId))
}

func (ev *EcVolume) ShardSize() uint64 {
	if len(ev.Shards) > 0 {
		return uint64(ev.Shards[0].Size())
	}
	return 0
}

func (ev *EcVolume) Size() (size int64) {
	for _, shard := range ev.Shards {
		size += shard.Size()
	}
	return
}

func (ev *EcVolume) CreatedAt() time.Time {
	return ev.ecxCreatedAt
}

func (ev *EcVolume) ShardIdList() (shardIds []ShardId) {
	for _, s := range ev.Shards {
		shardIds = append(shardIds, s.ShardId)
	}
	return
}

func (ev *EcVolume) ToVolumeEcShardInformationMessage() (messages []*master_pb.VolumeEcShardInformationMessage) {
	prevVolumeId := needle.VolumeId(math.MaxUint32)
	var m *master_pb.VolumeEcShardInformationMessage
	for _, s := range ev.Shards {
		if s.VolumeId != prevVolumeId {
			m = &master_pb.VolumeEcShardInformationMessage{
				Id:          uint32(s.VolumeId),
				Collection:  s.Collection,
				DiskType:    string(ev.diskType),
				DestroyTime: ev.DestroyTime,
				Dir:         ev.dir,
			}
			messages = append(messages, m)
		}
		prevVolumeId = s.VolumeId
		m.EcIndexBits = uint32(ShardBits(m.EcIndexBits).AddShardId(s.ShardId))
	}
	return
}

func (ev *EcVolume) LocateEcShardNeedle(needleId types.NeedleId, version needle.Version) (offset types.Offset, size types.Size, intervals []Interval, err error) {

	// find the needle from ecx file
	offset, size, err = ev.FindNeedleFromEcx(needleId)
	if err != nil {
		return types.Offset{}, 0, nil, fmt.Errorf("FindNeedleFromEcx: %v", err)
	}

	intervals = ev.LocateEcShardNeedleInterval(version, offset.ToActualOffset(), types.Size(needle.GetActualSize(size, version)))
	ev.lastReadAt = time.Now()
	return
}

func (ev *EcVolume) LocateEcShardNeedleByNeedleIdStatus(needleId types.NeedleId) (offset types.Offset, size types.Size, err error) {

	// find the needle from ecx file
	offset, size, err = ev.FindNeedleFromEcx(needleId)
	if err != nil {
		return types.Offset{}, 0, fmt.Errorf("FindNeedleFromEcxAndClose: %v", err)
	}
	ev.lastReadAt = time.Now()
	return
}

func (ev *EcVolume) LocateEcShardNeedleInterval(version needle.Version, offset int64, size types.Size) (intervals []Interval) {
	shard := ev.Shards[0]
	// calculate the locations in the ec shards
	intervals = LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, DataShardsCount*shard.ecdFileSize, offset, types.Size(needle.GetActualSize(size, version)))

	return
}

func (ev *EcVolume) FindNeedleFromEcx(needleId types.NeedleId) (offset types.Offset, size types.Size, err error) {

	// 如果文件已关闭，则先打开，这里涉及读锁 升级 写锁
	err = ev.tryOpenEcxFile()
	defer ev.releaseEcxFileRef() // 使用完毕后减少引用计数

	ev.ecxFileAccessLock.RLock()
	defer ev.ecxFileAccessLock.RUnlock()

	if err != nil {
		return types.Offset{}, types.TombstoneFileSize, err
	}

	return SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId, nil)
}

func (ev *EcVolume) FindNeedleFromEcxAndClose(needleId types.NeedleId) (offset types.Offset, size types.Size, err error) {
	ev.ecxFileAccessLock.RLock()
	// 如果文件已关闭，则先打开，这里涉及读锁 升级 写锁
	err = ev.tryOpenEcxFileAndClose()
	defer ev.Close()
	// 后声明RUnlock，会先执行
	defer ev.ecxFileAccessLock.RUnlock()
	if err != nil {
		return types.Offset{}, types.TombstoneFileSize, err
	}

	return SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId, nil)
}

func (ev *EcVolume) tryOpenEcxFile() (err error) {
	ev.ecxFileAccessLock.Lock()
	defer ev.ecxFileAccessLock.Unlock()
	// 增加引用计数
	atomic.AddInt64(&ev.ecxFileRefCount, 1)
	if ev.ecxFile == nil {
		indexBaseFileName := EcShardFileName(ev.Collection, ev.dirIdx, int(ev.VolumeId))
		if ev.ecxFile, err = os.OpenFile(indexBaseFileName+".ecx", os.O_RDWR, 0644); err != nil {
			// 打开失败时减少引用计数
			atomic.AddInt64(&ev.ecxFileRefCount, -1)
			return fmt.Errorf("cannot open ec volume index %s.ecx: %v", indexBaseFileName, err)
		}
	}
	ev.lastReadAt = time.Now()
	return nil
}

// releaseEcxFileRef 减少ecx文件引用计数，当计数为0时关闭文件
func (ev *EcVolume) releaseEcxFileRef() {
	ev.ecxFileAccessLock.Lock()
	defer ev.ecxFileAccessLock.Unlock()
	refCount := atomic.AddInt64(&ev.ecxFileRefCount, -1)
	// 防止引用计数小于0的情况
	if refCount <= 0 {
		ev.CloseAndNotEcxLock()
	}
}

func (ev *EcVolume) tryOpenEcxFileAndClose() (err error) {
	//if ev.ecxFile == nil {
	//ev.ecxFileAccessLock.Lock()
	//defer ev.ecxFileAccessLock.Unlock()
	if ev.ecxFile == nil {
		indexBaseFileName := EcShardFileName(ev.Collection, ev.dirIdx, int(ev.VolumeId))
		if ev.ecxFile, err = os.OpenFile(indexBaseFileName+".ecx", os.O_RDWR, 0644); err != nil {
			return fmt.Errorf("cannot open ec volume index %s.ecx: %v", indexBaseFileName, err)
		}
		ev.lastReadAt = time.Now()
	}
	return nil
	//}

	//return nil

}

func SearchNeedleFromSortedIndex(ecxFile *os.File, ecxFileSize int64, needleId types.NeedleId, processNeedleFn func(file *os.File, offset int64) error) (offset types.Offset, size types.Size, err error) {
	var key types.NeedleId
	buf := make([]byte, types.NeedleMapEntrySize)
	l, h := int64(0), ecxFileSize/types.NeedleMapEntrySize
	for l < h {
		m := (l + h) / 2
		if _, err := ecxFile.ReadAt(buf, m*types.NeedleMapEntrySize); err != nil {
			return types.Offset{}, types.TombstoneFileSize, fmt.Errorf("ecx file %d read at %d: %v", ecxFileSize, m*types.NeedleMapEntrySize, err)
		}
		key, offset, size = idx.IdxFileEntry(buf)
		if key == needleId {
			if processNeedleFn != nil {
				err = processNeedleFn(ecxFile, m*types.NeedleHeaderSize)
			}
			return
		}
		if key < needleId {
			l = m + 1
		} else {
			h = m
		}
	}

	err = NotFoundError
	return
}

func (ev *EcVolume) IsExpire() bool {
	if ev.ecxFile != nil && time.Now().After(ev.lastReadAt.Add(time.Minute*time.Duration(ev.expireTime))) {
		return true
	}
	return false
}

func (ev *EcVolume) IsTimeToDestroy() bool {
	return ev.DestroyTime > 0 && time.Now().Unix() > (int64(ev.DestroyTime)+destroyDelaySeconds)
}

// checks if all needles in the ecx file are deleted
// GetAllNeedleIds 获取EC卷中的所有needleId列表
func (ev *EcVolume) GetAllNeedleIds() ([]types.NeedleId, error) {
	// 如果文件已关闭，则先打开
	err := ev.tryOpenEcxFile()
	defer ev.releaseEcxFileRef() // 使用完毕后减少引用计数
	if err != nil {
		return nil, fmt.Errorf("failed to open ecx file: %v", err)
	}

	ev.ecxFileAccessLock.RLock()
	defer ev.ecxFileAccessLock.RUnlock()

	if ev.ecxFileSize == 0 {
		// 空文件，没有needle
		return []types.NeedleId{}, nil
	}

	// 遍历ecx文件中的所有条目，收集所有needleId
	var needleIds []types.NeedleId
	buf := make([]byte, types.NeedleMapEntrySize)
	for i := int64(0); i < ev.ecxFileSize/types.NeedleMapEntrySize; i++ {
		if _, err := ev.ecxFile.ReadAt(buf, i*types.NeedleMapEntrySize); err != nil {
			return nil, fmt.Errorf("ecx file read at %d: %v", i*types.NeedleMapEntrySize, err)
		}
		needleId, _, _ := idx.IdxFileEntry(buf)
		needleIds = append(needleIds, needleId)
	}

	return needleIds, nil
}

func (ev *EcVolume) IsAllNeedlesDeleted() (bool, error) {
	// 如果文件已关闭，则先打开
	err := ev.tryOpenEcxFile()
	defer ev.releaseEcxFileRef() // 使用完毕后减少引用计数
	if err != nil {
		return false, fmt.Errorf("failed to open ecx file: %v", err)
	}

	ev.ecxFileAccessLock.RLock()
	defer ev.ecxFileAccessLock.RUnlock()

	if ev.ecxFileSize == 0 {
		// 空文件，认为没有needle
		return true, nil
	}

	// 遍历ecx文件中的所有条目
	buf := make([]byte, types.NeedleMapEntrySize)
	for i := int64(0); i < ev.ecxFileSize/types.NeedleMapEntrySize; i++ {
		if _, err := ev.ecxFile.ReadAt(buf, i*types.NeedleMapEntrySize); err != nil {
			return false, fmt.Errorf("ecx file read at %d: %v", i*types.NeedleMapEntrySize, err)
		}
		_, offset, size := idx.IdxFileEntry(buf)
		// 检查needle是否被删除
		if !size.IsDeleted() {
			// 如果size不是已删除状态，则需要检查intervals
			intervals := ev.LocateEcShardNeedleInterval(ev.Version, offset.ToActualOffset(), types.Size(needle.GetActualSize(size, ev.Version)))

			// 检查每个interval是否都被标记为已删除
			for _, interval := range intervals {
				shardId, _ := interval.ToShardIdAndOffset(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
				// 检查本地分片
				if _, found := ev.FindEcVolumeShard(shardId); found {
					continue
				} else {
					return false, nil
					//ev.ShardLocationsLock.RLock()
					//sourceDataNodes, hasShardIdLocation := ev.ShardLocations[shardId]
					//ev.ShardLocationsLock.RUnlock()
					//// try reading directly
					//if hasShardIdLocation {
					//	_, is_deleted, err = s.ReadRemoteEcShardInterval(sourceDataNodes, needleId, ev.VolumeId, shardId, data, actualOffset)
					//	if err == nil {
					//		return
					//	}
					//	glog.V(0).Infof("clearing ec shard %d.%d locations: %v", ev.VolumeId, shardId, err)
					//}
					//// try reading by recovering from other shards
					//_, is_deleted, err = s.recoverOneRemoteEcShardInterval(needleId, ev, shardId, data, actualOffset)
					//if err == nil {
					//	return
					//}
				}
				// 如果本地分片不存在，则认为已经被删除,因为其他shard也会逐个检查
			}
		}
		// 如果size.IsDeleted()为true或者所有intervals都已检查完毕，继续检查下一个needle
	}

	// 所有needle和它们的intervals都已删除
	return true, nil
}
