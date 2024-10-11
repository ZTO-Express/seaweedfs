package weed_server

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

/*

Steps to apply erasure coding to .dat .idx files
0. ensure the volume is readonly
1. client call VolumeEcShardsGenerate to generate the .ecx and .ec00 ~ .ec13 files
2. client ask master for possible servers to hold the ec files
3. client call VolumeEcShardsCopy on above target servers to copy ec files from the source server
4. target servers report the new ec files to the master
5.   master stores vid -> [14]*DataNode
6. client checks master. If all 14 slices are ready, delete the original .idx, .idx files

*/

// VolumeEcShardsGenerate generates the .ecx and .ec00 ~ .ec13 files
func (vs *VolumeServer) VolumeEcShardsGenerate(ctx context.Context, req *volume_server_pb.VolumeEcShardsGenerateRequest) (*volume_server_pb.VolumeEcShardsGenerateResponse, error) {

	glog.V(0).Infof("VolumeEcShardsGenerate: %v", req)

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}
	baseFileName := v.DataFileName()

	if v.Collection != req.Collection {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	shouldCleanup := true
	defer func() {
		if !shouldCleanup {
			return
		}
		for i := 0; i < erasure_coding.TotalShardsCount; i++ {
			os.Remove(fmt.Sprintf("%s.ec%2d", baseFileName, i))
		}
		os.Remove(v.IndexFileName() + ".ecx")
	}()

	// write .ec00 ~ .ec13 files
	if err := erasure_coding.WriteEcFiles(baseFileName); err != nil {
		return nil, fmt.Errorf("WriteEcFiles %s: %v", baseFileName, err)
	}

	// write .ecx file
	if err := erasure_coding.WriteSortedFileFromIdx(v.IndexFileName(), ".ecx"); err != nil {
		return nil, fmt.Errorf("WriteSortedFileFromIdx %s: %v", v.IndexFileName(), err)
	}

	// write .vif files
	var destroyTime uint64
	if v.Ttl != nil {
		ttlMills := v.Ttl.ToSeconds()
		if ttlMills > 0 {
			destroyTime = uint64(time.Now().Unix()) + v.Ttl.ToSeconds() //生成ec文件开始计算失效时间
		}
	}
	volumeInfo := &volume_server_pb.VolumeInfo{Version: uint32(v.Version())}
	if destroyTime == 0 {
		glog.Warningf("gen ec volume,cal ec volume destory time fail,set time to 0,ttl:%v", v.Ttl)
	} else {
		volumeInfo.DestroyTime = destroyTime
	}
	if err := volume_info.SaveVolumeInfo(baseFileName+".vif", volumeInfo); err != nil {
		return nil, fmt.Errorf("SaveVolumeInfo %s: %v", baseFileName, err)
	}

	shouldCleanup = false

	return &volume_server_pb.VolumeEcShardsGenerateResponse{
		SourceDiskType: v.DiskType().String(),
	}, nil
}

// VolumeEcShardsRebuild generates the any of the missing .ec00 ~ .ec13 files
func (vs *VolumeServer) VolumeEcShardsRebuild(ctx context.Context, req *volume_server_pb.VolumeEcShardsRebuildRequest) (*volume_server_pb.VolumeEcShardsRebuildResponse, error) {

	glog.V(0).Infof("VolumeEcShardsRebuild: %v", req)

	baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))

	var rebuiltShardIds []uint32

	for _, location := range vs.store.Locations {
		_, _, existingShardCount, err := checkEcVolumeStatus(baseFileName, location)
		if err != nil {
			return nil, err
		}

		if existingShardCount == 0 {
			continue
		}

		if util.FileExists(path.Join(location.IdxDirectory, baseFileName+".ecx")) {
			// write .ec00 ~ .ec13 files
			dataBaseFileName := path.Join(location.Directory, baseFileName)
			if generatedShardIds, err := erasure_coding.RebuildEcFiles(dataBaseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcFiles %s: %v", dataBaseFileName, err)
			} else {
				rebuiltShardIds = generatedShardIds
			}

			indexBaseFileName := path.Join(location.IdxDirectory, baseFileName)
			if err := erasure_coding.RebuildEcxFile(indexBaseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcxFile %s: %v", dataBaseFileName, err)
			}

			break
		}
	}

	return &volume_server_pb.VolumeEcShardsRebuildResponse{
		RebuiltShardIds: rebuiltShardIds,
	}, nil
}

// VolumeEcShardsCopy copy the .ecx and some ec data slices
func (vs *VolumeServer) VolumeEcShardsCopy(ctx context.Context, req *volume_server_pb.VolumeEcShardsCopyRequest) (*volume_server_pb.VolumeEcShardsCopyResponse, error) {

	glog.V(0).Infof("VolumeEcShardsCopy: %v", req)

	location := vs.store.FindFreeLocation(types.HardDriveType)
	if location == nil {
		return nil, fmt.Errorf("no space left")
	}

	dataBaseFileName := storage.VolumeFileName(location.Directory, req.Collection, int(req.VolumeId))
	indexBaseFileName := storage.VolumeFileName(location.IdxDirectory, req.Collection, int(req.VolumeId))

	err := operation.WithVolumeServerClient(true, pb.ServerAddress(req.SourceDataNode), vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy ec data slices
		for _, shardId := range req.ShardIds {
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, dataBaseFileName, erasure_coding.ToExt(int(shardId)), false, false, nil); err != nil {
				return err
			}
		}

		if req.CopyEcxFile {

			// copy ecx file
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, indexBaseFileName, ".ecx", false, false, nil); err != nil {
				return err
			}
		}

		if req.CopyEcjFile {
			// copy ecj file
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, indexBaseFileName, ".ecj", true, true, nil); err != nil {
				return err
			}
		}

		if req.CopyVifFile {
			// copy vif file
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, dataBaseFileName, ".vif", false, true, nil); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("VolumeEcShardsCopy volume %d: %v", req.VolumeId, err)
	}

	return &volume_server_pb.VolumeEcShardsCopyResponse{}, nil
}

// VolumeEcShardsDelete local delete the .ecx and some ec data slices if not needed
// the shard should not be mounted before calling this.
func (vs *VolumeServer) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {

	bName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))

	glog.V(0).Infof("ec volume %s shard delete %v", bName, req.ShardIds)

	for _, location := range vs.store.Locations {
		if _, err := deleteEcShardIdsForEachLocation(bName, location, req.ShardIds, req.Soft); err != nil {
			glog.Errorf("deleteEcShards from %s %s.%v: %v", location.Directory, bName, req.ShardIds, err)
			return nil, err
		}
		glog.V(0).Infof("ec volume %s shard delete %v, directory:%s", bName, req.ShardIds, location.Directory)
	}

	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

func deleteAllEcShardIdsForEachLocation(bName string, location *storage.DiskLocation, shardIds []uint32, soft bool) error {
	if len(shardIds) == 0 {
		// delete all ec shards with bName
		for i := 0; i < erasure_coding.TotalShardsCount; i++ {
			shardIds = append(shardIds, uint32(i))
		}
	}

	deleted, err := deleteEcShardIdsForEachLocation(bName, location, shardIds, soft)
	if err != nil {
		return err
	}
	if len(deleted) == 0 {
		glog.V(0).Infof("ec volume %s no shards found in directory:%s", bName, location.Directory)
	} else {
		glog.V(0).Infof("ec volume %s shard deleted %v, directory:%s", bName, deleted, location.Directory)
	}
	return nil
}

func deleteEcShardIdsForEachLocation(bName string, location *storage.DiskLocation, shardIds []uint32, soft bool) ([]int, error) {

	deletedShards := make([]int, 0)

	indexBaseFilename := path.Join(location.IdxDirectory, bName)
	dataBaseFilename := path.Join(location.Directory, bName)

	//if util.FileExists(path.Join(location.IdxDirectory, bName+".ecx")) {
	for _, shardId := range shardIds {
		shardFileName := dataBaseFilename + erasure_coding.ToExt(int(shardId))
		if util.FileExists(shardFileName) {
			if soft {
				err := erasure_coding.MoveFile(shardFileName, erasure_coding.GetSoftDeleteDir(shardFileName))
				return deletedShards, err
			} else {
				os.Remove(shardFileName)
			}
			deletedShards = append(deletedShards, int(shardId))
		}
	}
	//}

	if len(deletedShards) == 0 {
		return nil, nil
	}

	hasEcxFile, hasIdxFile, existingShardCount, err := checkEcVolumeStatus(bName, location)
	if err != nil {
		return deletedShards, err
	}

	if hasEcxFile && existingShardCount == 0 {
		if soft {
			if err := erasure_coding.MoveFile(indexBaseFilename+".ecx", erasure_coding.GetSoftDeleteDir(indexBaseFilename+".ecx")); err != nil {
				return deletedShards, err
			}
			err = erasure_coding.MoveFile(indexBaseFilename+".ecj", erasure_coding.GetSoftDeleteDir(indexBaseFilename+".ecj"))
			if err != nil {
				glog.Errorf("move [%s] ecj file err:%v", bName, err)
			}

			if !hasIdxFile {
				// .vif is used for ec volumes and normal volumes
				err = erasure_coding.MoveFile(indexBaseFilename+".vif", erasure_coding.GetSoftDeleteDir(indexBaseFilename+".vif"))
				if err != nil {
					glog.Errorf("move [%s] vif file err:%v", bName, err)
				}
			}
			return deletedShards, nil
		}
		if err := os.Remove(indexBaseFilename + ".ecx"); err != nil {
			return deletedShards, err
		}
		os.Remove(indexBaseFilename + ".ecj")

		if !hasIdxFile {
			// .vif is used for ec volumes and normal volumes
			os.Remove(dataBaseFilename + ".vif")
		}
	}

	return deletedShards, nil
}

func checkEcVolumeStatus(bName string, location *storage.DiskLocation) (hasEcxFile bool, hasIdxFile bool, existingShardCount int, err error) {
	// check whether to delete the .ecx and .ecj file also
	fileInfos, err := os.ReadDir(location.Directory)
	if err != nil {
		return false, false, 0, err
	}
	if location.IdxDirectory != location.Directory {
		idxFileInfos, err := os.ReadDir(location.IdxDirectory)
		if err != nil {
			return false, false, 0, err
		}
		fileInfos = append(fileInfos, idxFileInfos...)
	}
	for _, fileInfo := range fileInfos {
		if fileInfo.Name() == bName+".ecx" || fileInfo.Name() == bName+".ecj" {
			hasEcxFile = true
			continue
		}
		if fileInfo.Name() == bName+".idx" {
			hasIdxFile = true
			continue
		}
		if strings.HasPrefix(fileInfo.Name(), bName+".ec") {
			existingShardCount++
		}
	}
	return hasEcxFile, hasIdxFile, existingShardCount, nil
}

func (vs *VolumeServer) VolumeEcShardsMount(ctx context.Context, req *volume_server_pb.VolumeEcShardsMountRequest) (*volume_server_pb.VolumeEcShardsMountResponse, error) {

	glog.V(0).Infof("VolumeEcShardsMount: %v", req)

	for _, shardId := range req.ShardIds {
		err := vs.store.MountEcShards(req.Collection, needle.VolumeId(req.VolumeId), erasure_coding.ShardId(shardId))

		if err != nil {
			glog.Errorf("ec shard mount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard mount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("mount %d.%d: %v", req.VolumeId, shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {

	glog.V(0).Infof("VolumeEcShardsUnmount: %v", req)

	for _, shardId := range req.ShardIds {
		err := vs.store.UnmountEcShards(needle.VolumeId(req.VolumeId), erasure_coding.ShardId(shardId))

		if err != nil {
			glog.Errorf("ec shard unmount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard unmount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("unmount %d.%d: %v", req.VolumeId, shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {

	ecVolume, found := vs.store.FindEcVolume(needle.VolumeId(req.VolumeId))
	if !found {
		return fmt.Errorf("VolumeEcShardRead not found ec volume id %d", req.VolumeId)
	}
	ecShard, found := ecVolume.FindEcVolumeShard(erasure_coding.ShardId(req.ShardId))
	if !found {
		return fmt.Errorf("not found ec shard %d.%d", req.VolumeId, req.ShardId)
	}

	if req.FileKey != 0 {
		_, size, _ := ecVolume.FindNeedleFromEcx(types.Uint64ToNeedleId(req.FileKey))
		if size.IsDeleted() {
			return stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				IsDeleted: true,
			})
		}
	}

	bufSize := req.Size
	if bufSize > BufferSizeLimit {
		bufSize = BufferSizeLimit
	}
	buffer := make([]byte, bufSize)

	startOffset, bytesToRead := req.Offset, req.Size

	for bytesToRead > 0 {
		// min of bytesToRead and bufSize
		bufferSize := bufSize
		if bufferSize > bytesToRead {
			bufferSize = bytesToRead
		}
		bytesread, err := ecShard.ReadAt(buffer[0:bufferSize], startOffset)

		// println("read", ecShard.FileName(), "startOffset", startOffset, bytesread, "bytes, with target", bufferSize)
		if bytesread > 0 {

			if int64(bytesread) > bytesToRead {
				bytesread = int(bytesToRead)
			}
			err = stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				Data: buffer[:bytesread],
			})
			if err != nil {
				// println("sending", bytesread, "bytes err", err.Error())
				return err
			}

			startOffset += int64(bytesread)
			bytesToRead -= int64(bytesread)

		}

		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}

	}

	return nil

}

func (vs *VolumeServer) VolumeEcBlobDelete(ctx context.Context, req *volume_server_pb.VolumeEcBlobDeleteRequest) (*volume_server_pb.VolumeEcBlobDeleteResponse, error) {

	glog.V(0).Infof("VolumeEcBlobDelete: %v", req)

	resp := &volume_server_pb.VolumeEcBlobDeleteResponse{}

	for _, location := range vs.store.Locations {
		if localEcVolume, found := location.FindEcVolume(needle.VolumeId(req.VolumeId)); found {

			_, size, _, err := localEcVolume.LocateEcShardNeedle(types.NeedleId(req.FileKey), needle.Version(req.Version))
			if err != nil {
				return nil, fmt.Errorf("locate in local ec volume: %v", err)
			}
			if size.IsDeleted() {
				return resp, nil
			}

			err = localEcVolume.DeleteNeedleFromEcx(types.NeedleId(req.FileKey))
			if err != nil {
				return nil, err
			}

			break
		}
	}

	return resp, nil
}

// VolumeEcShardsToVolume generates the .idx, .dat files from .ecx, .ecj and .ec01 ~ .ec14 files
func (vs *VolumeServer) VolumeEcShardsToVolume(ctx context.Context, req *volume_server_pb.VolumeEcShardsToVolumeRequest) (*volume_server_pb.VolumeEcShardsToVolumeResponse, error) {

	glog.V(0).Infof("VolumeEcShardsToVolume: %v", req)

	// collect .ec00 ~ .ec09 files
	shardFileNames := make([]string, erasure_coding.DataShardsCount)
	v, found := vs.store.CollectEcShards(needle.VolumeId(req.VolumeId), shardFileNames)
	if !found {
		return nil, fmt.Errorf("ec volume %d not found", req.VolumeId)
	}

	if v.Collection != req.Collection {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	for shardId := 0; shardId < erasure_coding.DataShardsCount; shardId++ {
		if shardFileNames[shardId] == "" {
			return nil, fmt.Errorf("ec volume %d missing shard %d", req.VolumeId, shardId)
		}
	}

	dataBaseFileName, indexBaseFileName := v.DataBaseFileName(), v.IndexBaseFileName()
	// calculate .dat file size
	datFileSize, err := erasure_coding.FindDatFileSize(dataBaseFileName, indexBaseFileName)
	if err != nil {
		return nil, fmt.Errorf("FindDatFileSize %s: %v", dataBaseFileName, err)
	}

	// write .dat file from .ec00 ~ .ec09 files
	if err := erasure_coding.WriteDatFile(dataBaseFileName, datFileSize, shardFileNames); err != nil {
		return nil, fmt.Errorf("WriteDatFile %s: %v", dataBaseFileName, err)
	}

	// write .idx file from .ecx and .ecj files
	if err := erasure_coding.WriteIdxFileFromEcIndex(indexBaseFileName); err != nil {
		return nil, fmt.Errorf("WriteIdxFileFromEcIndex %s: %v", v.IndexBaseFileName(), err)
	}

	return &volume_server_pb.VolumeEcShardsToVolumeResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardsMove(ctx context.Context, req *volume_server_pb.VolumeEcShardsMoveRequest) (*volume_server_pb.VolumeEcShardsMoveResponse, error) {
	glog.V(0).Infof("VolumeEcShardsMove: %v", req)

	location := vs.store.FindFreeLocation(types.HardDriveType)
	if location == nil {
		return nil, fmt.Errorf("no space left")
	}

	dataBaseFileName := storage.VolumeFileName(location.Directory, req.Collection, int(req.VolumeId))
	indexBaseFileName := storage.VolumeFileName(location.IdxDirectory, req.Collection, int(req.VolumeId))

	var fileName string
	for _, shardId := range req.ShardIds {
		baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId)) + erasure_coding.ToExt(int(shardId))
		for _, location := range vs.store.Locations {
			tName := util.Join(location.Directory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
			tName = util.Join(location.IdxDirectory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
		}
		if fileName == "" {
			glog.V(3).Infof("CopyFile not found ec volume id %d %d", req.VolumeId, shardId)
			continue
		}
		glog.V(3).Infof("move ec data %s -> %s", fileName, dataBaseFileName+erasure_coding.ToExt(int(shardId)))
		if err := erasure_coding.MoveFile(fileName, dataBaseFileName+erasure_coding.ToExt(int(shardId))); err != nil {
			glog.V(3).Infof("CopyFile not found ec volume id %v", err)
		}
		fileName = ""
	}

	fileName = ""
	if req.CopyEcxFile {
		baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId)) + ".ecx"
		for _, location := range vs.store.Locations {
			tName := util.Join(location.Directory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
			tName = util.Join(location.IdxDirectory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
		}
		if fileName == "" {
			glog.V(3).Infof("CopyFile ecx not found ec volume id %d", req.VolumeId)
		} else {
			if err := copyFile(fileName, indexBaseFileName+".ecx"); err != nil {
				glog.V(3).Infof("copy ecx %s -> %s", fileName, indexBaseFileName+".ecx")
			}
		}
	}
	fileName = ""
	if req.CopyEcjFile {
		baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId)) + ".ecj"
		for _, location := range vs.store.Locations {
			tName := util.Join(location.Directory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
			tName = util.Join(location.IdxDirectory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
		}
		if fileName == "" {
			glog.V(3).Infof("CopyFile ecj not found ec volume id %d %d", req.VolumeId)
		} else {
			if err := copyFile(fileName, indexBaseFileName+".ecj"); err != nil {
				glog.V(3).Infof("move ecj %s -> %s", fileName, indexBaseFileName+".ecj")
			}
		}
	}
	fileName = ""
	if req.CopyVifFile {
		baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId)) + ".vif"
		for _, location := range vs.store.Locations {
			tName := util.Join(location.Directory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
			tName = util.Join(location.IdxDirectory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
		}
		if fileName == "" {
			glog.V(3).Infof("CopyFile vif not found ec volume id %d %d", req.VolumeId)
		} else {
			if err := copyFile(fileName, dataBaseFileName+".vif"); err != nil {
				glog.V(3).Infof("move vif %s -> %s", fileName, indexBaseFileName+".ecj")
			}
		}
	}

	return &volume_server_pb.VolumeEcShardsMoveResponse{}, nil
}

//func moveFile(sourcePath, destPath string) error {
//	inputFile, err := os.Open(sourcePath)
//	if err != nil {
//		return fmt.Errorf("Couldn't open source file: %s", err)
//	}
//	outputFile, err := os.Create(destPath)
//	if err != nil {
//		inputFile.Close()
//		return fmt.Errorf("Couldn't open dest file: %s", err)
//	}
//	defer outputFile.Close()
//	_, err = io.Copy(outputFile, inputFile)
//	inputFile.Close()
//	if err != nil {
//		return fmt.Errorf("Writing to output file failed: %s", err)
//	}
//	// The copy was successful, so now delete the original file
//	err = os.Remove(sourcePath)
//	if err != nil {
//		return fmt.Errorf("Failed removing original file: %s", err)
//	}
//	return nil
//}

func copyFile(sourcePath, destPath string) error {
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("Couldn't open source file: %s", err)
	}
	outputFile, err := os.Create(destPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("Couldn't open dest file: %s", err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("Writing to output file failed: %s", err)
	}
	return err
}
