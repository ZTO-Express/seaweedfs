package erasure_coding

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type ShardId uint8

type EcVolumeShard struct {
	VolumeId          needle.VolumeId
	ShardId           ShardId
	Collection        string
	dir               string
	ecdFile           *os.File
	ecdFileAccessLock sync.RWMutex
	ecdFileSize       int64
	DiskType          types.DiskType
}

func NewEcVolumeShard(diskType types.DiskType, dirname string, collection string, id needle.VolumeId, shardId ShardId) (v *EcVolumeShard, e error) {

	v = &EcVolumeShard{dir: dirname, Collection: collection, VolumeId: id, ShardId: shardId, DiskType: diskType}

	baseFileName := v.FileName()

	// open ecd file
	if v.ecdFile, e = os.OpenFile(baseFileName+ToExt(int(shardId)), os.O_RDONLY, 0644); e != nil {
		if e == os.ErrNotExist || strings.Contains(e.Error(), "no such file or directory") || strings.Contains(e.Error(), "The system cannot find the file specified") {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("cannot read ec volume shard %s%s: %v", baseFileName, ToExt(int(shardId)), e)
	}
	ecdFi, statErr := v.ecdFile.Stat()
	if statErr != nil {
		_ = v.ecdFile.Close()
		return nil, fmt.Errorf("can not stat ec volume shard %s%s: %v", baseFileName, ToExt(int(shardId)), statErr)
	}
	v.ecdFileSize = ecdFi.Size()

	stats.VolumeServerVolumeGauge.WithLabelValues(v.Collection, "ec_shards").Inc()

	return
}

func (shard *EcVolumeShard) Size() int64 {
	return shard.ecdFileSize
}

func (shard *EcVolumeShard) String() string {
	return fmt.Sprintf("ec shard %v:%v, dir:%s, Collection:%s", shard.VolumeId, shard.ShardId, shard.dir, shard.Collection)
}

func (shard *EcVolumeShard) FileName() (fileName string) {
	return EcShardFileName(shard.Collection, shard.dir, int(shard.VolumeId))
}

func EcShardFileName(collection string, dir string, id int) (fileName string) {
	idString := strconv.Itoa(id)
	if collection == "" {
		fileName = path.Join(dir, idString)
	} else {
		fileName = path.Join(dir, collection+"_"+idString)
	}
	return
}

func EcShardBaseFileName(collection string, id int) (baseFileName string) {
	baseFileName = strconv.Itoa(id)
	if collection != "" {
		baseFileName = collection + "_" + baseFileName
	}
	return
}

func (shard *EcVolumeShard) Close() {
	shard.ecdFileAccessLock.Lock()
	defer shard.ecdFileAccessLock.Unlock()

	if shard.ecdFile != nil {
		_ = shard.ecdFile.Close()
		shard.ecdFile = nil
	}
}

func (shard *EcVolumeShard) Destroy(soft bool) error {
	shardFilename := shard.FileName() + ToExt(int(shard.ShardId))
	if soft {
		return MoveFile(shardFilename, GetSoftDeleteDir(shardFilename))
	}
	err := os.Remove(shardFilename)
	stats.VolumeServerVolumeGauge.WithLabelValues(shard.Collection, "ec_shards").Dec()
	return err
}

func (shard *EcVolumeShard) ReadAt(buf []byte, offset int64) (int, error) {
	if shard.ecdFile == nil {
		err := shard.tryOpenEcdFile()
		if err != nil {
			return 0, err
		}
	}

	shard.ecdFileAccessLock.RLock()
	defer shard.ecdFileAccessLock.RUnlock()

	return shard.ecdFile.ReadAt(buf, offset)
}

func (shard *EcVolumeShard) tryOpenEcdFile() (err error) {
	if shard.ecdFile == nil {
		shard.ecdFileAccessLock.Lock()
		defer shard.ecdFileAccessLock.Unlock()
		if shard.ecdFile == nil {
			baseFileName := shard.FileName()
			if shard.ecdFile, err = os.OpenFile(baseFileName+ToExt(int(shard.ShardId)), os.O_RDONLY, 0644); err != nil {
				if err == os.ErrNotExist || strings.Contains(err.Error(), "no such file or directory") {
					return err
				}
				return fmt.Errorf("cannot read ec volume shard %s%s: %v", baseFileName, ToExt(int(shard.ShardId)), err)
			}
		}
	}
	return
}

func GetSoftDeleteDir(sourceFile string) string {
	if strings.HasPrefix(sourceFile, "/") {
		sourceFile = sourceFile[1:]
	}
	sourceFile = strings.ReplaceAll(sourceFile, "/", ":")
	return os.TempDir() + "/backup/" + sourceFile
}

func MoveFile(sourcePath, destPath string) error {
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("couldn't open source file: %s", err)
	}
	destDir := filepath.Dir(destPath)

	// 创建所有必要的父级目录
	err = os.MkdirAll(destDir, os.ModePerm)
	if err != nil {
		return err
	}
	outputFile, err := os.Create(destPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("couldn't open dest file: %s", err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("writing to output file failed: %s", err)
	}
	// The copy was successful, so now delete the original file
	err = os.Remove(sourcePath)
	if err != nil {
		return fmt.Errorf("failed removing original file: %s", err)
	}
	return nil
}
