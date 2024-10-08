package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"google.golang.org/grpc"
	"io"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandEcVolumeDelete{})
}

type commandEcVolumeDelete struct {
}

func (c *commandEcVolumeDelete) Name() string {
	return "ecVolume.delete"
}

func (c *commandEcVolumeDelete) Help() string {
	return `delete a live ec volume from volume server

	ecVolume.delete -node <volume server host:port> -volumeId <volume id> -collection <collection>

	This command deletes a ec volume from volume server,.

`
}

func (c *commandEcVolumeDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volDeleteCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volDeleteCommand.Int("volumeId", 0, "the volume id")
	collectionStr := volDeleteCommand.String("collection", "", "the collection name")
	nodeStr := volDeleteCommand.String("node", "", "optional, specify the volume server <host>:<port> to delete")
	shardIds := volDeleteCommand.String("shards", "", "optional, ec shards to delete, delete all shards if not specify")
	if err = volDeleteCommand.Parse(args); err != nil {
		return nil
	}

	volumeId := *volumeIdInt
	collection := *collectionStr
	if collection == "" || volumeId == 0 {
		return fmt.Errorf("collection and volumeId are required")
	}

	var ecShards []uint32
	if *shardIds == "" {
		// try to delete all shards
		for i := 0; i < erasure_coding.TotalShardsCount; i++ {
			ecShards = append(ecShards, uint32(i))
		}
	} else {
		//delete specify ec shards
		for _, shard := range strings.Split(*shardIds, ",") {
			shardId, err := strconv.Atoi(shard)
			if err != nil {
				fmt.Printf("invalid shard id %v\n", shard)
				continue
			}
			ecShards = append(ecShards, uint32(shardId))
		}
	}

	//check
	if len(ecShards) == 0 {
		return fmt.Errorf("collection:%s, volumeId:%d, no ec shards to delete", collection, volumeId)
	}

	var sourceVolumeServerList []pb.ServerAddress
	if nodeStr == nil || *nodeStr == "" {
		// if don't specify volume node,delete the volumeId from all volume locations
		sourceVolumeServerList, err = findEcVolumeLocations(commandEnv, volumeId, collection)
		if err != nil {
			return err
		}
	} else {
		// delete from the specify volume node
		sourceVolumeServerList = append(sourceVolumeServerList, pb.ServerAddress(*nodeStr))
	}

	if len(sourceVolumeServerList) == 0 {
		return fmt.Errorf("collection:%s, volumeId:%d, no volume server found", collection, volumeId)
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	for _, sourceVolumeServer := range sourceVolumeServerList {
		err = deleteEcVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(volumeId), sourceVolumeServer)
		if err != nil {
			//delete error, interrupt
			return err
		}
	}

	return nil
}

func findEcVolumeLocations(commandEnv *CommandEnv, volumeId int, collection string) ([]pb.ServerAddress, error) {
	var err error
	var sourceVolumeServerList []pb.ServerAddress

	var resp *master_pb.LookupVolumeResponse
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err = client.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{
			VolumeOrFileIds: []string{strconv.Itoa(volumeId)},
			Collection:      collection,
		})
		if err != nil {
			return fmt.Errorf("LookupVolume from server,volumeId:%d, error: %v", volumeId, err)
		}
		return nil
	})
	if err == nil && resp != nil {
		//ec shards node deduplication
		deduplication := map[string]struct{}{}
		for _, loc := range resp.VolumeIdLocations {
			for _, l := range loc.Locations {
				url := l.Url
				if _, ok := deduplication[url]; !ok {
					deduplication[url] = struct{}{}
					sourceVolumeServerList = append(sourceVolumeServerList, pb.ServerAddress(url))
				}
			}
		}
	}
	return sourceVolumeServerList, err
}

func deleteEcVolume(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer pb.ServerAddress) (err error) {
	return operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
			VolumeId:   uint32(volumeId),
			OnlyEmpty:  false,
			IsEcVolume: true, //ec volume delete
		})
		return deleteErr
	})
}
