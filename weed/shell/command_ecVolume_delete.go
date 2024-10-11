package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
	"io"
	"strconv"
	"strings"
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

	ecVolume.delete -collection <collection> -volumeId <volume id>  -node <volume server host:port>

	This command deletes a ec volume from volume server.

`
}

func (c *commandEcVolumeDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volDeleteCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volDeleteCommand.Int("volumeId", 0, "the volume id")
	collectionStr := volDeleteCommand.String("collection", "", "the collection name")
	nodeStr := volDeleteCommand.String("node", "", "optional, specify the volume server <host>:<port> to delete")
	shards := volDeleteCommand.String("shards", "", "optional, specify the ec shards to delete")
	if err = volDeleteCommand.Parse(args); err != nil {
		return nil
	}

	volumeId := *volumeIdInt
	collection := *collectionStr
	if collection == "" || volumeId == 0 {
		return fmt.Errorf("collection and volumeId are required")
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

	var specifyShardsToDelete []uint32
	if *shards != "" {
		shardIds := strings.Split(*shards, ",")
		for _, shard := range shardIds {
			id, err := strconv.Atoi(shard)
			if err != nil {
				fmt.Printf("shard id is invaild:%s", shard)
				continue
			}
			specifyShardsToDelete = append(specifyShardsToDelete, uint32(id))
		}
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	for _, sourceVolumeServer := range sourceVolumeServerList {
		err = deleteEcVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(volumeId), collection, sourceVolumeServer, specifyShardsToDelete)
		if err != nil {
			//delete error, interrupt
			return err
		}
		fmt.Printf("deleteEcVolume %d from %s success\n", volumeId, sourceVolumeServer.String())
	}
	fmt.Printf("deleteEcVolume %d finish \n", volumeId)

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

func deleteEcVolume(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, collection string, sourceVolumeServer pb.ServerAddress, specifyShardsToDelete []uint32) (err error) {
	return operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.EcVolumeDelete(context.Background(), &volume_server_pb.EcVolumeDeleteRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   specifyShardsToDelete,
			Soft:       true, //软删除
		})
		return deleteErr
	})
}
