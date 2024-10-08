package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
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
	nodeStr := volDeleteCommand.String("node", "", "optional, the volume server <host>:<port>")
	shardIds := volDeleteCommand.String("shards", "", "optional, ec shards to delete, delete all shards if not specify")
	if err = volDeleteCommand.Parse(args); err != nil {
		return nil
	}

	volumeId := *volumeIdInt
	collection := *collectionStr
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
		return fmt.Errorf("no ec shards to delete")
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	var sourceVolumeServerList []pb.ServerAddress
	if nodeStr == nil || *nodeStr == "" {
		// if don't specify volume node,delete the volumeId from all volume nodes
		var resp *master_pb.LookupVolumeResponse
		err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
			resp, err = client.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{
				VolumeOrFileIds: []string{strconv.Itoa(volumeId)},
				Collection:      collection,
			})
			if err != nil {
				return fmt.Errorf("LookupVolume from server,volumeId:%d, error: %v", volumeIdInt, err)
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
	} else {
		// delete from the specify volume node
		sourceVolumeServerList = append(sourceVolumeServerList, pb.ServerAddress(*nodeStr))
	}

	for _, sourceVolumeServer := range sourceVolumeServerList {
		err = sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, needle.VolumeId(volumeId), sourceVolumeServer, ecShards)
		if err != nil {
			//delete error, interrupt
			return err
		}
	}

	return nil
}
