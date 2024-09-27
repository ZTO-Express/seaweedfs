package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"io"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeDelete{})
}

type commandVolumeDelete struct {
}

func (c *commandVolumeDelete) Name() string {
	return "volume.delete"
}

func (c *commandVolumeDelete) Help() string {
	return `delete a live volume from one volume server

	volume.delete -node <volume server host:port> -volumeId <volume id>

	This command deletes a volume from one volume server.

`
}

func (c *commandVolumeDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volDeleteCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volDeleteCommand.Int("volumeId", 0, "the volume id")
	nodeStr := volDeleteCommand.String("node", "", "the volume server <host>:<port>")
	if err = volDeleteCommand.Parse(args); err != nil {
		return nil
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
				VolumeOrFileIds: []string{strconv.Itoa(*volumeIdInt)},
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

	volumeId := needle.VolumeId(*volumeIdInt)
	for _, sourceVolumeServer := range sourceVolumeServerList {
		err = deleteVolume(commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer, false, true)
		if err != nil {
			//interrupt delete
			return err
		}
	}

	return nil
}
