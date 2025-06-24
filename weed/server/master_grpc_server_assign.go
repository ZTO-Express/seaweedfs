package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"time"

	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func (ms *MasterServer) StreamAssign(server master_pb.Seaweed_StreamAssignServer) error {
	for {
		req, err := server.Recv()
		if err != nil {
			glog.Errorf("StreamAssign failed to receive: %v", err)
			return err
		}
		resp, err := ms.Assign(context.Background(), req)
		if err != nil {
			glog.Errorf("StreamAssign failed to assign: %v", err)
			return err
		}
		if err = server.Send(resp); err != nil {
			glog.Errorf("StreamAssign failed to send: %v", err)
			return err
		}
	}
}
func (ms *MasterServer) Assign(ctx context.Context, req *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {

	if !ms.Topo.IsLeader() {
		return nil, raft.NotLeaderError
	}

	if req.Count == 0 {
		req.Count = 1
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
	diskType := types.ToDiskType(req.DiskType)

	option := &topology.VolumeGrowOption{
		Collection:         req.Collection,
		ReplicaPlacement:   replicaPlacement,
		Ttl:                ttl,
		DiskType:           diskType,
		Preallocate:        ms.preallocateSize,
		DataCenter:         req.DataCenter,
		Rack:               req.Rack,
		DataNode:           req.DataNode,
		MemoryMapMaxSizeMb: req.MemoryMapMaxSizeMb,
	}

	if !ms.Topo.DataCenterExists(option.DataCenter) {
		return nil, fmt.Errorf("data center %v not found in topology", option.DataCenter)
	}

	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)

	var (
		lastErr    error
		maxTimeout = time.Second * 10
		startTime  = time.Now()
	)

	for time.Now().Sub(startTime) < maxTimeout {
		glog.V(2).Infof("Assign: attempting to pick volume for write, elapsed: %v, option: %+v", time.Now().Sub(startTime), option)
		fid, count, dnList, shouldGrow, err := ms.Topo.PickForWrite(req.Count, option, vl, false)
		glog.V(2).Infof("Assign: PickForWrite result - fid: %s, count: %d, shouldGrow: %v, err: %v", fid, count, shouldGrow, err)

		if shouldGrow && !vl.HasGrowRequest() {
			// if picked volume is almost full, trigger a volume-grow request
			glog.V(1).Infof("Assign: volume needs to grow, checking available space for option: %+v", option)
			availableSpace := ms.Topo.AvailableSpaceFor(option)
			glog.V(1).Infof("Assign: available space: %d", availableSpace)

			if availableSpace <= 0 {
				glog.Errorf("Assign: No more writable volumes! No free volumes left for %s, available space: %d", option.String(), availableSpace)
				return nil, fmt.Errorf("no free volumes left for " + option.String())
			}

			vl.AddGrowRequest()
			growRequest := &topology.VolumeGrowRequest{
				Option: option,
				Count:  int(req.WritableVolumeCount),
			}
			glog.V(1).Infof("Assign: sending volume grow request: %+v", growRequest)
			ms.volumeGrowthRequestChan <- growRequest
			glog.V(1).Infof("Assign: volume grow request sent successfully")
		}
		if err != nil {
			glog.V(2).Infof("Assign: PickForWrite failed, will retry: %v", err)
			// glog.Warningf("PickForWrite %+v: %v", req, err)
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		dn := dnList.Head()
		if dn == nil {
			glog.V(2).Infof("Assign: no data node available, will retry")
			continue
		}
		var replicas []*master_pb.Location
		for _, r := range dnList.Rest() {
			replicas = append(replicas, &master_pb.Location{
				Url:        r.Url(),
				PublicUrl:  r.PublicUrl,
				GrpcPort:   uint32(r.GrpcPort),
				DataCenter: r.GetDataCenterId(),
			})
		}
		return &master_pb.AssignResponse{
			Fid: fid,
			Location: &master_pb.Location{
				Url:        dn.Url(),
				PublicUrl:  dn.PublicUrl,
				GrpcPort:   uint32(dn.GrpcPort),
				DataCenter: dn.GetDataCenterId(),
			},
			Count:    count,
			Auth:     string(security.GenJwtForVolumeServer(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, fid)),
			Replicas: replicas,
		}, nil
	}
	return nil, lastErr
}
