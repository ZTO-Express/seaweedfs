package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	master           = flag.String("master", "127.0.0.1:9333", "the master server")
	repeat           = flag.Int("n", 5, "repeat how many times")
	garbageThreshold = flag.Float64("garbageThreshold", 0.3, "garbageThreshold")
	replication      = flag.String("replication", "", "replication 000, 001, 002, etc")
)

func main() {
	flag.Parse()

	util.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	genFile(grpcDialOption, 0)

	go func() {
		for {
			println("vacuum threshold", *garbageThreshold)
			_, _, err := util.Get(fmt.Sprintf("http://%s/vol/vacuum?garbageThreshold=%f", pb.ServerAddress(*master).ToHttpAddress(), *garbageThreshold))
			if err != nil {
				log.Fatalf("vacuum: %v", err)
			}
			time.Sleep(time.Second)
		}
	}()

	for i := 0; i < *repeat; i++ {
		// create 2 files, and delete one of them

		assignResult, targetUrl := genFile(grpcDialOption, i)

		util.Delete(targetUrl, string(assignResult.Auth), "")

	}

}

func genFile(grpcDialOption grpc.DialOption, i int) (*operation.AssignResult, string) {
	assignResult, err := operation.Assign(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(*master) }, grpcDialOption, &operation.VolumeAssignRequest{
		Count:       1,
		Replication: *replication,
	})
	if err != nil {
		log.Fatalf("assign: %v", err)
	}

	data := make([]byte, 1024)
	rand.Read(data)

	targetUrl := fmt.Sprintf("http://%s/%s", assignResult.Url, assignResult.Fid)

	uploadOption := &operation.UploadOption{
		UploadUrl:         targetUrl,
		Filename:          fmt.Sprintf("test%d", i),
		Cipher:            false,
		IsInputCompressed: true,
		MimeType:          "bench/test",
		PairMap:           nil,
		Jwt:               assignResult.Auth,
	}
	_, err = operation.UploadData(data, uploadOption)
	if err != nil {
		log.Fatalf("upload: %v", err)
	}
	return assignResult, targetUrl
}
