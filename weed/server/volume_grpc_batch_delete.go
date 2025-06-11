package weed_server

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) BatchDelete(ctx context.Context, req *volume_server_pb.BatchDeleteRequest) (*volume_server_pb.BatchDeleteResponse, error) {

	resp := &volume_server_pb.BatchDeleteResponse{}

	now := uint64(time.Now().Unix())

	for _, fid := range req.FileIds {
		vid, idCookie, err := operation.ParseFileId(fid)
		if err != nil {
			resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusBadRequest,
				Error:  err.Error()})
			continue
		}

		n := new(needle.Needle)
		volumeId, _ := needle.NewVolumeId(vid)
		ecVolume, isEcVolume := vs.store.FindEcVolume(volumeId)
		if req.SkipCookieCheck {
			n.Id, _, err = needle.ParseNeedleIdCookie(idCookie)
			if err != nil {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusBadRequest,
					Error:  err.Error()})
				continue
			}
		} else {
			n.ParsePath(idCookie)
			cookie := n.Cookie
			if !isEcVolume {
				if _, err := vs.store.ReadVolumeNeedle(volumeId, n, nil, nil); err != nil {
					resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
						FileId: fid,
						Status: http.StatusNotFound,
						Error:  err.Error(),
					})
					continue
				}
			} else {
				if _, err := vs.store.ReadEcShardNeedle(volumeId, n, nil); err != nil {
					if errors.Is(err, storage.ErrorDeleted) {
						continue
					}
					resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
						FileId: fid,
						Status: http.StatusNotFound,
						Error:  err.Error(),
					})
					continue
				}
			}
			if n.Cookie != cookie {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusBadRequest,
					Error:  "File Random Cookie does not match.",
				})
				break
			}
		}

		if n.IsChunkedManifest() {
			resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusNotAcceptable,
				Error:  "ChunkManifest: not allowed in batch delete mode.",
			})
			continue
		}

		n.LastModified = now
		if !isEcVolume {
			if size, err := vs.store.DeleteVolumeNeedle(volumeId, n); err != nil {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusInternalServerError,
					Error:  err.Error()},
				)
			} else if size == 0 {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusNotModified},
				)
			} else {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusAccepted,
					Size:   uint32(size)},
				)
			}
		} else {
			if size, err := vs.store.DeleteEcShardNeedleSkipCookieCheck(ecVolume, n); err != nil {
				if errors.Is(err, storage.ErrorDeleted) {
					continue
				}
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusInternalServerError,
					Error:  err.Error()},
				)
			} else {
				resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusAccepted,
					Size:   uint32(size)},
				)
			}
		}
	}

	return resp, nil

}

// deleteTask represents a single file deletion task
type deleteTask struct {
	index int
	fid   string
}

// deleteResult represents the result of a deletion operation
type deleteResult struct {
	index  int
	result *volume_server_pb.DeleteResult
}

func (vs *VolumeServer) FastBatchDeleteEc(ctx context.Context, req *volume_server_pb.FastBatchDeleteEcRequest) (*volume_server_pb.FastBatchDeleteEcResponse, error) {
	now := uint64(time.Now().Unix())

	// If there are few files, process sequentially
	if len(req.FileIds) <= 10 {
		return vs.fastBatchDeleteEcSequential(ctx, req, now)
	}

	// Use concurrent processing for larger batches
	return vs.fastBatchDeleteEcConcurrent(ctx, req, now)
}

// Sequential processing for small batches
func (vs *VolumeServer) fastBatchDeleteEcSequential(ctx context.Context, req *volume_server_pb.FastBatchDeleteEcRequest, now uint64) (*volume_server_pb.FastBatchDeleteEcResponse, error) {
	resp := &volume_server_pb.FastBatchDeleteEcResponse{}

	for _, fid := range req.FileIds {
		result := vs.processSingleDelete(fid, req.SkipCookieCheck, now)
		if result != nil {
			resp.Results = append(resp.Results, result)
		}
	}

	return resp, nil
}

// Concurrent processing for larger batches
func (vs *VolumeServer) fastBatchDeleteEcConcurrent(ctx context.Context, req *volume_server_pb.FastBatchDeleteEcRequest, now uint64) (*volume_server_pb.FastBatchDeleteEcResponse, error) {
	resp := &volume_server_pb.FastBatchDeleteEcResponse{}

	// Determine the number of workers based on CPU cores and file count
	numWorkers := runtime.NumCPU()
	if numWorkers > len(req.FileIds) {
		numWorkers = len(req.FileIds)
	}
	if numWorkers > 20 { // Limit max workers to avoid resource exhaustion
		numWorkers = 20
	}

	// Create channels for task distribution and result collection
	taskChan := make(chan deleteTask, len(req.FileIds))
	resultChan := make(chan deleteResult, len(req.FileIds))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskChan {
				// Check context cancellation
				select {
				case <-ctx.Done():
					return
				default:
				}

				result := vs.processSingleDelete(task.fid, req.SkipCookieCheck, now)
				if result != nil {
					resultChan <- deleteResult{
						index:  task.index,
						result: result,
					}
				}
			}
		}()
	}

	// Send tasks to workers
	go func() {
		defer close(taskChan)
		for i, fid := range req.FileIds {
			select {
			case <-ctx.Done():
				return
			case taskChan <- deleteTask{index: i, fid: fid}:
			}
		}
	}()

	// Wait for workers to finish and close result channel
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results and maintain order
	results := make([]*volume_server_pb.DeleteResult, 0, len(req.FileIds))
	resultMap := make(map[int]*volume_server_pb.DeleteResult)

	for result := range resultChan {
		resultMap[result.index] = result.result
	}

	// Maintain the original order of results
	for i := 0; i < len(req.FileIds); i++ {
		if result, exists := resultMap[i]; exists {
			results = append(results, result)
		}
	}

	resp.Results = results
	return resp, nil
}

// Process a single file deletion
func (vs *VolumeServer) processSingleDelete(fid string, skipCookieCheck bool, now uint64) *volume_server_pb.DeleteResult {
	vid, idCookie, err := operation.ParseFileId(fid)
	if err != nil {
		return &volume_server_pb.DeleteResult{
			FileId: fid,
			Status: http.StatusBadRequest,
			Error:  err.Error(),
		}
	}

	n := new(needle.Needle)
	volumeId, _ := needle.NewVolumeId(vid)
	ecVolume, isEcVolume := vs.store.FindEcVolume(volumeId)

	if skipCookieCheck {
		n.Id, _, err = needle.ParseNeedleIdCookie(idCookie)
		if err != nil {
			return &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusBadRequest,
				Error:  err.Error(),
			}
		}
	} else {
		n.ParsePath(idCookie)
		cookie := n.Cookie
		if !isEcVolume {
			if _, err := vs.store.ReadVolumeNeedle(volumeId, n, nil, nil); err != nil {
				return &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusNotFound,
					Error:  err.Error(),
				}
			}
		} else {
			if _, err := vs.store.ReadEcShardNeedle(volumeId, n, nil); err != nil {
				if errors.Is(err, storage.ErrorDeleted) {
					return nil // Skip already deleted files
				}
				return &volume_server_pb.DeleteResult{
					FileId: fid,
					Status: http.StatusNotFound,
					Error:  err.Error(),
				}
			}
		}
		if n.Cookie != cookie {
			return &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusBadRequest,
				Error:  "File Random Cookie does not match.",
			}
		}
	}

	if n.IsChunkedManifest() {
		return &volume_server_pb.DeleteResult{
			FileId: fid,
			Status: http.StatusNotAcceptable,
			Error:  "ChunkManifest: not allowed in batch delete mode.",
		}
	}

	n.LastModified = now
	if !isEcVolume {
		if size, err := vs.store.DeleteVolumeNeedle(volumeId, n); err != nil {
			return &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusInternalServerError,
				Error:  err.Error(),
			}
		} else if size == 0 {
			return &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusNotModified,
			}
		} else {
			return &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusAccepted,
				Size:   uint32(size),
			}
		}
	} else {
		if size, err := vs.store.FastDeleteEcShardNeedleSkipCookieCheck(ecVolume, n); err != nil {
			if errors.Is(err, storage.ErrorDeleted) {
				return nil // Skip already deleted files
			}
			return &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusInternalServerError,
				Error:  err.Error(),
			}
		} else {
			return &volume_server_pb.DeleteResult{
				FileId: fid,
				Status: http.StatusAccepted,
				Size:   uint32(size),
			}
		}
	}
}
