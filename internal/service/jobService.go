package service

import (
	"context"
	"log"
	"time"

	"github.com/harshith-2411-2002/JobQueue-Assignment/internal/db"
	pb "github.com/harshith-2411-2002/JobQueue-Assignment/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxActiveJobsPerWorker = 5
	maxJobsPerMinute       = 10
)

// JobServiceServer is the gRPC server implementation.
type JobServiceServer struct {
	pb.UnimplementedJobServiceServer
	store *db.Store
}

// NewJobServiceServer creates a new JobServiceServer with the given Store.
func NewJobServiceServer(store *db.Store) *JobServiceServer {
	return &JobServiceServer{
		store: store,
	}
}

// SubmitJob handles job creation from clients.
func (s *JobServiceServer) SubmitJob(
	ctx context.Context,
	req *pb.SubmitJobRequest,
) (*pb.SubmitJobResponse, error) {
	// Basic validation
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	if req.GetPayloadJson() == "" {
		return nil, status.Error(codes.InvalidArgument, "payload_json is required")
	}

	workerID := req.GetWorkerId()
	idemKey := req.GetIdempotencyKey()

	log.Printf("[SubmitJob] worker=%s idem=%s incoming", workerID, idemKey)

	// 1) Enforce max active jobs (pending + running)
	activeCount, err := s.store.CountActiveJobs(ctx, workerID)
	if err != nil {
		log.Printf("[SubmitJob] worker=%s error=countActive: %v", workerID, err)
		return nil, status.Errorf(codes.Internal, "failed to check active jobs: %v", err)
	}
	if activeCount >= maxActiveJobsPerWorker {
		log.Printf("[SubmitJob] worker=%s rejected: active=%d limit=%d",
			workerID, activeCount, maxActiveJobsPerWorker)
		return nil, status.Errorf(
			codes.ResourceExhausted,
			"worker %s already has %d or more active jobs",
			workerID,
			maxActiveJobsPerWorker,
		)
	}

	// 2) Enforce simple rate limit: max jobs per minute
	cutoff := time.Now().Add(-1 * time.Minute)
	recentCount, err := s.store.CountRecentJobs(ctx, workerID, cutoff)
	if err != nil {
		log.Printf("[SubmitJob] worker=%s error=countRecent: %v", workerID, err)
		return nil, status.Errorf(codes.Internal, "failed to check recent jobs: %v", err)
	}
	if recentCount >= maxJobsPerMinute {
		log.Printf("[SubmitJob] worker=%s rejected: recent=%d limit=%d",
			workerID, recentCount, maxJobsPerMinute)
		return nil, status.Errorf(
			codes.ResourceExhausted,
			"worker %s exceeded rate limit of %d jobs per minute",
			workerID,
			maxJobsPerMinute,
		)
	}

	job, dedup, err := s.store.CreateJob(
		ctx,
		req.GetWorkerId(),
		req.GetPayloadJson(),
		req.GetIdempotencyKey(),
		req.GetMaxRetries(),
	)
	if err != nil {
		log.Printf("[SubmitJob] worker=%s error=createJob: %v", workerID, err)
		return nil, status.Errorf(codes.Internal, "failed to create job: %v", err)
	}

	if dedup {
		log.Printf("[SubmitJob] worker=%s job=%s deduplicated", workerID, job.ID)
	} else {
		log.Printf("[SubmitJob] worker=%s job=%s created", workerID, job.ID)
	}

	return &pb.SubmitJobResponse{
		JobId:        job.ID,
		Deduplicated: dedup,
	}, nil
}

// GetJob returns a job by ID.
func (s *JobServiceServer) GetJob(
	ctx context.Context,
	req *pb.GetJobRequest,
) (*pb.GetJobResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	jobID := req.GetJobId()
	log.Printf("[GetJob] job=%s requested", jobID)

	job, err := s.store.GetJob(ctx, req.GetJobId())
	if err != nil {
		log.Printf("[GetJob] job=%s error=getJob: %v", jobID, err)
		return nil, status.Errorf(codes.Internal, "failed to get job: %v", err)
	}

	log.Printf("[GetJob] job=%s status=%s worker=%s", job.ID, job.Status, job.WorkerID)

	return &pb.GetJobResponse{
		Job: dbJobToProto(job),
	}, nil
}

// ListJobs lists jobs for a specific worker.
func (s *JobServiceServer) ListJobs(
	ctx context.Context,
	req *pb.ListJobsRequest,
) (*pb.ListJobsResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	workerID := req.GetWorkerId()
	log.Printf("[ListJobs] worker=%s requested", workerID)

	// We cap the limit on server side to keep things safe
	const maxListLimit = 100
	jobs, err := s.store.ListJobs(ctx, req.GetWorkerId(), maxListLimit)
	if err != nil {
		log.Printf("[ListJobs] worker=%s error=listJobs: %v", workerID, err)
		return nil, status.Errorf(codes.Internal, "failed to list jobs: %v", err)
	}

	log.Printf("[ListJobs] worker=%s returned=%d", workerID, len(jobs))

	resp := &pb.ListJobsResponse{}
	for _, j := range jobs {
		resp.Jobs = append(resp.Jobs, dbJobToProto(j))
	}

	return resp, nil
}

// Helper: convert db.Job -> proto.Job
func dbJobToProto(j db.Job) *pb.Job {

	var lastError string
	if j.LastError != nil {
		lastError = *j.LastError
	}

	return &pb.Job{
		Id:           j.ID,
		WorkerId:     j.WorkerID,
		Status:       j.Status,
		PayloadJson:  j.PayloadJSON,
		AttemptCount: j.AttemptCount,
		MaxRetries:   j.MaxRetries,
		LastError:    lastError,
		CreatedAt:    j.CreatedAt.Format(time.RFC3339),
		UpdatedAt:    j.UpdatedAt.Format(time.RFC3339),
	}
}

func (s *JobServiceServer) ListFailedJobs(ctx context.Context, req *pb.ListFailedJobsRequest) (*pb.ListFailedJobsResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	workerID := req.GetWorkerId()
	log.Printf("[ListFailedJobs] worker=%s requested", workerID)

	jobs, err := s.store.ListJobsByStatus(ctx, req.GetWorkerId(), "failed")
	if err != nil {
		log.Printf("[ListFailedJobs] worker=%s error=listFailed: %v", workerID, err)
		return nil, status.Errorf(codes.Internal, "failed to list failed jobs: %v", err)
	}

	log.Printf("[ListFailedJobs] worker=%s returned=%d", workerID, len(jobs))

	resp := &pb.ListFailedJobsResponse{}
	for _, j := range jobs {
		resp.Jobs = append(resp.Jobs, dbJobToProto(j))
	}

	return resp, nil
}
