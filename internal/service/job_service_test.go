package service

import (
	"context"
	"testing"
	"time"

	"github.com/harshith-2411-2002/JobQueue-Assignment/internal/db"
	pb "github.com/harshith-2411-2002/JobQueue-Assignment/proto"
)

type fakeStore struct {
	job       db.Job
	createErr error

	activeCount int32
	recentCount int32
}

func (f *fakeStore) CreateJob(ctx context.Context, workerID, payload, idempotency string, maxRetries int32) (db.Job, bool, error) {
	if f.createErr != nil {
		return db.Job{}, false, f.createErr
	}
	return f.job, false, nil
}

func (f *fakeStore) GetJob(ctx context.Context, id string) (db.Job, error) {
	return f.job, nil
}

func (f *fakeStore) ListJobs(ctx context.Context, workerID string, limit int32) ([]db.Job, error) {
	return []db.Job{f.job}, nil
}

func (f *fakeStore) CountActiveJobs(ctx context.Context, workerID string) (int32, error) {
	return f.activeCount, nil
}

func (f *fakeStore) CountRecentJobs(ctx context.Context, workerID string, since time.Time) (int32, error) {
	return f.recentCount, nil
}

func (f *fakeStore) ListJobsByStatus(ctx context.Context, workerID string, status string) ([]db.Job, error) {
	return []db.Job{f.job}, nil
}

func TestSubmitJob_Success(t *testing.T) {
	f := &fakeStore{
		job: db.Job{
			ID:          "test-job",
			WorkerID:    "worker-1",
			PayloadJSON: `{"task":"test"}`,
		},
		activeCount: 0,
		recentCount: 0,
	}

	svc := NewJobServiceServer(f)

	req := &pb.SubmitJobRequest{
		WorkerId:    "worker-1",
		PayloadJson: `{"task": "test"}`,
		MaxRetries:  3,
	}

	resp, err := svc.SubmitJob(context.Background(), req)
	if err != nil {
		t.Fatalf("expected success, got err: %v", err)
	}

	if resp.JobId != "test-job" {
		t.Fatalf("expected job id test-job, got %s", resp.JobId)
	}
}

func TestSubmitJob_ActiveLimit(t *testing.T) {
	f := &fakeStore{
		activeCount: 5, // limit is 5
	}

	svc := NewJobServiceServer(f)

	req := &pb.SubmitJobRequest{
		WorkerId:    "worker-1",
		PayloadJson: `{"task": "test"}`,
	}

	_, err := svc.SubmitJob(context.Background(), req)
	if err == nil {
		t.Fatal("expected rate-limit error, got nil")
	}
}

func TestGetJob_Success(t *testing.T) {
	f := &fakeStore{
		job: db.Job{
			ID:       "job-123",
			WorkerID: "worker-1",
			Status:   "pending",
		},
	}

	svc := NewJobServiceServer(f)

	req := &pb.GetJobRequest{JobId: "job-123"}

	resp, err := svc.GetJob(context.Background(), req)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	if resp.Job.Id != "job-123" {
		t.Fatalf("expected job ID job-123, got %s", resp.Job.Id)
	}
}
