package service

import (
	"context"
	"time"

	"github.com/harshith-2411-2002/JobQueue-Assignment/internal/db"
)

type Store interface {
	CreateJob(ctx context.Context, workerID, payload, idempotency string, maxRetries int32) (db.Job, bool, error)
	GetJob(ctx context.Context, id string) (db.Job, error)
	ListJobs(ctx context.Context, workerID string, limit int32) ([]db.Job, error)
	CountActiveJobs(ctx context.Context, workerID string) (int32, error)
	CountRecentJobs(ctx context.Context, workerID string, since time.Time) (int32, error)
	ListJobsByStatus(ctx context.Context, workerID string, status string) ([]db.Job, error)
}
