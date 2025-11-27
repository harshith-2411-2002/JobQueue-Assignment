package db

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps the pgx connection pool and exposes DB methods.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a new Store with a pgx connection pool.
func NewStore(ctx context.Context, connString string) (*Store, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgx pool: %w", err)
	}

	// Simple ping to verify connection works
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Store{pool: pool}, nil
}

// Close closes the underlying pool. Call this when shutting down the app.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Job is the DB representation of a job row.
type Job struct {
	ID             string
	WorkerID       string
	Status         string
	PayloadJSON    string
	IdempotencyKey *string
	AttemptCount   int32
	MaxRetries     int32
	LeasedUntil    *time.Time
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CreateJob inserts a new job into the jobs table.
// If idempotencyKey is non-empty and a job already exists for (worker_id, idempotency_key),
// it returns the existing job and deduplicated=true.
func (s *Store) CreateJob(ctx context.Context, workerID string, payloadJSON string, idempotencyKey string, maxRetries int32) (job Job, deduplicated bool, err error) {
	// maxretries default to 3
	if maxRetries <= 0 {
		maxRetries = 3
	}

	// If we have an idempotency key, check if a job already exists
	if idempotencyKey != "" {
		const selectExisting = `
			SELECT id, worker_id, status, payload, idempotency_key,
			       attempt_count, max_retries, leased_until, last_error,
			       created_at, updated_at
			FROM jobs
			WHERE worker_id = $1 AND idempotency_key = $2
			LIMIT 1;
		`
		row := s.pool.QueryRow(ctx, selectExisting, workerID, idempotencyKey)
		err = scanJob(row, &job)
		if err == nil {
			// Found existing job, deduplicated
			return job, true, nil
		}
		// If err != nil  continue to insert
	}

	// No existing job, insert a new one
	newID := uuid.New().String()

	const insertJobSQL = `
		INSERT INTO jobs (
			id,
			worker_id,
			status,
			payload,
			idempotency_key,
			attempt_count,
			max_retries
		)
		VALUES ($1, $2, 'pending', $3, NULLIF($4, ''), 0, $5)
		RETURNING id, worker_id, status, payload, idempotency_key,
		          attempt_count, max_retries, leased_until, last_error,
		          created_at, updated_at;
	`

	row := s.pool.QueryRow(ctx, insertJobSQL,
		newID,
		workerID,
		payloadJSON,
		idempotencyKey,
		maxRetries,
	)

	err = scanJob(row, &job)
	if err != nil {
		return Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	return job, false, nil
}

// GetJob fetches a single job by ID.
func (s *Store) GetJob(ctx context.Context, jobID string) (Job, error) {
	const getJobSQL = `
		SELECT id, worker_id, status, payload, idempotency_key,
		       attempt_count, max_retries, leased_until, last_error,
		       created_at, updated_at
		FROM jobs
		WHERE id = $1;
	`

	row := s.pool.QueryRow(ctx, getJobSQL, jobID)

	var job Job
	if err := scanJob(row, &job); err != nil {
		return Job{}, fmt.Errorf("get job: %w", err)
	}

	return job, nil
}

// ListJobs returns jobs for a specific worker, most recent first.
// We add a LIMIT to keep it simple and safe.
func (s *Store) ListJobs(ctx context.Context, workerID string, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 100
	}

	const listJobsSQL = `
		SELECT id, worker_id, status, payload, idempotency_key,
		       attempt_count, max_retries, leased_until, last_error,
		       created_at, updated_at
		FROM jobs
		WHERE worker_id = $1
		ORDER BY created_at DESC
		LIMIT $2;
	`

	rows, err := s.pool.Query(ctx, listJobsSQL, workerID, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var job Job
		if err := scanJob(rows, &job); err != nil {
			return nil, fmt.Errorf("scan job in list: %w", err)
		}
		jobs = append(jobs, job)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("rows error in list: %w", rows.Err())
	}

	return jobs, nil
}

// scanJob scans a row into a Job struct.
// It works for both QueryRow and Query rows.
type scannable interface {
	Scan(dest ...any) error
}

func scanJob(row scannable, job *Job) error {
	return row.Scan(
		&job.ID,
		&job.WorkerID,
		&job.Status,
		&job.PayloadJSON,
		&job.IdempotencyKey,
		&job.AttemptCount,
		&job.MaxRetries,
		&job.LeasedUntil,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
}

// LeaseNextPendingJob finds one pending job and marks it as running with a lease.
func (s *Store) LeaseNextPendingJob(ctx context.Context, workerID string) (Job, error) {
	const sql = `
		UPDATE jobs
		SET 
			status='running',
			leased_until=NOW() + INTERVAL '30 seconds',
			updated_at=NOW()
		WHERE id = (
			SELECT id FROM jobs
			WHERE status='pending' AND worker_id=$1
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, worker_id, status, payload, idempotency_key,
		          attempt_count, max_retries, leased_until, last_error,
		          created_at, updated_at;
	`

	row := s.pool.QueryRow(ctx, sql, workerID)

	var job Job
	if err := scanJob(row, &job); err != nil {
		return Job{}, err
	}

	return job, nil
}

func (s *Store) MarkJobDone(ctx context.Context, jobID string) error {
	const sql = `
		UPDATE jobs
		SET status='done', updated_at=NOW()
		WHERE id=$1;
	`
	_, err := s.pool.Exec(ctx, sql, jobID)
	return err
}

func (s *Store) MarkJobFailed(ctx context.Context, job Job, errorMsg string) error {
	// If attempts left, retry by setting back to pending
	if job.AttemptCount+1 < job.MaxRetries {
		const sql = `
			UPDATE jobs
			SET 
				status='pending',
				attempt_count=attempt_count + 1,
				last_error=$2,
				updated_at=NOW()
			WHERE id=$1;
		`
		_, err := s.pool.Exec(ctx, sql, job.ID, errorMsg)
		return err
	}

	// No retries left, permanently fail
	const sql = `
		UPDATE jobs
		SET 
			status='failed',
			attempt_count=attempt_count + 1,
			last_error=$2,
			updated_at=NOW()
		WHERE id=$1;
	`
	_, err := s.pool.Exec(ctx, sql, job.ID, errorMsg)
	return err
}

// CountActiveJobs returns how many jobs are currently pending or running for this worker.
func (s *Store) CountActiveJobs(ctx context.Context, workerID string) (int32, error) {
	const sql = `
		SELECT COUNT(*)
		FROM jobs
		WHERE worker_id = $1
		  AND status IN ('pending', 'running');
	`

	var count int32
	err := s.pool.QueryRow(ctx, sql, workerID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count active jobs: %w", err)
	}

	return count, nil
}

// CountRecentJobs returns how many jobs were created for this worker since the given cutoff time.
func (s *Store) CountRecentJobs(ctx context.Context, workerID string, since time.Time) (int32, error) {
	const sql = `
		SELECT COUNT(*)
		FROM jobs
		WHERE worker_id = $1
		  AND created_at >= $2;
	`

	var count int32
	err := s.pool.QueryRow(ctx, sql, workerID, since).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count recent jobs: %w", err)
	}

	return count, nil
}

func (s *Store) ListJobsByStatus(ctx context.Context, workerID, status string) ([]Job, error) {
	var (
		query string
		args  []any
	)

	// Build query dynamically
	if workerID == "" {
		// No worker filter → show all jobs with this status
		query = `
			SELECT id, worker_id, status, payload, idempotency_key,
			       attempt_count, max_retries, leased_until, last_error,
			       created_at, updated_at
			FROM jobs
			WHERE status = $1
			ORDER BY created_at DESC;
		`
		args = append(args, status)
	} else {
		// Filter by worker
		query = `
			SELECT id, worker_id, status, payload, idempotency_key,
			       attempt_count, max_retries, leased_until, last_error,
			       created_at, updated_at
			FROM jobs
			WHERE worker_id = $1 AND status = $2
			ORDER BY created_at DESC;
		`
		args = append(args, workerID, status)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		var j Job
		if err := scanJob(rows, &j); err != nil {
			return nil, err
		}
		out = append(out, j)
	}

	return out, rows.Err()
}

// JobStats holds counts of jobs per status.
type JobStats struct {
	Total   int32
	Pending int32
	Running int32
	Done    int32
	Failed  int32
}

// GetJobStats returns counts of jobs grouped by status.
// If workerID is specified, it filters stats for that worker only.
func (s *Store) GetJobStats(ctx context.Context, workerID string) (JobStats, error) {
	stats := JobStats{}

	query := "SELECT status, COUNT(*) FROM jobs"
	args := []any{}

	if workerID != "" {
		query += " WHERE worker_id = $1"
		args = append(args, workerID)
	}
	query += " GROUP BY status;"

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return stats, fmt.Errorf("get job stats query: %w", err)
	}
	defer rows.Close()

	var total int32
	for rows.Next() {
		var status string
		var count int32
		if err := rows.Scan(&status, &count); err != nil {
			return stats, fmt.Errorf("scan job stats: %w", err)
		}
		total += count

		switch status {
		case "pending":
			stats.Pending = count
		case "running":
			stats.Running = count
		case "done":
			stats.Done = count
		case "failed":
			stats.Failed = count
		}
	}

	if err := rows.Err(); err != nil {
		return stats, fmt.Errorf("job stats rows err: %w", err)
	}

	stats.Total = total
	return stats, nil
}
