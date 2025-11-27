package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/harshith-2411-2002/JobQueue-Assignment/internal/db"
	"github.com/harshith-2411-2002/JobQueue-Assignment/internal/service"
	pb "github.com/harshith-2411-2002/JobQueue-Assignment/proto"
	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Actual Postgres password needed here
	connString := "postgres://postgres:Honey2411@localhost:5432/jobqueue?sslmode=disable"

	store, err := db.NewStore(ctx, connString)
	if err != nil {
		log.Fatalf("failed to connect to DB: %v", err)
	}
	defer store.Close()

	jobSvc := service.NewJobServiceServer(store)

	grpcServer := grpc.NewServer()
	pb.RegisterJobServiceServer(grpcServer, jobSvc)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen on :50051: %v", err)
	}

	log.Println("gRPC JobService server listening on :50051")

	// HTTP SERVER
	fs := http.FileServer(http.Dir("web/dashboard"))
	http.Handle("/", fs)

	// HTTP: /metrics endpoint
	// Used by dashboard and external monitoring to get job stats.
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// filter by worker_id
		workerID := r.URL.Query().Get("worker_id")

		stats, err := store.GetJobStats(ctx, workerID)
		if err != nil {
			log.Printf("metrics: failed to get stats: %v", err)
			http.Error(w, "failed to get stats", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			log.Printf("metrics: failed to encode: %v", err)
		}
	})

	// HTTP: /failed-jobs endpoint
	// Used by dashboard to show DLQ items (jobs with status='failed').
	http.HandleFunc("/failed-jobs", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		workerID := r.URL.Query().Get("worker_id")

		jobs, err := store.ListJobsByStatus(ctx, workerID, "failed")
		if err != nil {
			log.Printf("failed-jobs: list failed jobs error: %v", err)
			http.Error(w, "failed to list failed jobs", http.StatusInternalServerError)
			return
		}

		type failedJobDTO struct {
			ID           string `json:"id"`
			AttemptCount int32  `json:"attemptCount"`
			MaxRetries   int32  `json:"maxRetries"`
			LastError    string `json:"lastError"`
		}
		var out struct {
			Jobs []failedJobDTO `json:"jobs"`
		}

		for _, j := range jobs {
			errStr := ""
			if j.LastError != nil {
				errStr = *j.LastError
			}
			out.Jobs = append(out.Jobs, failedJobDTO{
				ID:           j.ID,
				AttemptCount: j.AttemptCount,
				MaxRetries:   j.MaxRetries,
				LastError:    errStr,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(out); err != nil {
			log.Printf("failed-jobs: failed to encode: %v", err)
		}
	})

	// HTTP: server object and goroutine
	httpServer := &http.Server{
		Addr: ":8080",
	}

	go func() {
		log.Println("HTTP server listening on :8080 (dashboard & metrics)")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown handling
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// Wait for interrupt signal (Ctrl+C) to gracefully shut down
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	log.Println("Shutting down gRPC server...")

	// Give some time for ongoing requests to complete
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Println("Server stopped gracefully")
	case <-time.After(5 * time.Second):
		log.Println("Timed out, forcing server stop")
		grpcServer.Stop()
	}
}
