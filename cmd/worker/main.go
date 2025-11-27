package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/harshith-2411-2002/JobQueue-Assignment/internal/db"
)

func main() {
	ctx := context.Background()

	connString := "postgres://postgres:Honey2411@localhost:5432/jobqueue?sslmode=disable"

	store, err := db.NewStore(ctx, connString)
	if err != nil {
		log.Fatalf("DB connect failed: %v", err)
	}
	defer store.Close()

	workerID := "worker-r4"

	log.Println("Worker started for:", workerID)

	for {
		job, err := store.LeaseNextPendingJob(ctx, workerID)
		if err != nil {
			// no job available, wait for a second
			time.Sleep(1 * time.Second)
			continue
		}

		log.Println("Processing job:", job.ID, "Payload:", job.PayloadJSON)

		// Simulate processing
		time.Sleep(2 * time.Second)

		// Random fail simulation (30% chance)
		if rand.Intn(100) < 70 {
			err := store.MarkJobFailed(ctx, job, "simulated failure")
			if err != nil {
				log.Println("Failed to mark job failed:", err)
			} else {
				log.Println("Job FAILED:", job.ID, "(will retry if attempts left)")
			}
			continue
		}

		err = store.MarkJobDone(ctx, job.ID)
		if err != nil {
			log.Println("Failed to mark done:", err)
			continue
		}

		log.Println("Job completed:", job.ID)
	}
}
