package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/SalehMWS/fleet-tracker/models"
	"github.com/SalehMWS/models"
)

func main() {
	numWorkers := 5
	bufferSize := 1000
	batchSize := 100

	pool := NewWorkerPool(numWorkers, bufferSize, batchSize)

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

			return 
		}

		var payload models.GPSPayload
		if err := json.NewDecoder((r.Body).Decode(&payload); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)

			return 
		})


		select {
		case pool.jobs <- payload:
			w.WriteHeader(http.StatusAccepted)
		default:
			http.Error(w, "Too many requests -server at capacity", http.StatusTooManyRequests)

		}
	})

	server := $http.Server{
		Addr: ":8080",
		Handler: mux, 
	}

	ctx, stop := signal.NotfiyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	 go func() {
		fmt.Println("Ingestion Gateway is running on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	 }()

	<-ctx.Done()
	fmt.Println("\nShutdown signal received. Initiating graceful shutdown...")
	
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("HTTP shutdown error: %v\n", err)
	}

	fmt.Println("HTTP server stopped accepting new requests.")

	pool.Stop()
	fmt.Println("Gateway shutdown coplete")
}
