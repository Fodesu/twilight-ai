package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/store/postgres"
	"github.com/felinics/twilight/agent/store/postgres/postgrestest"
)

func TestConcurrentOpenFreshSchema(t *testing.T) {
	for attempt := range 3 {
		t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			dsn := postgrestest.Schema(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			start := make(chan struct{})
			results := make(chan error, 8)
			var ready sync.WaitGroup
			ready.Add(8)
			for range 8 {
				go func() {
					ready.Done()
					<-start
					db, err := postgres.Open(ctx, dsn)
					if err == nil {
						err = db.Close()
					}
					results <- err
				}()
			}
			ready.Wait()
			close(start)
			for range 8 {
				if err := <-results; err != nil {
					t.Errorf("concurrent open: %v", err)
				}
			}
		})
	}
}
