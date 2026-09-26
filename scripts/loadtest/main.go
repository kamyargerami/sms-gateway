// Command loadtest measures end-to-end SMS throughput with the load spread over
// many users, so the result isn't capped by row-lock contention on a single
// users row (which is what a single-user `hey` run measures).
//
// It:
//  1. creates users [first-user, first-user+users) if missing and tops each one
//     up through the API (so MySQL and the Redis cache stay in sync);
//  2. sends -n SMS with -c concurrent clients, round-robin over those users, and
//     reports API throughput, latency and status codes;
//  3. waits until the workers have finalized every accepted SMS (or stop making
//     progress) and reports worker throughput and final statuses from MySQL.
//
// Run from the repository root while the stack is up (make up):
//
//	go run ./scripts/loadtest -users 1000 -n 100000 -c 500
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type options struct {
	apiURL      string
	dsn         string
	users       int
	firstUserID int
	requests    int
	concurrency int
	express     bool
	smsCost     int
	waitTimeout time.Duration
}

func main() {
	var opts options
	flag.StringVar(&opts.apiURL, "api", "http://localhost:8080", "API base URL")
	flag.StringVar(&opts.dsn, "dsn", "sms_user:smspassword@tcp(localhost:3306)/sms_db?parseTime=true&loc=Local", "MySQL DSN (host port from MYSQL_PORT)")
	flag.IntVar(&opts.users, "users", 1000, "number of distinct users to spread the load over")
	flag.IntVar(&opts.firstUserID, "first-user", 1000, "first user id to use (ids first-user .. first-user+users-1)")
	flag.IntVar(&opts.requests, "n", 100000, "total number of SMS to send")
	flag.IntVar(&opts.concurrency, "c", 500, "number of concurrent HTTP clients")
	flag.BoolVar(&opts.express, "express", false, "send express SMS (subject to EXPRESS_SMS_TTL; expired ones are not counted as throughput)")
	flag.IntVar(&opts.smsCost, "sms-cost", 10, "SMS_COST configured on the API (used to size each user's top-up)")
	flag.DurationVar(&opts.waitTimeout, "wait-no-progress", 30*time.Second, "stop waiting for workers after this long without progress")
	flag.Parse()

	if opts.users <= 0 || opts.requests <= 0 || opts.concurrency <= 0 || opts.firstUserID <= 0 {
		log.Fatal("-users, -n, -c and -first-user must be positive")
	}

	database, err := sql.Open("mysql", opts.dsn)
	if err != nil {
		log.Fatalf("open MySQL: %v", err)
	}
	defer database.Close()
	if err := database.Ping(); err != nil {
		log.Fatalf("connect to MySQL (%s): %v", opts.dsn, err)
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        opts.concurrency,
			MaxIdleConnsPerHost: opts.concurrency,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	fmt.Printf("== Preparing %d users (ids %d..%d)\n", opts.users, opts.firstUserID, opts.firstUserID+opts.users-1)
	if err := prepareUsers(database, client, opts); err != nil {
		log.Fatalf("prepare users: %v", err)
	}

	// sms_records.created_at is the worker's INSERT time (second precision), so
	// leave a second of margin when selecting this run's rows.
	runStart := time.Now().Add(-time.Second).Truncate(time.Second)

	fmt.Printf("== Sending %d SMS (express=%v) with %d concurrent clients\n", opts.requests, opts.express, opts.concurrency)
	accepted := sendLoad(client, opts)

	fmt.Println("== Waiting for workers to finalize accepted SMS")
	waitForWorkers(database, opts, runStart, accepted)
}

// prepareUsers inserts missing users and tops each one up through the API with
// enough credit for its share of the run.
func prepareUsers(database *sql.DB, client *http.Client, opts options) error {
	var values []string
	var args []any
	for index := 0; index < opts.users; index++ {
		values = append(values, "(?, 0)")
		args = append(args, opts.firstUserID+index)
		if len(values) == 1000 || index == opts.users-1 {
			query := "INSERT IGNORE INTO users (id, balance) VALUES " + strings.Join(values, ",")
			if _, err := database.Exec(query, args...); err != nil {
				return err
			}
			values, args = values[:0], args[:0]
		}
	}

	perUser := (opts.requests/opts.users + 1) * opts.smsCost
	jobs := make(chan int)
	var failures atomic.Int64
	var waitGroup sync.WaitGroup
	for worker := 0; worker < min(opts.concurrency, 50); worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for userID := range jobs {
				body, _ := json.Marshal(map[string]int{"user_id": userID, "amount": perUser})
				response, err := client.Post(opts.apiURL+"/api/v1/users/charge", "application/json", bytes.NewReader(body))
				if err != nil {
					failures.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode != http.StatusOK {
					failures.Add(1)
				}
			}
		}()
	}
	for index := 0; index < opts.users; index++ {
		jobs <- opts.firstUserID + index
	}
	close(jobs)
	waitGroup.Wait()

	if failures.Load() > 0 {
		return fmt.Errorf("%d top-ups failed", failures.Load())
	}
	fmt.Printf("   topped up each user with %d credit\n", perUser)
	return nil
}

// sendLoad fires the SMS requests and returns how many the API accepted (200).
func sendLoad(client *http.Client, opts options) int {
	var next atomic.Int64
	var mutex sync.Mutex
	statusCounts := map[string]int{}
	latencies := make([]time.Duration, 0, opts.requests)

	started := time.Now()
	var waitGroup sync.WaitGroup
	for worker := 0; worker < opts.concurrency; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			localCounts := map[string]int{}
			localLatencies := make([]time.Duration, 0, opts.requests/opts.concurrency+1)
			for {
				index := int(next.Add(1)) - 1
				if index >= opts.requests {
					break
				}
				body, _ := json.Marshal(map[string]any{
					"user_id":    opts.firstUserID + index%opts.users,
					"to_number":  "09123456789",
					"text":       "Load Test",
					"is_express": opts.express,
				})
				requestStarted := time.Now()
				response, err := client.Post(opts.apiURL+"/api/v1/sms/send", "application/json", bytes.NewReader(body))
				localLatencies = append(localLatencies, time.Since(requestStarted))
				if err != nil {
					localCounts["error"]++
					continue
				}
				_, _ = io.Copy(io.Discard, response.Body)
				response.Body.Close()
				localCounts[fmt.Sprint(response.StatusCode)]++
			}
			mutex.Lock()
			for key, count := range localCounts {
				statusCounts[key] += count
			}
			latencies = append(latencies, localLatencies...)
			mutex.Unlock()
		}()
	}
	waitGroup.Wait()
	elapsed := time.Since(started)

	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })
	percentile := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[min(int(float64(len(latencies))*p), len(latencies)-1)]
	}

	fmt.Printf("   duration:     %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("   API rate:     %.1f req/s\n", float64(opts.requests)/elapsed.Seconds())
	fmt.Printf("   latency:      p50=%v p95=%v p99=%v max=%v\n",
		percentile(0.50).Round(time.Microsecond), percentile(0.95).Round(time.Microsecond),
		percentile(0.99).Round(time.Microsecond), percentile(1).Round(time.Microsecond))
	fmt.Printf("   status codes: %v\n", statusCounts)
	return statusCounts["200"]
}

type runStats struct {
	byStatus  map[string]int
	total     int
	firstSeen time.Time
	lastDone  time.Time
}

func queryRunStats(database *sql.DB, opts options, runStart time.Time) (runStats, error) {
	stats := runStats{byStatus: map[string]int{}}
	queryContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := database.QueryContext(queryContext,
		"SELECT status, COUNT(*) FROM sms_records WHERE user_id BETWEEN ? AND ? AND created_at >= ? GROUP BY status",
		opts.firstUserID, opts.firstUserID+opts.users-1, runStart)
	if err != nil {
		return stats, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return stats, err
		}
		stats.byStatus[status] = count
		stats.total += count
	}
	if err := rows.Err(); err != nil {
		return stats, err
	}

	var firstSeen, lastDone sql.NullTime
	err = database.QueryRowContext(queryContext,
		"SELECT MIN(created_at), MAX(updated_at) FROM sms_records WHERE user_id BETWEEN ? AND ? AND created_at >= ? AND status <> 'PENDING'",
		opts.firstUserID, opts.firstUserID+opts.users-1, runStart).Scan(&firstSeen, &lastDone)
	stats.firstSeen, stats.lastDone = firstSeen.Time, lastDone.Time
	return stats, err
}

// waitForWorkers polls MySQL until every accepted SMS has a final status, or
// until nothing has changed for opts.waitTimeout, then prints the results.
func waitForWorkers(database *sql.DB, opts options, runStart time.Time, accepted int) {
	var stats runStats
	lastFinished, lastProgress := -1, time.Now()
	for {
		var err error
		stats, err = queryRunStats(database, opts, runStart)
		if err != nil {
			log.Fatalf("query results: %v", err)
		}
		finished := stats.total - stats.byStatus["PENDING"]
		fmt.Printf("\r   finalized %d / %d accepted", finished, accepted)
		if finished >= accepted {
			break
		}
		if finished != lastFinished {
			lastFinished, lastProgress = finished, time.Now()
		} else if time.Since(lastProgress) > opts.waitTimeout {
			fmt.Printf("\n   no progress for %v, giving up waiting", opts.waitTimeout)
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Println()

	sent := stats.byStatus["DELIVERED"] + stats.byStatus["FAILED"]
	fmt.Printf("== Results (this run's users only)\n")
	fmt.Printf("   statuses:     %v\n", stats.byStatus)
	if sent == 0 || stats.lastDone.IsZero() {
		fmt.Println("   no SMS reached the operator")
		os.Exit(1)
	}
	seconds := stats.lastDone.Sub(stats.firstSeen).Seconds()
	if seconds < 1 {
		seconds = 1 // created_at/updated_at have second precision
	}
	rate := float64(sent) / seconds
	fmt.Printf("   worker window: %.0fs (first worker insert -> last finalized)\n", seconds)
	fmt.Printf("   worker rate:   %.1f SMS/s sent to operator  (~%.1fM/day)\n", rate, rate*86400/1e6)
	fmt.Printf("   target:        ~1157 SMS/s average for 100M/day\n")
}
