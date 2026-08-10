// demo is a producer/consumer client that exercises every queue behavior over
// HTTP and doubles as the end-to-end test: priority ordering, FIFO vs LIFO
// tie-breaking, SQS-style delay, and concurrent consumers with no duplicate
// or lost deliveries.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

var server string

func main() {
	flag.StringVar(&server, "server", "http://localhost:8080", "queued server address")
	flag.Parse()
	// Unique names per run so the demo can be re-run against a persisted log.
	run := time.Now().Format("150405")

	fmt.Println("=== 1. Priority FIFO ===")
	orderingDemo("fifo-"+run, "fifo")

	fmt.Println("\n=== 2. Priority LIFO (same enqueues, different order) ===")
	orderingDemo("lifo-"+run, "lifo")

	fmt.Println("\n=== 3. Delay ===")
	delayDemo("delay-" + run)

	fmt.Println("\n=== 4. Concurrent producers/consumers ===")
	concurrencyDemo("load-" + run)
}

func orderingDemo(name, order string) {
	createQueue(name, order)
	for i := 1; i <= 3; i++ {
		enqueue(name, fmt.Sprintf("normal-%d", i), 0, 0)
	}
	enqueue(name, "urgent-1", 9, 0)
	enqueue(name, "urgent-2", 9, 0)
	fmt.Println("enqueued: normal-1 normal-2 normal-3 (prio 0), urgent-1 urgent-2 (prio 9)")
	fmt.Print("dequeued: ")
	for msg := dequeue(name); msg != nil; msg = dequeue(name) {
		fmt.Printf("%s ", msg.Body)
	}
	fmt.Println()
}

func delayDemo(name string) {
	createQueue(name, "fifo")
	enqueue(name, "delayed-2s", 0, 2)
	enqueue(name, "immediate", 0, 0)
	fmt.Println("enqueued: delayed-2s (delay=2s), immediate")
	fmt.Printf("dequeue now:      %s\n", body(dequeue(name)))
	fmt.Printf("dequeue now:      %s\n", body(dequeue(name)))
	fmt.Println("...waiting 2.5s...")
	time.Sleep(2500 * time.Millisecond)
	fmt.Printf("dequeue after 2.5s: %s\n", body(dequeue(name)))
}

func concurrencyDemo(name string) {
	createQueue(name, "fifo")
	const producers, perProducer, consumers = 4, 25, 4
	total := producers * perProducer

	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perProducer {
				enqueue(name, fmt.Sprintf("p%d-m%d", p, i), i%3, 0)
			}
		}()
	}
	wg.Wait()
	fmt.Printf("%d producers enqueued %d messages concurrently\n", producers, total)

	seen := make(chan string, total)
	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := dequeue(name); msg != nil; msg = dequeue(name) {
				seen <- msg.Body
			}
		}()
	}
	wg.Wait()
	close(seen)

	unique := map[string]bool{}
	dupes := 0
	for b := range seen {
		if unique[b] {
			dupes++
		}
		unique[b] = true
	}
	fmt.Printf("%d consumers dequeued %d messages: %d unique, %d duplicates, %d lost\n",
		consumers, len(unique)+dupes, len(unique), dupes, total-len(unique))
	if dupes != 0 || len(unique) != total {
		log.Fatal("FAIL: messages were lost or delivered twice")
	}
	fmt.Println("OK: every message delivered exactly once")
}

// --- tiny HTTP client ---

type message struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}

func createQueue(name, order string) {
	post("/queues", map[string]any{"name": name, "order": order}, nil)
}

func enqueue(queue, msgBody string, priority, delaySeconds int) {
	post("/queues/"+queue+"/messages",
		map[string]any{"body": msgBody, "priority": priority, "delay_seconds": delaySeconds}, nil)
}

func dequeue(queue string) *message {
	var msg message
	if post("/queues/"+queue+"/dequeue", nil, &msg) == http.StatusNoContent {
		return nil
	}
	return &msg
}

func post(path string, req, resp any) int {
	payload, _ := json.Marshal(req)
	r, err := http.Post(server+path, "application/json", bytes.NewReader(payload))
	if err != nil {
		log.Fatalf("POST %s: %v (is queued running?)", path, err)
	}
	defer r.Body.Close()
	if r.StatusCode >= 400 {
		log.Fatalf("POST %s: HTTP %d", path, r.StatusCode)
	}
	if resp != nil && r.StatusCode != http.StatusNoContent {
		json.NewDecoder(r.Body).Decode(resp)
	}
	return r.StatusCode
}

func body(m *message) string {
	if m == nil {
		return "(nothing ready — 204)"
	}
	return m.Body
}
