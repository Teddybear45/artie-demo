package queue

import (
	"testing"
	"time"
)

func drain(t *testing.T, q *Queue) []string {
	t.Helper()
	var got []string
	for {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if msg == nil {
			return got
		}
		got = append(got, msg.Body)
	}
}

func TestPriorityOrdering(t *testing.T) {
	for order, want := range map[Order][]string{
		FIFO: {"hi-1", "hi-2", "lo-1", "lo-2"},
		LIFO: {"hi-2", "hi-1", "lo-2", "lo-1"},
	} {
		q := newQueue("t", order, nil)
		for _, m := range []struct {
			body string
			prio int
		}{{"lo-1", 0}, {"hi-1", 5}, {"lo-2", 0}, {"hi-2", 5}} {
			if _, err := q.Enqueue(m.body, m.prio, 0); err != nil {
				t.Fatal(err)
			}
		}
		got := drain(t, q)
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s: got %v, want %v", order, got, want)
			}
		}
	}
}

func TestDelayVisibility(t *testing.T) {
	q := newQueue("t", FIFO, nil)
	now := time.Now()
	q.now = func() time.Time { return now }
	q.Enqueue("delayed", 9, 5*time.Second) // high priority but invisible
	q.Enqueue("ready", 0, 0)

	if got := drain(t, q); len(got) != 1 || got[0] != "ready" {
		t.Fatalf("before delay elapsed: got %v, want [ready]", got)
	}
	now = now.Add(6 * time.Second)
	if got := drain(t, q); len(got) != 1 || got[0] != "delayed" {
		t.Fatalf("after delay elapsed: got %v, want [delayed]", got)
	}
}

func TestReplayRestoresState(t *testing.T) {
	dir := t.TempDir()

	reg, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	q, err := reg.Create("jobs", FIFO)
	if err != nil {
		t.Fatal(err)
	}
	q.Enqueue("consumed", 0, 0)
	q.Enqueue("survives", 0, 0)
	q.Enqueue("still-delayed", 0, time.Hour)
	if msg, _ := q.Dequeue(); msg == nil || msg.Body != "consumed" {
		t.Fatalf("dequeue got %v", msg)
	}

	// "Restart": reopen the same data dir and replay the log.
	reg2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	q2, ok := reg2.Get("jobs")
	if !ok {
		t.Fatal("queue not restored")
	}
	stats := q2.Stats()
	if stats.Ready != 1 || stats.Delayed != 1 {
		t.Fatalf("stats after replay = %+v, want 1 ready and 1 delayed", stats)
	}
	if got := drain(t, q2); len(got) != 1 || got[0] != "survives" {
		t.Fatalf("after replay: got %v, want [survives]", got)
	}
}
