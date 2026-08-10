// Package queue implements a "frankenstein" message queue: a single engine
// whose FIFO/LIFO, priority, and delay behaviors are configuration, not code
// paths. Ordering is a comparator over (priority, sequence number); delay is a
// visibility rule, not an ordering rule — a delayed message simply cannot be
// dequeued until its VisibleAt has passed, mirroring SQS delay queues.
package queue

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

type Order string

const (
	FIFO Order = "fifo"
	LIFO Order = "lifo"
)

func (o Order) Valid() bool { return o == FIFO || o == LIFO }

type Message struct {
	ID        string    `json:"id"`
	Body      string    `json:"body"`
	Priority  int       `json:"priority"`
	Seq       uint64    `json:"seq"`
	VisibleAt time.Time `json:"visible_at"`
}

// readyHeap holds visible messages: higher priority first, ties broken by
// sequence number — ascending for FIFO, descending for LIFO.
type readyHeap struct {
	msgs  []*Message
	order Order
}

func (h *readyHeap) Len() int { return len(h.msgs) }
func (h *readyHeap) Less(i, j int) bool {
	a, b := h.msgs[i], h.msgs[j]
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if h.order == LIFO {
		return a.Seq > b.Seq
	}
	return a.Seq < b.Seq
}
func (h *readyHeap) Swap(i, j int) { h.msgs[i], h.msgs[j] = h.msgs[j], h.msgs[i] }
func (h *readyHeap) Push(x any)    { h.msgs = append(h.msgs, x.(*Message)) }
func (h *readyHeap) Pop() any {
	n := len(h.msgs)
	m := h.msgs[n-1]
	h.msgs = h.msgs[:n-1]
	return m
}

// delayedHeap holds not-yet-visible messages, soonest VisibleAt first.
type delayedHeap []*Message

func (h delayedHeap) Len() int           { return len(h) }
func (h delayedHeap) Less(i, j int) bool { return h[i].VisibleAt.Before(h[j].VisibleAt) }
func (h delayedHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *delayedHeap) Push(x any)        { *h = append(*h, x.(*Message)) }
func (h *delayedHeap) Pop() any {
	n := len(*h)
	m := (*h)[n-1]
	*h = (*h)[:n-1]
	return m
}

type Queue struct {
	Name  string
	Order Order

	// mu guards everything below plus the log append, so a message is either
	// fully applied (memory + log) or not at all.
	mu      sync.Mutex
	seq     uint64
	ready   readyHeap
	delayed delayedHeap
	store   *Store
	now     func() time.Time
}

func newQueue(name string, order Order, store *Store) *Queue {
	return &Queue{
		Name:  name,
		Order: order,
		ready: readyHeap{order: order},
		store: store,
		now:   time.Now,
	}
}

func (q *Queue) Enqueue(body string, priority int, delay time.Duration) (*Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	msg := &Message{
		ID:        fmt.Sprintf("%s-%d", q.Name, q.seq+1),
		Body:      body,
		Priority:  priority,
		Seq:       q.seq + 1,
		VisibleAt: q.now().Add(delay),
	}
	// Log before applying (write-ahead): an acknowledged enqueue is on disk.
	if q.store != nil {
		if err := q.store.Append(Record{Op: opEnqueue, Queue: q.Name, Msg: msg}); err != nil {
			return nil, err
		}
	}
	q.seq++
	q.insert(msg)
	return msg, nil
}

// Dequeue pops the best visible message, or nil if none is ready.
func (q *Queue) Dequeue() (*Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.promote()
	if q.ready.Len() == 0 {
		return nil, nil
	}
	msg := q.ready.msgs[0]
	if q.store != nil {
		if err := q.store.Append(Record{Op: opDequeue, Queue: q.Name, ID: msg.ID}); err != nil {
			return nil, err
		}
	}
	heap.Pop(&q.ready)
	return msg, nil
}

type Stats struct {
	Name    string `json:"name"`
	Order   Order  `json:"order"`
	Ready   int    `json:"ready"`
	Delayed int    `json:"delayed"`
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.promote()
	return Stats{Name: q.Name, Order: q.Order, Ready: q.ready.Len(), Delayed: q.delayed.Len()}
}

// promote moves messages whose delay has elapsed into the ready heap.
// Called lazily under the lock; there is no background timer to coordinate.
func (q *Queue) promote() {
	now := q.now()
	for q.delayed.Len() > 0 && !q.delayed[0].VisibleAt.After(now) {
		heap.Push(&q.ready, heap.Pop(&q.delayed))
	}
}

func (q *Queue) insert(msg *Message) {
	if msg.VisibleAt.After(q.now()) {
		heap.Push(&q.delayed, msg)
	} else {
		heap.Push(&q.ready, msg)
	}
}

// restore re-inserts a message during log replay without writing to the log.
func (q *Queue) restore(msg *Message) {
	if msg.Seq > q.seq {
		q.seq = msg.Seq
	}
	q.insert(msg)
}
