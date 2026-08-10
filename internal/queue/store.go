package queue

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Persistence is a single append-only operation log. Every state change is one
// JSON line, fsync'd before the caller's request is acknowledged. Restart
// replays the log from the top: enqueues insert, dequeues remove, and whatever
// survives is the state. VisibleAt is an absolute timestamp, so pending delays
// survive restarts. Known tradeoff: the log grows with history and replay is
// O(history); compaction (rewriting only live messages) is the fix, described
// in the README rather than implemented.

const (
	opCreate  = "create"
	opEnqueue = "enq"
	opDequeue = "deq"
)

type Record struct {
	Op    string   `json:"op"`
	Queue string   `json:"queue"`
	Order Order    `json:"order,omitempty"`
	Msg   *Message `json:"msg,omitempty"`
	ID    string   `json:"id,omitempty"`
}

type Store struct {
	mu sync.Mutex
	f  *os.File
}

func (s *Store) Append(rec Record) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.f.Write(append(line, '\n')); err != nil {
		return err
	}
	return s.f.Sync()
}

// Registry owns all queues and the shared log.
type Registry struct {
	mu     sync.RWMutex
	queues map[string]*Queue
	store  *Store
}

var ErrExists = errors.New("queue already exists")

// Open replays dir/queue.log (if present) and opens it for appending.
func Open(dir string) (*Registry, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "queue.log")

	queues := map[string]*Queue{}
	live := map[string]map[string]*Message{} // queue -> id -> message
	if err := replay(path, func(rec Record) {
		switch rec.Op {
		case opCreate:
			queues[rec.Queue] = newQueue(rec.Queue, rec.Order, nil)
			live[rec.Queue] = map[string]*Message{}
		case opEnqueue:
			if m, ok := live[rec.Queue]; ok && rec.Msg != nil {
				m[rec.Msg.ID] = rec.Msg
			}
		case opDequeue:
			if m, ok := live[rec.Queue]; ok {
				delete(m, rec.ID)
			}
		}
	}); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	store := &Store{f: f}
	for name, q := range queues {
		q.store = store
		for _, msg := range live[name] {
			q.restore(msg)
		}
	}
	return &Registry{queues: queues, store: store}, nil
}

// replay streams records to apply, stopping silently at a torn final line
// (the one write a crash can leave half-finished).
func replay(path string, apply func(Record)) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		var rec Record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return nil
		}
		apply(rec)
	}
	if err := sc.Err(); err != nil && err != io.ErrUnexpectedEOF {
		return err
	}
	return nil
}

func (r *Registry) Create(name string, order Order) (*Queue, error) {
	if name == "" || !order.Valid() {
		return nil, fmt.Errorf("queue needs a name and order %q or %q", FIFO, LIFO)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.queues[name]; ok {
		return nil, ErrExists
	}
	if err := r.store.Append(Record{Op: opCreate, Queue: name, Order: order}); err != nil {
		return nil, err
	}
	q := newQueue(name, order, r.store)
	r.queues[name] = q
	return q, nil
}

func (r *Registry) Get(name string) (*Queue, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	q, ok := r.queues[name]
	return q, ok
}
