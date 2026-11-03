package eventbus

import (
	"encoding/json"
	"sync"
)

type Event struct {
	Type    string          `json:"type"`
	TaskID  string          `json:"taskId,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

type Bus struct {
	mu   sync.RWMutex
	subs map[int]chan Event
	next int
}

func New() *Bus {
	return &Bus{subs: map[int]chan Event{}}
}

func (b *Bus) Subscribe() (int, <-chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.next
	b.next++
	ch := make(chan Event, 64)
	b.subs[id] = ch
	return id, ch
}

func (b *Bus) Unsubscribe(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.subs[id]; ok {
		close(ch)
		delete(b.subs, id)
	}
}

func (b *Bus) Publish(ev Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

func JSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
