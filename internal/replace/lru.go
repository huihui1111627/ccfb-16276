package replace

import (
	"container/list"

	"catalogbuffer/internal/disk"
)

type lruEntry struct {
	id disk.PageID
}

type lruQueue struct {
	l   *list.List
	idx map[disk.PageID]*list.Element
}

func newLRUQueue() *lruQueue {
	return &lruQueue{l: list.New(), idx: make(map[disk.PageID]*list.Element)}
}

func (q *lruQueue) lookup(id disk.PageID) *list.Element {
	return q.idx[id]
}

func (q *lruQueue) moveFront(e *list.Element) {
	q.l.MoveToFront(e)
}

func (q *lruQueue) insertFront(id disk.PageID) {
	e := q.l.PushFront(lruEntry{id: id})
	q.idx[id] = e
}

func (q *lruQueue) len() int { return q.l.Len() }

func (q *lruQueue) evictOldest() {
	e := q.l.Back()
	if e == nil {
		return
	}
	q.l.Remove(e)
	id := e.Value.(lruEntry).id
	delete(q.idx, id)
}
