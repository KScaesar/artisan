package rabbit

import (
	"errors"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConnectionPool interface {
	GetConnection() (**amqp.Connection, error)
	ReConnection(failed **amqp.Connection) error
}

func NewConnectionPool(url string, maxSize int) ConnectionPool {
	const defaultSize = 1
	if maxSize <= 0 {
		maxSize = defaultSize
	}
	return &connectionPool{
		newConnection: func() (*amqp.Connection, error) {
			return amqp.Dial(url)
		},
		mutex:       make([]sync.Mutex, maxSize),
		connections: make([]*amqp.Connection, maxSize),
		maxSize:     maxSize,
		cursors:     make(map[**amqp.Connection]int, maxSize),
	}
}

type connectionPool struct {
	newConnection func() (*amqp.Connection, error)
	mutex         []sync.Mutex
	connections   []*amqp.Connection

	maxSize      int
	cursors      map[**amqp.Connection]int
	cursorsMutex sync.RWMutex
	idx          int
	idxMutex     sync.Mutex
}

func (pool *connectionPool) GetConnection() (**amqp.Connection, error) {
	cursor := pool.cursor()
	if pool.connections[cursor] != nil && !pool.connections[cursor].IsClosed() {
		return &pool.connections[cursor], nil
	}

	pool.mutex[cursor].Lock()
	defer pool.mutex[cursor].Unlock()

	if pool.connections[cursor] != nil && !pool.connections[cursor].IsClosed() {
		return &pool.connections[cursor], nil
	}

	connection, err := pool.newConnection()
	if err != nil {
		return nil, err
	}

	pool.connections[cursor] = connection
	pool.cursorsMutex.Lock()
	pool.cursors[&pool.connections[cursor]] = cursor
	pool.cursorsMutex.Unlock()
	return &pool.connections[cursor], nil
}

func (pool *connectionPool) ReConnection(failed **amqp.Connection) error {
	pool.cursorsMutex.RLock()
	cursor, ok := pool.cursors[failed]
	pool.cursorsMutex.RUnlock()
	if !ok {
		return errors.New("ReConnection: connection not match")
	}

	pool.mutex[cursor].Lock()
	defer pool.mutex[cursor].Unlock()

	if !pool.connections[cursor].IsClosed() {
		return nil
	}

	connection, err := pool.newConnection()
	if err != nil {
		return err
	}

	pool.connections[cursor] = connection
	return nil
}

func (pool *connectionPool) cursor() int {
	pool.idxMutex.Lock()
	defer pool.idxMutex.Unlock()
	idx := pool.idx
	pool.idx++
	pool.idx %= pool.maxSize
	return idx
}
