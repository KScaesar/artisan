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

func NewSingleConnection(url string) ConnectionPool {
	return &singleConnection{
		newConnection: func() (*amqp.Connection, error) {
			return amqp.Dial(url)
		},
		mutex:       make([]sync.Mutex, 1),
		connections: make([]*amqp.Connection, 1),
	}
}

type singleConnection struct {
	newConnection func() (*amqp.Connection, error)
	mutex         []sync.Mutex
	connections   []*amqp.Connection
}

func (pool *singleConnection) GetConnection() (**amqp.Connection, error) {
	if pool.connections[0] != nil && !pool.connections[0].IsClosed() {
		return &pool.connections[0], nil
	}

	pool.mutex[0].Lock()
	defer pool.mutex[0].Unlock()

	if pool.connections[0] != nil && !pool.connections[0].IsClosed() {
		return &pool.connections[0], nil
	}

	connection, err := pool.newConnection()
	if err != nil {
		return nil, err
	}
	pool.connections[0] = connection
	return &pool.connections[0], nil
}

func (pool *singleConnection) ReConnection(failed **amqp.Connection) error {
	if &pool.connections[0] != failed {
		return errors.New("ReConnection :connection not match")
	}

	pool.mutex[0].Lock()
	defer pool.mutex[0].Unlock()

	if !(*failed).IsClosed() {
		return nil
	}

	connection, err := pool.newConnection()
	if err != nil {
		return err
	}
	pool.connections[0] = connection
	return nil
}
