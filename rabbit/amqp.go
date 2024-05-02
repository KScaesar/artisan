package rabbit

import (
	"github.com/KScaesar/Artifex"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SetupAmqp func(channel *amqp.Channel) error

type SetupAmqpAll []SetupAmqp

func (all SetupAmqpAll) Execute(channel *amqp.Channel) error {
	for _, setupAmqp := range all {
		err := setupAmqp(channel)
		if err != nil {
			return err
		}
	}
	return nil
}

func fixup(
	connection **amqp.Connection,
	channel **amqp.Channel,
	pool ConnectionPool,
	setupAmqp SetupAmqp,
) func(adp Artifex.IAdapter) error {

	connCloseNotify := (*connection).NotifyClose(make(chan *amqp.Error, 1))
	chCloseNotify := (*channel).NotifyClose(make(chan *amqp.Error, 1))
	retry := 0

	return func(adp Artifex.IAdapter) error {
		logger := adp.Log()

		select {
		case Err := <-connCloseNotify:
			if Err != nil {
				logger.Error("amqp connection close: %v", Err)
				connCloseNotify = nil
			}
		case Err := <-chCloseNotify:
			if Err != nil {
				logger.Error("amqp channel close: %v", Err)
				chCloseNotify = nil
			}
		default:
		}

		retry++
		logger.Info("retry %v times", retry)

		if (*connection).IsClosed() {
			logger.Info("retry amqp conn start")
			err := pool.ReConnection(connection)
			if err != nil {
				logger.Error("retry amqp conn fail: %v", err)
				return err
			}
			logger.Info("retry amqp conn ok")
			connCloseNotify = (*connection).NotifyClose(make(chan *amqp.Error, 1))
		}

		if (*channel).IsClosed() {
			logger.Info("retry amqp channel start")
			ch, err := (*connection).Channel()
			if err != nil {
				logger.Error("retry amqp channel fail: %v", err)
				return err
			}
			(*channel) = ch
			logger.Info("retry amqp channel ok")
			chCloseNotify = (*channel).NotifyClose(make(chan *amqp.Error, 1))
		}

		logger.Info("retry setup amqp start")
		err := setupAmqp(*channel)
		if err != nil {
			logger.Error("retry setup amqp fail: %v", err)
			return err
		}
		logger.Info("retry setup amqp ok")

		retry = 0
		return nil
	}
}
