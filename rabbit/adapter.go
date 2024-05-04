package rabbit

import (
	"errors"

	"github.com/KScaesar/art"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer = art.Producer
type Consumer = art.Consumer

//

type ProducerFactory struct {
	Pool            ConnectionPool
	Hub             art.AdapterHub
	Logger          art.Logger
	SendPingSeconds int
	MaxRetrySeconds int

	SetupAmqp    SetupAmqpAll
	ProducerName string

	EgressMux       *EgressMux
	DecorateAdapter func(adapter art.IAdapter) (application art.IAdapter)
	Lifecycle       func(lifecycle *art.Lifecycle)
}

func (f *ProducerFactory) CreateProducer() (producer Producer, err error) {
	connection, err := f.Pool.GetConnection()
	if err != nil {
		return nil, err
	}

	channel, err := (*connection).Channel()
	if err != nil {
		return nil, err
	}

	err = f.SetupAmqp.Execute(channel)
	if err != nil {
		return nil, err
	}

	opt := art.NewAdapterOption().
		RawInfra(&channel).
		Identifier(f.ProducerName).
		AdapterHub(f.Hub).
		Logger(f.Logger).
		EgressMux(f.EgressMux).
		DecorateAdapter(f.DecorateAdapter).
		Lifecycle(f.Lifecycle)

	waitPong := art.NewWaitPingPong()
	sendPing := func(adp art.IAdapter) error {
		defer waitPong.Ack()
		if channel.IsClosed() {
			return errors.New("amqp publisher is closed")
		}
		return nil
	}
	opt.SendPing(f.SendPingSeconds, waitPong, sendPing)

	opt.RawStop(func(logger art.Logger) (err error) {
		err = channel.Close()
		if err != nil {
			logger.Error("stop: %v", err)
			return
		}
		return nil
	})

	doFixup := fixup(connection, &channel, f.Pool, f.SetupAmqp.Execute)
	opt.RawFixup(f.MaxRetrySeconds, doFixup)

	adp, err := opt.Build()
	if err != nil {
		return
	}
	return adp.(Producer), err
}

type ConsumerFactory struct {
	Pool            ConnectionPool
	Hub             art.AdapterHub
	Logger          art.Logger
	MaxRetrySeconds int

	SetupAmqp    SetupAmqpAll
	NewConsumer  func(ch *amqp.Channel) (<-chan amqp.Delivery, error)
	ConsumerName string

	IngressMux      *IngressMux
	DecorateAdapter func(adapter art.IAdapter) (application art.IAdapter)
	Lifecycle       func(lifecycle *art.Lifecycle)
}

func (f *ConsumerFactory) CreateConsumer() (Consumer, error) {
	connection, err := f.Pool.GetConnection()
	if err != nil {
		return nil, err
	}

	channel, err := (*connection).Channel()
	if err != nil {
		return nil, err
	}

	err = f.SetupAmqp.Execute(channel)
	if err != nil {
		return nil, err
	}

	consumer, err := f.NewConsumer(channel)
	if err != nil {
		return nil, err
	}

	opt := art.NewAdapterOption().
		Identifier(f.ConsumerName).
		AdapterHub(f.Hub).
		Logger(f.Logger).
		IngressMux(f.IngressMux).
		DecorateAdapter(f.DecorateAdapter).
		Lifecycle(f.Lifecycle)

	consumerIsClose := false
	opt.RawRecv(func(logger art.Logger) (*art.Message, error) {
		amqpMsg, ok := <-consumer
		if !ok {
			consumerIsClose = true
			err := errors.New("amqp consumer close")
			logger.Error("recv: %v", err)
			return nil, err
		}
		return NewIngress(&amqpMsg), nil
	})

	opt.RawStop(func(logger art.Logger) (err error) {
		err = channel.Close()
		if err != nil {
			logger.Error("stop: %v", err)
			return
		}
		return nil
	})

	doFixup := fixup(connection, &channel, f.Pool, f.SetupAmqp.Execute)
	opt.RawFixup(f.MaxRetrySeconds, func(adp art.IAdapter) error {
		if err := doFixup(adp); err != nil {
			return err
		}
		if consumerIsClose {
			logger := adp.Log()

			logger.Info("retry amqp consumer start")
			freshConsumer, err := f.NewConsumer(channel)
			if err != nil {
				logger.Error("retry amqp consumer fail: %v", err)
				return err
			}
			logger.Info("retry amqp consumer success")
			consumerIsClose = false
			consumer = freshConsumer
			return nil
		}
		return nil
	})

	adp, err := opt.Build()
	if err != nil {
		return nil, err
	}
	return adp.(Consumer), nil
}
