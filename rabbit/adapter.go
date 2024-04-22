package rabbit

import (
	"errors"

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

//

type Publisher interface {
	Artifex.IAdapter
	Send(messages ...*Egress) error
}

type Subscriber interface {
	Artifex.IAdapter
	Listen() (err error)
}

//

func NewAdapterHub() *Artifex.Hub[Artifex.IAdapter] {
	hub := Artifex.NewAdapterHub(func(adp Artifex.IAdapter) {
		adp.Stop()
	})
	return hub
}

//

type PublisherFactory struct {
	Pool         ConnectionPool
	SetupAmqp    SetupAmqpAll
	ProducerName string

	NewEgressMux func(ch **amqp.Channel) *EgressMux
	Hub          *Artifex.Hub[Artifex.IAdapter]
	Logger       Artifex.Logger

	SendPingSeconds int
	DecorateAdapter func(old Artifex.IAdapter) (fresh Artifex.IAdapter)
	Lifecycle       func(lifecycle *Artifex.Lifecycle)
}

func (f *PublisherFactory) CreatePublisher() (Publisher, error) {
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

	waitNotify := make(chan error, 1)
	opt := Artifex.NewPublisherOption[Egress]().
		Identifier(f.ProducerName).
		Logger(f.Logger).
		AdapterHub(f.Hub).
		DecorateAdapter(f.DecorateAdapter).
		Lifecycle(f.Lifecycle).
		SendPing(func() error {
			defer func() {
				waitNotify <- nil
			}()
			if channel.IsClosed() {
				return errors.New("publisher is closed")
			}
			return nil
		}, waitNotify, f.SendPingSeconds*2)

	egressMux := f.NewEgressMux(&channel)
	opt.AdapterSend(func(adp Artifex.IAdapter, message *Egress) error {
		err := egressMux.HandleMessage(message, nil)
		logger := adp.Log().WithKeyValue("msg_id", message.MsgId())
		if err != nil {
			logger.Error("send %q: %v", message.Subject, err)
			return err
		}
		logger.Info("send %q", message.Subject)
		return nil
	})

	opt.AdapterStop(func(adp Artifex.IAdapter) error {
		err := channel.Close()
		if err != nil {
			adp.Log().Error("amqp stop: %v", err)
			return err
		}
		adp.Log().Info("amqp stop")
		return nil
	})

	fixup := fixupAmqp(connection, &channel, f.Pool, f.SetupAmqp.Execute)
	opt.AdapterFixup(0, func(adp Artifex.IAdapter) error { return fixup(adp) })

	adp, err := opt.Build()
	if err != nil {
		return nil, err
	}
	return adp.(Publisher), err
}

type SubscriberFactory struct {
	Pool         ConnectionPool
	SetupAmqp    SetupAmqpAll
	NewConsumer  func(ch *amqp.Channel) (<-chan amqp.Delivery, error)
	ConsumerName string

	IngressMux *IngressMux
	Hub        *Artifex.Hub[Artifex.IAdapter]
	Logger     Artifex.Logger

	SendPingSeconds int
	DecorateAdapter func(old Artifex.IAdapter) (fresh Artifex.IAdapter)
	Lifecycle       func(lifecycle *Artifex.Lifecycle)
}

func (f *SubscriberFactory) CreateSubscriber() (Subscriber, error) {
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

	opt := Artifex.NewSubscriberOption[Ingress]().
		Identifier(f.ConsumerName).
		Logger(f.Logger).
		AdapterHub(f.Hub).
		DecorateAdapter(f.DecorateAdapter).
		Lifecycle(f.Lifecycle).
		HandleRecv(f.IngressMux.HandleMessage)

	consumerIsClose := false
	opt.AdapterRecv(func(adp Artifex.IAdapter) (*Ingress, error) {
		amqpMsg, ok := <-consumer
		if !ok {
			consumerIsClose = true
			err := errors.New("amqp consumer close")
			adp.Log().Error("%v", err)
			return nil, err
		}
		return NewIngress(&amqpMsg, adp), nil
	})

	opt.AdapterStop(func(adp Artifex.IAdapter) error {
		err := channel.Close()
		if err != nil {
			adp.Log().Error("stop: %v", err)
			return err
		}
		adp.Log().Info("stop")
		return nil
	})

	fixup := fixupAmqp(connection, &channel, f.Pool, f.SetupAmqp.Execute)
	opt.AdapterFixup(0, func(adp Artifex.IAdapter) error {
		if err := fixup(adp); err != nil {
			return err
		}
		if consumerIsClose {
			adp.Log().Info("retry amqp consumer start")
			freshConsumer, err := f.NewConsumer(channel)
			if err != nil {
				adp.Log().Error("retry amqp consumer fail: %v", err)
				return err
			}
			adp.Log().Info("retry amqp consumer success")
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
	return adp.(Subscriber), err
}

//

func fixupAmqp(
	connection **amqp.Connection,
	channel **amqp.Channel,
	pool ConnectionPool,
	setupAmqp SetupAmqp,
) func(adp Artifex.IAdapter) error {

	connCloseNotify := (*connection).NotifyClose(make(chan *amqp.Error, 1))
	chCloseNotify := (*channel).NotifyClose(make(chan *amqp.Error, 1))
	retryCnt := 0

	return func(adp Artifex.IAdapter) error {

		select {
		case Err := <-connCloseNotify:
			if Err != nil {
				adp.Log().Error("amqp connection close: %v", Err)
				connCloseNotify = nil
			}
		case Err := <-chCloseNotify:
			if Err != nil {
				adp.Log().Error("amqp channel close: %v", Err)
				chCloseNotify = nil
			}
		default:
		}

		retryCnt++
		adp.Log().Info("retry %v times", retryCnt)

		if (*connection).IsClosed() {
			adp.Log().Info("retry amqp conn start")
			err := pool.ReConnection(connection)
			if err != nil {
				adp.Log().Error("retry amqp conn fail: %v", err)
				return err
			}
			adp.Log().Info("retry amqp conn success")
			connCloseNotify = (*connection).NotifyClose(make(chan *amqp.Error, 1))
		}

		if (*channel).IsClosed() {
			adp.Log().Info("retry amqp channel start")
			ch, err := (*connection).Channel()
			if err != nil {
				adp.Log().Error("retry amqp channel fail: %v", err)
				return err
			}
			(*channel) = ch
			adp.Log().Info("retry amqp channel success")
			chCloseNotify = (*channel).NotifyClose(make(chan *amqp.Error, 1))
		}

		adp.Log().Info("retry setup amqp start")
		err := setupAmqp(*channel)
		if err != nil {
			adp.Log().Error("retry setup amqp fail: %v", err)
			return err
		}
		adp.Log().Info("retry setup amqp success")
		retryCnt = 0
		return nil
	}
}
