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

func MergeSetupAmqp(all ...SetupAmqp) SetupAmqpAll {
	result := make(SetupAmqpAll, 0, len(all))
	for _, setupAmqp := range all {
		result = append(result, setupAmqp)
	}
	return result
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

type PublisherHub = Artifex.Hub[Publisher]

func NewPublisherHub() *PublisherHub {
	stop := func(publisher Publisher) {
		publisher.Stop()
	}
	hub := Artifex.NewHub(stop)
	return hub
}

type SubscriberHub = Artifex.Hub[Subscriber]

func NewSubscriberHub() *SubscriberHub {
	stop := func(subscriber Subscriber) {
		subscriber.Stop()
	}
	hub := Artifex.NewHub(stop)
	return hub
}

//

type PublisherFactory struct {
	Pool         ConnectionPool
	SetupAll     SetupAmqpAll
	ProducerName string
	NewEgressMux func(ch **amqp.Channel) *EgressMux
	PubHub       *PublisherHub
	NewLifecycle func() (*Artifex.Lifecycle, error)
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

	err = f.SetupAll.Execute(channel)
	if err != nil {
		return nil, err
	}

	logger := Artifex.DefaultLogger().
		WithKeyValue("amqp_id", Artifex.GenerateRandomCode(6)).
		WithKeyValue("amqp_pub", f.ProducerName)
	logger.Info("create amqp publisher success!")

	waitNotify := make(chan error, 1)
	opt := Artifex.NewPublisherOption[Egress]().
		Identifier(f.ProducerName).
		SendPing(func() error {
			defer func() {
				waitNotify <- nil
			}()
			if channel.IsClosed() {
				return errors.New("amqp publisher is closed")
			}
			return nil
		}, waitNotify, 30)

	egressMux := f.NewEgressMux(&channel)
	opt.AdapterSend(func(adp Artifex.IAdapter, message *Egress) error {
		err := egressMux.HandleMessage(message, nil)
		if err != nil {
			logger.WithKeyValue("msg_id", message.MsgId()).Error("send %q: %v", message.RoutingKey, err)
			return err
		}
		logger.WithKeyValue("msg_id", message.MsgId()).Info("send %q success", message.RoutingKey)
		return nil
	})

	opt.AdapterStop(func(adp Artifex.IAdapter) error {
		logger.Info("active stop")
		return channel.Close()
	})

	fixup := fixupAmqp(connection, &channel, logger, f.Pool, f.SetupAll.Execute)
	opt.AdapterFixup(0, func(adp Artifex.IAdapter) error {
		return fixup()
	})

	var lifecycle *Artifex.Lifecycle
	if f.NewLifecycle != nil {
		lifecycle, err = f.NewLifecycle()
		if err != nil {
			return nil, err
		}
	} else {
		lifecycle = new(Artifex.Lifecycle)
	}
	lifecycle.OnOpen(
		func(adp Artifex.IAdapter) error {
			err := f.PubHub.Join(adp.Identifier(), adp.(Publisher))
			if err != nil {
				return err
			}
			lifecycle.OnStop(func(adp Artifex.IAdapter) {
				go f.PubHub.RemoveOne(func(pub Publisher) bool { return pub == adp })
			})
			return nil
		},
	)
	opt.NewLifecycle(func() *Artifex.Lifecycle { return lifecycle })

	return opt.BuildPublisher()
}

type SubscriberFactory struct {
	Pool          ConnectionPool
	SetupAll      SetupAmqpAll
	NewConsumer   func(ch *amqp.Channel) (<-chan amqp.Delivery, error)
	ConsumerName  string
	NewIngressMux func() *IngressMux
	SubHub        *SubscriberHub
	NewLifecycle  func() (*Artifex.Lifecycle, error)
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

	err = f.SetupAll.Execute(channel)
	if err != nil {
		return nil, err
	}

	consumer, err := f.NewConsumer(channel)
	if err != nil {
		return nil, err
	}

	logger := Artifex.DefaultLogger().
		WithKeyValue("amqp_id", Artifex.GenerateRandomCode(6)).
		WithKeyValue("amqp_sub", f.ConsumerName)
	logger.Info("create amqp subscriber success!")

	ingressMux := f.NewIngressMux()

	opt := Artifex.NewSubscriberOption[Ingress]().
		Identifier(f.ConsumerName).
		HandleRecv(ingressMux.HandleMessage)

	consumerIsClose := false
	opt.AdapterRecv(func(adp Artifex.IAdapter) (*Ingress, error) {
		amqpMsg, ok := <-consumer
		if !ok {
			consumerIsClose = true
			err := errors.New("amqp consumer close")
			logger.Error("%v", err)
			return nil, err
		}
		return NewIngress(amqpMsg, logger), nil
	})

	opt.AdapterStop(func(adp Artifex.IAdapter) error {
		logger.Info("active stop")
		return channel.Close()
	})

	fixup := fixupAmqp(connection, &channel, logger, f.Pool, f.SetupAll.Execute)
	opt.AdapterFixup(0, func(adp Artifex.IAdapter) error {
		if err := fixup(); err != nil {
			return err
		}
		if consumerIsClose {
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

	var lifecycle *Artifex.Lifecycle
	if f.NewLifecycle != nil {
		lifecycle, err = f.NewLifecycle()
		if err != nil {
			return nil, err
		}
	} else {
		lifecycle = new(Artifex.Lifecycle)
	}
	lifecycle.OnOpen(
		func(adp Artifex.IAdapter) error {
			err := f.SubHub.Join(adp.Identifier(), adp.(Subscriber))
			if err != nil {
				return err
			}
			lifecycle.OnStop(func(adp Artifex.IAdapter) {
				go f.SubHub.RemoveOne(func(sub Subscriber) bool { return sub == adp })
			})
			return nil
		},
	)
	opt.NewLifecycle(func() *Artifex.Lifecycle { return lifecycle })

	return opt.BuildSubscriber()
}

//

func fixupAmqp(
	connection **amqp.Connection,
	channel **amqp.Channel,
	logger Artifex.Logger,
	pool ConnectionPool,
	setupAmqp SetupAmqp,
) func() error {

	connCloseNotify := (*connection).NotifyClose(make(chan *amqp.Error, 1))
	chCloseNotify := (*channel).NotifyClose(make(chan *amqp.Error, 1))
	retryCnt := 0

	return func() error {

		select {
		case Err := <-connCloseNotify:
			if Err != nil {
				logger.Error("amqp connection close: %v", Err)
			}
		case Err := <-chCloseNotify:
			if Err != nil {
				logger.Error("amqp channel close: %v", Err)
			}
		default:
		}

		retryCnt++
		logger.Info("retry %v times", retryCnt)

		if (*connection).IsClosed() {
			logger.Info("retry amqp conn start")
			err := pool.ReConnection(connection)
			if err != nil {
				logger.Error("retry amqp conn fail: %v", err)
				return err
			}
			logger.Info("retry amqp conn success")
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
			logger.Info("retry amqp channel success")
			chCloseNotify = (*channel).NotifyClose(make(chan *amqp.Error, 1))
		}

		logger.Info("retry setup amqp start")
		err := setupAmqp(*channel)
		if err != nil {
			logger.Error("retry setup amqp fail: %v", err)
			return err
		}
		logger.Info("retry setup amqp success")
		retryCnt = 0
		return nil
	}
}
