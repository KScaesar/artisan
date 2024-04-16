package rabbit

import (
	"errors"

	"github.com/KScaesar/Artifex"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SetupAmqp struct {
	SetupQos      func(ch *amqp.Channel) error
	SetupExchange func(ch *amqp.Channel) error
	SetupQueue    func(ch *amqp.Channel) error
	SetupBind     func(ch *amqp.Channel) error
}

func (fn *SetupAmqp) Execute(channel *amqp.Channel) error {
	if fn.SetupExchange != nil {
		if err := fn.SetupQos(channel); err != nil {
			return err
		}
	}
	if fn.SetupExchange != nil {
		if err := fn.SetupExchange(channel); err != nil {
			return err
		}
	}
	if fn.SetupQueue != nil {
		if err := fn.SetupQueue(channel); err != nil {
			return err
		}
	}
	if fn.SetupBind != nil {
		if err := fn.SetupBind(channel); err != nil {
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

type PublisherHub = Artifex.Hub[Publisher]

func NewPublisherHub() *PublisherHub {
	stop := func(publisher Publisher) {
		publisher.Stop()
	}
	return Artifex.NewHub(stop)
}

type SubscriberHub = Artifex.Hub[Subscriber]

func NewSubscriberHub() *SubscriberHub {
	stop := func(subscriber Subscriber) {
		subscriber.Stop()
	}
	return Artifex.NewHub(stop)
}

//

type PublisherFactory struct {
	Pool         ConnectionPool
	SetupAll     []SetupAmqp
	NewEgressMux func(ch **amqp.Channel) *EgressMux
	ProducerName string
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

	for _, setupAmqp := range f.SetupAll {
		err := setupAmqp.Execute(channel)
		if err != nil {
			return nil, err
		}
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
	opt.AdapterSend(func(adp Artifex.IAdapter, egress *Egress) error {
		err := egressMux.HandleMessage(egress, nil)
		if err != nil {
			logger.Error("send message:", err)
			return err
		}
		return nil
	})

	opt.AdapterStop(func(adp Artifex.IAdapter, _ *Egress) error {
		logger.Info("active stop")
		return channel.Close()
	})

	fixup := fixupAmqp(connection, &channel, logger, f.Pool, func(channel *amqp.Channel) error {
		for _, setupAmqp := range f.SetupAll {
			err := setupAmqp.Execute(channel)
			if err != nil {
				return err
			}
		}
		return nil
	})
	opt.AdapterFixup(0, func(adp Artifex.IAdapter) error {
		if adp.IsStop() {
			return nil
		}
		return fixup()
	})

	lifecycle, err := f.NewLifecycle()
	if err != nil {
		return nil, err
	}

	opt.NewLifecycle(func() *Artifex.Lifecycle { return lifecycle })

	return opt.BuildPublisher()
}

type SubscriberFactory struct {
	Pool          ConnectionPool
	Setup         SetupAmqp
	NewIngressMux func() *IngressMux
	NewConsumer   func(ch *amqp.Channel) (<-chan amqp.Delivery, error)
	ConsumerName  string
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

	if err := f.Setup.Execute(channel); err != nil {
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
			if adp.IsStop() {
				return nil, nil
			}
			consumerIsClose = true
			err := errors.New("amqp consumer close")
			logger.Error("%v", err)
			return nil, err
		}
		return NewIngress(amqpMsg, logger), nil
	})

	opt.AdapterStop(func(adp Artifex.IAdapter, _ *struct{}) error {
		logger.Info("active stop")
		return channel.Close()
	})

	fixup := fixupAmqp(connection, &channel, logger, f.Pool, f.Setup.Execute)
	opt.AdapterFixup(0, func(adp Artifex.IAdapter) error {
		if adp.IsStop() {
			return nil
		}

		if err := fixup(); err != nil {
			return err
		}
		if consumerIsClose {
			logger.Info("retry amqp consumer start")
			if err := f.Setup.Execute(channel); err != nil {
				logger.Error("retry setup amqp fail: %v", err)
				return err
			}
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

	lifecycle, err := f.NewLifecycle()
	if err != nil {
		return nil, err
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
	setupAmqp func(channel *amqp.Channel) error,
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

		logger.Info("retry rebuild amqp start")
		err := setupAmqp(*channel)
		if err != nil {
			logger.Error("retry rebuild amqp fail: %v", err)
			return err
		}
		logger.Info("retry rebuild amqp success")
		retryCnt = 0
		return nil
	}
}
