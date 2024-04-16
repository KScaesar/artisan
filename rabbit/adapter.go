package rabbit

import (
	"sync"

	"github.com/KScaesar/Artifex"
	amqp "github.com/rabbitmq/amqp091-go"
)

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

type Factory struct {
	NewMux func() (ingressMux *IngressMux, egressMux *EgressMux)

	PubHub *PublisherHub
	SubHub *SubscriberHub

	NewLifecycle func() (*Artifex.Lifecycle, error)
}

func (f *Factory) CreatePublisher() (Publisher, error) {

	_, egressMux := f.NewMux()

	waitNotify := make(chan error, 1)
	opt := Artifex.NewPublisherOption[Egress]().
		Identifier("").
		SendPing(func() error { return nil }, waitNotify, 30)

	var mu sync.Mutex
	opt.AdapterSend(func(adp Artifex.IAdapter, egress *Egress) error {
		mu.Lock()
		defer mu.Unlock()

		err := egressMux.HandleMessage(egress, nil)
		if err != nil {
			return err
		}
		return nil
	})

	opt.AdapterStop(func(adp Artifex.IAdapter, _ *Egress) error {
		return nil
	})

	opt.AdapterFixup(0, func(adp Artifex.IAdapter) error {
		return nil
	})

	lifecycle, err := f.NewLifecycle()
	if err != nil {
		return nil, err
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

func (f *Factory) CreateSubscriber() (Subscriber, error) {

	ingressMux, _ := f.NewMux()

	waitNotify := make(chan error, 1)
	opt := Artifex.NewSubscriberOption[Ingress]().
		Identifier("").
		HandleRecv(ingressMux.HandleMessage).
		SendPing(func() error { return nil }, waitNotify, 30)

	opt.AdapterRecv(func(adp Artifex.IAdapter) (*Ingress, error) {
		return NewIngress(amqp.Delivery{}), nil
	})

	opt.AdapterStop(func(adp Artifex.IAdapter, _ *struct{}) error {
		return nil
	})

	opt.AdapterFixup(0, func(adp Artifex.IAdapter) error {
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
