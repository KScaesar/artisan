package main

import (
	"github.com/KScaesar/Artifex"

	"github.com/KScaesar/Artifex-Adapter/rabbit"
)

func main() {
	url := "amqp://guest:guest@127.0.0.1:5672"
	pool := rabbit.NewSingleConnection(url)

	subscriberHub := rabbit.NewSubscriberHub()
	newIngressMux := NewIngressMux()
	subFactories := []*rabbit.SubscriberFactory{
		{
			Pool:          pool,
			Setup:         Case1SetupAmqp(),
			NewIngressMux: newIngressMux,
			NewConsumer:   NewConsumer("test-q1", "test-c1", true),
			ConsumerName:  "test-c1",
			SubHub:        subscriberHub,
			NewLifecycle: func() (*Artifex.Lifecycle, error) {
				return &Artifex.Lifecycle{}, nil
			},
		},
		{
			Pool:          pool,
			Setup:         Case2SetupAmqp(),
			NewIngressMux: newIngressMux,
			NewConsumer:   NewConsumer("test-q2", "test-c2", true),
			ConsumerName:  "test-c2",
			SubHub:        subscriberHub,
			NewLifecycle: func() (*Artifex.Lifecycle, error) {
				return &Artifex.Lifecycle{}, nil
			},
		},
	}

	for _, factory := range subFactories {
		_, err := factory.CreateSubscriber()
		if err != nil {
			panic(err)
		}
	}

	subscriberHub.DoAsync(func(s rabbit.Subscriber) {
		s.Listen()
	})

	pubFactory := &rabbit.PublisherFactory{
		Pool:         pool,
		SetupAll:     []rabbit.SetupAmqp{Case1SetupAmqp(), Case2SetupAmqp()},
		NewEgressMux: NewEgressMux(),
		ProducerName: "example_pub",
		NewLifecycle: func() (*Artifex.Lifecycle, error) {
			return &Artifex.Lifecycle{}, nil
		},
	}

	pub, err := pubFactory.CreatePublisher()
	if err != nil {
		panic(err)
	}

	fireMessage(pub)

	Artifex.NewShutdown().
		SetStopAction("pub", func() error {
			return pub.Stop()
		}).
		SetStopAction("sub_hub", func() error {
			subscriberHub.StopAll()
			return nil
		}).
		Listen(nil)
}

func fireMessage(pub rabbit.Publisher) {

}
