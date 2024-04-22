package main

import (
	"context"
	"fmt"
	"time"

	"github.com/KScaesar/Artifex"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/KScaesar/Artifex-Adapter/rabbit"
)

func main() {
	Artifex.SetDefaultLogger(Artifex.NewLogger(false, Artifex.LogLevelDebug))

	url := "amqp://guest:guest@127.0.0.1:5672"
	pool := rabbit.NewConnectionPool(url, 2)

	subHub := NewSubscribers(pool)

	pub := NewPublisher(pool)
	fireMessage(pub)

	Artifex.NewShutdown().
		SetStopAction("amqp_pub", func() error {
			pub.Stop()
			return nil
		}).
		SetStopAction("amqp_sub", func() error {
			subHub.Shutdown()
			return nil
		}).
		Listen(nil)
}

func NewSubscribers(pool rabbit.ConnectionPool) *Artifex.Hub[Artifex.IAdapter] {
	mux := NewIngressMux()
	hub := rabbit.NewAdapterHub()

	subFactories := []*rabbit.SubscriberFactory{
		{
			Pool: pool,
			SetupAmqp: []rabbit.SetupAmqp{
				SetupQos(1),
				SetupExchange("test-ex1", "direct"),
				SetupTemporaryQueue("test-q1", 10*time.Second),
				SetupBind("test-q1", "test-ex1", []string{"key1-hello", "key1-world"}),
			},
			IngressMux:      mux,
			NewConsumer:     NewConsumer("test-q1", "test-c1", true),
			ConsumerName:    "test-c1",
			Hub:             hub,
			DecorateAdapter: DecorateAdapter,
		},
		{
			Pool: pool,
			SetupAmqp: []rabbit.SetupAmqp{
				SetupQos(1),
				SetupExchange("test-ex2", "topic"),
				SetupQueue("test-q2"),
				SetupTemporaryBind("test-q2", "test-ex2", []string{"key2.*.Game"}, 10*time.Second),
			},
			IngressMux:      mux,
			NewConsumer:     NewConsumer("test-q2", "test-c2", true),
			ConsumerName:    "test-c2",
			Hub:             hub,
			DecorateAdapter: DecorateAdapter,
		},
	}

	for _, factory := range subFactories {
		_, err := factory.CreateSubscriber()
		if err != nil {
			panic(err)
		}
	}

	hub.DoAsync(func(adapter Artifex.IAdapter) {
		subscriber := adapter.(rabbit.Subscriber)
		subscriber.Listen()
	})

	return hub
}

func NewIngressMux() *rabbit.IngressMux {
	mux := rabbit.NewIngressMux()

	mux.Handler("key1-hello", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("print key1-hello: %v", string(message.Body))
		return nil
	})
	mux.Handler("key1-world", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("print key1-world: %v", string(message.Body))
		return nil
	})

	mux.Handler("key2.Created.Game", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("print key2.Created.Game: %v", string(message.Body))
		return nil
	})
	mux.Handler("key2.Restarted.Game", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("print key2.Restarted.Game: %v", string(message.Body))
		return nil
	})

	fmt.Println()
	for _, v := range mux.Endpoints() {
		fmt.Printf("[Rabbit Ingress] Subject=%-40q f=%q\n", v[0], v[1])
	}

	return mux
}

func NewPublisher(pool rabbit.ConnectionPool) rabbit.Publisher {
	hub := rabbit.NewAdapterHub()

	pubFactory := &rabbit.PublisherFactory{
		Pool: pool,
		SetupAmqp: []rabbit.SetupAmqp{
			SetupExchange("test-ex1", "direct"),
			SetupTemporaryQueue("test-q1", 10*time.Second),
			SetupBind("test-q1", "test-ex1", []string{"key1-hello", "key1-world"}),

			SetupExchange("test-ex2", "topic"),
			SetupQueue("test-q2"),
			SetupTemporaryBind("test-q2", "test-ex2", []string{"key2.*.Game"}, 10*time.Second),
		},
		ProducerName:    "example_pub",
		NewEgressMux:    NewEgressMux(),
		Hub:             hub,
		SendPingSeconds: 30,
		DecorateAdapter: DecorateAdapter,
	}

	pub, err := pubFactory.CreatePublisher()
	if err != nil {
		panic(err)
	}

	return pub
}

func NewEgressMux() func(ch **amqp.Channel) *rabbit.EgressMux {
	ctx := context.Background()

	return func(channel **amqp.Channel) *rabbit.EgressMux {
		mux := rabbit.NewEgressMux().
			Middleware(rabbit.EncodeJson().PreMiddleware())

		key1 := mux.Group("key1-")
		key1.SetDefaultHandler(func(message *rabbit.Egress, route *Artifex.RouteParam) error {
			return (*channel).PublishWithContext(
				ctx,
				"test-ex1",
				message.Subject,
				false,
				false,
				amqp.Publishing{
					MessageId: message.MsgId(),
					Body:      message.Body,
				},
			)
		})

		mux.Handler("key2.{action}.Game", func(message *rabbit.Egress, route *Artifex.RouteParam) error {
			return (*channel).PublishWithContext(
				ctx,
				"test-ex2",
				message.Subject,
				false,
				false,
				amqp.Publishing{
					MessageId: message.MsgId(),
					Body:      message.Body,
				},
			)
		})

		fmt.Println()
		for _, v := range mux.Endpoints() {
			fmt.Printf("[Rabbit Egress] Subject=%-40q f=%q\n", v[0], v[1])
		}

		return mux
	}
}

func fireMessage(pub rabbit.Publisher) {
	messages := []*rabbit.Egress{
		rabbit.NewEgress("key1-hello", map[string]any{
			"detail":      "hello everyone!",
			"sender":      "Fluffy Bunny",
			"description": "This is a friendly greeting message.",
		}),
		rabbit.NewEgress("key1-world", map[string]any{
			"detail":      "Beautiful World",
			"sender":      "Sneaky Cat",
			"description": "This is another cheerful greeting.",
		}),
		rabbit.NewEgress("key2.Created.Game", map[string]any{
			"detail":      "A new game has been created!",
			"creator":     "GameMaster123",
			"description": "This message indicates the creation of a new game.",
		}),
		rabbit.NewEgress("key2.Restarted.Game", map[string]any{
			"detail":      "Game restarted successfully.",
			"admin":       "AdminPlayer",
			"description": "The game has been restarted by the admin.",
		}),
	}

	for i := range messages {
		i := i
		go func() {
			err := pub.Send(messages[i])
			if err != nil {
				fmt.Println("pub send fail:", err)
			}
		}()
	}
}

func DecorateAdapter(adp Artifex.IAdapter) Artifex.IAdapter {
	amqpId := Artifex.GenerateRandomCode(6)
	amqpName := adp.Identifier()

	logger := adp.Log().
		WithKeyValue("amqp_id", amqpId).
		WithKeyValue("amqp_name", amqpName)
	adp.SetLog(logger)
	return adp
}
