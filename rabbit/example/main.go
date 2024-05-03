package main

import (
	"fmt"
	"time"

	"github.com/KScaesar/art"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/KScaesar/artisan/rabbit"
)

func main() {
	art.SetDefaultLogger(art.NewLogger(false, art.LogLevelDebug))

	url := "amqp://guest:guest@127.0.0.1:5672"
	pool := rabbit.NewConnectionPool(url, 2)

	consumers := NewConsumers(pool)
	producer := NewProducer(pool)
	fireMessage(producer)

	art.NewShutdown().
		StopService("amqp_producer", func() error {
			producer.Stop()
			return nil
		}).
		StopService("amqp_consumers", func() error {
			consumers.Shutdown()
			return nil
		}).
		Serve(nil)
}

func NewConsumers(pool rabbit.ConnectionPool) *art.Hub {
	mux := NewIngressMux()
	hub := art.NewHub()

	consumers := []*rabbit.ConsumerFactory{
		{
			Pool: pool,
			Hub:  hub,
			SetupAmqp: []rabbit.SetupAmqp{
				SetupQos(1),
				SetupExchange("test-ex1", "direct"),
				SetupTemporaryQueue("test-q1", 10*time.Second),
				SetupBind("test-q1", "test-ex1", []string{"key1-hello", "key1-world"}),
			},
			NewConsumer:  NewConsumer("test-q1", "test-c1", true),
			ConsumerName: "test-c1",
			IngressMux:   mux,
			Lifecycle:    Lifecycle(),
		},
		{
			Pool:            pool,
			Hub:             hub,
			MaxRetrySeconds: 0,
			SetupAmqp: []rabbit.SetupAmqp{
				SetupQos(1),
				SetupExchange("test-ex2", "topic"),
				SetupQueue("test-q2"),
				SetupTemporaryBind("test-q2", "test-ex2", []string{"key2.*.Game"}, 10*time.Second),
			},
			NewConsumer:  NewConsumer("test-q2", "test-c2", true),
			ConsumerName: "test-c2",
			IngressMux:   mux,
			Lifecycle:    Lifecycle(),
		},
	}

	for _, factory := range consumers {
		_, err := factory.CreateConsumer()
		if err != nil {
			panic(err)
		}
	}

	hub.DoAsync(func(adapter art.IAdapter) {
		consumer := adapter.(rabbit.Consumer)
		consumer.Listen()
	})

	return hub
}

func NewProducer(pool rabbit.ConnectionPool) rabbit.Producer {
	mux := NewEgressMux()

	producerFactory := &rabbit.ProducerFactory{
		Pool:            pool,
		SendPingSeconds: 20,
		MaxRetrySeconds: 0,
		SetupAmqp: []rabbit.SetupAmqp{
			SetupExchange("test-ex1", "direct"),
			SetupTemporaryQueue("test-q1", 10*time.Second),
			SetupBind("test-q1", "test-ex1", []string{"key1-hello", "key1-world"}),

			SetupExchange("test-ex2", "topic"),
			SetupQueue("test-q2"),
			SetupTemporaryBind("test-q2", "test-ex2", []string{"key2.*.Game"}, 10*time.Second),
		},
		ProducerName: "test-p",
		EgressMux:    mux,
		Lifecycle:    Lifecycle(),
	}

	producer, err := producerFactory.CreateProducer()
	if err != nil {
		panic(err)
	}

	return producer
}

func NewIngressMux() *rabbit.IngressMux {
	mux := rabbit.NewIngressMux().
		EnableMessagePool().
		ErrorHandler(art.UsePrintResult(false, nil)).
		Middleware(
			art.UseRecover(),
			art.UseLogger(true, art.SafeConcurrency_Skip),
			art.UseAdHocFunc(func(message *art.Message, dep any) error {
				logger := art.CtxGetLogger(message.Ctx, dep)
				logger.Info("recv %q", message.Subject)
				return nil
			}).PreMiddleware(),
		)

	mux.Handler("key1-hello", art.UsePrintDetail())
	mux.Handler("key1-world", art.UsePrintDetail())
	mux.Handler("key2.{action}.Game", art.UsePrintDetail())
	return mux
}

func NewEgressMux() *rabbit.EgressMux {
	mux := rabbit.NewEgressMux().
		ErrorHandler(art.UsePrintResult(true, nil)).
		Middleware(
			art.UseRecover(),
			art.UseLogger(true, art.SafeConcurrency_Skip),
			rabbit.UseEncodeJson(),
		)

	mux.Group("key1-").
		DefaultHandler(func(message *art.Message, dep any) error {
			channel := dep.(rabbit.Producer).RawInfra().(**amqp.Channel)
			return (*channel).PublishWithContext(
				message.Ctx,
				"test-ex1",
				message.Subject,
				false,
				false,
				amqp.Publishing{
					MessageId: message.MsgId(),
					Body:      message.Bytes,
				},
			)
		})

	mux.Handler("key2.{action}.Game", func(message *art.Message, dep any) error {
		channel := dep.(rabbit.Producer).RawInfra().(**amqp.Channel)
		return (*channel).PublishWithContext(
			message.Ctx,
			"test-ex2",
			message.Subject,
			false,
			false,
			amqp.Publishing{
				MessageId: message.MsgId(),
				Body:      message.Bytes,
			},
		)
	})

	return mux
}

func fireMessage(producer rabbit.Producer) {
	messages := []*art.Message{
		rabbit.NewBodyEgressWithRoutingKey("key1-hello", map[string]any{
			"detail":      "hello everyone!",
			"sender":      "Fluffy Bunny",
			"description": "This is a friendly greeting message.",
		}),
		rabbit.NewBodyEgressWithRoutingKey("key1-world", map[string]any{
			"detail":      "Beautiful World",
			"sender":      "Sneaky Cat",
			"description": "This is another cheerful greeting.",
		}),
		rabbit.NewBodyEgressWithRoutingKey("key2.Created.Game", map[string]any{
			"detail":      "A new game has been created!",
			"creator":     "GameMaster123",
			"description": "This message indicates the creation of a new game.",
		}),
		rabbit.NewBodyEgressWithRoutingKey("key2.Restarted.Game", map[string]any{
			"detail":      "Game restarted successfully.",
			"admin":       "AdminPlayer",
			"description": "The game has been restarted by the admin.",
		}),
	}

	for i := range messages {
		i := i
		go func() {
			err := producer.Send(messages[i])
			if err != nil {
				fmt.Println("producer send fail:", err)
			}
		}()
	}
}

func Lifecycle() func(lifecycle *art.Lifecycle) {
	return func(lifecycle *art.Lifecycle) {

		lifecycle.OnConnect(func(adp art.IAdapter) error {
			amqpId := art.GenerateRandomCode(6)
			amqpName := adp.Identifier()

			logger := adp.Log().
				WithKeyValue("amqp_id", amqpId).
				WithKeyValue("amqp_name", amqpName)

			logger.Info("  >> connect <<")
			adp.SetLog(logger)
			return nil
		})

		lifecycle.OnDisconnect(func(adp art.IAdapter) {
			adp.Log().Info("  >> disconnect <<")
		})
	}
}
