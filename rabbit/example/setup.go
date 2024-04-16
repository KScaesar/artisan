package main

import (
	"context"
	"fmt"
	"time"

	"github.com/KScaesar/Artifex"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/KScaesar/Artifex-Adapter/rabbit"
)

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
				message.RoutingKey,
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain",
					MessageId:   message.MsgId(),
					Body:        message.ByteBody,
				},
			)
		})

		mux.Handler("key2.{action}.Game", func(message *rabbit.Egress, route *Artifex.RouteParam) error {
			return (*channel).PublishWithContext(
				ctx,
				"test-ex2",
				message.RoutingKey,
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain",
					MessageId:   message.MsgId(),
					Body:        message.ByteBody,
				},
			)
		})

		fmt.Println()
		for _, v := range mux.Endpoints() {
			fmt.Printf("[Rabbit Egress] RoutingKey=%-40q f=%q\n", v[0], v[1])
		}

		return mux
	}
}

func NewIngressMux() func() *rabbit.IngressMux {
	mux := rabbit.NewIngressMux()

	mux.Handler("key1-hello", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("key1-hello")
		return nil
	})
	mux.Handler("key1-world", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("key1-world")
		return nil
	})

	mux.Handler("key2.Created.Game", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("key2.Created.Game")
		return nil
	})
	mux.Handler("key2.Restarted.Game", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("key2.Restarted.Game")
		return nil
	})

	fmt.Println()
	for _, v := range mux.Endpoints() {
		fmt.Printf("[Rabbit Ingress] RoutingKey=%-40q f=%q\n", v[0], v[1])
	}

	return func() *rabbit.IngressMux {
		return mux
	}
}

func Case1SetupAmqp() rabbit.SetupAmqp {
	return rabbit.SetupAmqp{
		SetupQos:      SetupQos(1),
		SetupExchange: SetupEx("test-ex1", "direct"),
		SetupQueue:    SetupTemporaryQueue("test-q1", 10*time.Second),
		SetupBind:     SetupBind("test-q1", "test-ex1", []string{"key1-hello", "key1-world"}),
	}
}

func Case2SetupAmqp() rabbit.SetupAmqp {
	return rabbit.SetupAmqp{
		SetupQos:      SetupQos(1),
		SetupExchange: SetupEx("test-ex2", "topic"),
		SetupQueue:    SetupQueue("test-q2"),
		SetupBind:     SetupTemporaryBind("test-q2", "test-ex2", []string{"key2.*.Game"}, 10*time.Second),
	}
}

//

func SetupQos(prefetchCount int) func(channel *amqp.Channel) error {
	return func(channel *amqp.Channel) error {
		return channel.Qos(
			prefetchCount,
			0,
			false)
	}
}

func SetupEx(name, kind string) func(channel *amqp.Channel) error {
	return func(channel *amqp.Channel) error {
		return channel.ExchangeDeclare(
			name,
			kind,
			true,
			false,
			false,
			false,
			nil,
		)
	}
}

func SetupQueue(name string) func(channel *amqp.Channel) error {
	return func(channel *amqp.Channel) error {
		_, err := channel.QueueDeclare(
			name,
			true,
			false,
			false,
			false,
			nil,
		)
		return err
	}
}

func SetupTemporaryQueue(name string, ttl time.Duration) func(channel *amqp.Channel) error {
	queueArgs := make(amqp.Table)
	queueArgs[amqp.QueueTTLArg] = ttl.Milliseconds()
	return func(channel *amqp.Channel) error {
		_, err := channel.QueueDeclare(
			name,
			false,
			false,
			false,
			false,
			queueArgs,
		)
		return err
	}
}

func SetupBind(queueName, exName string, bindingKeys []string) func(channel *amqp.Channel) error {
	return func(channel *amqp.Channel) error {
		for _, key := range bindingKeys {
			err := channel.QueueBind(
				queueName,
				key,
				exName,
				false,
				nil,
			)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func SetupTemporaryBind(queueName, exName string, bindingKeys []string, ttl time.Duration) func(channel *amqp.Channel) error {
	bindArgs := make(amqp.Table)
	bindArgs[amqp.QueueTTLArg] = ttl.Milliseconds()
	return func(channel *amqp.Channel) error {
		for _, key := range bindingKeys {
			err := channel.QueueBind(
				queueName,
				key,
				exName,
				false,
				bindArgs,
			)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func NewConsumer(queueName, consumerName string, autoAck bool) func(*amqp.Channel) (<-chan amqp.Delivery, error) {
	return func(channel *amqp.Channel) (<-chan amqp.Delivery, error) {
		return channel.Consume(
			queueName,
			consumerName,
			autoAck,
			false,
			false,
			false,
			nil,
		)
	}
}
