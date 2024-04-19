package main

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SetupQos(prefetchCount int) func(channel *amqp.Channel) error {
	return func(channel *amqp.Channel) error {
		return channel.Qos(
			prefetchCount,
			0,
			false,
		)
	}
}

func SetupExchange(name, kind string) func(channel *amqp.Channel) error {
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
