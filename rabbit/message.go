package rabbit

import (
	"github.com/KScaesar/Artifex"
	amqp "github.com/rabbitmq/amqp091-go"
)

func NewIngress(amqpMsg *amqp.Delivery) *Artifex.Message {
	message := Artifex.GetMessage()

	message.Subject = amqpMsg.RoutingKey
	message.Bytes = amqpMsg.Body
	message.SetMsgId(amqpMsg.MessageId)
	message.RawInfra = amqpMsg
	return message
}

func NewBodyEgressWithRoutingKey(routingKey string, body any) *Artifex.Message {
	message := Artifex.GetMessage()

	message.Subject = routingKey
	message.Body = body
	return message
}
