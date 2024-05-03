package rabbit

import (
	"github.com/KScaesar/art"
	amqp "github.com/rabbitmq/amqp091-go"
)

func NewIngress(amqpMsg *amqp.Delivery) *art.Message {
	message := art.GetMessage()

	message.Subject = amqpMsg.RoutingKey
	message.Bytes = amqpMsg.Body
	message.SetMsgId(amqpMsg.MessageId)
	message.RawInfra = amqpMsg
	return message
}

func NewBodyEgressWithRoutingKey(routingKey string, body any) *art.Message {
	message := art.GetMessage()

	message.Subject = routingKey
	message.Body = body
	return message
}
