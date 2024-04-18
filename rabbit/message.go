package rabbit

import (
	"github.com/gookit/goutil/maputil"

	"github.com/KScaesar/Artifex"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RoutingKey = string

//

func NewIngress(amqpMsg *amqp.Delivery, logger Artifex.Logger) *Ingress {
	msgId := amqpMsg.MessageId
	if msgId == "" {
		msgId = Artifex.GenerateRandomCode(12)
	}

	logger = logger.WithKeyValue("msg_id", msgId)
	logger.Info("recv %q", amqpMsg.RoutingKey)
	return &Ingress{
		MsgId:      msgId,
		ByteBody:   amqpMsg.Body,
		RoutingKey: amqpMsg.RoutingKey,
		AmqpMsg:    amqpMsg,
		Logger:     logger,
	}
}

type Ingress struct {
	MsgId    string
	ByteBody []byte

	RoutingKey RoutingKey
	AmqpMsg    *amqp.Delivery
	Logger     Artifex.Logger
}

type IngressHandleFunc = Artifex.HandleFunc[Ingress]
type IngressMiddleware = Artifex.Middleware[Ingress]
type IngressMux = Artifex.Mux[Ingress]

func NewIngressMux() *IngressMux {
	getRoutingKey := func(message *Ingress) string {
		return message.RoutingKey
	}

	mux := Artifex.NewMux(".", getRoutingKey)
	mux.SetHandleError(PrintIngressError())
	return mux
}

func IngressSkip() IngressHandleFunc {
	return func(_ *Ingress, _ *Artifex.RouteParam) (err error) {
		return nil
	}
}

//

func NewEgress(key RoutingKey, message any) *Egress {
	return &Egress{
		RoutingKey: key,
		Metadata:   make(map[string]any),
		AppMsg:     message,
	}
}

type Egress struct {
	msgId      string
	ByteBody   []byte
	StringBody string

	RoutingKey RoutingKey
	Metadata   maputil.Data
	AppMsg     any
}

func (e *Egress) MsgId() string {
	if e.msgId == "" {
		e.msgId = Artifex.GenerateRandomCode(12)
	}
	return e.msgId
}

func (e *Egress) SetMsgId(msgId string) {
	e.msgId = msgId
}

type EgressHandleFunc = Artifex.HandleFunc[Egress]
type EgressMiddleware = Artifex.Middleware[Egress]
type EgressMux = Artifex.Mux[Egress]

func NewEgressMux() *EgressMux {
	getRoutingKey := func(message *Egress) string {
		return message.RoutingKey
	}

	mux := Artifex.NewMux(".", getRoutingKey)
	return mux
}

func EgressSkip() EgressHandleFunc {
	return func(_ *Egress, _ *Artifex.RouteParam) (err error) {
		return nil
	}
}
