package rabbit

import (
	"context"

	"github.com/gookit/goutil/maputil"

	"github.com/KScaesar/Artifex"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RoutingKey = string

//

func NewIngress(amqpMsg *amqp.Delivery, logger Artifex.Logger) *Ingress {
	msgId := amqpMsg.MessageId
	if msgId == "" {
		msgId = Artifex.GenerateUlid()
	}

	logger = logger.WithKeyValue("msg_id", msgId)
	logger.Info("recv %q", amqpMsg.RoutingKey)
	return &Ingress{
		MsgId:      msgId,
		Body:       amqpMsg.Body,
		RoutingKey: amqpMsg.RoutingKey,
		AmqpMsg:    amqpMsg,
		Logger:     logger,
	}
}

type Ingress struct {
	MsgId string
	Body  []byte

	RoutingKey RoutingKey
	AmqpMsg    *amqp.Delivery
	Logger     Artifex.Logger

	ctx context.Context
}

func (in *Ingress) Context() context.Context {
	if in.ctx == nil {
		in.ctx = context.Background()
	}
	return in.ctx
}

func (in *Ingress) SetContext(ctx context.Context) {
	in.ctx = ctx
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

//

func NewEgress(key RoutingKey, message any) *Egress {
	return &Egress{
		Subject:  key,
		Metadata: make(map[string]any),
		AppMsg:   message,
	}
}

type Egress struct {
	msgId string
	Body  []byte

	Subject  string
	Metadata maputil.Data
	AppMsg   any

	ctx context.Context
}

func (e *Egress) MsgId() string {
	if e.msgId == "" {
		e.msgId = Artifex.GenerateUlid()
	}
	return e.msgId
}

func (e *Egress) SetMsgId(msgId string) {
	e.msgId = msgId
}

func (e *Egress) Context() context.Context {
	if e.ctx == nil {
		e.ctx = context.Background()
	}
	return e.ctx
}

func (e *Egress) SetContext(ctx context.Context) {
	e.ctx = ctx
}

type EgressHandleFunc = Artifex.HandleFunc[Egress]
type EgressMiddleware = Artifex.Middleware[Egress]
type EgressMux = Artifex.Mux[Egress]

func NewEgressMux() *EgressMux {
	getRoutingKey := func(message *Egress) string {
		return message.Subject
	}

	mux := Artifex.NewMux(".", getRoutingKey)
	return mux
}
