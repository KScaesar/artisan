package rabbit

import (
	"github.com/gookit/goutil/maputil"

	"github.com/KScaesar/Artifex"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RoutingKey = string

//

func NewIngress(amqpMsg amqp.Delivery) *Ingress {
	return &Ingress{
		MsgId:      "",
		ByteBody:   amqpMsg.Body,
		RoutingKey: amqpMsg.RoutingKey,
		AmqpMsg:    amqpMsg,
	}
}

type Ingress struct {
	MsgId    string
	ByteBody []byte

	RoutingKey RoutingKey
	AmqpMsg    amqp.Delivery
}

type IngressHandleFunc = Artifex.HandleFunc[Ingress]
type IngressMiddleware = Artifex.Middleware[Ingress]
type IngressMux = Artifex.Mux[Ingress]

func NewIngressMux() *IngressMux {
	getRoutingKey := func(message *Ingress) string {
		// TODO
		return ""
	}

	mux := Artifex.NewMux("/", getRoutingKey)
	return mux
}

func IngressSkip() IngressHandleFunc {
	return func(_ *Ingress, _ *Artifex.RouteParam) (err error) {
		return nil
	}
}

//

func NewEgress() *Egress {
	return &Egress{}
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
		return ""
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
		// TODO
		return ""
	}

	mux := Artifex.NewMux("/", getRoutingKey)
	return mux
}

func EgressSkip() EgressHandleFunc {
	return func(_ *Egress, _ *Artifex.RouteParam) (err error) {
		return nil
	}
}
