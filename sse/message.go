package sse

import (
	"context"

	"github.com/gookit/goutil/maputil"

	"github.com/KScaesar/Artifex"
)

type Event = string

//

func NewEgress(event Event, message any) *Egress {
	return &Egress{
		Subject:  event,
		Metadata: make(maputil.Data),
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
	getEvent := func(message *Egress) string {
		return message.Subject
	}
	mux := Artifex.DefaultMux(getEvent)
	return mux
}
