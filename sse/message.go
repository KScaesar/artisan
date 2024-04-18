package sse

import (
	"github.com/gookit/goutil/maputil"

	"github.com/KScaesar/Artifex"
)

type Event = string

//

func NewEgress(event Event, message any) *Egress {
	return &Egress{
		Event:    event,
		Metadata: make(maputil.Data),
		AppMsg:   message,
	}
}

type Egress struct {
	msgId      string
	StringBody string

	Event    Event
	Metadata maputil.Data
	AppMsg   any
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
	getEvent := func(message *Egress) string {
		version := message.Metadata.Str("version")
		if version != "" {
			message.Event = version + message.Event
			delete(message.Metadata, "version")
		}
		return message.Event
	}

	mux := Artifex.DefaultMux(getEvent)
	return mux
}
