package sse

import (
	"github.com/KScaesar/art"
)

func NewBytesEgress(bMessage []byte) *art.Message {
	message := art.GetMessage()

	message.Bytes = bMessage
	return message
}

func NewBodyEgress(body any) *art.Message {
	message := art.GetMessage()

	message.Body = body
	return message
}

func NewBodyEgressWithEvent(event string, body any) *art.Message {
	message := art.GetMessage()

	message.Subject = event
	message.Body = body
	return message
}
