package sse

import (
	"github.com/KScaesar/art"
)

func NewBodyEgress(body any) *art.Message {
	message := art.GetMessage()

	// The browser onMessage handler assumes the event name is 'message'
	// https://stackoverflow.com/a/42803814/9288569
	message.Subject = "message"
	message.Body = body
	return message
}

func NewBodyEgressWithEvent(event string, body any) *art.Message {
	message := art.GetMessage()

	message.Subject = event
	message.Body = body
	return message
}
