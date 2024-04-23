package sse

import (
	"github.com/KScaesar/Artifex"
)

func Broadcast(hub *Hub) func(message *Egress, _ *Artifex.RouteParam) error {
	return func(message *Egress, _ *Artifex.RouteParam) error {
		hub.Local.DoSync(func(adp Artifex.IAdapter) (stop bool) {
			publisher := adp.(Publisher)
			publisher.Send(message)
			return false
		})
		return nil
	}
}
