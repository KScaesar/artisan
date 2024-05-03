package sse

import (
	"github.com/KScaesar/art"
	"github.com/gin-gonic/gin"
)

type HandleFunc func(message *art.Message, hub *art.Hub) error

type EgressMux = art.Mux

func NewEgressMux() *EgressMux {
	out := art.NewMux("/")
	return out
}

func UseHeadersByGin(enableCORS bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if enableCORS {
			c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		}
		c.Writer.Header().Set("Access-Control-Expose-Headers", "Content-Type")
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Next()
	}
}
