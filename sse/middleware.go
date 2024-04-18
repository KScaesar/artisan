package sse

import (
	"github.com/gin-gonic/gin"
)

func HeadersByGin(enableCORS bool) gin.HandlerFunc {
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
