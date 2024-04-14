package sse

import (
	"encoding/json"
	"fmt"

	"github.com/KScaesar/Artifex"
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

func EncodeJson() EgressHandleFunc {
	return func(message *Egress, _ *Artifex.RouteParam) (err error) {
		if message.AppMsg == nil {
			message.AppMsg = struct{}{}
		}
		bBody, err := json.Marshal(message.AppMsg)
		if err != nil {
			return err
		}
		message.StringBody = string(bBody)
		return nil
	}
}

func EncodeText() EgressHandleFunc {
	return func(message *Egress, _ *Artifex.RouteParam) (err error) {
		if message.AppMsg == nil {
			message.AppMsg = struct{}{}
		}
		message.StringBody = fmt.Sprint(message.AppMsg)
		return nil
	}
}
