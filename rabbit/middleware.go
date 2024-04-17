package rabbit

import (
	"encoding/json"

	"github.com/KScaesar/Artifex"
)

func PrintIngressError() IngressMiddleware {
	return func(next IngressHandleFunc) IngressHandleFunc {
		return func(message *Ingress, route *Artifex.RouteParam) error {
			err := next(message, route)
			if err != nil {
				message.Logger.Error("handle %q fail: %v", message.RoutingKey, err)
			}
			message.Logger.Info("handle %q success", message.RoutingKey)
			return nil
		}
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
		message.ByteBody = bBody
		return nil
	}
}
