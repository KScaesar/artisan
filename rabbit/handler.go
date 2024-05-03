package rabbit

import (
	"encoding/json"

	"github.com/KScaesar/art"
)

type IngressMux = art.Mux
type EgressMux = art.Mux

func NewIngressMux() *IngressMux {
	in := art.NewMux(".")
	return in
}

func NewEgressMux() *EgressMux {
	out := art.NewMux(".")
	return out
}

func UseEncodeJson() art.Middleware {
	return func(next art.HandleFunc) art.HandleFunc {
		return func(message *art.Message, dep any) error {
			if message.Body == nil {
				message.Body = struct{}{}
			}

			bBody, err := json.Marshal(message.Body)
			if err != nil {
				return err
			}

			message.Bytes = bBody
			return next(message, dep)
		}
	}
}
