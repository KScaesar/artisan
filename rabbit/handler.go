package rabbit

import (
	"encoding/json"

	"github.com/KScaesar/Artifex"
)

type IngressMux = Artifex.Mux
type EgressMux = Artifex.Mux

func NewIngressMux() *IngressMux {
	in := Artifex.NewMux(".")
	return in
}

func NewEgressMux() *EgressMux {
	out := Artifex.NewMux(".")
	return out
}

func UseEncodeJson() Artifex.Middleware {
	return func(next Artifex.HandleFunc) Artifex.HandleFunc {
		return func(message *Artifex.Message, dep any) error {
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
