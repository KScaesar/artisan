package sse

import (
	"net/http"

	"github.com/KScaesar/Artifex"
	"github.com/gin-gonic/gin"
)

//

type MultiPublisher interface {
	Send(messages ...*Egress) error
	StopAll()
	StopPublishers(filter func(SinglePublisher) bool)
}

type SinglePublisher interface {
	Artifex.IAdapter
	Send(messages ...*Egress) error
}

type Hub = Artifex.Hub[SinglePublisher]

func NewHub() *Hub {
	stop := func(publisher SinglePublisher) {
		publisher.Stop()
	}
	return Artifex.NewHub(stop)
}

//

func DefaultServer() *Server {
	return &Server{
		Authenticate: func(w http.ResponseWriter, r *http.Request, _ *Hub) (sseId string, err error) {
			return Artifex.GenerateRandomCode(6), nil
		},
		Mux:          NewEgressMux(),
		Hub:          NewHub(),
		StopMessage:  NewEgress("Disconnect", struct{}{}),
		NewLifecycle: func(r *http.Request) (*Artifex.Lifecycle, error) { return new(Artifex.Lifecycle), nil },
	}
}

type Server struct {
	Authenticate func(w http.ResponseWriter, r *http.Request, hub *Hub) (sseId string, err error)
	Mux          *EgressMux
	Hub          *Hub
	StopMessage  *Egress
	NewLifecycle func(r *http.Request) (*Artifex.Lifecycle, error)
}

func (f *Server) Send(messages ...*Egress) error {
	for _, message := range messages {
		err := f.Mux.HandleMessage(message, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Server) StopAll() {
	f.Hub.StopAll()
}

func (f *Server) StopPublishers(filter func(SinglePublisher) bool) {
	f.Hub.RemoveMulti(filter)
}

func (f *Server) CreatePublisherByGin(c *gin.Context) (SinglePublisher, error) {
	sseId, err := f.Authenticate(c.Writer, c.Request, f.Hub)
	if err != nil {
		return nil, err
	}

	opt := Artifex.NewPublisherOption[Egress]().
		Identifier(sseId)

	opt.AdapterSend(func(adp Artifex.IAdapter, egress *Egress) error {
		c.SSEvent(egress.Event, egress.StringBody)
		c.Writer.Flush()
		return nil
	})

	opt.AdapterStop(func(adp Artifex.IAdapter, _ *Egress) error {
		if f.StopMessage == nil {
			return nil
		}

		select {
		case <-c.Request.Context().Done():
			return nil

		default:
			c.SSEvent(f.StopMessage.Event, f.StopMessage.StringBody)
			c.Writer.Flush()
			return nil
		}
	})

	lifecycle, err := f.NewLifecycle(c.Request)
	if err != nil {
		return nil, err
	}

	lifecycle.AddInitialize(
		func(adp Artifex.IAdapter) error {
			err := f.Hub.Join(adp.Identifier(), adp.(SinglePublisher))
			if err != nil {
				return err
			}
			lifecycle.AddTerminate(func(adp Artifex.IAdapter) {
				go f.Hub.RemoveOne(func(pub SinglePublisher) bool { return pub == adp })
			})
			return nil
		},
	)
	opt.NewLifecycle(func() *Artifex.Lifecycle { return lifecycle })

	return opt.BuildPublisher()
}
