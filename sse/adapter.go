package sse

import (
	"net/http"
	"sync"

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
		Authenticate: func(w http.ResponseWriter, r *http.Request, hub *Hub) (sseId string, err error) {
			return Artifex.GenerateRandomCode(6), nil
		},
		Mux:              NewEgressMux(),
		Hub:              NewHub(),
		StopMessage:      NewEgress("Disconnect", struct{}{}),
		AdapterLifecycle: func(w http.ResponseWriter, r *http.Request, lifecycle *Artifex.Lifecycle) {},
	}
}

type Server struct {
	Authenticate     func(w http.ResponseWriter, r *http.Request, hub *Hub) (sseId string, err error)
	Mux              *EgressMux
	Hub              *Hub
	StopMessage      *Egress
	AdapterLifecycle func(w http.ResponseWriter, r *http.Request, lifecycle *Artifex.Lifecycle)
}

func (f *Server) ServeByGin(c *gin.Context) {
	pub, err := f.CreatePublisherByGin(c)
	if err != nil {
		return
	}
	c.Writer.Flush()
	<-c.Request.Context().Done()
	pub.Stop()
}

func (f *Server) CreatePublisherByGin(c *gin.Context) (SinglePublisher, error) {
	var mu sync.Mutex

	sseId, err := f.Authenticate(c.Writer, c.Request, f.Hub)
	if err != nil {
		return nil, err
	}

	opt := Artifex.NewPublisherOption[Egress]().
		Identifier(sseId)

	opt.AdapterSend(func(adp Artifex.IAdapter, egress *Egress) error {
		mu.Lock()
		defer mu.Unlock()
		c.SSEvent(egress.Event, egress.StringBody)
		c.Writer.Flush()
		return nil
	})

	opt.AdapterStop(func(adp Artifex.IAdapter) error {
		if f.StopMessage == nil {
			return nil
		}

		select {
		case <-c.Request.Context().Done():
			return nil

		default:
			mu.Lock()
			defer mu.Unlock()
			c.SSEvent(f.StopMessage.Event, f.StopMessage.StringBody)
			c.Writer.Flush()
			return nil
		}
	})

	lifecycle := new(Artifex.Lifecycle)
	lifecycle.OnOpen(
		func(adp Artifex.IAdapter) error {
			err := f.Hub.Join(adp.Identifier(), adp.(SinglePublisher))
			if err != nil {
				return err
			}
			lifecycle.OnStop(func(adp Artifex.IAdapter) {
				go f.Hub.RemoveOne(func(pub SinglePublisher) bool { return pub == adp })
			})
			return nil
		},
	)
	f.AdapterLifecycle(c.Writer, c.Request, lifecycle)
	opt.NewLifecycle(func() *Artifex.Lifecycle { return lifecycle })

	return opt.BuildPublisher()
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
