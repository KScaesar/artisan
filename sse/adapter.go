package sse

import (
	"net/http"
	"sync"

	"github.com/KScaesar/Artifex"
	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
)

//

type MultiPublisher interface {
	Send(messages ...*Egress) error
	Shutdown()
	StopPublisher(filter func(Publisher) bool)
}

type Publisher interface {
	Artifex.IAdapter
	Send(messages ...*Egress) error
}

//

type RemotePubHub interface {
	FindByKey(sseId string) (pub Publisher, found bool)
	RemoveByKey(sseId string)
	StopAll()
}

type PubHub = Artifex.Hub[Publisher]

func NewHub() *Hub {
	stop := func(publisher Publisher) {
		publisher.Stop()
	}
	return &Hub{
		Local: Artifex.NewHub(stop),
	}
}

type Hub struct {
	Remote RemotePubHub
	Local  *PubHub
}

func (hub *Hub) Join(sseId string, pub Publisher) error {
	if hub.Remote == nil {
		return hub.Local.Join(sseId, pub)
	}

	_, found := hub.Remote.FindByKey(sseId)
	if found {
		hub.Remote.RemoveByKey(sseId)
	}
	return hub.Local.Join(sseId, pub)
}

func (hub *Hub) FindByKey(sseId string) (pub Publisher, found bool) {
	if hub.Remote == nil {
		return hub.Local.FindByKey(sseId)
	}

	pub, found = hub.Remote.FindByKey(sseId)
	if found {
		return pub, found
	}
	return hub.Local.FindByKey(sseId)
}

func (hub *Hub) StopAll() {
	if hub.Remote != nil {
		hub.Remote.StopAll()
	}
	hub.Local.StopAll()
}

//

func DefaultServer() *Server {
	return &Server{
		Authenticate: func(w http.ResponseWriter, r *http.Request) (sseId string, err error) {
			return Artifex.GenerateRandomCode(6), nil
		},
		Mux:         NewEgressMux(),
		Hub:         NewHub(),
		StopMessage: NewEgress("Disconnect", struct{}{}),
		PingMessage: NewEgress("Ping", struct{}{}),
		Lifecycle:   func(w http.ResponseWriter, r *http.Request, lifecycle *Artifex.Lifecycle) {},
	}
}

type Server struct {
	Authenticate func(w http.ResponseWriter, r *http.Request) (sseId string, err error)
	Mux          *EgressMux
	Hub          *Hub
	StopMessage  *Egress
	PingMessage  *Egress
	Lifecycle    func(w http.ResponseWriter, r *http.Request, lifecycle *Artifex.Lifecycle)
}

func (f *Server) ServeByGin(c *gin.Context) {
	pub, err := f.createPublisherByGin(c)
	if err != nil {
		return
	}
	c.Writer.Flush()
	<-c.Request.Context().Done()
	f.Hub.Local.RemoveOne(func(publisher Publisher) bool { return publisher == pub })
}

func (f *Server) createPublisherByGin(c *gin.Context) (Publisher, error) {
	sseId, err := f.Authenticate(c.Writer, c.Request)
	if err != nil {
		return nil, err
	}

	var mu sync.Mutex
	disconnect := c.Request.Context().Done()
	waitPong := make(chan error, 1)

	opt := Artifex.NewPublisherOption[Egress]().
		Identifier(sseId).
		SendPing(func() error {
			select {
			case <-disconnect:
			default:
				mu.Lock()
				defer mu.Unlock()
				message := f.PingMessage
				c.Render(-1, sse.Event{
					Event: message.Subject,
					Data:  message.AppMsg,
				})
				c.Writer.Flush()
			}
			waitPong <- nil
			return nil
		}, waitPong, 30).
		AdapterSend(func(adp Artifex.IAdapter, message *Egress) error {
			select {
			case <-disconnect:
			default:
				mu.Lock()
				defer mu.Unlock()
				c.Render(-1, sse.Event{
					Event: message.Subject,
					Id:    message.MsgId(),
					Data:  message.AppMsg,
				})
				c.Writer.Flush()
			}
			return nil
		}).
		AdapterStop(func(adp Artifex.IAdapter) error {
			select {
			case <-disconnect:
			default:
				mu.Lock()
				defer mu.Unlock()
				message := f.StopMessage
				c.Render(-1, sse.Event{
					Event: message.Subject,
					Data:  message.AppMsg,
				})
				c.Writer.Flush()
			}
			return nil
		}).
		Lifecycle(func(life *Artifex.Lifecycle) {
			life.OnOpen(func(adp Artifex.IAdapter) error {
				return f.Hub.Join(adp.Identifier(), adp.(Publisher))
			})
			f.Lifecycle(c.Writer, c.Request, life)
		})

	return opt.Build()
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

func (f *Server) Shutdown() {
	f.Hub.StopAll()
}

func (f *Server) StopPublisher(filter func(Publisher) bool) {
	f.Hub.Local.RemoveMulti(filter)
}
