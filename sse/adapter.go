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
		Mux:         NewEgressMux(),
		Hub:         NewHub(),
		StopMessage: NewEgress("Disconnect", struct{}{}),
		PingMessage: NewEgress("Ping", struct{}{}),
		Lifecycle:   func(w http.ResponseWriter, r *http.Request, lifecycle *Artifex.Lifecycle) {},
	}
}

type Server struct {
	Authenticate func(w http.ResponseWriter, r *http.Request, hub *Hub) (sseId string, err error)
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
	f.Hub.RemoveOne(func(publisher SinglePublisher) bool { return publisher == pub })
}

func (f *Server) createPublisherByGin(c *gin.Context) (SinglePublisher, error) {
	sseId, err := f.Authenticate(c.Writer, c.Request, f.Hub)
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
				return f.Hub.Join(adp.Identifier(), adp.(SinglePublisher))
			})
			f.Lifecycle(c.Writer, c.Request, life)
		})

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

func (f *Server) Shutdown() {
	f.Hub.StopAll()
}

func (f *Server) StopPublishers(filter func(SinglePublisher) bool) {
	f.Hub.RemoveMulti(filter)
}
