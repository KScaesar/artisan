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

type RemoteHub interface {
	FindByKey(sseId string) (pub Publisher, found bool)
	RemoveByKey(sseId string)
	Shutdown()
}

func NewHub() *Hub {
	stop := func(publisher Artifex.IAdapter) {
		publisher.Stop()
	}
	return &Hub{
		Local: Artifex.NewAdapterHub(stop),
	}
}

type Hub struct {
	Remote RemoteHub
	Local  *Artifex.Hub[Artifex.IAdapter]
}

func (hub *Hub) Join(sseId string, pub Artifex.IAdapter) error {
	if hub.Remote == nil {
		return hub.Local.Join(sseId, pub)
	}

	_, found := hub.Remote.FindByKey(sseId)
	if found {
		hub.Remote.RemoveByKey(sseId)
	}
	return hub.Local.Join(sseId, pub)
}

func (hub *Hub) RemoveOne(filter func(Artifex.IAdapter) bool) {
	hub.Local.RemoveOne(filter)
}

//

func DefaultServer() *Server {
	hub := NewHub()
	mux := NewEgressMux()

	mux.SetDefaultHandler(func(message *Egress, _ *Artifex.RouteParam) error {
		hub.Local.DoSync(func(adp Artifex.IAdapter) (stop bool) {
			publisher := adp.(Publisher)
			publisher.Send(message)
			return false
		})
		return nil
	})

	return &Server{
		Hub:             hub,
		SendPingSeconds: 15,
		StopMessage:     NewEgress("Disconnect", struct{}{}),
		PingMessage:     NewEgress("Ping", struct{}{}),

		Authenticate: func(w http.ResponseWriter, r *http.Request) (sseId string, err error) {
			return Artifex.GenerateRandomCode(6), nil
		},
		EgressMux: mux,
	}
}

type Server struct {
	Hub             *Hub
	Logger          Artifex.Logger
	SendPingSeconds int
	PingMessage     *Egress
	StopMessage     *Egress

	Authenticate    func(w http.ResponseWriter, r *http.Request) (sseId string, err error)
	EgressMux       *EgressMux
	DecorateAdapter func(old Artifex.IAdapter) (fresh Artifex.IAdapter)
	Lifecycle       func(w http.ResponseWriter, r *http.Request, lifecycle *Artifex.Lifecycle)
}

func (f *Server) ServeByGin(c *gin.Context) {
	pub, err := f.createPublisherByGin(c)
	if err != nil {
		return
	}
	c.Writer.Flush()
	<-c.Request.Context().Done()
	f.Hub.RemoveOne(func(adapter Artifex.IAdapter) bool { return adapter == pub })
}

func (f *Server) createPublisherByGin(c *gin.Context) (Publisher, error) {
	sseId, err := f.Authenticate(c.Writer, c.Request)
	if err != nil {
		return nil, err
	}

	var mu sync.Mutex
	disconnect := c.Request.Context().Done()

	opt := Artifex.NewPublisherOption[Egress]().
		Identifier(sseId).
		AdapterHub(f.Hub).
		Logger(f.Logger).
		DecorateAdapter(f.DecorateAdapter).
		Lifecycle(func(life *Artifex.Lifecycle) {
			f.Lifecycle(c.Writer, c.Request, life)
		}).
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
				adp.Log().WithKeyValue("msg_id", message.MsgId()).Info("send %q", message.Subject)
				c.Writer.Flush()
			}
			return nil
		}).
		AdapterStop(func(adp Artifex.IAdapter) error {
			if f.StopMessage == nil {
				return nil
			}

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
				adp.Log().Info("sse stop")
				c.Writer.Flush()
			}
			return nil
		})

	if f.SendPingSeconds > 0 {
		waitPong := make(chan error, 1)
		opt.SendPing(func() error {
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
		}, waitPong, f.SendPingSeconds*2)
	}

	pub, err := opt.Build()
	if err != nil {
		return nil, err
	}
	return pub.(Publisher), nil
}

func (f *Server) Send(messages ...*Egress) error {
	for _, message := range messages {
		err := f.EgressMux.HandleMessage(message, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Server) Shutdown() {
	if f.Hub.Remote != nil {
		f.Hub.Remote.Shutdown()
	}
	f.Hub.Local.Shutdown()
}

func (f *Server) StopPublisher(filter func(Publisher) bool) {
	f.Hub.Local.RemoveMulti(func(adapter Artifex.IAdapter) bool {
		return filter(adapter.(Publisher))
	})
}
