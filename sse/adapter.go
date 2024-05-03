package sse

import (
	"net/http"
	"sync"

	"github.com/KScaesar/art"
	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
)

type Producer interface {
	art.IAdapter
	RawSend(messages ...*art.Message) error
}

type Server interface {
	Serve(c *gin.Context)
	Broadcast(messages ...*art.Message)
	DisconnectClient(filter func(producer Producer) (found bool))
	Shutdown()
}

//

func NewArtisan() *Artisan {
	mux := NewEgressMux()

	mux.DefaultHandler(func(message *art.Message, dep any) error {
		hub := dep.(*art.Hub)
		hub.DoAsync(func(adapter art.IAdapter) {
			adapter.(Producer).RawSend(message)
		})
		return nil
	})

	return &Artisan{
		Hub:             art.NewHub(),
		SendPingSeconds: 15,
		StopMessage:     NewBodyEgressWithEvent("Disconnect", struct{}{}),
		PingMessage:     NewBodyEgressWithEvent("Ping", struct{}{}),

		Authenticate: func(w http.ResponseWriter, r *http.Request) (name string, err error) {
			return art.GenerateRandomCode(8), nil
		},
		EgressMux: mux,
	}
}

type Artisan struct {
	Hub             *art.Hub
	Logger          art.Logger
	SendPingSeconds int
	PingMessage     *art.Message
	StopMessage     *art.Message

	Authenticate func(w http.ResponseWriter, r *http.Request) (name string, err error)

	EgressMux       *EgressMux
	DecorateAdapter func(adapter art.IAdapter) (application art.IAdapter)
	Lifecycle       func(w http.ResponseWriter, r *http.Request, lifecycle *art.Lifecycle)
}

func (f *Artisan) Broadcast(messages ...*art.Message) {
	for _, message := range messages {
		egress := message
		f.EgressMux.HandleMessage(egress, f.Hub)
	}
}

func (f *Artisan) DisconnectClient(filter func(producer Producer) bool) {
	f.Hub.RemoveMulti(func(adapter art.IAdapter) bool {
		producer := adapter.(Producer)
		return filter(producer)
	})
}

func (f *Artisan) Shutdown() {
	f.Hub.Shutdown()
}

func (f *Artisan) Serve(c *gin.Context) {
	producer, err := f.CreateProducer(c)
	if err != nil {
		return
	}

	c.Writer.Flush()

	select {
	case <-producer.WaitStop():
	case <-c.Request.Context().Done():
		f.Hub.RemoveOne(func(adapter art.IAdapter) bool { return adapter == producer })
	}
}

func (f *Artisan) CreateProducer(c *gin.Context) (producer Producer, err error) {
	name, err := f.Authenticate(c.Writer, c.Request)
	if err != nil {
		return nil, err
	}

	opt := art.NewAdapterOption().
		Identifier(name).
		AdapterHub(f.Hub).
		Logger(f.Logger).
		DecorateAdapter(f.DecorateAdapter).
		Lifecycle(func(life *art.Lifecycle) {
			f.Lifecycle(c.Writer, c.Request, life)
		})

	var mu sync.Mutex
	disconnect := c.Request.Context().Done()

	// send ping, wait pong
	waitPong := art.NewWaitPingPong()
	sendPing := func(adp art.IAdapter) error {
		defer waitPong.Ack()
		return adp.(Producer).RawSend(f.PingMessage)
	}
	opt.SendPing(f.SendPingSeconds, waitPong, sendPing)

	opt.AdapterSend(func(logger art.Logger, message *art.Message) (err error) {
		mu.Lock()
		defer mu.Unlock()

		select {
		case <-disconnect:
			return art.ErrClosed
		default:
			c.Render(-1, sse.Event{
				Event: message.Subject,
				Id:    message.MsgId(),
				Data:  message.Body,
			})
			c.Writer.Flush()
		}
		return nil
	})

	opt.AdapterStop(func(logger art.Logger) (err error) {
		mu.Lock()
		defer mu.Unlock()

		select {
		case <-disconnect:
			return art.ErrClosed
		default:
			message := f.StopMessage
			c.Render(-1, sse.Event{
				Event: message.Subject,
				Data:  message.Body,
			})
			c.Writer.Flush()
		}
		return nil
	})

	adp, err := opt.Build()
	if err != nil {
		return
	}
	return adp.(Producer), err
}
