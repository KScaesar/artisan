package sse

import (
	"errors"
	"net/http"
	"sync"

	"github.com/KScaesar/art"
	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
)

type Producer = art.Producer

type Server interface {
	Serve(c *gin.Context)
	Broadcast(messages ...*art.Message)
	DisconnectClient(filter func(producer Producer) (found bool))
	Shutdown()
}

const (
	Metadata_Retry = "retry"
)

var ErrBroadcastNotMatch = errors.New("broadcast not match")

//

func NewArtisan() *Artisan {
	mux := NewEgressMux()

	mux.DefaultHandler(func(message *art.Message, dep any) error {
		producer := dep.(Producer)
		return producer.RawSend(message)
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
	f.Hub.DoAsync(func(adp art.IAdapter) {
		producer := adp.(Producer)
		producer.Send(messages...)
	})
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
		EgressMux(f.EgressMux).
		DecorateAdapter(f.DecorateAdapter).
		Lifecycle(func(life *art.Lifecycle) {
			f.Lifecycle(c.Writer, c.Request, life)
		})

	var mu sync.Mutex
	disconnect := c.Request.Context().Done()

	// send ping, wait pong
	waitPong := art.NewWaitPingPong()
	sendPing := func(adp art.IAdapter) error {
		mu.Lock()
		defer mu.Unlock()
		defer waitPong.Ack()

		select {
		case <-disconnect:
			return art.ErrClosed
		default:
			c.Render(-1, sse.Event{
				Event: f.PingMessage.Subject,
				Data:  f.PingMessage.Body,
			})
			c.Writer.Flush()
		}
		return nil
	}
	opt.SendPing(f.SendPingSeconds, waitPong, sendPing)

	opt.RawSend(func(logger art.Logger, message *art.Message) (err error) {
		mu.Lock()
		defer mu.Unlock()

		select {
		case <-disconnect:
			return art.ErrClosed
		default:
			// https://github.com/gin-contrib/sse?tab=readme-ov-file#sample-code
			// https://github.com/gin-gonic/examples/blob/master/server-sent-event/main.go#L65-L72
			// https://github.com/gin-gonic/gin/blob/v1.9.1/context.go#L1073
			// https://github.com/gin-gonic/gin/blob/v1.9.1/context.go#L1082
			c.Render(-1, sse.Event{
				Event: message.Subject,
				Id:    message.MsgId(),
				Retry: message.Metadata.Uint(Metadata_Retry),
				Data:  message.Body,
			})
			c.Writer.Flush()
		}
		return nil
	})

	opt.RawStop(func(logger art.Logger) (err error) {
		mu.Lock()
		defer mu.Unlock()

		select {
		case <-disconnect:
			return art.ErrClosed
		default:
			c.Render(-1, sse.Event{
				Event: f.StopMessage.Subject,
				Data:  f.StopMessage.Body,
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
