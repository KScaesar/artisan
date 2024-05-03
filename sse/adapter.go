package sse

import (
	"net/http"
	"sync"

	"github.com/KScaesar/art"
	"github.com/gin-gonic/gin"
)

type Producer = art.Producer

//

func NewArtisan() *Artisan {
	hub := art.NewHub()
	mux := NewEgressMux()

	return &Artisan{
		Hub:             hub,
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

func (f *Artisan) Serve(c *gin.Context) {
	producer, err := f.CreateProducer(c)
	if err != nil {
		return
	}

	c.Writer.Flush()

	select {
	case <-c.Request.Context().Done():
		f.Hub.RemoveOne(func(adapter art.IAdapter) bool { return adapter == producer })

	case <-producer.WaitStop():

	}
}

func (f *Artisan) CreateProducer(c *gin.Context) (producer Producer, err error) {
	name, err := f.Authenticate(c.Writer, c.Request)
	if err != nil {
		return nil, err
	}

	opt := art.NewAdapterOption().
		RawInfra(nil).
		Identifier(name).
		AdapterHub(f.Hub).
		Logger(f.Logger).
		EgressMux(f.EgressMux).
		DecorateAdapter(f.DecorateAdapter).
		Lifecycle(func(life *art.Lifecycle) {
			f.Lifecycle(c.Writer, c.Request, life)
		})

	var mu sync.Mutex

	// send ping, wait pong
	sendPing := func(adp art.IAdapter) error {
		return nil
	}
	waitPong := art.NewWaitPingPong()
	opt.SendPing(f.SendPingSeconds, waitPong, sendPing)

	opt.AdapterSend(func(logger art.Logger, message *art.Message) (err error) {
		mu.Lock()
		defer mu.Unlock()
		return
	})

	opt.AdapterStop(func(logger art.Logger) (err error) {
		mu.Lock()
		defer mu.Unlock()

		if err != nil {
			logger.Error("stop: %v", err)
			return
		}
		return nil
	})

	adp, err := opt.Build()
	if err != nil {
		return
	}
	return adp.(Producer), err
}
