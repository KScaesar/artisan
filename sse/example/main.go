package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/KScaesar/art"
	"github.com/gin-gonic/gin"

	"github.com/KScaesar/artisan/sse"
)

func main() {
	art.SetDefaultLogger(art.NewLogger(false, art.LogLevelDebug))

	sseServer := NewSseServer()
	go fireMessage(sseServer)

	shutdown := art.NewShutdown()
	httpServer := NewHttpServer(sseServer, shutdown)

	ctx := context.Background()
	shutdown.
		StopService("sse", func() error {
			sseServer.Shutdown()
			return nil
		}).
		StopService("http", func() error {
			return httpServer.Shutdown(ctx)
		}).
		Serve(ctx)
}

var mqFire = make(chan string)

// init

func NewSseServer() sse.Server {
	sseServer := sse.NewArtisan()

	sseServer.Authenticate = func(w http.ResponseWriter, r *http.Request) (name string, err error) {
		userId := r.URL.Query().Get("user_id")
		return userId, nil
	}

	sseServer.EgressMux = NewMux()

	sseServer.Lifecycle = Lifecycle(sseServer.Hub)

	sseServer.DecorateAdapter = NewSession
	return sseServer
}

func NewMux() *sse.EgressMux {
	mux := sse.NewEgressMux().
		Transform(Transform).
		Middleware(
			art.UseLogger(true, art.SafeConcurrency_Copy),
			art.UsePrintResult{}.PrintEgress().IgnoreErrors(sse.ErrBroadcastNotMatch).PostMiddleware(),
			art.UseRecover(),
			art.UsePrintDetail().PostMiddleware(),
		)

	mux.DefaultHandler(art.UseGenericFunc(Broadcast))

	v0 := mux.Group("v0/")
	v0.Handler("Notification", art.UseGenericFunc(Notification))

	v1 := mux.Group("v1/")
	v1.Handler("PausedGame", art.UseGenericFunc(PausedGame))
	v1.Handler("ChangedRoomMap", art.UseGenericFunc(ChangedRoomMap))

	return mux
}

func NewHttpServer(sseServer sse.Server, shutdown *art.Shutdown) *http.Server {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	router.StaticFile("/", "./index.html")
	router.GET("/stream", sse.UseHeadersByGin(true), sseServer.Serve)
	router.GET("/shutdown", func(c *gin.Context) {
		shutdown.Notify(nil)
		c.String(200, "")
	})

	httpServer := &http.Server{Handler: router, Addr: ":18888"}
	go func() {
		err := httpServer.ListenAndServe()
		if err != nil {
			art.DefaultLogger().Error("http server fail: %v", err)
		}
	}()

	return httpServer
}

// handler

func Transform(message *art.Message, dep any) error {
	if !message.Metadata.Has("version") {
		return nil
	}

	message.Mutex.Lock()
	defer message.Mutex.Unlock()

	if message.Metadata.Has("version") {
		message.Subject = message.Metadata.Str("version") + message.Subject
		delete(message.Metadata, "version")
	}
	return nil
}

func Broadcast(message *art.Message, sess *Session) error {
	return sess.RawSend(message)
}

func Notification(message *art.Message, sess *Session) error {
	if !message.Metadata.Get("user_ids").(map[string]bool)[sess.UserId] {
		return sse.ErrBroadcastNotMatch
	}
	return sess.RawSend(message)
}

func PausedGame(message *art.Message, sess *Session) error {
	if sess.GameId != message.Metadata.Str("game_id") {
		return sse.ErrBroadcastNotMatch
	}
	return sess.RawSend(message)
}

func ChangedRoomMap(message *art.Message, sess *Session) error {
	if sess.RoomId != message.Metadata.Int("room_id") {
		return sse.ErrBroadcastNotMatch
	}
	return sess.RawSend(message)
}

func fireMessage(sseServer sse.Server) {
	<-mqFire

	messages := []func() *art.Message{
		func() *art.Message {
			egress := sse.NewBodyEgress(`Hello World`)
			egress.MsgId()
			return egress
		},
		func() *art.Message {
			egress := sse.NewBodyEgress(json.RawMessage(`{"disk":"2T","event":"CreatedGcpVm","team":"devops"}`))
			egress.MsgId()
			return egress
		},
		func() *art.Message {
			egress := sse.NewBodyEgress([]byte(`Domain Driven Design`))
			egress.MsgId()
			return egress
		},
		func() *art.Message {
			egress := sse.NewBodyEgressWithEvent("Notification", "Gcp VM closed")
			egress.MsgId()
			egress.Metadata.Set("version", "v0/")
			egress.Metadata.Set("user_ids", map[string]bool{"1": true, "3": true, "5": true, "7": true, "9": true})
			return egress
		},
		func() *art.Message {
			egress := sse.NewBodyEgressWithEvent("v1/PausedGame", map[string]any{"x_game_id": "1", "event": "v1/PausedGame"})
			egress.MsgId()
			egress.Metadata.Set("game_id", "1")
			return egress
		},
		func() *art.Message {
			egress := sse.NewBodyEgressWithEvent("v1/PausedGame", map[string]any{"x_game_id": "2", "event": "v1/PausedGame"})
			egress.MsgId()
			egress.Metadata.Set("game_id", "2")
			return egress
		},
		func() *art.Message {
			egress := sse.NewBodyEgressWithEvent("v1/PausedGame", map[string]any{"x_game_id": "3", "event": "v1/PausedGame"})
			egress.MsgId()
			egress.Metadata.Set("game_id", "3")
			return egress
		},
		func() *art.Message {
			egress := sse.NewBodyEgressWithEvent("ChangedRoomMap", map[string]any{"y_room_id": 1, "z_map_id": "a"})
			egress.MsgId()
			egress.Metadata.Set("version", "v1/")
			egress.Metadata.Set("room_id", 1)
			return egress
		},
		func() *art.Message {
			egress := sse.NewBodyEgressWithEvent("ChangedRoomMap", map[string]any{"y_room_id": 2, "z_map_id": "b"})
			egress.MsgId()
			egress.Metadata.Set("version", "v1/")
			egress.Metadata.Set("room_id", 2)
			return egress
		},
		func() *art.Message {
			egress := sse.NewBodyEgressWithEvent("ChangedRoomMap", map[string]any{"y_room_id": 3, "z_map_id": "c"})
			egress.MsgId()
			egress.Metadata.Set("version", "v1/")
			egress.Metadata.Set("room_id", 3)
			return egress
		},
	}

	for _, message := range messages {
		time.Sleep(1 * time.Second)
		sseServer.Broadcast(message())
	}

	time.Sleep(time.Second)
	art.DefaultLogger().Info("fireMessage finish !!!")

	sseServer.DisconnectClient(func(pub sse.Producer) bool {
		return true
	})
}

// session

func Lifecycle(hub *art.Hub) func(w http.ResponseWriter, r *http.Request, lifecycle *art.Lifecycle) {
	return func(w http.ResponseWriter, r *http.Request, lifecycle *art.Lifecycle) {
		gameId := r.URL.Query().Get("game_id")
		roomId := r.URL.Query().Get("room_id")

		once := sync.Once{}
		lifecycle.OnConnect(func(adp art.IAdapter) error {
			sess := adp.(*Session)

			sessId := art.GenerateRandomCode(6)
			userId := adp.Identifier()
			sess.Init(sessId, userId, gameId, roomId)

			adp.SetLog(sess.Producer.Log().
				WithKeyValue("sess_id", sessId).
				WithKeyValue("user_id", userId),
			)

			sess.Log().Info("  connect: total=%v\n", hub.Total())
			if hub.Total() == 4 {
				once.Do(func() {
					close(mqFire)
				})
			}
			return nil
		})

		lifecycle.OnDisconnect(func(adp art.IAdapter) {
			sess := adp.(*Session)
			sess.Log().Info("  disconnect: total=%v\n", hub.Total())
		})
	}
}

func NewSession(adp art.IAdapter) art.IAdapter {
	return &Session{
		Producer: adp.(sse.Producer),
	}
}

type Session struct {
	SessId string
	UserId string
	GameId string
	RoomId int

	sse.Producer
}

func (sess *Session) Log() art.Logger {
	return sess.Producer.Log().
		WithKeyValue("game_id", sess.GameId).
		WithKeyValue("room_id", sess.RoomId)
}

func (sess *Session) Init(sessId, userId, gameId, roomId string) {
	sess.SessId = sessId
	sess.UserId = userId
	sess.GameId = gameId

	v, _ := strconv.Atoi(roomId)
	sess.RoomId = v
}
