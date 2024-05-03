package main

import (
	"context"
	"fmt"
	"net/http"
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

	shutdown.
		SetStopAction("sse", func() error {
			sseServer.Shutdown()
			return nil
		}).
		SetStopAction("http", func() error {
			return httpServer.Shutdown(context.Background())
		}).
		Listen(nil)
}

var mqFire = make(chan string)

// init

func NewSseServer() sse.Server {
	sseServer := sse.NewArtisan()
	sseServer.Authenticate = func(w http.ResponseWriter, r *http.Request) (name string, err error) {
		userId := r.URL.Query().Get("user_id")
		return userId, nil
	}
	RegisterMux(sseServer.EgressMux)
	sseServer.Lifecycle = Lifecycle(sseServer.Hub)
	sseServer.DecorateAdapter = NewSession
	return sseServer
}

func RegisterMux(mux *sse.EgressMux) *sse.EgressMux {
	v0 := mux.Group("v0/")
	v0.Handler("Notification", Notification())

	v1 := mux.Group("v1/")
	v1.Handler("PausedGame", PausedGame())
	v1.Handler("ChangedRoomMap", ChangedRoomMap())

	// [art-SSE] event=".*"                                  f="main.broadcast.func1"
	// [art-SSE] event="v0/Notification"                        f="main.Notification.func1"
	// [art-SSE] event="v1/ChangedRoomMap"            f="main.ChangedRoomMap.func1"
	// [art-SSE] event="v1/PausedGame"                          f="main.PausedGame.func1"
	mux.Endpoints(func(subject, handler string) {
		fmt.Printf("[art-SSE] event=%-40q f=%q\n", subject, handler)
	})
	fmt.Println()

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

func broadcast() art.HandleFunc {
	return func(message *art.Message, dep any) error {
		// broadcast by onMessage:

		// The browser onMessage handler assumes the event name is 'message'
		// https://stackoverflow.com/a/42803814/9288569
		event := message.Subject
		message.Subject = "message"
		msgId := message.MsgId()

		hub.Local.DoAsync(func(adp art.IAdapter) {
			sess := adp.(*Session)
			sess.Logger().WithKeyValue("msg_id", msgId).Info("send broadcast %v\n", event)
			sess.Send(message)
		})
		return nil
	}
}

// handler

func Notification() art.HandleFunc {
	return func(message *art.Message, dep any) error {}
	// Notification by addEventListener:
	// version from metadata
	// user_id from metadata

	user_ids := message.Metadata.StringsByStr("user_id")
	msgId := message.MsgId()

	adapters, found := hub.Local.FindMultiByKey(user_ids)
	if !found {
		return fmt.Errorf("not found: user_id=%v", user_ids)
	}

	for _, adp := range adapters {
		sess := adp.(*Session)
		sess.Logger().WithKeyValue("msg_id", msgId).Info("send %v\n", message.Subject)
		go sess.Send(message)
	}
	return nil
}
}

func PausedGame() art.HandleFunc {
	return func(message *art.Message, dep any) error {}
	// PausedGame by onMessage:
	// version from subject
	// game_id from metadata

	// The browser onMessage handler assumes the event name is 'message'
	// https://stackoverflow.com/a/42803814/9288569
	event := message.Subject
	message.Subject = "message"
	gameId := message.Metadata.Str("game_id")
	msgId := message.MsgId()

	hub.Local.DoSync(func(adp art.IAdapter) bool {
		sess := adp.(*Session)

		if sess.GameId != gameId {
			return false
		}

		sess.Logger().WithKeyValue("msg_id", msgId).Info("send %v\n", event)
		sess.Send(message)
		return false
	})

	message.Subject = event

	return nil
}
}

func ChangedRoomMap() art.HandleFunc {
	return func(message *art.Message, dep any) error {}
	// ChangedRoomMap by addEventListener:
	// version from metadata
	// room_id from route param

	// Note: In DoAsync function will get empty data because 'RouteParam' has been reset
	roomId := route.Str("room_id")

	// In order to remove 'RouteParam', original Subject = "v1/ChangedRoomMap/{room_id}"
	event := message.Subject
	message.Subject = "v1/ChangedRoomMap"
	msgId := message.MsgId()

	hub.Local.DoAsync(func(adp art.IAdapter) {
		sess := adp.(*Session)
		if sess.RoomId != roomId {
			return
		}

		sess.Logger().WithKeyValue("msg_id", msgId).Info("send %v:\n", event)
		sess.Send(message)
	})

	return nil
}
}

func fireMessage(sseServer sse.Server) {
	<-mqFire

	messages := []func() *art.Message{
		// broadcast by onMessage:
		func() *art.Message {
			return sse.NewBodyEgress( map[string]any{
				"event": "v0/CreatedGcpVm",
				"team":  "devops",
				"disk":  "2T",
			})
		},

		// broadcast by onMessage:
		func() *art.Message {
			return sse.NewEgress("v0/Hello", "v0/Hello: World")
		},

		// Notification by addEventListener:
		// version from metadata
		// user_id from metadata
		func() *art.Message {
			egress := sse.NewEgress("Notification", "Gcp VM closed")
			egress.Metadata.Set("version", "v0/")
			egress.Metadata.Set("user_id", "1,3,5,7,9")
			return egress
		},

		// PausedGame by onMessage:
		// version from subject
		// game_id from metadata
		func() *art.Message {
			egress := sse.NewEgress("v1/PausedGame", map[string]any{"x_game_id": "1", "event": "v1/PausedGame"})
			egress.Metadata.Set("game_id", "1")
			return egress
		},
		func() *art.Message {
			egress := sse.NewEgress("v1/PausedGame", map[string]any{"x_game_id": "2", "event": "v1/PausedGame"})
			egress.Metadata.Set("game_id", "2")
			return egress
		},
		func() *art.Message {
			egress := sse.NewEgress("v1/PausedGame", map[string]any{"x_game_id": "3", "event": "v1/PausedGame"})
			egress.Metadata.Set("game_id", "3")
			return egress
		},

		// ChangedRoomMap by addEventListener:
		// version from metadata
		// room_id from route param
		func() *art.Message {
			egress := sse.NewEgress("ChangedRoomMap/1", map[string]any{"y_room_id": "1", "z_map_id": "a"})
			egress.Metadata.Set("version", "v1/")
			return egress
		},
		func() *art.Message {
			egress := sse.NewEgress("ChangedRoomMap/2", map[string]any{"y_room_id": "2", "z_map_id": "b"})
			egress.Metadata.Set("version", "v1/")
			return egress
		},
		func() *art.Message {
			egress := sse.NewEgress("ChangedRoomMap/3", map[string]any{"y_room_id": "3", "z_map_id": "c"})
			egress.Metadata.Set("version", "v1/")
			return egress
		},
	}

	for _, message := range messages {
		time.Sleep(1 * time.Second)
		sseServer.Send(message())
	}

	art.DefaultLogger().Info("fireMessage finish !!!")
	time.Sleep(time.Second)

	sseServer.StopPublisher(func(pub sse.Publisher) bool {
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

			logger := sess.Log().
				WithKeyValue("sess_id", sessId).
				WithKeyValue("user_id", userId)
			adp.SetLog(logger)

			sess.Init(gameId, roomId)

			sess.Logger().Info("  connect: total=%v\n", hub.Total())
			if hub.Total() == 4 {
				once.Do(func() {
					close(mqFire)
				})
			}
			return nil
		})

		lifecycle.OnDisconnect(func(adp art.IAdapter) {
			sess := adp.(*Session)
			sess.Logger().Info("  disconnect: total=%v\n", hub.Total())
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
	RoomId string

	sse.Producer
}

func (sess *Session) Logger() art.Logger {
	return sess.Log().
		WithKeyValue("game_id", sess.GameId).
		WithKeyValue("room_id", sess.RoomId)
}

func (sess *Session) Init(gameId string, roomId string) {
	sess.GameId = gameId
	sess.RoomId = roomId
}
