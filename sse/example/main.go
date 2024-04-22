package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/KScaesar/Artifex"
	"github.com/gin-gonic/gin"

	"github.com/KScaesar/Artifex-Adapter/sse"
)

func main() {
	Artifex.SetDefaultLogger(Artifex.NewLogger(false, Artifex.LogLevelDebug))

	sseServer := NewSseServer()
	go fireMessage(sseServer)

	shutdown := Artifex.NewShutdown()
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

func NewSseServer() *sse.Server {
	sseServer := sse.DefaultServer()

	sseServer.Authenticate = func(w http.ResponseWriter, r *http.Request) (sseId string, err error) {
		userId := r.URL.Query().Get("user_id")
		return userId, nil
	}
	sseServer.DecorateAdapter = WrapSession

	once := sync.Once{}
	sseServer.Lifecycle = func(w http.ResponseWriter, r *http.Request, lifecycle *Artifex.Lifecycle) {
		gameId := r.URL.Query().Get("game_id")
		roomId := r.URL.Query().Get("room_id")

		lifecycle.OnOpen(func(adp Artifex.IAdapter) error {
			sess := adp.(*Session)
			sess.Init(gameId, roomId)

			sess.Logger().Info("enter: total=%v\n", sseServer.Hub.Local.Total())
			if sseServer.Hub.Local.Total() == 4 {
				once.Do(func() {
					close(mqFire)
				})
			}
			return nil
		})

		lifecycle.OnStop(func(adp Artifex.IAdapter) {
			sess := adp.(*Session)
			sess.Logger().Info("leave: total=%v\n", sseServer.Hub.Local.Total())
		})
	}

	root := sseServer.Mux

	v0 := root.Group("v0/")
	v0.SetDefaultHandler(broadcast(sseServer.Hub))
	v0.Handler("Notification", Notification(sseServer.Hub))

	v1 := root.Group("v1/")
	v1.Handler("PausedGame", PausedGame(sseServer.Hub))
	v1.Handler("ChangedRoomMap/{room_id}", ChangedRoomMap(sseServer.Hub))

	fmt.Println()
	// [Artifex-SSE] event="v0/.*"                                  f="main.broadcast.func1"
	// [Artifex-SSE] event="v0/Notification"                        f="main.Notification.func1"
	// [Artifex-SSE] event="v1/ChangedRoomMap/{room_id}"            f="main.ChangedRoomMap.func1"
	// [Artifex-SSE] event="v1/PausedGame"                          f="main.PausedGame.func1"
	root.PrintEndpoints(func(subject, fn string) { fmt.Printf("[Artifex-SSE] event=%-40q f=%q\n", subject, fn) })

	return sseServer
}

func NewHttpServer(sseServer *sse.Server, shutdown *Artifex.Shutdown) *http.Server {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	router.StaticFile("/", "./index.html")
	router.GET("/stream", sse.HeadersByGin(true), sseServer.ServeByGin)
	router.GET("/shutdown", func(c *gin.Context) {
		shutdown.NotifyStop(nil)
		c.String(200, "")
	})

	httpServer := &http.Server{Handler: router, Addr: ":18888"}
	go func() {
		err := httpServer.ListenAndServe()
		if err != nil {
			Artifex.DefaultLogger().Error("http server fail: %v", err)
		}
	}()

	return httpServer
}

// handler

func broadcast(hub *sse.Hub) sse.EgressHandleFunc {
	return func(message *sse.Egress, route *Artifex.RouteParam) error {
		// broadcast by onMessage:

		// The browser onMessage handler assumes the event name is 'message'
		// https://stackoverflow.com/a/42803814/9288569
		event := message.Subject
		message.Subject = "message"

		hub.Local.DoAsync(func(adp Artifex.IAdapter) {
			sess := UnwrapSession(adp)
			sess.Logger().Info("send broadcast %v\n", event)
			sess.Send(message)
		})
		return nil
	}
}

func Notification(hub *sse.Hub) sse.EgressHandleFunc {
	return func(message *sse.Egress, route *Artifex.RouteParam) error {
		// Notification by addEventListener:
		// version from metadata
		// user_id from metadata

		user_ids := message.Metadata.StringsByStr("user_id")

		adapters, found := hub.Local.FindMultiByKey(user_ids)
		if !found {
			return fmt.Errorf("not found: user_id=%v", user_ids)
		}

		for _, adp := range adapters {
			sess := UnwrapSession(adp)
			sess.Logger().Info("send %v\n", message.Subject)
			go sess.Send(message)
		}
		return nil
	}
}

func PausedGame(hub *sse.Hub) sse.EgressHandleFunc {
	return func(message *sse.Egress, _ *Artifex.RouteParam) error {
		// PausedGame by onMessage:
		// version from subject
		// game_id from metadata

		// The browser onMessage handler assumes the event name is 'message'
		// https://stackoverflow.com/a/42803814/9288569
		event := message.Subject
		message.Subject = "message"
		gameId := message.Metadata.Str("game_id")

		hub.Local.DoAsync(func(adp Artifex.IAdapter) {
			sess := UnwrapSession(adp)

			if sess.GameId != gameId {
				return
			}

			sess.Logger().Info("send %v\n", event)
			sess.Send(message)
		})

		message.Subject = event

		return nil
	}
}

func ChangedRoomMap(hub *sse.Hub) sse.EgressHandleFunc {
	return func(message *sse.Egress, route *Artifex.RouteParam) error {
		// ChangedRoomMap by addEventListener:
		// version from metadata
		// room_id from route param

		// Note: In DoAsync function will get empty data because 'RouteParam' has been reset
		roomId := route.Str("room_id")

		// In order to remove 'RouteParam', original Subject = "v1/ChangedRoomMap/{room_id}"
		event := message.Subject
		message.Subject = "v1/ChangedRoomMap"

		hub.Local.DoAsync(func(adp Artifex.IAdapter) {
			sess := UnwrapSession(adp)
			if sess.RoomId != roomId {
				return
			}

			sess.Logger().Info("send %v:\n", event)
			sess.Send(message)
		})

		return nil
	}
}

//

func fireMessage(sseServer sse.MultiPublisher) {
	<-mqFire

	messages := []func() *sse.Egress{
		// broadcast by onMessage:
		func() *sse.Egress {
			return sse.NewEgress("v0/CreatedGcpVm", map[string]any{
				"event": "v0/CreatedGcpVm",
				"team":  "devops",
				"disk":  "2T",
			})
		},

		// broadcast by onMessage:
		func() *sse.Egress {
			return sse.NewEgress("v0/Hello", "v0/Hello: World")
		},

		// Notification by addEventListener:
		// version from metadata
		// user_id from metadata
		func() *sse.Egress {
			egress := sse.NewEgress("Notification", "Gcp VM closed")
			egress.Metadata.Set("version", "v0/")
			egress.Metadata.Set("user_id", "1,3,5,7,9")
			return egress
		},

		// PausedGame by onMessage:
		// version from subject
		// game_id from metadata
		func() *sse.Egress {
			egress := sse.NewEgress("v1/PausedGame", map[string]any{"x_game_id": "1", "event": "v1/PausedGame"})
			egress.Metadata.Set("game_id", "1")
			return egress
		},
		func() *sse.Egress {
			egress := sse.NewEgress("v1/PausedGame", map[string]any{"x_game_id": "2", "event": "v1/PausedGame"})
			egress.Metadata.Set("game_id", "2")
			return egress
		},
		func() *sse.Egress {
			egress := sse.NewEgress("v1/PausedGame", map[string]any{"x_game_id": "3", "event": "v1/PausedGame"})
			egress.Metadata.Set("game_id", "3")
			return egress
		},

		// ChangedRoomMap by addEventListener:
		// version from metadata
		// room_id from route param
		func() *sse.Egress {
			egress := sse.NewEgress("ChangedRoomMap/1", map[string]any{"y_room_id": "1", "z_map_id": "a"})
			egress.Metadata.Set("version", "v1/")
			return egress
		},
		func() *sse.Egress {
			egress := sse.NewEgress("ChangedRoomMap/2", map[string]any{"y_room_id": "2", "z_map_id": "b"})
			egress.Metadata.Set("version", "v1/")
			return egress
		},
		func() *sse.Egress {
			egress := sse.NewEgress("ChangedRoomMap/3", map[string]any{"y_room_id": "3", "z_map_id": "c"})
			egress.Metadata.Set("version", "v1/")
			return egress
		},
	}

	for _, message := range messages {
		time.Sleep(1 * time.Second)
		sseServer.Send(message())
	}

	fmt.Println("fireMessage finish !!!")
	time.Sleep(time.Second)

	sseServer.StopPublisher(func(pub sse.Publisher) bool {
		return true
	})
}

// session

func WrapSession(adp Artifex.IAdapter) Artifex.IAdapter {
	logger := adp.Log().WithKeyValue("sse_id", adp.Identifier())
	adp.SetLog(logger)
	return &Session{
		Publisher: adp.(sse.Publisher),
	}
}

func UnwrapSession(pub Artifex.IAdapter) *Session {
	return pub.(*Session)
}

type Session struct {
	GameId string
	RoomId string

	sse.Publisher
}

func (sess *Session) Logger() Artifex.Logger {
	return sess.Log().
		WithKeyValue("game_id", sess.GameId).
		WithKeyValue("room_id", sess.RoomId)
}

func (sess *Session) Init(gameId string, roomId string) {
	sess.GameId = gameId
	sess.RoomId = roomId
}
