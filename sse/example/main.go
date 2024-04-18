package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/KScaesar/Artifex"
	"github.com/gin-gonic/gin"
	"github.com/gookit/goutil/maputil"

	"github.com/KScaesar/Artifex-Adapter/sse"
)

func main() {
	Artifex.SetDefaultLogger(Artifex.NewLogger(false, Artifex.LogLevelDebug))

	sseServer := NewSseServer()
	go fireMessage(sseServer)

	httpServer := NewHttpServer(sseServer)

	Artifex.NewShutdown().
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

func NewSseServer() *sse.Server {
	server := sse.DefaultServer()

	server.Authenticate = func(w http.ResponseWriter, r *http.Request, hub *sse.Hub) (sseId string, err error) {
		user_id := r.URL.Query().Get("user_id")
		return user_id, nil
	}

	once := sync.Once{}
	server.Lifecycle = func(w http.ResponseWriter, r *http.Request, lifecycle *Artifex.Lifecycle) {
		user_id := r.URL.Query().Get("user_id")
		game_id := r.URL.Query().Get("game_id")
		room_id := r.URL.Query().Get("room_id")

		lifecycle.OnOpen(func(adp Artifex.IAdapter) error {
			adp.Update(func(id *string, appData maputil.Data) {
				appData.Set("game_id", game_id)
				appData.Set("room_id", room_id)
			})

			fmt.Printf("%v %v %v enter: total=%v\n", user_id, game_id, room_id, server.Hub.Total())
			if server.Hub.Total() == 4 {
				once.Do(func() {
					close(mqFire)
				})
			}
			return nil
		})

		lifecycle.OnStop(func(adp Artifex.IAdapter) {
			fmt.Printf("%v %v %v leave: total=%v\n", user_id, game_id, room_id, server.Hub.Total())
		})
	}

	root := server.Mux

	v0 := root.Group("v0/")
	v0.SetDefaultHandler(broadcast(server.Hub))
	v0.Handler("Notification", Notification(server.Hub))

	v1 := root.Group("v1/")
	v1.Handler("PausedGame", PausedGame(server.Hub))
	v1.Handler("ChangedRoomMap/{room_id}", ChangedRoomMap(server.Hub))

	fmt.Println()
	// [Artifex-SSE] event="v0/.*"                                  f="main.broadcast.func1"
	// [Artifex-SSE] event="v0/Notification"                        f="main.Notification.func1"
	// [Artifex-SSE] event="v1/ChangedRoomMap/{room_id}"            f="main.ChangedRoomMap.func1"
	// [Artifex-SSE] event="v1/PausedGame"                          f="main.PausedGame.func1"
	root.PrintEndpoints(func(subject, fn string) { fmt.Printf("[Artifex-SSE] event=%-40q f=%q\n", subject, fn) })

	return server
}

func NewHttpServer(sseServer *sse.Server) *http.Server {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	router.StaticFile("/", "./index.html")
	router.GET("/stream", sse.HeadersByGin(true), sseServer.ServeByGin)

	httpServer := &http.Server{Handler: router, Addr: ":18888"}
	go func() {
		err := httpServer.ListenAndServe()
		if err != nil {
			fmt.Println("http server fail:", err)
		}
	}()

	return httpServer
}

func broadcast(hub *sse.Hub) sse.EgressHandleFunc {
	return func(message *sse.Egress, route *Artifex.RouteParam) error {

		event := message.Subject

		// The browser onMessage handler assumes the event name is 'message'
		// https://stackoverflow.com/a/42803814/9288569
		message.Subject = "message"

		action := func(pub sse.SinglePublisher) {
			fmt.Printf("send broadcast %v: user_id=%v\n", event, pub.Identifier())
			pub.Send(message)
		}
		hub.DoAsync(action)
		return nil
	}
}

func Notification(hub *sse.Hub) sse.EgressHandleFunc {
	return func(message *sse.Egress, route *Artifex.RouteParam) error {

		user_ids := message.Metadata.StringsByStr("user_id")
		pubs, found := hub.FindMultiByKey(user_ids)
		if !found {
			return fmt.Errorf("not found: user_id=%v\n", user_ids)
		}

		for _, pub := range pubs {
			fmt.Printf("send Notification: user_id=%v\n", pub.Identifier())
			go pub.Send(message)
		}
		return nil
	}
}

func PausedGame(hub *sse.Hub) sse.EgressHandleFunc {
	return func(message *sse.Egress, _ *Artifex.RouteParam) error {

		event := message.Subject

		// The browser onMessage handler assumes the event name is 'message'
		// https://stackoverflow.com/a/42803814/9288569
		message.Subject = "message"

		action := func(pub sse.SinglePublisher) (stop bool) {
			pub.Query(func(_ string, appData maputil.Data) {
				game_id := message.Metadata.Str("game_id")
				if appData.Str("game_id") != game_id {
					return
				}
				fmt.Printf("send %v: user_id=%v game_id=%v\n", event, pub.Identifier(), game_id)

				pub.Send(message)
			})
			return false
		}
		hub.DoSync(action)
		message.Subject = event // because DoSync
		return nil
	}
}

func ChangedRoomMap(hub *sse.Hub) sse.EgressHandleFunc {
	return func(message *sse.Egress, route *Artifex.RouteParam) error {
		// Note: In DoAsync will get empty data because route param has been reset
		room_id := route.Str("room_id") // success

		event := message.Subject
		message.Subject = "v1/ChangedRoomMap"

		action := func(pub sse.SinglePublisher) {
			pub.Query(func(_ string, appData maputil.Data) {
				// Note: In DoAsync will get empty data because route param has been reset
				// room_id := route.Str("room_id") // fail

				if appData.Str("room_id") != room_id {
					return
				}
				fmt.Printf("send %v: user_id=%v room_id=%v\n", event, pub.Identifier(), room_id)
				pub.Send(message)
			})
		}
		hub.DoAsync(action)
		return nil
	}
}

func fireMessage(pubs sse.MultiPublisher) {
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
			egress := sse.NewEgress("ChangedRoomMap/1", map[string]any{"y_room_id": "1"})
			egress.Metadata.Set("version", "v1/")
			return egress
		},
		func() *sse.Egress {
			egress := sse.NewEgress("ChangedRoomMap/2", map[string]any{"y_room_id": "2"})
			egress.Metadata.Set("version", "v1/")
			return egress
		},
		func() *sse.Egress {
			egress := sse.NewEgress("ChangedRoomMap/3", map[string]any{"y_room_id": "3"})
			egress.Metadata.Set("version", "v1/")
			return egress
		},
	}

	for _, message := range messages {
		time.Sleep(1 * time.Second)
		pubs.Send(message())
	}

	fmt.Println("fireMessage finish !!!")
	time.Sleep(time.Second)

	pubs.StopPublishers(func(pub sse.SinglePublisher) bool {
		return true
	})
}
