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

// init

func NewSseServer() *sse.Server {
	sseServer := sse.DefaultServer()

	sseServer.Authenticate = func(w http.ResponseWriter, r *http.Request) (sseId string, err error) {
		userId := r.URL.Query().Get("user_id")
		return userId, nil
	}

	once := sync.Once{}
	sseServer.Lifecycle = func(w http.ResponseWriter, r *http.Request, lifecycle *Artifex.Lifecycle) {
		userId := r.URL.Query().Get("user_id")
		gameId := r.URL.Query().Get("game_id")
		roomId := r.URL.Query().Get("room_id")
		game := NewGame(gameId, roomId)

		lifecycle.OnOpen(func(adp Artifex.IAdapter) error {
			Artifex.CreateAppData(adp, AppData_Game, game)
			fmt.Printf("%v %v %v enter: total=%v\n", userId, game.GameId, game.RoomId, sseServer.Hub.Local.Total())
			if sseServer.Hub.Local.Total() == 4 {
				once.Do(func() {
					close(mqFire)
				})
			}
			return nil
		})

		lifecycle.OnStop(func(adp Artifex.IAdapter) {
			fmt.Printf("%v %v %v leave: total=%v\n", userId, game.GameId, game.RoomId, sseServer.Hub.Local.Total())
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

// handler

func broadcast(hub *sse.Hub) sse.EgressHandleFunc {
	return func(message *sse.Egress, route *Artifex.RouteParam) error {
		// broadcast by onMessage:

		// The browser onMessage handler assumes the event name is 'message'
		// https://stackoverflow.com/a/42803814/9288569
		event := message.Subject
		message.Subject = "message"

		hub.Local.DoAsync(func(pub sse.Publisher) {
			fmt.Printf("send broadcast %v: user_id=%v\n", event, pub.Identifier())
			pub.Send(message)
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

		pubs, found := hub.Local.FindMultiByKey(user_ids)
		if !found {
			return fmt.Errorf("not found: user_id=%v\n", user_ids)
		}

		for _, pub := range pubs {
			fmt.Printf("send %v: user_id=%v\n", message.Subject, pub.Identifier())
			go pub.Send(message)
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

		hubFilter := func(pub sse.Publisher) bool {
			return Artifex.HasAppData(pub, true, AppData_Game, func(game *Game) bool {
				return game.GameId == gameId
			})
		}
		pubs, found := hub.Local.FindMulti(hubFilter)
		if !found {
			return fmt.Errorf("not found: game_id=%v\n", gameId)
		}

		for _, pub := range pubs {
			fmt.Printf("send %v: user_id=%v game_id=%v\n", event, pub.Identifier(), gameId)
			pub.Send(message)
		}

		message.Subject = event
		return nil
	}
}

func ChangedRoomMap(hub *sse.Hub) sse.EgressHandleFunc {
	return func(message *sse.Egress, route *Artifex.RouteParam) error {
		// ChangedRoomMap by addEventListener:
		// version from metadata
		// map_id from route metadata
		// room_id from route param

		// Note: In DoAsync function will get empty data because 'RouteParam' has been reset
		roomId := route.Str("room_id") // success

		// In order to remove 'RouteParam', original Subject = "v1/ChangedRoomMap/{room_id}"
		event := message.Subject
		message.Subject = "v1/ChangedRoomMap"
		mapId := message.Metadata.Str("map_id")

		hub.Local.DoAsync(func(pub sse.Publisher) {
			game, found := Artifex.GetAppData(pub, false, AppData_Game, func(game *Game) bool {
				return game.RoomId == roomId
			})
			if !found {
				return
			}

			Artifex.UpdateAppData(pub, AppData_Game, func(game *Game) {
				game.ChangeMap(mapId)
			})

			fmt.Printf("send %v: user_id=%v room_id=%v map_id=%v\n", event, pub.Identifier(), game.RoomId, game.MapId)
			pub.Send(message)
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
		// map_id from route metadata
		// room_id from route param
		func() *sse.Egress {
			egress := sse.NewEgress("ChangedRoomMap/1", map[string]any{"y_room_id": "1", "z_map_id": "a"})
			egress.Metadata.Set("version", "v1/")
			egress.Metadata.Set("map_id", "a")
			return egress
		},
		func() *sse.Egress {
			egress := sse.NewEgress("ChangedRoomMap/2", map[string]any{"y_room_id": "2", "z_map_id": "b"})
			egress.Metadata.Set("version", "v1/")
			egress.Metadata.Set("map_id", "b")
			return egress
		},
		func() *sse.Egress {
			egress := sse.NewEgress("ChangedRoomMap/3", map[string]any{"y_room_id": "3", "z_map_id": "c"})
			egress.Metadata.Set("version", "v1/")
			egress.Metadata.Set("map_id", "c")
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

// AppData

const AppData_Game = "game"

func NewGame(gameId string, roomId string) *Game {
	return &Game{
		GameId: gameId,
		RoomId: roomId,
		MapId:  "-1",
	}
}

type Game struct {
	GameId string
	RoomId string
	MapId  string
}

func (g *Game) ChangeMap(mapId string) {
	g.MapId = mapId
}
