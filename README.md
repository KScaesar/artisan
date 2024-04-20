# Artifex-Adapter

Provide examples of implementing Artifex's adapters
- [Artifex-Adapter](#artifex-adapter)
	- [sse](#sse)
	- [rabbitmq](#rabbitmq)

## sse

[sse example](./sse/example/main.go)


```go
package main

func NewSseServer() *sse.Server {
	server := sse.DefaultServer()

	server.Authenticate = func(w http.ResponseWriter, r *http.Request) (sseId string, err error) {
		userId := r.URL.Query().Get("user_id")
		return userId, nil
	}

	once := sync.Once{}
	server.Lifecycle = func(w http.ResponseWriter, r *http.Request, lifecycle *Artifex.Lifecycle) {
		userId := r.URL.Query().Get("user_id")
		gameId := r.URL.Query().Get("game_id")
		roomId := r.URL.Query().Get("room_id")
		game := NewGame(gameId, roomId)

		lifecycle.OnOpen(func(adp Artifex.IAdapter) error {
			CreateGame(game, adp.(sse.Publisher))
			fmt.Printf("%v %v %v enter: total=%v\n", userId, game.GameId, game.RoomId, server.Hub.Local.Total())
			if server.Hub.Local.Total() == 4 {
				once.Do(func() {
					close(mqFire)
				})
			}
			return nil
		})

		lifecycle.OnStop(func(adp Artifex.IAdapter) {
			fmt.Printf("%v %v %v leave: total=%v\n", userId, game.GameId, game.RoomId, server.Hub.Local.Total())
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
```

sse result:  
![sse result](./asset/sse.png)

sse gif:  
![sse gif](./asset/sse.gif)

## rabbitmq

[rabbitmq example](./rabbit/example/main.go)


```go
func NewIngressMux() func() *rabbit.IngressMux {
	mux := rabbit.NewIngressMux()

	mux.Handler("key1-hello", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("print key1-hello: %v", string(message.ByteBody))
		return nil
	})
	mux.Handler("key1-world", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("print key1-world: %v", string(message.ByteBody))
		return nil
	})

	mux.Handler("key2.Created.Game", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("print key2.Created.Game: %v", string(message.ByteBody))
		return nil
	})
	mux.Handler("key2.Restarted.Game", func(message *rabbit.Ingress, _ *Artifex.RouteParam) error {
		message.Logger.Info("print key2.Restarted.Game: %v", string(message.ByteBody))
		return nil
	})

	fmt.Println()
	for _, v := range mux.Endpoints() {
		fmt.Printf("[Rabbit Ingress] RoutingKey=%-40q f=%q\n", v[0], v[1])
	}

	return func() *rabbit.IngressMux {
		return mux
	}
}
```

```go
func NewEgressMux() func(ch **amqp.Channel) *rabbit.EgressMux {
	ctx := context.Background()

	return func(channel **amqp.Channel) *rabbit.EgressMux {
		mux := rabbit.NewEgressMux().
			Middleware(rabbit.EncodeJson().PreMiddleware())

		key1 := mux.Group("key1-")
		key1.SetDefaultHandler(func(message *rabbit.Egress, route *Artifex.RouteParam) error {
			return (*channel).PublishWithContext(
				ctx,
				"test-ex1",
				message.Subject,
				false,
				false,
				amqp.Publishing{
					MessageId: message.MsgId(),
					Body:      message.Body,
				},
			)
		})

		mux.Handler("key2.{action}.Game", func(message *rabbit.Egress, route *Artifex.RouteParam) error {
			return (*channel).PublishWithContext(
				ctx,
				"test-ex2",
				message.Subject,
				false,
				false,
				amqp.Publishing{
					MessageId: message.MsgId(),
					Body:      message.Body,
				},
			)
		})

		fmt.Println()
		for _, v := range mux.Endpoints() {
			fmt.Printf("[Rabbit Egress] Subject=%-40q f=%q\n", v[0], v[1])
		}

		return mux
	}
}
```