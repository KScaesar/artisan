# Artifex-Adapter

Provide examples of implementing Artifex's adapters

## sse

[sse example](./sse/example/main.go)


```go
package main

func NewSseServer() *sse.Server {
	server := sse.DefaultServer()
	server.Authenticate = func(w http.ResponseWriter, r *http.Request, hub *sse.Hub) (sseId string, err error) {
		user_id := r.URL.Query().Get("user_id")
		return user_id, nil
	}

	root := server.Mux

	v0 := root.Group("v0/")
	v0.Middleware(sse.EncodeText().PreMiddleware())
	v0.SetDefaultHandler(broadcast(server.Hub))
	v0.Handler("Notification", Notification(server.Hub))

	v1 := root.Group("v1/")
	v1.PreMiddleware(sse.EncodeJson())
	v1.Handler("PausedGame", PausedGame(server.Hub))
	v1.Handler("ChangedRoomMap/{room_id}", ChangedRoomMap(server.Hub))

	fmt.Println() 
	// [Artifex] event="v0/.*"                                  f="main.NewSseServer.broadcast.func4" 
	// [Artifex] event="v0/Notification"                        f="main.NewSseServer.Notification.func5" 
	// [Artifex] event="v1/ChangedRoomMap/{room_id}"            f="main.NewSseServer.ChangedRoomMap.func8" 
	// [Artifex] event="v1/PausedGame"                          f="main.NewSseServer.PausedGame.func7"
	for _, v := range root.Endpoints() {
		fmt.Printf("[Artifex] event=%-40q f=%q\n", v[0], v[1])
	}

	return server
}

func sseServe(server *sse.Server, canFireMessage func(hub *sse.Hub) bool) func(*gin.Context) {
	once := sync.Once{}
	return func(c *gin.Context) {
		pub, err := server.CreatePublisherByGin(c)
		if err != nil {
			fmt.Println("sseServe fail:", err)
			return
		}

		game_id := c.Query("game_id")
		room_id := c.Query("room_id")

		pub.Update(func(id *string, appData maputil.Data) {
			appData.Set("game_id", game_id)
			appData.Set("room_id", room_id)
		})

		fmt.Println("Join:", c.Query("user_id"), game_id, room_id, " cnt:", server.Hub.Total())
		if canFireMessage(server.Hub) {
			once.Do(func() {
				close(mqFire)
			})
		}

		c.Writer.Flush()

		<-c.Request.Context().Done()
		pub.Stop()
		fmt.Println(c.Query("user_id"), game_id, room_id, "stop")
	}
}
```

sse result:  
![sse result](./asset/sse.png)

sse gif:  
![sse gif](./asset/sse.gif)
