# art-Adapter

Provide examples of implementing art's adapters
- [art-Adapter](#art-adapter)
	- [sse](#sse)
	- [rabbitmq](#rabbitmq)

## sse

[sse example](./sse/example/main.go)

```go
package main

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

// Producer
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
```

## rabbitmq

[rabbitmq example](./rabbit/example/main.go)


```go
// Consumer
func NewIngressMux() *rabbit.IngressMux {
	mux := rabbit.NewIngressMux().
		EnableMessagePool().
		ErrorHandler(art.UsePrintResult{}.PrintIngress().PostMiddleware()).
		Middleware(
			art.UseRecover(),
			art.UseLogger(true, art.SafeConcurrency_Skip),
			art.UseAdHocFunc(func(message *art.Message, dep any) error {
				logger := art.CtxGetLogger(message.Ctx)
				logger.Info("recv %q", message.Subject)
				return nil
			}).PreMiddleware(),
		)

	mux.Handler("key1-hello", art.UsePrintDetail())
	mux.Handler("key1-world", art.UsePrintDetail())
	mux.Handler("key2.{action}.Game", art.UsePrintDetail())
	return mux
}

// Producer
func NewEgressMux() *rabbit.EgressMux {
	mux := rabbit.NewEgressMux().
		ErrorHandler(art.UsePrintResult{}.PrintEgress().PostMiddleware()).
		Middleware(
			art.UseRecover(),
			art.UseLogger(true, art.SafeConcurrency_Skip),
			rabbit.UseEncodeJson(),
		)

	mux.Group("key1-").
		DefaultHandler(func(message *art.Message, dep any) error {
			channel := dep.(rabbit.Producer).RawInfra().(**amqp.Channel)
			return (*channel).PublishWithContext(
				message.Ctx,
				"test-ex1",
				message.Subject,
				false,
				false,
				amqp.Publishing{
					MessageId: message.MsgId(),
					Body:      message.Bytes,
				},
			)
		})

	mux.Handler("key2.{action}.Game", func(message *art.Message, dep any) error {
		channel := dep.(rabbit.Producer).RawInfra().(**amqp.Channel)
		return (*channel).PublishWithContext(
			message.Ctx,
			"test-ex2",
			message.Subject,
			false,
			false,
			amqp.Publishing{
				MessageId: message.MsgId(),
				Body:      message.Bytes,
			},
		)
	})

	return mux
}
```