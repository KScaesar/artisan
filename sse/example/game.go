package main

import (
	"github.com/gookit/goutil/maputil"

	"github.com/KScaesar/Artifex-Adapter/sse"
)

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

//

func CreateGame(game *Game, pubs ...sse.Publisher) {
	for _, pub := range pubs {
		pub.Update(func(_ *string, appData maputil.Data) {
			appData.Set(AppData_Game, game)
		})
	}
}

func UpdateGame(update func(game *Game), pubs ...sse.Publisher) {
	for _, pub := range pubs {
		pub.Update(func(_ *string, appData maputil.Data) {
			game, ok := appData.Get(AppData_Game).(*Game)
			if ok {
				update(game)
			}
		})
	}
}

func GetGame(filter func(game *Game) bool, pub sse.Publisher) (game *Game, found bool) {
	pub.Query(func(_ string, appData maputil.Data) {
		g, ok := appData.Get(AppData_Game).(*Game)
		if ok && filter(g) {
			game = g
			found = true
			return
		}
	})
	return
}

func GetPublishersByGame(filter func(game *Game) bool, hub *sse.PubHub) (pubs []sse.Publisher, ok bool) {
	return hub.FindMulti(func(pub sse.Publisher) bool {
		_, found := GetGame(filter, pub)
		return found
	})
}
