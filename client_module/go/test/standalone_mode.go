package test

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	smartclient "smartresource/clientmodule/go/rdb"
)

// サンプル用の一時 config を作成し、Init が config を読むことと
// Get が未初期化でエラーになることを確認する
func MakeTestData(count int) {
	log.Println("MakeTestData start")

	dbClient := smartclient.Get("crm-system")
	dbClient.Execute(
		"TRUNCATE TABLE users CASCADE",
		smartclient.Params{},
	)
	sql := "INSERT INTO users (id, name, email, password, icon, active, anonymous, email_verified, created_at, updated_at, last_logged_in_at)VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
	for i := range count {
		id := "0000000" + strconv.Itoa(i)
		id = id[len(id)-7:]
		params := smartclient.Params{
			// d5794b1b-5f92-4dc6-aa48-085dbb08b813
			smartclient.ParamVal("d5794b1b-5f92-4dc6-aa48-085d_"+id, smartclient.ValueType_STRING),
			smartclient.ParamVal("John Doe_"+id, smartclient.ValueType_STRING),
			smartclient.ParamVal("john@example.com_"+id, smartclient.ValueType_STRING),
			smartclient.ParamVal("password_"+id, smartclient.ValueType_STRING),
			smartclient.ParamVal("https://example.com/static/icon_"+id, smartclient.ValueType_STRING),
			smartclient.ParamVal(true, smartclient.ValueType_BOOL),
			smartclient.ParamVal(true, smartclient.ValueType_BOOL),
			smartclient.ParamVal(true, smartclient.ValueType_BOOL),
			smartclient.ParamVal(time.Now(), smartclient.ValueType_DATETIME),
			smartclient.ParamVal(time.Now(), smartclient.ValueType_DATETIME),
			smartclient.ParamVal(time.Now(), smartclient.ValueType_DATETIME),
		}
		_, err := dbClient.Execute(
			sql,
			params,
		)
		if err != nil {
			log.Printf("MakeTestData error: %v\n", err)
			return
		}
	}
}

func Test1() error {

	dbClient := smartclient.Get("crm-system")
	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	_, err := dbClient.Query(
		"SELECT * FROM users where id = $1",
		smartclient.Params{
			smartclient.ParamVal("d5794b1b-5f92-4dc6-aa48-085d_"+idStr, smartclient.ValueType_STRING),
		},
		smartclient.QueryOptions{
			LimitRows:  100,
			TimeoutSec: 15,
		},
	)
	if err != nil {
		return fmt.Errorf("Test error: %w", err)
	}
	//fmt.Printf("Test1 result: %s %+v\n", idStr, result)

	return nil
}
