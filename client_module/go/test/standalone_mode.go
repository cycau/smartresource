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
		nil,
	)
	sql := "INSERT INTO users (id, name, email, password, icon, active, anonymous, email_verified, created_at, updated_at, last_logged_in_at)VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
	for i := range count {
		id := "0000000" + strconv.Itoa(i)
		id = id[len(id)-7:]
		params := smartclient.NewParams().
			Add("d5794b1b-5f92-4dc6-aa48-085d_"+id, smartclient.ValueType_STRING).
			Add("John Doe_"+id, smartclient.ValueType_STRING).
			Add("john@example.com_"+id, smartclient.ValueType_STRING).
			Add("password_"+id, smartclient.ValueType_STRING).
			Add("https://example.com/static/icon_"+id, smartclient.ValueType_STRING).
			Add(true, smartclient.ValueType_BOOL).
			Add(true, smartclient.ValueType_BOOL).
			Add(true, smartclient.ValueType_BOOL).
			Add(time.Now(), smartclient.ValueType_DATETIME).
			Add(time.Now(), smartclient.ValueType_DATETIME).
			Add(time.Now(), smartclient.ValueType_DATETIME)

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

func Test1() (*smartclient.Records, error) {

	dbClient := smartclient.Get("crm-system")
	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	params := smartclient.NewParams().
		Add("01916e5a-2345-7002-b000-000000000002", smartclient.ValueType_STRING)
	records, err := dbClient.Query(
		"SELECT * FROM sys_users where user_id = $1",
		params,
		smartclient.QueryOptions{
			LimitRows:  100,
			TimeoutSec: 180,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("Test error: %w", err)
	}
	fmt.Printf("Test1 result: %s %s\n", idStr, records.Get(0).Get("user_id"))

	return records, nil
}
