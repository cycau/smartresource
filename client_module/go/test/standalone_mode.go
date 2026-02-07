package sclient_test

import (
	"fmt"

	"smartresource/clientmodule/go/smartclient"
)

// サンプル用の一時 config を作成し、Init が config を読むことと
// Get が未初期化でエラーになることを確認する
func ExampleInit() {

	// 一時 config を作成（healz は失敗するが Init の読み込みまでは動く）

	dsClient, err := smartclient.Get("crm-system")
	if err != nil {
		fmt.Println("Get error:", err)
	}

	queryResult, err := dsClient.Query(
		"SELECT * FROM users",
		smartclient.Params{},
		smartclient.QueryOptions{
			LimitRows:  100,
			TimeoutSec: 15,
		},
	)
	if err != nil {
		fmt.Println("Query error:", err)
	}

	fmt.Println(queryResult.Rows)

	execResult, err := dsClient.Execute(
		"INSERT INTO users (name, email) VALUES ($1, $2)",
		smartclient.Params{
			smartclient.ParamVal(1, smartclient.ValueType_INT),
			smartclient.ParamVal("john@example.com", smartclient.ValueType_STRING),
		},
	)
	if err != nil {
		fmt.Println("Execute error:", err)
		return
	}
	fmt.Println(execResult.EffectedRows)
}
