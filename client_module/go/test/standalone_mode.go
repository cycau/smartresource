package test

import (
	"fmt"

	smartclient "smartresource/clientmodule/go/pkg"
)

// サンプル用の一時 config を作成し、Init が config を読むことと
// Get が未初期化でエラーになることを確認する
func Test1() {

	dsClient := smartclient.Get("crm-system")

	_, err := dsClient.Query(
		"SELECT * FROM users",
		smartclient.Params{},
		smartclient.QueryOptions{
			LimitRows:  100,
			TimeoutSec: 15,
		},
	)
	if err != nil {
		fmt.Println("Test error:", err)
	}

	//fmt.Printf("queryResult: %+v\n", queryResult)

	// id := rand.Intn(100000000)
	// execResult, err := dsClient.Execute(
	// 	"INSERT INTO users (id, name, email, active) VALUES ($1, $2, $3, $4)",
	// 	smartclient.Params{
	// 		smartclient.ParamVal(strconv.Itoa(id), smartclient.ValueType_STRING),
	// 		smartclient.ParamVal("XXX_"+strconv.Itoa(id), smartclient.ValueType_STRING),
	// 		smartclient.ParamVal("john@example.com", smartclient.ValueType_STRING),
	// 		smartclient.ParamVal(true, smartclient.ValueType_BOOL),
	// 	},
	// )
	// if err != nil {
	// 	fmt.Println("Execute error:", err)
	// 	return
	// }
	// fmt.Printf("execResult: %+v\n", execResult)
}
