package client

import (
	"fmt"
	"testing"

	smartclient "smartdatastream/client/go/module"
)

func TestClient(t *testing.T) {
	client, err := smartclient.Initialize("config.yaml")
	if err != nil {
		t.Fatalf("failed to get client: %v", err)
	}

	// 同じ設定ファイルパスで再度取得すると、同じインスタンスが返される
	client2, err := smartclient.Get()
	if err != nil {
		t.Fatalf("failed to get client: %v", err)
	}

	// 同じインスタンスか確認
	if client != client2 {
		t.Error("GetSmartClient should return the same instance")
	}

	txHandle, err := client.BeginTx("testdb", nil)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}
	fmt.Println(txHandle)
}
