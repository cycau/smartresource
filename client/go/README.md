# SmartDataStream Go Client

SmartDataStreamサーバー群に対してHTTP経由でSQLを実行するGoクライアントライブラリです。

## 機能

- 複数ノードへの自動負荷分散
- ノード障害時の自動リトライ
- リダイレクト対応
- トランザクションサポート
- Readonlyデータソースの自動検出

## 使用方法

### 初期化

#### シングルトンを使用（推奨）

```go
import "smartdatastream/client/go/module"

// シングルトンインスタンスを取得（設定ファイルパスを指定）
// 同じ設定ファイルパスで複数回呼び出しても、同じインスタンスが返される
smartClient, err := module.GetSmartClient("config.yaml")
if err != nil {
    log.Fatal(err)
}
defer smartClient.Close()
```

#### 非シングルトンインスタンスを作成

```go
import "smartdatastream/client/go/module"

// 設定ファイルから読み込み
config, err := module.LoadConfig("config.yaml")
if err != nil {
    log.Fatal(err)
}

// 新しいインスタンスを作成（毎回新しいインスタンスが作成される）
smartClient, err := module.NewSmartClientInstance(config)
if err != nil {
    log.Fatal(err)
}
defer smartClient.Close()
```

#### 後方互換性のための方法

```go
import "smartdatastream/client/go/module"

// 設定ファイルから読み込み
config, err := module.LoadConfig("config.yaml")
if err != nil {
    log.Fatal(err)
}

// NewSmartClient（後方互換性のため残されている）
smartClient, err := module.NewSmartClient(config)
if err != nil {
    log.Fatal(err)
}
defer smartClient.Close()
```

### クエリ実行

```go
// SELECTクエリ
result, err := smartClient.Query("pg_master", "SELECT * FROM users WHERE id = $1", 
    []client.ParamValue{
        {Type: "int32", Value: 1},
    },
    &client.QueryOptions{
        LimitRows: intPtr(100),
    },
)
if err != nil {
    log.Fatal(err)
}

for _, row := range result.Rows {
    fmt.Println(row)
}
```

### 実行（INSERT/UPDATE/DELETE）

```go
// INSERT/UPDATE/DELETE
result, err := smartClient.Execute("pg_master", 
    "INSERT INTO users (name, email) VALUES ($1, $2)",
    []client.ParamValue{
        {Type: "text", Value: "John Doe"},
        {Type: "text", Value: "john@example.com"},
    },
    nil,
)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Affected rows: %d\n", result.EffectedRows)
```

### トランザクション

```go
// トランザクション開始
tx, err := smartClient.BeginTx("pg_master", nil)
if err != nil {
    log.Fatal(err)
}

// トランザクション内でクエリ
result, err := tx.Query("SELECT * FROM users WHERE id = $1",
    []client.ParamValue{
        {Type: "int32", Value: 1},
    },
    nil,
)

// トランザクション内で実行
_, err = tx.Execute("UPDATE users SET name = $1 WHERE id = $2",
    []client.ParamValue{
        {Type: "text", Value: "Jane Doe"},
        {Type: "int32", Value: 1},
    },
    nil,
)

// コミット
if err := tx.Commit(); err != nil {
    log.Fatal(err)
}

// またはロールバック
// if err := tx.Rollback(); err != nil {
//     log.Fatal(err)
// }
```

## パラメータタイプ

以下のパラメータタイプがサポートされています：

- `null`
- `int32`
- `int64`
- `float64`
- `bool`
- `text`
- `bytes_base64`
- `timestamp_rfc3339`

## エラーハンドリング

- ネットワークエラー: 自動的に別ノードにリトライ（最大3回）
- リダイレクト（307）: 自動的にリダイレクト先ノードにリトライ
- Readonlyデータソース: `Execute()`は自動的にwritableデータソースを選択

## 設定ファイル

`config.yaml`の形式：

```yaml
defaultSecretKey: "change-default"

nodes:
  - nodeId: "node-1"
    baseUrl: "https://sqlgw-1.example.com"
    datasources:
      - dbName: "pg_master"
        readonly: false
      - dbName: "pg_slave01"
        readonly: true
  - nodeId: "node-2"
    baseUrl: "https://sqlgw-2.example.com"
    datasources:
      - dbName: "pg_master"
        readonly: false
```

