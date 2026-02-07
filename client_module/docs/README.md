# SmartResource Go Client

SmartDatasourceサーバー群に対してHTTP経由でSQLを実行するGoクライアント用ライブラリです。

## 機能

- 複数ノードへの自動負荷分散
- ノード障害時の自動リトライ
- リダイレクト対応
- トランザクションサポート
- Readonlyデータソースの自動検出

## 使用方法

### 初期化（システム起動時）
```go
import "smartresource/clientmodule/go/smartclient"

// config.yamlのbaseURLから各ノードのhealz情報を読み込み詳細設定ファイルとして保持する
err := smartclient.Init("config.yaml")
if err != nil {
    log.Fatal(err)
}
```

### 各種実行サンプルイメージ

```go
import "smartresource/clientmodule/go/smartclient"

dbclient, err := smartclient.Get("databaseName")
// または config の defaultDatabase を設定した上で
// dbclient, err := smartclient.Get()
if err != nil {
    log.Fatal(err)
}

// SELECTクエリ
result, err := dbclient.Query("SELECT * FROM users WHERE id = $1",
    []smartclient.ParamValue{
        {Type: smartclient.ValTypeINT, Value: 1},
    },
    smartclient.QueryOptions{
        LimitRows:  100,
        TimeoutSec: 15,
    },
)
if err != nil {
    log.Fatal(err)
}
for _, row := range result.Rows {
    fmt.Println(row)
}

// 実行（INSERT/UPDATE/DELETE）
result, err := dbclient.Execute("INSERT INTO users (name, email) VALUES ($1, $2)",
    []smartclient.ParamValue{
        {Type: smartclient.ValTypeSTRING, Value: "John Doe"},
        {Type: smartclient.ValTypeSTRING, Value: "john@example.com"},
    },
)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Affected rows: %d\n", result.EffectedRows)

// トランザクション
tx, err := dbclient.BeginTx(smartclient.IsolationReadCommitted)
if err != nil {
    log.Fatal(err)
}
// トランザクション内でクエリ
result, err = tx.Query("SELECT * FROM users WHERE id = $1",
    []smartclient.ParamValue{
        {Type: smartclient.ValTypeINT, Value: 1},
    },
    smartclient.QueryOptions{
        LimitRows:  100,
        TimeoutSec: 15,
    },
)
// トランザクション内で実行
_, err = tx.Execute("UPDATE users SET name = $1 WHERE id = $2",
    []smartclient.ParamValue{
        {Type: smartclient.ValTypeSTRING, Value: "Jane Doe"},
        {Type: smartclient.ValTypeINT, Value: 1},
    },
)
// コミット
if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
// またはロールバック
// if err := tx.Rollback(); err != nil { ... }
// 最後はクローズ
if err := tx.Close(); err != nil {
    log.Fatal(err)
}
```


## サーバー側APIサンプルは以下の通り

### health
```json
curl -X GET http://localhost:5678/healz

response:
{
  "nodeId": "dbapi-1-9e7tkrDmx",
  "status": "SERVING",
  "healthInfo": {
    "maxHttpQueue": 1000,
    "runningHttp": 0,
    "upTime": "2026-02-07T13:26:41.810751+09:00",
    "checkTime": "2026-02-07T14:16:53.538684+09:00",
    "datasources": [
      {
        "datasourceId": "pg_master",
        "databaseName": "crm-system",
        "active": true,
        "readonly": false,
        "maxOpenConns": 100,
        "maxIdleConns": 10,
        "maxTxConns": 5,
        "openConns": 0,
        "idleConns": 0,
        "runningQuery": 0,
        "runningTx": 0,
        "latencyP95Ms": 16,
        "errorRate1m": 0,
        "timeoutRate1m": 0
      },
      {
        "datasourceId": "pg_slave01",
        "databaseName": "crm-system",
        "active": true,
        "readonly": true,
        "maxOpenConns": 100,
        "maxIdleConns": 10,
        "maxTxConns": 0,
        "openConns": 0,
        "idleConns": 0,
        "runningQuery": 0,
        "runningTx": 0,
        "latencyP95Ms": 16,
        "errorRate1m": 0,
        "timeoutRate1m": 0
      }
    ]
  }
}
```


### query
```json
curl -X POST "http://localhost:5678/rdb/query?_DbName=crm-system" \
  -H "X-Secret-Key: secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "SQL": "SELECT * FROM users WHERE id = $1",
    "Params": [
      {
        "Type": "STRING",
        "Value": "XXX"
      }
    ]
  }'

response:
{
  "meta": [
    {
      "name": "id",
      "dbType": "TEXT",
      "nullable": false
    },
    {
      "name": "name",
      "dbType": "TEXT",
      "nullable": false
    },
    {
      "name": "email",
      "dbType": "TEXT",
      "nullable": false
    },
    {
      "name": "password",
      "dbType": "TEXT",
      "nullable": false
    },
    {
      "name": "icon",
      "dbType": "TEXT",
      "nullable": false
    },
    {
      "name": "active",
      "dbType": "BOOL",
      "nullable": false
    },
    {
      "name": "anonymous",
      "dbType": "BOOL",
      "nullable": false
    },
    {
      "name": "created_at",
      "dbType": "TIMESTAMP",
      "nullable": false
    },
    {
      "name": "updated_at",
      "dbType": "TIMESTAMP",
      "nullable": false
    },
    {
      "name": "last_logged_in_at",
      "dbType": "TIMESTAMP",
      "nullable": false
    },
    {
      "name": "email_verified",
      "dbType": "BOOL",
      "nullable": false
    }
  ],
  "rows": [],
  "totalCount": 0,
  "elapsedTimeMs": 3
}
```


### execute
```json
curl -X POST "http://localhost:5678/rdb/execute?_DbName=crm-system" \
  -H "X-Secret-Key: secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "SQL": "UPDATE users SET email = $2 WHERE id = $1",
    "Params": [
      {
        "Type": "STRING",
        "Value": "XXX"
      },
      {
        "Type": "STRING",
        "Value": "080-1234-5678"
      }
    ]
  }' 

response:
{
  "effectedRows": 0,
  "elapsedTimeMs": 4
}
```

### beginTx
```json
curl -X POST "http://localhost:5678/rdb/tx/begin?_DbName=crm-system" \
  -H "X-Secret-Key: secret-key" \
  -H "Content-Type: application/json" \

response:
{
  "txId": "aYbRX3xdKxMAAmNP6KF-",
  "nodeId": "dbapi-1-eASEFNBiJ",
  "expiresAt": "2026-02-07T14:45:18.721136+09:00"
}

```


## エラーハンドリング
- ネットワークエラー: 自動的に別ノードにリトライ（最大3回）、ただしトランザクション内の場合はカレントノードのみ
- リダイレクト（307）: カレントノードから別ノードへの切り替え要求のため、自動的にリダイレクト先ノードにリトライ


## 自動負荷分散ロジック
- 最初の `smartclient.Init("config.yaml")` の時だけ各ノードの Health 情報を取得する
- 各ノードHealth情報から得た全てのデーターソースが、負荷分散ロジックの対象である
- 全ノードのdatasourcesからdatabaseNameが一致するdatasourceに対し分散ロジックを適応する
- Readonlyデータソースは `Execute(), BeginTx()` の対象外
- maxTxConns=0データソースは `BeginTx()` の対象外
- 対象データソースの選択は単純な重みづけランダムで行う、重みとして、Query時は（maxOpenConns-maxTxConns）、Execute時は（maxOpenConns-maxTxConns/2）、BeginTx時は（maxTxConns）


### トランザクションに関して
- `tx, err := dbclient.BeginTx(...)` で、tx 内部では txId が保持される
- txId をトランザクションサイクル（Query/Execute/Commit/Rollback/Close）で使用する

## config.yaml の項目
| 項目 | 説明 |
|------|------|
| `defaultSecretKey` | ノード未指定時の X-Secret-Key |
| `defaultDatabase` | （任意）`Get()` 無引数で使うデータベース名 |
| `clusterNodes` | ノード一覧。各要素は `baseUrl` 必須、`secretKey` は省略時 defaultSecretKey |

