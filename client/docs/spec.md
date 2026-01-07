## 概要
AWS DATA API like Datasouceサーバー群があるので、それに対しHTTP経由でSQLを投げて実行する
クラアントサイドモジュールを開発する


### 主要モジュール(参考用)
```ts
class Datasource {
  dbName string
  readonly boolean
  unavailableUntil time
}
class NodeInfo {
  nodeId string
  baseUrl string
  secretKey string
  datasources Datasource[]
  unavailableUntil time
}

static nodes = NodeInfo[]

static dbMap as Map = {
  dbName: NodeInfo[]
}

class SmartClient {
  constructor(config)
  query(dbName, sql, params?, opts?)
  execute(dbName, sql, params?, opts?)
  beginTx(dbName, opts?)
  close()
}

class TxHandle {
  query(txId, sql, params?, opts?)
  execute(txId, sql, params?, opts?)
  commit(txId)
  rollback(txId)
}
```

### txIdに関して
SmartClient.beginTx(dbName)の時、
nodesの該当Indexを一緒にサーバー側に送ることで、NodeIndex情報を含んだtxIdが返却される。
txIdをbase64.RawURLEncoding.DecodeStringを行い、先頭1バイトからNodeIndexを還元できるため、
続きのquery(), execute()などはdbName指定しなくてもtxIdだけで、開始したトランザクションのノードを特定できる


### 処理流れ

#### 初期処理
モジュールロード時config.yamlを読み込みNodeInfoを準備して置く

#### メイン処理
- SmartClient系処理の場合、dbNameからdbMapより実行可能NodeInfo一覧を取得し、ランダムで特定する。
- 処理が正常の場合は、レスポンス結果を返却する
- ネットワークエラーの場合、該当NodeInfo.unavailableUntilを15秒先に設定する
  - 次のノードをランダムで選択し、リトライを行う
- レスポンスがリダイレクトの場合、該当Datasource.unavailableUntilを5秒先に設定する
  - リダイレクト情報からNodeIdを取得し、該当ノードに対しリトライ行う

#### 補足
- TxHandle系の処理はリトライを行わない
- readonlyのDatasourceに対してのexecute()は行えない
- unavailableUntilまでは処理を投げない
- secretKeyをHeaderにつけて認証を行う
