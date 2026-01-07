## Prompt 0: 目的と制約

Go + chiでDB Data APIサーバを実装したい。
DBはPostgreSQLとMySQL(MariaDB)に対応し、database/sql を基盤にする。
Txは 同一コネクション固定が必須なので、Begin時に db.Conn(ctx) を取得し、
その *sql.Conn 上で BeginTx した *sql.Tx を保持する。
Tx終了時（commit/rollback/timeout）には必ず conn.Close() してプールへ返却する。
Txは明示commit/rollbackがない場合、30秒で自動rollbackして解放する。
SQL方言（プレースホルダ $1 / ?）は変換しない。クライアント責任。
datasourceId で接続先DBを切替可能（設定ファイルで複数定義）。
まずは認証なしで良い。OpenAPIは /server/docs/openapi.yaml を参照。


## Prompt 1: リポジトリ雛形生成（chi + config + router）

/serverフォルダに次の構成でGoプロジェクトを作ってください。go modules使用。
HTTPはchi。設定は config.yaml を読み込む（環境変数でも上書き可能な設計だと尚良いが、最初はyamlのみでOK）。

#### ディレクトリ：
- /main.go
- /rdb/datasources.go
- /http/router.go
- /http/health.go
- /docs/openapi.yaml
- /config.yaml

#### ルート：
- GET /health → ok/nodeId/timeを返す（DB接続チェックは後で追加で良い）
- POST /v1/rdb/execute（未実装でOK、501を返しても良い）
- POST /v1/rdb/tx/begin, /v1/rdb/tx/commit, /v1/rdb/tx/rollback（未実装でOK）


## Prompt 2: datasources 実装（Postgres/MySQL両対応）

/rdb/datasources.go を実装して、設定の datasources から map[string]*sql.DB を構築してください。

対応driver：
- postgres: github.com/jackc/pgx/v5/stdlib をimportして sql.Open("pgx", dsn)
- mysql: github.com/go-sql-driver/mysql をimportして sql.Open("mysql", dsn)

datasourceごとに以下設定を反映：
- maxOpenConns
- maxIdleConns
- connMaxLifetimeSeconds

PingContext で起動時に接続確認して、失敗なら起動を止める（最初はmainだけ必須でもOK）。
main.go から datasources を初期化して routerへ渡す。


## Prompt 3: TxID（owner固定 + HMAC）を実装

/rdb/txid.go を作り、txIdの生成/検証/デコードを実装してください。
フォーマット：base64url( version|ownerNodeShort|datasourceShort|issuedAtMs|random16|hmac32 )
- version: 1 byte (=1)
- ownerNodeShort: 2 bytes（configで nodeShortId を指定しても良い。最初は固定で01でもOK）
- datasourceShort: 2 bytes（datasourceIdをshortにマッピング。configに datasourceShortMap がなくても、main=1などで暫定実装可）
- issuedAtMs: 8 bytes (uint64)
- random16: 16 bytes
- hmac32: HMAC-SHA256(secret, payload)
secretは config の txSecret（base64でも平文でも良いが、実装は平文stringでHMAC）

APIとしては：
- Generate(ownerShort uint16, dsShort uint16) (string, error)
- VerifyAndParse(txId string) (TxInfo, error)
- TxInfoには ownerShort, dsShort, issuedAt を入れる
※ 後でSmart Clientがowner判定できるように、owner/dsが必ず取り出せるようにする。


## Prompt 4: TxManager（同一Conn固定 + 30秒TTL + maxTxSessions）

/rdb/txmgr.go を作り、Tx管理を実装してください。必須要件：
- Begin時：db.Conn(ctx) → conn.BeginTx → TxEntry登録
- TxEntry：txId, datasourceId, conn *sql.Conn, tx *sql.Tx, expiresAt, lastTouch
- maxTxSessions を semaphore（channel）で制限。begin前にacquire、終了時にrelease
- TTL：txTimeoutSeconds（デフォルト30）
- 背景掃除：time.Ticker で 1秒ごとに期限切れを走査して Rollback + conn.Close() + エントリ削除
- Commit/Rollback：成功/失敗に関わらず conn.Close() して返却する
- Execute(txIdあり) で触れたら expiresAt = now + ttl に延長する（延命する仕様）

API：
- Begin(ctx, datasourceId, isolationLevel) (txId string, err)
- Get(txId) (*TxEntry, err)（Touch込みでも良い）
- Commit(txId) error
- Rollback(txId) error

Txが存在しない/期限切れの場合はエラー種別を分ける（例：ErrTxNotFound, ErrTxExpired）。



## Prompt 5: Execute実装（query/exec明示、型付きparams）

/v1/rdb/execute を実装してください。最初は mode は query|exec のみサポートして auto は 400で弾いてOK。
- リクエスト：datasourceId, sql, params[{type,value}], txId?, timeoutMs, mode
- params変換：typeに応じてGo型へ変換（int64/float64/bool/text/bytes_base64/timestamp_rfc3339/null）
- ctx timeout：timeoutMs を context.WithTimeout
- txIdなし：db.QueryContext / db.ExecContext
- txIdあり：TxManagerからTxEntry取得して entry.tx.QueryContext / entry.tx.ExecContext（TouchしてTTL延長）

結果のJSON化：
- Queryの場合：rows.Columns() と rows.ColumnTypes() で meta.columns を作る（dbType, nullable）
- 各行は map[string]any で返す
- []byte は base64文字列にする
- time.Time は RFC3339
- rowCount は数える（ただしDBによっては重い。最初は数えてOK）

エラーHTTPコード：
- txIdありでTxなし/期限切れ：409
- pool枯渇（Begin時）：503
- ctx timeout：408
- その他：503（最初は単純でOK）



## Prompt 6: Tx API（begin/commit/rollback）をハンドラとして結線

/v1/rdb/tx/begin /v1/rdb/tx/commit /v1/rdb/tx/rollback を実装。
BeginはTxManager.Beginを呼び、txId/ownerNodeId/datasourceId/expiresInMsを返す。ownerNodeIdはconfigのnodeId文字列。
Commit/RollbackはTxManager呼び出しで ok:true を返す。
