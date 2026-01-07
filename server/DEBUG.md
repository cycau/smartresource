# デバッグガイド

このプロジェクトをデバッグする方法を説明します。

## 前提条件

### 1. Delveのインストール

GoのデバッガーであるDelveをインストールします：

```bash
go install github.com/go-delve/delve/cmd/dlv@latest
```

インストール後、`$GOPATH/bin`が`$PATH`に含まれていることを確認してください。

### 2. VS Code拡張機能

VS Codeを使用する場合、以下の拡張機能をインストールしてください：

- Go (golang.go)

## デバッグ方法

### VS Codeでのデバッグ

1. **ブレークポイントの設定**
   - デバッグしたい行の左側をクリックしてブレークポイントを設定
   - 赤い点が表示されます

2. **デバッグの開始**
   - `F5`キーを押す、または
   - デバッグパネル（Cmd+Shift+D）を開き、「Launch Server」を選択して実行

3. **利用可能なデバッグ設定**
   - **Launch Server**: デフォルト設定でサーバーを起動
   - **Launch Server (with config)**: config.yamlを指定して起動
   - **Attach to Process**: 実行中のプロセスにアタッチ
   - **Debug Tests**: テストをデバッグモードで実行

### コマンドラインでのデバッグ

```bash
cd server
dlv debug --listen=:2345 --headless=true --api-version=2 --accept-multiclient
```

別のターミナルで：

```bash
dlv connect :2345
```

### 主要なデバッグポイント

以下の関数にブレークポイントを設定すると便利です：

#### HTTPハンドラ
- `server/http/execute.go:119` - `handleQuery`関数の開始
- `server/http/execute.go:246` - `handleExec`関数の開始
- `server/http/tx.go:50` - `BeginTx`関数の開始
- `server/http/tx.go:110` - `CommitTx`関数の開始
- `server/http/tx.go:145` - `RollbackTx`関数の開始

#### トランザクション管理
- `server/rdb/txmgr.go:83` - `Begin`関数の開始
- `server/rdb/txmgr.go:160` - `Get`関数の開始
- `server/rdb/txmgr.go:180` - `Commit`関数の開始
- `server/rdb/txmgr.go:202` - `Rollback`関数の開始

#### データソース管理
- `server/rdb/datasources.go:43` - `Initialize`関数の開始
- `server/rdb/datasources.go:74` - `Get`関数の開始

#### メイン関数
- `server/main.go:53` - `main`関数の開始
- `server/main.go:60` - 設定読み込み後
- `server/main.go:81` - データソース初期化後
- `server/main.go:100` - ルーター作成後

## デバッグ時の注意点

1. **データベース接続**
   - デバッグ前にデータベースが起動していることを確認
   - `config.yaml`のDSNが正しいことを確認

2. **タイムアウト**
   - ブレークポイントで停止している間、タイムアウトが発生する可能性があります
   - 必要に応じてタイムアウト時間を延長

3. **並行処理**
   - トランザクション管理は並行処理を使用しているため、デバッグ時は注意が必要です

## ログ出力

デバッグ情報を確認するには、標準出力のログを確認してください：

```bash
cd server
go run main.go 2>&1 | tee debug.log
```

## トラブルシューティング

### Delveが起動しない場合

```bash
# Delveのバージョンを確認
dlv version

# 再インストール
go install github.com/go-delve/delve/cmd/dlv@latest
```

### VS Codeでデバッグが開始できない場合

1. Go拡張機能が最新であることを確認
2. `go env`でGOPATHとGOROOTが正しく設定されていることを確認
3. VS Codeを再起動

### ブレークポイントが機能しない場合

1. コードが最適化されていないことを確認（`-gcflags="-N -l"`フラグを使用）
2. デバッグモードで実行されていることを確認

