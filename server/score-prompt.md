
## ロードバランシング設計

### 前提要件
- 自分に余力がない場合のみ、スコア計算を行い、最有力候補のノード情報（リダイレクト先）を返す
- トランザクション処理はいかなる場合も受け入れる（req.txIdが存在する場合、自分で処理するしかない）

以下を **LBの判断材料として使用**してください。

#### キャパシティ系（量）
- runningHttpSession
- runningSql
- runningTx
- maxHttpSessions
- maxOpenConns
- maxTransactionConns
- openConns
- idleConns
- waitConnCount

#### 品質系（質）
- p95LatencyMs
- errorRate1m
- timeouts1m

#### Tx安全性・運用系
- uptimeSec
- status（SERVING / DRAINING / DOWN）

### 前処理：正規化（0〜1）

```ts
clamp01(x) = min(1, max(0, x))

httpUsage = runningHttpSession / maxHttpSessions
dbUsage   = openConns / maxOpenConns
txUsage   = runningTx / maxTransactionConns

httpFree = 1 - clamp01(httpUsage)
dbFree   = 1 - clamp01(dbUsage)
txFree   = 1 - clamp01(txUsage)
```

### 品質指標の正規化

```
LAT_BAD_MS = 2000   # 2秒を"かなり悪い"基準
ERR_BAD    = 0.05   # 5%をかなり悪い
TO_BAD     = 20     # 1分20回timeoutをかなり悪い
WAIT_BAD   = 10
UPTIME_OK  = 300
```

```
# 遅延は小さいほど良い。p95は相対評価が安定。
latScore   = 1 - clamp01( log1p(p95LatencyMs) / log1p(LAT_BAD_MS) )
# 失敗率/タイムアウトは小さいほど良い
errScore   = 1 - clamp01( errorRate1m / ERR_BAD )
toScore    = 1 - clamp01( timeouts1m / TO_BAD )
# waitConnCount は「プール枯渇の兆候」なので強めにペナルティ
waitScore  = 1 - clamp01( waitConnCount / WAIT_BAD )
# idleConns は多いほど良いが、maxに対して比率で見る
idleScore  = clamp01( idleConns / maxOpenConns )
# uptimeは再起動直後を避けたいだけなので「下限ゲート or 弱い加点」
uptimeScore= clamp01( uptimeSec / UPTIME_OK )
```

### ゲート（除外条件）※スコア前に必ず適用

#### 共通ゲート
- status != "SERVING" → 除外
- maxHttpSessions <= 0 OR maxOpenConns <= 0 OR maxTransactionConns <= 0 → 除外
- dbFree <= 0 → 原則除外（DB接続枯渇）

#### Tx begin 専用ゲート
- txFree < 0.05 → 除外
- waitConnCount >= 20 → 除外

#### query / execute 用軽量ゲート
- errScore == 0 → 除外
- latScore == 0 → 除外


### スコア計算式（用途別）

#### SELECT（/query）
```
scoreQuery =
  0.22*dbFree +
  0.18*httpFree +
  0.10*txFree +
  0.20*latScore +
  0.12*errScore +
  0.08*toScore +
  0.06*waitScore +
  0.02*idleScore +
  0.02*uptimeScore
```

#### DML（/execute）
DB枯渇・失敗回避重視。
```
scoreExecute =
  0.30*dbFree +
  0.14*httpFree +
  0.08*txFree +
  0.14*latScore +
  0.14*errScore +
  0.10*toScore +
  0.08*waitScore +
  0.02*idleScore
  ```

#### トランザクション開始（/tx/begin）
Tx枠の空き最優先。
```
scoreBeginTx =
  0.42*txFree +
  0.22*dbFree +
  0.08*httpFree +
  0.10*errScore +
  0.06*toScore +
  0.06*waitScore +
  0.04*latScore +
  0.02*idleScore
```

### 相対レイテンシ補正（強く推奨）
可能な場合、p95LatencyMs は 候補ノード内の中央値に対する比率で評価せよ。
```
latRatio = p95LatencyMs / median(p95LatencyMs of candidates)
latScore = 1 / (1 + (latRatio - 1) * K)   // K = 1.5〜3
```


### ノード選択方式
- スコア上位 TopK（default: 3）を抽出
- スコアを重みにした Weighted Random で1ノード選択
- node設定の weight があれば score * weight を使用


