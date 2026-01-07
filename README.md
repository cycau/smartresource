# smartresource

### RDB
- GET /health
- POST /rdb/query
- POST /rdb/execute
- POST /rdb/tx/begin
- PUT  /rdb/tx/commit
- PUT  /rdb/tx/rollback
- PUT /rdb/tx/done/:requestId
- GET(x) /rdb/${table} also POST PUT DELETE

### key-value
- POST /keyval
- GET  /keyval/:key

### PDF(x)
- POST /report
  {callbackurl?: when done}
- GET  /report/:id

### client
- SmartClient.query(sql, parameters)
- SmartClient.execute(sql, parameters)
- SmartClient.begin()
- tx.commit()
- tx.rollback()
- SmartClient.midleware(request)
- SmartClient.setGlobal(key || null, value, expiresInSeconds || default300)
- SmartClient.getGlobal(key)
