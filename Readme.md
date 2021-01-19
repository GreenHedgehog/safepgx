# Safepgx 

Soft wrapper around [pgxpool](https://github.com/jackc/pgx/tree/master/pgxpool) postgreSQL connection pool. Designed for simple resources-leak-free API.

## Instalalation
```bash
go get -v github.com/GreenHedgehog/safepgx
```

### Example
```golang
pool, destructor := safepgx.New(pool) // Init safe pool with already defined `*pgxpool.Pool`
defer destructor(context.Background()) // Register resource close in the end

err := pool.WithConnection(ctx, func(c safepgx.Connection) error { // Connection wrapping
    return c.WithTransaction(ctx, pgx.Serializable, func(tx safepgx.Transaction) error { // Transaction wrapping
        var key, value int64
        return tx.Query(ctx, // some in transaction work
            func(row pgx.Row) error {
                err := rows.Scan(&key, &value)
                if err != nil {
                    return err
                }
                print(key, value)
                return
            },
            "select key, value from example where id=$1",
            12345,
        )
    })
})
if err != nil {
    // handler error
}
```