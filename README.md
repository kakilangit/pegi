# Pegi

[![PkgGoDev](https://pkg.go.dev/badge/github.com/kakilangit/pegi)](https://pkg.go.dev/github.com/kakilangit/pegi)
[![Build Status](https://travis-ci.org/kakilangit/pegi.svg?branch=main)](https://travis-ci.org/kakilangit/pegi)


A sqlx wrapper mainly for PostgreSQL.

```shell 
go get -u -v github.com/kakilangit/pegi
```

Example:

```go
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/kakilangit/pegi"
)

func main() {
	db := pegi.NewDB(&sqlx.DB{}, &sql.TxOptions{})
	if err := db.RunInTransaction(context.Background(), func(ctx context.Context) error {
		var id string
		if err := db.GetAccessor(ctx).Get(&id, "SELECT id FROM users WHERE name = ?", "name"); err != nil {
			return err
		}

		_, err := db.QueryBuilder(ctx).Update("users").Set("address", "new address").Where("id = ?", id).Exec()
		return err
	}); err != nil {
		log.Fatal(err)
	}
}


```