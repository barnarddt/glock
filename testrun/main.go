package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/barnarddt/glock"
	"github.com/corverroos/goku/client/logical"
	"github.com/corverroos/goku/db"
	_ "github.com/go-sql-driver/mysql"
	"math/rand"
	"sync"
)

func main() {
	dbc, err := sql.Open("mysql", "tcp(127.0.0.1:3306)/goku?parseTime=true")

	if err != nil {
		panic(err.Error())
	}

	mp := make(map[int]int)
	db.FillGaps(dbc)
	go db.ExpireLeasesForever(dbc)

	lc := logical.New(dbc, dbc)

	wg := sync.WaitGroup{}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(num int) {
			lock, pid := glock.New(lc, "test_key")
			for y := 0; y < 100; y++ {
				lock.Lock(context.TODO())
				ran := rand.Int()
				mp[1] = ran
				fmt.Println("lock for thread ", num)
				if mp[1] != ran {
					sm, _ := lc.Get(context.Background(), "test_key")
					fmt.Println("Unexpected value in lock", ran, mp[1], string(sm.Value), pid)
				}

				lock.Unlock(context.TODO())
			}
			defer wg.Done()
		}(i)
	}

	wg.Wait()
}
