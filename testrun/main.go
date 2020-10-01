package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/barnarddt/glock"
	"github.com/corverroos/goku/client/logical"
	"github.com/corverroos/goku/db"
	_ "github.com/go-sql-driver/mysql"
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
			lock := glock.New(lc, "test_key")
			for y := 0; y < 10; y++ {
				lock.Lock()
				ran := rand.Int()
				mp[1] = ran
				fmt.Println("lock for thread ", num)
				if mp[1] != ran {
					fmt.Println("Unexpected value in lock", ran, mp[1])
				}

				lock.Unlock()
			}
			defer wg.Done()
		}(i)
	}

	wg.Wait()

	lock := glock.New(lc, "long_key")
	ctx := lock.Lock()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second * 10)
		fmt.Println("ctx err", ctx.Err())
	}
	lock.Unlock()
	fmt.Println("ctx err", ctx.Err())
}
