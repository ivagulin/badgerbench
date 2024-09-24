package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/olekukonko/tablewriter"
	"github.com/samber/lo"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	concurrency   = flag.Int("concurrency", 24, "Number of concurrent goroutines")
	benchtime     = flag.Duration("benchtime", 10*time.Second, "Bench time")
	scale         = flag.Int("scale", 1000, "Scaling factor")
	readWriteMode = flag.Bool("rwmode", true, "Read write mode")
	initMode      = flag.Bool("init", false, "init")
)

const (
	accountPrefix = "accounts:"
	tellerPrefix  = "tellers:"
	branchPrefix  = "branches:"
	historyPrefix = "history:"
)

type Account struct {
	AID      int    `db:"aid"`
	BID      int64  `db:"bid"`
	Abalance int64  `db:"abalance"`
	Filler   string `db:"filler"`
}

type Teller struct {
	TID      int    `db:"tid"`
	BID      int64  `db:"bid"`
	Tbalance int64  `db:"tbalance"`
	Filler   string `db:"filler"`
}

type Branche struct {
	BID      int    `db:"bid"`
	Bbalance int64  `db:"bbalance"`
	Filler   string `db:"filler"`
}

type History struct {
	TID    int64     `db:"tid"`
	BID    int64     `db:"bid"`
	AID    int64     `db:"aid"`
	Delta  int64     `db:"delta"`
	Mtime  time.Time `db:"mtime"`
	Filler string    `db:"filler"`
}

func keyFor(prefix string, id int) []byte {
	return []byte(prefix + strconv.Itoa(id))
}

func valueFor(val interface{}) []byte {
	rv, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return rv
}

func fillTable(db *badger.DB, prefix string, limit int, genfunc func(it int) interface{}) {
	created := 0
	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(prefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			created++
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	for created < limit {
		slog.Info("filling table", "prefix", prefix, "limit", limit, "created", created)
		err = db.Update(func(txn *badger.Txn) error {
			for it := created; it < limit && it-created < 1000; it++ {
				val := genfunc(it)
				txn.Set(keyFor(prefix, it), valueFor(val))
			}
			created += 1000
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}

func fill(db *badger.DB) {
	accountsToCreate := *scale * 100_000
	tellersToCreate := *scale * 10
	branchesToCreate := *scale * 1

	fillTable(db, accountPrefix, accountsToCreate, func(it int) interface{} {
		return Account{AID: it}
	})

	fillTable(db, tellerPrefix, tellersToCreate, func(it int) interface{} {
		return Teller{TID: it}
	})

	fillTable(db, branchPrefix, branchesToCreate, func(it int) interface{} {
		return Branche{BID: it}
	})
}

func readWrite(db *badger.DB, historySeq *badger.Sequence, conflictCounter *uint64) {
	aid := rand.IntN(*scale * 100_000)
	tid := rand.IntN(*scale * 10)
	bid := rand.IntN(*scale * 1)
	adelta := rand.Int64N(10000) - 5000
	err := db.Update(func(txn *badger.Txn) error {
		//SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
		accItem := lo.Must(txn.Get(keyFor(accountPrefix, aid)))
		var acc Account
		lo.Must0(accItem.Value(func(val []byte) error {
			lo.Must0(json.Unmarshal(val, &acc))
			return nil
		}))

		//UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
		acc.Abalance += adelta
		txn.Set(keyFor(accountPrefix, aid), valueFor(acc))

		//UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
		tellerItem := lo.Must(txn.Get(keyFor(tellerPrefix, tid)))
		var teller Teller
		lo.Must0(tellerItem.Value(func(val []byte) error {
			lo.Must0(json.Unmarshal(val, &teller))
			return nil
		}))
		teller.Tbalance += adelta
		txn.Set(keyFor(tellerPrefix, tid), valueFor(teller))

		//UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
		branchItem := lo.Must(txn.Get(keyFor(branchPrefix, bid)))
		var branch Branche
		lo.Must0(branchItem.Value(func(val []byte) error {
			lo.Must0(json.Unmarshal(val, &branch))
			return nil
		}))
		branch.Bbalance += adelta
		txn.Set(keyFor(branchPrefix, tid), valueFor(branch))

		//INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
		historyID := lo.Must(historySeq.Next())
		txn.Set(keyFor(historyPrefix, int(historyID)), valueFor(History{
			AID:   int64(aid),
			TID:   int64(tid),
			BID:   int64(bid),
			Delta: adelta,
			Mtime: time.Now(),
		}))
		return nil
	})
	if err == badger.ErrConflict {
		atomic.AddUint64(conflictCounter, 1)
	} else if err != nil {
		panic(err)
	}
}

func read(db *badger.DB, historySeq *badger.Sequence) {
	aid := rand.IntN(*scale * 100_000)
	err := db.View(func(txn *badger.Txn) error {
		//SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
		accItem := lo.Must(txn.Get(keyFor(accountPrefix, aid)))
		var acc Account
		lo.Must0(accItem.Value(func(val []byte) error {
			lo.Must0(json.Unmarshal(val, &acc))
			return nil
		}))
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func main() {
	flag.Parse()

	db, err := badger.Open(badger.DefaultOptions("db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	slog.Info("filling...", "scale", *scale)
	if *initMode {
		fill(db)
	}

	slog.Info("testing...")
	var iterations uint64
	var conflicts uint64
	finishTimer, cancelFunc := context.WithTimeout(context.Background(), *benchtime)
	defer cancelFunc()
	historySeq, err := db.GetSequence([]byte(historyPrefix), 1000)
	if err != nil {
		panic(err)
	}
	defer historySeq.Release()
	var wg sync.WaitGroup
	wg.Add(*concurrency)
	for _ = range *concurrency {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-finishTimer.Done():
					return
				default:
					atomic.AddUint64(&iterations, 1)
					if *readWriteMode {
						readWrite(db, historySeq, &conflicts)
					} else {
						read(db, historySeq)
					}
				}
			}
		}()
	}
	wg.Wait()

	slog.Info("throughtput results", "concurrency", *concurrency, "iterations", iterations, "conflicts", conflicts)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "Latency(us)", "Throughput(rps)"})
	table.SetBorder(false)
	table.SetHeaderLine(false)
	table.SetRowLine(false)
	testName := lo.Ternary(*readWriteMode, "tpcb-like", "tpcb-readonly")
	latency := fmt.Sprintf("%0.3f", float64(benchtime.Microseconds())/float64(iterations)*float64(*concurrency))
	throughput := fmt.Sprintf("%0.3f", float64(iterations)/benchtime.Seconds())
	table.Append([]string{testName, latency, throughput})
	table.Render()

}
