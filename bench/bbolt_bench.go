// BBolt benchmark for comparison with Thunder.
// Run with: go run bbolt_bench.go

package main

import (
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	numKeys   = 100000
	valueSize = 100
	batchSize = 100
	batchTxs  = 1000
)

var bucketName = []byte("benchmark")

func main() {
	fmt.Println("=== BBolt Benchmark Suite ===")
	fmt.Printf("Keys: %d, Value size: %d bytes\n\n", numKeys, valueSize)

	dbPath := "/tmp/bbolt_benchmark.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	runBenchmarks(dbPath)
}

func runBenchmarks(dbPath string) {
	// Sequential writes (single transaction)
	benchSequentialWrites(dbPath)

	// Sequential reads
	benchSequentialReads(dbPath)

	// Random reads
	benchRandomReads(dbPath)

	// Iterator scan
	benchIteratorScan(dbPath)

	// Mixed workload
	benchMixedWorkload(dbPath)

	// Batch writes (multiple transactions)
	benchBatchWrites(dbPath)

	// Large value benchmarks
	benchLargeValues(dbPath)
}

func benchSequentialWrites(dbPath string) {
	os.Remove(dbPath)
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	value := make([]byte, valueSize)
	for i := range value {
		value[i] = 'v'
	}

	start := time.Now()
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key_%08d", i)
			if err := b.Put([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)

	opsPerSec := float64(numKeys) / elapsed.Seconds()
	fmt.Printf("Sequential writes (%dK keys, 1 tx): %v (%.0f ops/sec)\n",
		numKeys/1000, elapsed, opsPerSec)
}

func benchSequentialReads(dbPath string) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	start := time.Now()
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key_%08d", i)
			_ = b.Get([]byte(key))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)

	opsPerSec := float64(numKeys) / elapsed.Seconds()
	fmt.Printf("Sequential reads (%dK keys): %v (%.0f ops/sec)\n",
		numKeys/1000, elapsed, opsPerSec)
}

func benchRandomReads(dbPath string) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Generate random access pattern (deterministic)
	indices := make([]int, numKeys)
	for i := 0; i < numKeys; i++ {
		indices[i] = (i*7919 + 104729) % numKeys
	}

	start := time.Now()
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for _, idx := range indices {
			key := fmt.Sprintf("key_%08d", idx)
			_ = b.Get([]byte(key))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)

	opsPerSec := float64(numKeys) / elapsed.Seconds()
	fmt.Printf("Random reads (%dK lookups): %v (%.0f ops/sec)\n",
		numKeys/1000, elapsed, opsPerSec)
}

func benchIteratorScan(dbPath string) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	start := time.Now()
	count := 0
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)

	if count != numKeys {
		panic(fmt.Sprintf("expected %d keys, got %d", numKeys, count))
	}

	opsPerSec := float64(numKeys) / elapsed.Seconds()
	fmt.Printf("Iterator scan (%dK keys): %v (%.0f ops/sec)\n",
		numKeys/1000, elapsed, opsPerSec)
}

func benchMixedWorkload(dbPath string) {
	os.Remove(dbPath)
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	value := make([]byte, valueSize)
	for i := range value {
		value[i] = 'v'
	}

	// Pre-populate with 10K keys
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		for i := 0; i < 10000; i++ {
			key := fmt.Sprintf("key_%08d", i)
			if err := b.Put([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Mixed workload: 70% reads, 30% writes
	const mixedOps = 10000
	indices := make([]int, mixedOps)
	for i := 0; i < mixedOps; i++ {
		indices[i] = (i*7919 + 104729) % 10000
	}

	start := time.Now()
	for opIdx, idx := range indices {
		if opIdx%10 < 7 {
			// 70% reads
			err = db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket(bucketName)
				key := fmt.Sprintf("key_%08d", idx)
				_ = b.Get([]byte(key))
				return nil
			})
		} else {
			// 30% writes
			err = db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(bucketName)
				key := fmt.Sprintf("mixed_%08d", opIdx)
				return b.Put([]byte(key), value)
			})
		}
		if err != nil {
			panic(err)
		}
	}
	elapsed := time.Since(start)

	opsPerSec := float64(mixedOps) / elapsed.Seconds()
	fmt.Printf("Mixed workload (%dK ops, 70%% read): %v (%.0f ops/sec)\n",
		mixedOps/1000, elapsed, opsPerSec)
}

func benchBatchWrites(dbPath string) {
	os.Remove(dbPath)
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create bucket first
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		panic(err)
	}

	value := make([]byte, valueSize)
	for i := range value {
		value[i] = 'v'
	}

	start := time.Now()
	for txIdx := 0; txIdx < batchTxs; txIdx++ {
		err = db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			for opIdx := 0; opIdx < batchSize; opIdx++ {
				key := fmt.Sprintf("batch_%06d_%04d", txIdx, opIdx)
				if err := b.Put([]byte(key), value); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	elapsed := time.Since(start)

	totalOps := batchTxs * batchSize
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	txPerSec := float64(batchTxs) / elapsed.Seconds()
	fmt.Printf("Batch writes (%dK tx, %d ops/tx): %v (%.0f ops/sec, %.0f tx/sec)\n",
		batchTxs/1000, batchSize, elapsed, opsPerSec, txPerSec)
}

func benchLargeValues(dbPath string) {
	sizes := []struct {
		size  int
		label string
	}{
		{1024, "1KB"},
		{10 * 1024, "10KB"},
		{100 * 1024, "100KB"},
		{1024 * 1024, "1MB"},
	}

	for _, s := range sizes {
		os.Remove(dbPath)
		db, err := bolt.Open(dbPath, 0600, nil)
		if err != nil {
			panic(err)
		}

		value := make([]byte, s.size)
		for i := range value {
			value[i] = 'x'
		}

		const numLarge = 100

		start := time.Now()
		err = db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return err
			}
			for i := 0; i < numLarge; i++ {
				key := fmt.Sprintf("large_%04d", i)
				if err := b.Put([]byte(key), value); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		elapsed := time.Since(start)

		db.Close()

		totalBytes := numLarge * s.size
		mbPerSec := float64(totalBytes) / (1024 * 1024) / elapsed.Seconds()
		fmt.Printf("Large values (%d × %s): %v (%.1f MB/sec)\n",
			numLarge, s.label, elapsed, mbPerSec)
	}
}
