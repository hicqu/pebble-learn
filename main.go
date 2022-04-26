package main

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"unsafe"

	"github.com/cockroachdb/pebble"
)

func string2Bytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func infinateL1() {
	db, err := pebble.Open("./infinate-L1", &pebble.Options{
		// NOTE: seems it conflicts with db.Set.
		DisableWAL: true,
		EventListener: pebble.EventListener{
			CompactionEnd: func(info pebble.CompactionInfo) {
				outfiles := make([]string, 0, len(info.Output.Tables))
				for _, table := range info.Output.Tables {
					x := fmt.Sprintf("(%d@%d[%d])", table.FileNum, info.Output.Level, table.Size)
					outfiles = append(outfiles, x)
				}
				infiles := make([]string, 0, 128) // enough for tests.
				for _, level := range info.Input {
					for _, table := range level.Tables {
						x := fmt.Sprintf("(%d@%d[%d])", table.FileNum, level.Level, table.Size)
						infiles = append(infiles, x)
					}
				}
				fmt.Printf("compact %s to %s at %d\n", strings.Join(infiles, ","), strings.Join(outfiles, ","), info.Output.Level)
			},
		},
		Levels:                      []pebble.LevelOptions{pebble.LevelOptions{Compression: pebble.NoCompression}},
		MemTableSize:                1024 * 256,
		DisableAutomaticCompactions: true,
		L0StopWritesThreshold:       math.MaxInt64,
	})
	if err != nil {
		fmt.Printf("open pebble db error: %v\n", err)
		return
	}

	for i := 0; i < 1024*1024; i++ {
		k := string2Bytes(fmt.Sprintf("key-%09d", i))
		_, closer, err := db.Get(k)
		if err == nil {
			closer.Close()
			continue
		} else if err == pebble.ErrNotFound {
			if err = db.Set(k, k, &pebble.WriteOptions{Sync: false}); err != nil {
				fmt.Printf("set error: %v\n", err)
				return
			}
		} else {
			fmt.Printf("db get error: %v\n", err)
			return
		}
	}

	it := db.NewIter(&pebble.IterOptions{
		LowerBound: string2Bytes(fmt.Sprintf("key-%09d", 1024)),
		UpperBound: string2Bytes(fmt.Sprintf("key-%09d", 1024*1000)),
	})
	if err = it.Close(); err != nil {
		fmt.Printf("close iter error: %v\n", err)
	}

	if err = db.Close(); err != nil {
		fmt.Printf("close db error: %v\n", err)
	}
}

func main() {
	infinateL1()
}
