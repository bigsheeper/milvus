package querynode

//import (
//	"sync"
//	"testing"
//)
//
//type state int32
//
//const (
//	start     state = 0
//	pause     state = 1
//	resume    state = 2
//	terminate state = 3
//)
//
//var wg *sync.WaitGroup
//
///*
//Benchmark result:
//	goos: linux
//	goarch: amd64
//	pkg: github.com/milvus-io/milvus/internal/querynode
//	cpu: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
//	BenchmarkAAA_AAA
//	BenchmarkAAA_AAA-12    	   86773	     12095 ns/op
//	PASS
//*/
//func BenchmarkAAA_AAA(b *testing.B) {
//	wg = &sync.WaitGroup{}
//
//	signaller := make(chan state)
//	go handler(signaller)
//	signaller <- start
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		signaller <- pause
//		wg.Wait()
//		signaller <- resume
//	}
//}
//
//func handler(signaller chan state) {
//	done := make(chan struct{})
//	for {
//		signal := <-signaller
//		switch signal {
//		case start:
//			wg.Add(1)
//			go work(done)
//		case pause:
//			done <- struct{}{}
//		case resume:
//			wg.Add(1)
//			go work(done)
//		case terminate:
//			done <- struct{}{}
//			return
//		default:
//			panic("invalid signal")
//		}
//	}
//}
//
//func work(done <-chan struct{}) {
//	defer wg.Done()
//	for {
//		select {
//		case <-done:
//			return
//		default:
//			//log.Println("working...")
//		}
//	}
//}
