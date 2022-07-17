// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"
	"time"
)

var numElements = 10 * 1024 * 1024
var vectorDim = 128
var runTimes = 100

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func getData() []float32 {
	data := make([]float32, 0, numElements*vectorDim)
	return data
}

func run() {
	w, _ := storage.NewPayloadWriter(schemapb.DataType_FloatVector)
	defer w.ReleasePayloadWriter()
	data := getData()
	for i := 0; i < numElements; i++ {
		data = append(data, rand.Float32())
	}
	err := w.AddFloatVectorToPayload(data, vectorDim)
	if err != nil {
		panic(err)
	}
	err = w.FinishPayloadWriter()
	if err != nil {
		panic(err)
	}
	buffer, _ := w.GetPayloadBufferFromWriter()

	for i := 0; i < runTimes; i++ {
		//used := metricsinfo.GetUsedMemoryCount()
		//fmt.Println("running ...", i, ", used = ", float64(used)/1024.0/1024.0, " MB")
		fmt.Println("running ...", i)
		PrintMemUsage()
		r, _ := storage.NewPayloadReader(schemapb.DataType_FloatVector, buffer)
		_, _, err = r.GetFloatVectorFromPayload()
		if err != nil {
			panic(err)
		}
		r.ReleasePayloadReader()
	}
}

func main() {
	go func() {
		if err := http.ListenAndServe("localhost:8001", nil); err != nil {
			panic(err)
		}
	}()

	fmt.Println("enter")
	time.Sleep(5 * time.Second)
	fmt.Println("begin run")

	run()

	debug.FreeOSMemory()
	//used := metricsinfo.GetUsedMemoryCount()
	//fmt.Println("all run, used = ", float64(used)/1024.0/1024.0, " MB")
	fmt.Println("all run")
	PrintMemUsage()
	time.Sleep(100 * time.Second)
}
