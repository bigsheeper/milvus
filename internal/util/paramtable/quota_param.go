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

package paramtable

import (
	"sync"
)

// quotaConfig is configuration for quota and limitations.
type quotaConfig struct {
	Base *BaseTable
	once sync.Once

	EnableQuotaAndLimits bool

	QuotaCenterCollectInterval int64
	WarmUpSpeed                float64

	// ddl
	DDLCollectionRate float64
	DDLPartitionRate  float64
	DDLIndexRate      float64
	DDLSegmentsRate   float64

	// dml
	DMLInsertRate float64
	DMLDeleteRate float64
	MaxInsertSize int
	MaxDeleteSize int

	// dql
	DQLSearchRate float64
	DQLQueryRate  float64
	MaxSearchSize int
	MaxQuerySize  int

	// limits
	MaxCollectionNum int

	ForceDenyWriting          bool
	DataNodeMemoryWaterLevel  float64
	QueryNodeMemoryWaterLevel float64

	ForceDenyReading bool
}

func (p *quotaConfig) init(base *BaseTable) {
	p.Base = base

	p.initEnableQuotaAndLimits()

	p.initQuotaCenterCollectInterval()
	p.initWarmUpSpeed()

	p.initDDLCollectionRate()
	p.initDDLPartitionRate()
	p.initDDLIndexRate()
	p.initDDLSegmentsRate()

	p.initDMLInsertRate()
	p.initDMLDeleteRate()
	p.initMaxInsertSize()
	p.initMaxDeleteSize()

	p.initDQLSearchRate()
	p.initDQLQueryRate()
	p.initMaxSearchSize()
	p.initMaxQuerySize()

	p.initMaxCollectionNum()

	p.initForceDenyWriting()
	p.initDataNodeMemoryWaterLevel()
	p.initQueryNodeMemoryWaterLevel()
	p.initForceDenyReading()
}

func (p *quotaConfig) initEnableQuotaAndLimits() {
	p.EnableQuotaAndLimits = p.Base.ParseBool("quotaAndLimits.enable", false)
}

func (p *quotaConfig) initQuotaCenterCollectInterval() {
	p.QuotaCenterCollectInterval = p.Base.ParseInt64("quotaAndLimits.quotaCenterCollectInterval")
}

func (p *quotaConfig) initWarmUpSpeed() {
	p.WarmUpSpeed = p.Base.ParseFloat("quotaAndLimits.warmUpSpeed")
	if p.WarmUpSpeed <= 0 || p.WarmUpSpeed > 1 {
		panic("warm-up speed must in the interval of `(0, 1]`")
	}
}

func (p *quotaConfig) initDDLCollectionRate() {
	p.DDLCollectionRate = p.Base.ParseFloat("quotaAndLimits.ddl.collectionRate")
}

func (p *quotaConfig) initDDLPartitionRate() {
	p.DDLPartitionRate = p.Base.ParseFloat("quotaAndLimits.ddl.partitionRate")
}

func (p *quotaConfig) initDDLIndexRate() {
	p.DDLIndexRate = p.Base.ParseFloat("quotaAndLimits.ddl.indexRate")
}

func (p *quotaConfig) initDDLSegmentsRate() {
	p.DDLSegmentsRate = p.Base.ParseFloat("quotaAndLimits.ddl.segmentsRate")
}

func megaBytesRate2Bytes(f float64) float64 {
	return f * 1024 * 1024
}

func megaBytesSize2Bytes(i int) int {
	return i * 1024 * 1024
}

func (p *quotaConfig) initDMLInsertRate() {
	rate := p.Base.ParseFloat("quotaAndLimits.dml.insertRate")
	p.DMLInsertRate = megaBytesRate2Bytes(rate)
}

func (p *quotaConfig) initDMLDeleteRate() {
	rate := p.Base.ParseFloat("quotaAndLimits.dml.deleteRate")
	p.DMLDeleteRate = megaBytesRate2Bytes(rate)
}

func (p *quotaConfig) initMaxInsertSize() {
	size := p.Base.ParseInt("quotaAndLimits.dml.maxInsertSize")
	p.MaxInsertSize = megaBytesSize2Bytes(size)
}

func (p *quotaConfig) initMaxDeleteSize() {
	size := p.Base.ParseInt("quotaAndLimits.dml.maxDeleteSize")
	p.MaxDeleteSize = megaBytesSize2Bytes(size)
}

func (p *quotaConfig) initDQLSearchRate() {
	rate := p.Base.ParseFloat("quotaAndLimits.dql.searchRate")
	p.DQLSearchRate = megaBytesRate2Bytes(rate)
}

func (p *quotaConfig) initDQLQueryRate() {
	rate := p.Base.ParseFloat("quotaAndLimits.dql.queryRate")
	p.DQLQueryRate = megaBytesRate2Bytes(rate)
}

func (p *quotaConfig) initMaxSearchSize() {
	size := p.Base.ParseInt("quotaAndLimits.dql.maxSearchSize")
	p.MaxSearchSize = megaBytesSize2Bytes(size)
}

func (p *quotaConfig) initMaxQuerySize() {
	size := p.Base.ParseInt("quotaAndLimits.dql.maxQuerySize")
	p.MaxQuerySize = megaBytesSize2Bytes(size)
}

func (p *quotaConfig) initMaxCollectionNum() {
	p.MaxCollectionNum = p.Base.ParseInt("quotaAndLimits.limits.maxCollectionNum")
}

func (p *quotaConfig) initForceDenyWriting() {
	p.ForceDenyWriting = p.Base.ParseBool("quotaAndLimits.forceDenyWriting.enable", true)
}

func (p *quotaConfig) initDataNodeMemoryWaterLevel() {
	p.DataNodeMemoryWaterLevel = p.Base.ParseFloat("quotaAndLimits.forceDenyWriting.dataNodeMemoryWaterLevel")
	if p.DataNodeMemoryWaterLevel <= 0 || p.DataNodeMemoryWaterLevel > 1 {
		panic("DataNodeMemoryWaterLevel must in the range of `(0, 1]`")
	}
}

func (p *quotaConfig) initQueryNodeMemoryWaterLevel() {
	p.QueryNodeMemoryWaterLevel = p.Base.ParseFloat("quotaAndLimits.forceDenyWriting.queryNodeMemoryWaterLevel")
	if p.QueryNodeMemoryWaterLevel <= 0 || p.QueryNodeMemoryWaterLevel > 1 {
		panic("QueryNodeMemoryWaterLevel must in the range of `(0, 1]`")
	}
}

func (p *quotaConfig) initForceDenyReading() {
	p.ForceDenyReading = p.Base.ParseBool("quotaAndLimits.forceDenyReading.enable", true)
}
