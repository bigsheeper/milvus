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
	"time"
)

// quotaConfig is configuration for quota and limitations.
type quotaConfig struct {
	Base *BaseTable
	once sync.Once

	EnableQuotaAndLimits bool

	QuotaCenterCollectInterval int64

	// ddl
	DDLCollectionRate float64
	DDLPartitionRate  float64
	DDLIndexRate      float64
	DDLFlushRate      float64
	DDLCompactionRate float64

	// dml
	DMLInsertRate   float64
	DMLDeleteRate   float64
	DMLBulkLoadRate float64

	// dql
	DQLSearchRate float64
	DQLQueryRate  float64

	// limits
	MaxCollectionNum int

	ForceDenyWriting              bool
	MaxTSafeDelay                 time.Duration
	DataNodeMemoryLowWaterLevel   float64
	DataNodeMemoryHighWaterLevel  float64
	QueryNodeMemoryLowWaterLevel  float64
	QueryNodeMemoryHighWaterLevel float64

	ForceDenyReading     bool
	MaxNQInQueue         int64
	MaxQueryTasksInQueue int64
}

func (p *quotaConfig) init(base *BaseTable) {
	p.Base = base

	p.initEnableQuotaAndLimits()
	p.initQuotaCenterCollectInterval()

	p.initDDLCollectionRate()
	p.initDDLPartitionRate()
	p.initDDLIndexRate()
	p.initDDLFlushRate()
	p.initDDLCompactionRate()

	p.initDMLInsertRate()
	p.initDMLDeleteRate()
	p.initDMLBulkLoadRate()

	p.initDQLSearchRate()
	p.initDQLQueryRate()

	p.initMaxCollectionNum()

	p.initForceDenyWriting()
	p.initMaxTSafeDelay()
	p.initDataNodeMemoryLowWaterLevel()
	p.initDataNodeMemoryHighWaterLevel()
	p.initQueryNodeMemoryLowWaterLevel()
	p.initQueryNodeMemoryHighWaterLevel()

	p.initForceDenyReading()
	p.initMaxNQInQueue()
	p.initMaxQueryTasksInQueue()
}

func (p *quotaConfig) initEnableQuotaAndLimits() {
	p.EnableQuotaAndLimits = p.Base.ParseBool("quotaAndLimits.enable", false)
}

func (p *quotaConfig) initQuotaCenterCollectInterval() {
	p.QuotaCenterCollectInterval = p.Base.ParseInt64WithDefault("quotaAndLimits.quotaCenterCollectInterval", 1000)
}

func (p *quotaConfig) initDDLCollectionRate() {
	p.DDLCollectionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.collectionRate", 10)
}

func (p *quotaConfig) initDDLPartitionRate() {
	p.DDLPartitionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.partitionRate", 10)
}

func (p *quotaConfig) initDDLIndexRate() {
	p.DDLIndexRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.indexRate", 10)
}

func (p *quotaConfig) initDDLFlushRate() {
	p.DDLFlushRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.flushRate", 10)
}

func (p *quotaConfig) initDDLCompactionRate() {
	p.DDLCompactionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.compactionRate", 10)
}

func megaBytesRate2Bytes(f float64) float64 {
	return f * 1024 * 1024
}

func (p *quotaConfig) initDMLInsertRate() {
	rate := p.Base.ParseFloatWithDefault("quotaAndLimits.dml.insertRate", 64)
	p.DMLInsertRate = megaBytesRate2Bytes(rate)
}

func (p *quotaConfig) initDMLDeleteRate() {
	rate := p.Base.ParseFloatWithDefault("quotaAndLimits.dml.deleteRate", 1)
	p.DMLDeleteRate = megaBytesRate2Bytes(rate)
}

func (p *quotaConfig) initDMLBulkLoadRate() {
	p.DMLBulkLoadRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.deleteRate", 1)
}

func (p *quotaConfig) initDQLSearchRate() {
	rate := p.Base.ParseFloatWithDefault("quotaAndLimits.dql.searchRate", 0.1)
	p.DQLSearchRate = megaBytesRate2Bytes(rate)
}

func (p *quotaConfig) initDQLQueryRate() {
	rate := p.Base.ParseFloatWithDefault("quotaAndLimits.dql.queryRate", 0.01)
	p.DQLQueryRate = megaBytesRate2Bytes(rate)
}

func (p *quotaConfig) initMaxCollectionNum() {
	p.MaxCollectionNum = p.Base.ParseIntWithDefault("quotaAndLimits.limits.collection.maxNum", 64)
}

func (p *quotaConfig) initForceDenyWriting() {
	p.ForceDenyWriting = p.Base.ParseBool("quotaAndLimits.limitWriting.forceDeny", false)
}

func (p *quotaConfig) initMaxTSafeDelay() {
	delay := p.Base.ParseInt64WithDefault("quotaAndLimits.limitWriting.maxTSafeDelay", 10)
	p.MaxTSafeDelay = time.Duration(delay) * time.Second
}

func (p *quotaConfig) initDataNodeMemoryLowWaterLevel() {
	p.DataNodeMemoryLowWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.dataNodeMemoryLowWaterLevel", 0.8)
	if p.DataNodeMemoryLowWaterLevel <= 0 || p.DataNodeMemoryLowWaterLevel > 1 {
		panic("DataNodeMemoryLowWaterLevel must in the range of `(0, 1]`")
	}
}

func (p *quotaConfig) initDataNodeMemoryHighWaterLevel() {
	p.DataNodeMemoryHighWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.dataNodeMemoryHighWaterLevel", 0.9)
	if p.DataNodeMemoryHighWaterLevel <= 0 || p.DataNodeMemoryHighWaterLevel > 1 {
		panic("DataNodeMemoryHighWaterLevel must in the range of `(0, 1]`")
	}
}

func (p *quotaConfig) initQueryNodeMemoryLowWaterLevel() {
	p.QueryNodeMemoryLowWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.queryNodeMemoryLowWaterLevel", 0.8)
	if p.QueryNodeMemoryLowWaterLevel <= 0 || p.QueryNodeMemoryLowWaterLevel > 1 {
		panic("QueryNodeMemoryLowWaterLevel must in the range of `(0, 1]`")
	}
}

func (p *quotaConfig) initQueryNodeMemoryHighWaterLevel() {
	p.QueryNodeMemoryHighWaterLevel = p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.queryNodeMemoryHighWaterLevel", 0.9)
	if p.QueryNodeMemoryHighWaterLevel <= 0 || p.QueryNodeMemoryHighWaterLevel > 1 {
		panic("QueryNodeMemoryHighWaterLevel must in the range of `(0, 1]`")
	}
}

func (p *quotaConfig) initForceDenyReading() {
	p.ForceDenyReading = p.Base.ParseBool("quotaAndLimits.limitReading.forceDeny", false)
}

func (p *quotaConfig) initMaxNQInQueue() {
	p.MaxNQInQueue = p.Base.ParseInt64WithDefault("quotaAndLimits.limitReading.maxNQInQueue", 100000)
}

func (p *quotaConfig) initMaxQueryTasksInQueue() {
	p.MaxQueryTasksInQueue = p.Base.ParseInt64WithDefault("quotaAndLimits.limitReading.maxQueryTasksInQueue", 1024)
}
