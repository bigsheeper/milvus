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
	"fmt"
	"sync"
	"time"
)

// quotaConfig is configuration for quota and limitations.
type quotaConfig struct {
	Base *BaseTable
	once sync.Once

	EnableQuotaAndLimits bool

	QuotaCenterCollectInterval float64

	// ddl
	DDLCollectionRate float64
	DDLPartitionRate  float64
	DDLIndexRate      float64
	DDLFlushRate      float64
	DDLCompactionRate float64

	// dml
	DMLMaxInsertRate   float64
	DMLMinInsertRate   float64
	DMLMaxDeleteRate   float64
	DMLMinDeleteRate   float64
	DMLMaxBulkLoadRate float64
	DMLMinBulkLoadRate float64

	// dql
	DQLMaxSearchRate float64
	DQLMinSearchRate float64
	DQLMaxQueryRate  float64
	DQLMinQueryRate  float64

	// limits
	MaxCollectionNum int

	ForceDenyWriting              bool
	MaxTimeTickDelay              time.Duration
	DataNodeMemoryLowWaterLevel   float64
	DataNodeMemoryHighWaterLevel  float64
	QueryNodeMemoryLowWaterLevel  float64
	QueryNodeMemoryHighWaterLevel float64

	ForceDenyReading      bool
	NQInQueueThreshold    int64
	QueueLatencyThreshold float64
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

	p.initDMLMaxInsertRate()
	p.initDMLMinInsertRate()
	p.initDMLMaxDeleteRate()
	p.initDMLMinDeleteRate()
	p.initDMLMaxBulkLoadRate()
	p.initDMLMinBulkLoadRate()

	p.initDQLMaxSearchRate()
	p.initDQLMinSearchRate()
	p.initDQLMaxQueryRate()
	p.initDQLMinQueryRate()

	p.initMaxCollectionNum()

	p.initForceDenyWriting()
	p.initMaxTimeTickDelay()
	p.initDataNodeMemoryLowWaterLevel()
	p.initDataNodeMemoryHighWaterLevel()
	p.initQueryNodeMemoryLowWaterLevel()
	p.initQueryNodeMemoryHighWaterLevel()

	p.initForceDenyReading()
	p.initNQInQueueThreshold()
	p.initQueueLatencyThreshold()
}

func (p *quotaConfig) initEnableQuotaAndLimits() {
	p.EnableQuotaAndLimits = p.Base.ParseBool("quotaAndLimits.enable", false)
}

func (p *quotaConfig) initQuotaCenterCollectInterval() {
	p.QuotaCenterCollectInterval = p.Base.ParseFloatWithDefault("quotaAndLimits.quotaCenterCollectInterval", 3.0)
}

func (p *quotaConfig) initDDLCollectionRate() {
	p.DDLCollectionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.collectionRate", -1)
}

func (p *quotaConfig) initDDLPartitionRate() {
	p.DDLPartitionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.partitionRate", -1)
}

func (p *quotaConfig) initDDLIndexRate() {
	p.DDLIndexRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.indexRate", -1)
}

func (p *quotaConfig) initDDLFlushRate() {
	p.DDLFlushRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.flushRate", -1)
}

func (p *quotaConfig) initDDLCompactionRate() {
	p.DDLCompactionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddl.compactionRate", -1)
}

func megaBytesRate2Bytes(f float64) float64 {
	return f * 1024 * 1024
}

func (p *quotaConfig) checkMinMax(min, max float64) {
	if min > 0 && max > 0 && min > max {
		panic(fmt.Errorf("init QuotaConfig failed, max rate must be greater than or equal to min rate"))
	}
}

func (p *quotaConfig) initDMLMaxInsertRate() {
	p.DMLMaxInsertRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.insertRate.max", -1)
	if p.DMLMaxInsertRate > 0 {
		p.DMLMaxInsertRate = megaBytesRate2Bytes(p.DMLMaxInsertRate)
	}
}

func (p *quotaConfig) initDMLMinInsertRate() {
	p.DMLMinInsertRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.insertRate.min", -1)
	if p.DMLMinInsertRate > 0 {
		p.DMLMinInsertRate = megaBytesRate2Bytes(p.DMLMinInsertRate)
	}
	p.checkMinMax(p.DMLMinInsertRate, p.DMLMaxInsertRate)
}

func (p *quotaConfig) initDMLMaxDeleteRate() {
	p.DMLMaxDeleteRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.deleteRate.max", -1)
	if p.DMLMaxDeleteRate > 0 {
		p.DMLMaxDeleteRate = megaBytesRate2Bytes(p.DMLMaxDeleteRate)
	}
}

func (p *quotaConfig) initDMLMinDeleteRate() {
	p.DMLMinDeleteRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.deleteRate.min", -1)
	if p.DMLMinDeleteRate > 0 {
		p.DMLMinDeleteRate = megaBytesRate2Bytes(p.DMLMinDeleteRate)
	}
	p.checkMinMax(p.DMLMinDeleteRate, p.DMLMaxDeleteRate)
}

func (p *quotaConfig) initDMLMaxBulkLoadRate() {
	p.DMLMaxBulkLoadRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.bulkLoadRate.max", -1)
	if p.DMLMaxBulkLoadRate > 0 {
		p.DMLMaxBulkLoadRate = megaBytesRate2Bytes(p.DMLMaxBulkLoadRate)
	}
}

func (p *quotaConfig) initDMLMinBulkLoadRate() {
	p.DMLMinBulkLoadRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dml.bulkLoadRate.min", -1)
	if p.DMLMinBulkLoadRate > 0 {
		p.DMLMinBulkLoadRate = megaBytesRate2Bytes(p.DMLMinBulkLoadRate)
	}
	p.checkMinMax(p.DMLMinBulkLoadRate, p.DMLMaxBulkLoadRate)
}

func (p *quotaConfig) initDQLMaxSearchRate() {
	p.DQLMaxSearchRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dql.searchRate.max", -1)
}

func (p *quotaConfig) initDQLMinSearchRate() {
	p.DQLMinSearchRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dql.searchRate.min", -1)
	p.checkMinMax(p.DQLMinSearchRate, p.DQLMaxSearchRate)
}

func (p *quotaConfig) initDQLMaxQueryRate() {
	p.DQLMaxQueryRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dql.queryRate.max", -1)
}

func (p *quotaConfig) initDQLMinQueryRate() {
	p.DQLMinQueryRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dql.queryRate.min", -1)
	p.checkMinMax(p.DQLMinQueryRate, p.DQLMaxQueryRate)
}

func (p *quotaConfig) initMaxCollectionNum() {
	p.MaxCollectionNum = p.Base.ParseIntWithDefault("quotaAndLimits.limits.collection.maxNum", 64)
}

func (p *quotaConfig) initForceDenyWriting() {
	p.ForceDenyWriting = p.Base.ParseBool("quotaAndLimits.limitWriting.forceDeny", false)
}

func (p *quotaConfig) initMaxTimeTickDelay() {
	delay := p.Base.ParseFloatWithDefault("quotaAndLimits.limitWriting.maxTimeTickDelay", 10)
	p.MaxTimeTickDelay = time.Duration(delay * float64(time.Second))
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

func (p *quotaConfig) initNQInQueueThreshold() {
	p.NQInQueueThreshold = p.Base.ParseInt64WithDefault("quotaAndLimits.limitReading.NQInQueueThreshold", -1)
}

func (p *quotaConfig) initQueueLatencyThreshold() {
	p.QueueLatencyThreshold = p.Base.ParseFloatWithDefault("quotaAndLimits.limitReading.queueLatencyThreshold", -1)
}
