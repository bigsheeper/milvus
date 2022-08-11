// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package paramtable

import (
	"sync"
)

// quotaConfig is configuration for quota and limitations.
type quotaConfig struct {
	Base *BaseTable
	once sync.Once

	DDLCollectionRate float64
	DDLPartitionRate  float64
	DDLIndexRate      float64
	DDLSegmentsRate   float64

	DMLInsertRate float64
	DMLDeleteRate float64

	DQLSearchRate float64
	DQLQueryRate  float64

	QuotaCenterCollectInterval int64
}

func (p *quotaConfig) init(base *BaseTable) {
	p.Base = base

	p.initDDLCollectionRate()
	p.initDDLPartitionRate()
	p.initDDLIndexRate()
	p.initDDLSegmentsRate()
	p.initDMLInsertRate()
	p.initDMLDeleteRate()
	p.initDQLSearchRate()
	p.initDQLQueryRate()
	p.initQuotaCenterCollectInterval()
}

func (p *quotaConfig) initDDLCollectionRate() {
	p.DDLCollectionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddlRate.collectionRate", 1)
}

func (p *quotaConfig) initDDLPartitionRate() {
	p.DDLPartitionRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddlRate.partitionRate", 1)
}

func (p *quotaConfig) initDDLIndexRate() {
	p.DDLIndexRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddlRate.indexRate", 1)
}

func (p *quotaConfig) initDDLSegmentsRate() {
	p.DDLSegmentsRate = p.Base.ParseFloatWithDefault("quotaAndLimits.ddlRate.segmentsRate", 1)
}

func (p *quotaConfig) initDMLInsertRate() {
	p.DMLInsertRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dmlRate.insertRate", 10)
}

func (p *quotaConfig) initDMLDeleteRate() {
	p.DMLDeleteRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dmlRate.deleteRate", 1)
}

func (p *quotaConfig) initDQLSearchRate() {
	p.DQLSearchRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dqlRate.searchRate", 2)
}

func (p *quotaConfig) initDQLQueryRate() {
	p.DQLQueryRate = p.Base.ParseFloatWithDefault("quotaAndLimits.dqlRate.queryRate", 1)
}

func (p *quotaConfig) initQuotaCenterCollectInterval() {
	p.QuotaCenterCollectInterval = p.Base.ParseInt64WithDefault("quotaAndLimits.ddlRate.quotaCenterCollectInterval", 1000)
}
