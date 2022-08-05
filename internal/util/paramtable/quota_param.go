package paramtable

import (
	"sync"
)

// quotaConfig is configuration for quota and limitations.
type quotaConfig struct {
	BaseTable
	once sync.Once

	DDLCollectionRate float64
	DDLPartitionRate  float64
	DDLSegmentsRate   float64

	DMLInsertRate float64
	DMLDeleteRate float64

	DQLSearchRate float64
	DQLQueryRate  float64

	QuotaCenterCollectInterval int64
}

// InitOnce initialize grpc client config once
func (p *quotaConfig) InitOnce() {
	p.once.Do(func() {
		p.init()
	})
}

func (p *quotaConfig) init() {
	p.initDDLCollectionRate()
	p.initDDLPartitionRate()
	p.initDDLSegmentsRate()
	p.initDMLInsertRate()
	p.initDMLDeleteRate()
	p.initDQLSearchRate()
	p.initDQLQueryRate()
	p.initQuotaCenterCollectInterval()
}

func (p *quotaConfig) initDDLCollectionRate() {
	p.DDLCollectionRate = p.ParseFloatWithDefault("quotaAndLimits.ddlRate.collectionRate", 1)
}

func (p *quotaConfig) initDDLPartitionRate() {
	p.DDLPartitionRate = p.ParseFloatWithDefault("quotaAndLimits.ddlRate.partitionRate", 1)
}

func (p *quotaConfig) initDDLSegmentsRate() {
	p.DDLSegmentsRate = p.ParseFloatWithDefault("quotaAndLimits.ddlRate.segmentsRate", 1)
}

func (p *quotaConfig) initDMLInsertRate() {
	p.DMLInsertRate = p.ParseFloatWithDefault("quotaAndLimits.ddlRate.insertRate", 10)
}

func (p *quotaConfig) initDMLDeleteRate() {
	p.DMLDeleteRate = p.ParseFloatWithDefault("quotaAndLimits.ddlRate.deleteRate", 1)
}

func (p *quotaConfig) initDQLSearchRate() {
	p.DQLSearchRate = p.ParseFloatWithDefault("quotaAndLimits.ddlRate.searchRate", 2)
}

func (p *quotaConfig) initDQLQueryRate() {
	p.DQLQueryRate = p.ParseFloatWithDefault("quotaAndLimits.ddlRate.queryRate", 1)
}

func (p *quotaConfig) initQuotaCenterCollectInterval() {
	p.QuotaCenterCollectInterval = p.ParseInt64WithDefault("quotaAndLimits.ddlRate.quotaCenterCollectInterval", 1000)
}
