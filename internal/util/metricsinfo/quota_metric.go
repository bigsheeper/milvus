package metricsinfo

import "github.com/milvus-io/milvus/internal/proto/commonpb"

type QuotaMetrics struct {
	Rms []RateMetric
	Mm  MemMetric
}

type RateMetric struct {
	Rt         commonpb.RateType
	QPS        int32
	TPS        int32
	ThroughPut float64 // megabytes per second
}

// MemMetric is memory infos in megabytes
type MemMetric struct {
	UsedMem  uint64
	TotalMem uint64
	Buffers  map[string]uint64 // growing segments, bloom filter, etc...
}
