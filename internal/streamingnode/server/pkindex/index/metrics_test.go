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

package index

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func observations(t *testing.T, metric prometheus.Metric) uint64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, metric.Write(&m))
	return m.GetHistogram().GetSampleCount()
}

func TestMetricsFollowInstalledViewAndReadiness(t *testing.T) {
	ctx := context.Background()
	idx := openTestIndex(t)
	ready := metrics.StreamingNodePKIndexReadyDurationSeconds.WithLabelValues(idx.metricsNodeID).(prometheus.Metric)
	before := observations(t, ready)
	idx.MarkReady(ctx)
	idx.MarkReady(ctx)
	require.Equal(t, before+1, observations(t, ready), "ready is counted once")
	count := metrics.StreamingNodePKIndexCommittedTables.WithLabelValues(idx.metricsNodeID, idx.vchannel, strconv.FormatInt(idx.indexID, 10))
	bytes := metrics.StreamingNodePKIndexCommittedBytes.WithLabelValues(idx.metricsNodeID, idx.vchannel, strconv.FormatInt(idx.indexID, 10))
	require.Zero(t, testutil.ToFloat64(count))
	idx.insert(t, 10, 1, 2)
	idx.observed.Store(10)
	require.True(t, idx.handover(t))
	view := idx.committer.manifest()
	var size int64
	for _, table := range view.Tables {
		size += table.Size
	}
	require.Equal(t, float64(len(view.Tables)), testutil.ToFloat64(count))
	require.Equal(t, float64(size), testutil.ToFloat64(bytes))
	_, err := idx.Reset(ctx, 20)
	require.NoError(t, err)
	require.Zero(t, testutil.ToFloat64(count))
	require.Zero(t, testutil.ToFloat64(bytes))
	idx.Close(ctx)
	require.False(t, metrics.StreamingNodePKIndexCommittedTables.DeleteLabelValues(idx.metricsNodeID, idx.vchannel, strconv.FormatInt(idx.indexID, 10)), "Close removed the gauge")
	old := idx.Index
	idx.open(t)
	old.Close(ctx)
	require.True(t, metrics.StreamingNodePKIndexCommittedTables.DeleteLabelValues(idx.metricsNodeID, idx.vchannel, strconv.FormatInt(idx.indexID, 10)), "closing an old Index twice must not delete a reopened Index's gauge")
}

type timedOutUpload struct{ TableStore }

func (s timedOutUpload) UploadFile(context.Context, string, string) error {
	return context.DeadlineExceeded
}

func TestUploadMetricsCountAttemptsAndSuccessfulBytes(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 10, 1, 2)
	idx.observed.Store(10)
	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.Len(t, p.request.AddTables, 1)
	duration := metrics.StreamingNodePKIndexUploadDurationSeconds.WithLabelValues(idx.metricsNodeID).(prometheus.Metric)
	bytes := metrics.StreamingNodePKIndexUploadedBytes.WithLabelValues(idx.metricsNodeID).(prometheus.Metric)
	d, b := observations(t, duration), observations(t, bytes)
	idx.cfg.Store = timedOutUpload{idx.store}
	require.ErrorIs(t, idx.upload(ctx, p), context.DeadlineExceeded)
	require.Equal(t, d+1, observations(t, duration))
	require.Equal(t, b, observations(t, bytes), "failed upload has no successful byte observation")
	idx.cfg.Store = idx.store
	require.NoError(t, idx.upload(ctx, p))
	require.Equal(t, d+2, observations(t, duration))
	require.Equal(t, b+1, observations(t, bytes))
	require.NoError(t, idx.upload(ctx, p))
	require.Equal(t, d+2, observations(t, duration), "a skipped reupload is not an attempt")
}

func TestDownloadMetricsExcludeFailureAndLocalReuse(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 10, 1, 2)
	idx.observed.Store(10)
	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.Len(t, p.request.AddTables, 1)
	require.NoError(t, idx.upload(ctx, p))
	ref := p.request.AddTables[0]
	require.NoError(t, os.Remove(committedPath(idx.committedDir, ref.ID)))
	counter := metrics.StreamingNodePKIndexDownloadedBytes.WithLabelValues(idx.metricsNodeID)
	before := testutil.ToFloat64(counter)
	idx.store.FailDownloads(1)
	_, err = idx.resolveLocal(ctx, idx.sstBasePath, ref, nil, nil)
	require.Error(t, err)
	require.Equal(t, before, testutil.ToFloat64(counter))
	path, err := idx.resolveLocal(ctx, idx.sstBasePath, ref, nil, nil)
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, before+float64(info.Size()), testutil.ToFloat64(counter))
	_, err = idx.resolveLocal(ctx, idx.sstBasePath, ref, nil, nil)
	require.NoError(t, err)
	require.Equal(t, before+float64(info.Size()), testutil.ToFloat64(counter))
}
