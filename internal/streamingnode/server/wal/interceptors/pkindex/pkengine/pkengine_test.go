package pkengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWALCheckpointCovers(t *testing.T) {
	a := WALCheckpoint{TimeTick: 10}
	b := WALCheckpoint{TimeTick: 20}
	assert.True(t, b.Covers(a))
	assert.True(t, a.Covers(a)) // equal covers
	assert.False(t, a.Covers(b))

	var zero WALCheckpoint
	assert.True(t, zero.IsZero())
	assert.False(t, a.IsZero())
	// Any non-zero checkpoint covers the zero checkpoint.
	assert.True(t, a.Covers(zero))
}

func TestNewUnknownEngine(t *testing.T) {
	_, _, err := New("does-not-exist")
	assert.Error(t, err)
}
