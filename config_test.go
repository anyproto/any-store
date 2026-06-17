package anystore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_setDefaultsConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := &Config{}
			c.setDefaults()
		}()
	}
	wg.Wait()

	assert.Equal(t, "-2000", defaultSQLiteOptions["cache_size"])
	assert.Len(t, defaultSQLiteOptions, 1)
}
