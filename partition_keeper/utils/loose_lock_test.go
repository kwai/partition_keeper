package utils

import (
	"sync"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestResilientLock(t *testing.T) {
	var values []string
	got := make(chan string)

	lock := NewLooseLock()

	wg := sync.WaitGroup{}
	wg.Add(2)

	lock.LockWrite()
	go func() {
		defer wg.Done()

		time.Sleep(time.Millisecond * 500)
		values = append(values, "first")

		lock.AllowRead()
		time.Sleep(time.Millisecond * 500)
		lock.DisallowRead()

		values = append(values, "second")
		lock.UnlockWrite()
	}()

	go func() {
		defer wg.Done()
		lock.LockWrite()
		defer lock.UnlockWrite()
		values = append(values, "third")
	}()

	for i := 0; i < 10; i++ {
		go func() {
			lock.LockRead()
			defer lock.UnlockRead()
			got <- values[len(values)-1]
		}()
	}

	for i := 0; i < 10; i++ {
		v := <-got
		assert.Equal(t, v, "first")
	}
	wg.Wait()

	assert.DeepEqual(t, values, []string{"first", "second", "third"})
}
