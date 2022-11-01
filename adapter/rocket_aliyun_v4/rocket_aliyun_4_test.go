package rocket

import (
	"github.com/gone-io/emitter"
	"github.com/gone-io/gone"
	"github.com/gone-io/gone/goner/logrus"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

type Event struct {
	Info int `json:"info"`
}

type worker struct {
	gone.Flag
	logrus.Logger  `gone:"gone-logger"`
	emitter.Sender `gone:"gone-emitter"`
}

var ch = make(chan int)

func (w *worker) Consume(on emitter.OnEvent) {
	on(func(e *Event) error {
		ch <- e.Info
		return nil
	})
}

// 测试依赖rocket的配置，
func TestRocketMq_Consumer(t *testing.T) {
	gone.Test(func(w *worker) {
		info := rand.Intn(10000000)

		begin := time.Now()
		err := w.Send(&Event{Info: info})
		assert.Nil(t, err)

		var finish = make(chan struct{})
		var times int
		go func() {
			for true {
				i := <-ch
				if i == info {
					times++
					if times >= 2 {
						close(finish)
						break
					}
				}
			}
		}()

		select {
		case <-time.After(2 * time.Second):
		case <-finish:
		}
		assert.Equal(t, times, 2)
		w.Infof("use time: %v", time.Now().Sub(begin))
	}, func(cemetery gone.Cemetery) error {
		cemetery.Bury(&worker{})
		return nil
	}, Priest)
}
