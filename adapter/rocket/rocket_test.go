package rocket

import (
	"github.com/gone-io/emitter"
	"github.com/gone-io/gone"
	"github.com/stretchr/testify/assert"
	"testing"
)

type Pointer struct {
	X int
	Y int
}

type Worker struct {
	gone.Flag
	emitter.Emitter `gone:"gone-emitter"`

	P1 Pointer
	P2 *Pointer
}

var ch = make(chan *Pointer)

func (r *Worker) Consume(on emitter.OnEvent) {
	on(func(p1 Pointer) error {
		r.P1 = p1

		ch <- &r.P1
		return nil
	})
}

func TestRocket_Consumer(t *testing.T) {
	worker := Worker{}
	gone.Test(func(w *Worker) {
		err := w.Send(&Pointer{X: 100, Y: 200})
		assert.Nil(t, err)
		pointer := <-ch
		assert.Equal(t, pointer.X, 100)
		assert.Equal(t, pointer.Y, 200)
	}, func(cemetery gone.Cemetery) error {
		cemetery.Bury(&worker)
		return nil
	}, Priest)
}
