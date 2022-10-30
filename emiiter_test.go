package emitter

import (
	"github.com/gone-io/gone"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type Pointer struct {
	X int
	Y int
}

type Worker struct {
	gone.Flag
	Sender `gone:"gone-emitter"`
	P1     Pointer
	P2     *Pointer
}

func (r *Worker) Consume(on OnEvent) {
	on(func(p1 Pointer) error {
		r.P1 = p1
		return nil
	})

	on(func(p2 *Pointer) error {
		r.P2 = p2
		return nil
	})
}

func TestNewEmitter(t *testing.T) {
	worker := Worker{}
	gone.Test(func(w *Worker) {
		pointer := Pointer{X: 100, Y: 200}

		err := w.Send(pointer)
		assert.Nil(t, err)

		time.Sleep(1 * time.Millisecond)

		assert.Equal(t, w.P1.X, pointer.X)
		assert.Equal(t, w.P1.Y, pointer.Y)

		assert.Equal(t, w.P2.X, pointer.X)
		assert.Equal(t, w.P2.Y, pointer.Y)

		err = w.Send(&pointer)
		assert.Nil(t, err)

		time.Sleep(1 * time.Millisecond)

		assert.Equal(t, w.P1.X, pointer.X)
		assert.Equal(t, w.P1.Y, pointer.Y)

		assert.Equal(t, w.P2.X, pointer.X)
		assert.Equal(t, w.P2.Y, pointer.Y)

	}, LocalMQPriest, func(cemetery gone.Cemetery) error {
		cemetery.Bury(&worker)
		return nil
	})
}
