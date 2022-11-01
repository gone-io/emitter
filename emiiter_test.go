package emitter

import (
	"github.com/gone-io/gone"
	"github.com/stretchr/testify/assert"
	"math/rand"
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
	on(func(p2 *Pointer) error {
		r.P2 = p2
		return nil
	})
	on(func(p1 Pointer) error {
		r.P1 = p1
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

type Worker2[T any] struct {
	gone.Flag
	Sender `gone:"gone-emitter"`
	f      func(T) error
}

func (r *Worker2[T]) Consume(on OnEvent) {
	on(r.f)
}

func TestConsumeUnsupportedType(t *testing.T) {
	defer func() {
		a := recover()
		assert.Equal(t, a, "handler parameter only support struct or struct pointer")
	}()
	worker := Worker2[int]{}

	worker.f = func(x int) error {
		return nil
	}

	gone.Test(func(w *Worker2[int]) {

	}, func(cemetery gone.Cemetery) error {
		cemetery.Bury(&worker)
		return nil
	}, LocalMQPriest)
}

func TestConsumeStruct(t *testing.T) {
	type X struct {
		Info int
	}

	var n = X{
		Info: rand.Intn(10000),
	}

	worker := Worker2[X]{}
	called := false
	worker.f = func(x X) error {
		assert.Equal(t, x, n)
		called = true
		return nil
	}

	gone.Test(func(w *Worker2[X]) {
		err := w.Send(n)
		assert.Nil(t, err)
		time.Sleep(1 * time.Millisecond)
		assert.True(t, called)
	}, func(cemetery gone.Cemetery) error {
		cemetery.Bury(&worker)
		return nil
	}, LocalMQPriest)
}

func TestConsumeStructPointer(t *testing.T) {
	type X struct {
		Info int
	}

	worker := Worker2[*X]{}
	var n = X{
		Info: rand.Intn(10000),
	}

	called := false
	worker.f = func(x *X) error {
		assert.Equal(t, *x, n)
		called = true
		return nil
	}

	gone.Test(func(w *Worker2[*X]) {

		err := w.Send(n)
		assert.Nil(t, err)
		time.Sleep(1 * time.Millisecond)
		assert.True(t, called)
	}, func(cemetery gone.Cemetery) error {
		cemetery.Bury(&worker)
		return nil
	}, LocalMQPriest)
}
