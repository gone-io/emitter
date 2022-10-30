package emitter

import (
	"errors"
	"fmt"
	"github.com/gone-io/gone"
	"github.com/gone-io/gone/goner/logrus"
	"github.com/gone-io/gone/goner/tracer"
)

type AllCompleteFunc = func() (interface{}, error)
type AllCompleter interface {
	AllComplete(fnList ...AllCompleteFunc) ([]interface{}, error)
}

func NewAllCompleter() (gone.Goner, gone.GonerId) {
	return &all{}, IdGoneEmitterTool
}

type all struct {
	gone.Flag
	logrus.Logger `gone:"gone-logger"`
	tracer        tracer.Tracer `gone:"gone-tracer"`
}

func (a *all) AllComplete(fnList ...AllCompleteFunc) ([]interface{}, error) {
	type Pair struct {
		Rst   interface{}
		Index int
	}

	var n = len(fnList)
	pairChan := make(chan Pair, n)
	errChan := make(chan error, 1)

	for i := range fnList {
		pair := Pair{
			Index: i,
		}

		func(fn func() (interface{}, error)) {
			a.tracer.Go(func() {
				defer func() {
					if e := recover(); e != nil {
						a.Errorf("handles panic: %v, %s", e, gone.PanicTrace(2))
						err, ok := e.(error)
						if !ok {
							err = errors.New(fmt.Sprintf("%v", e))
						}
						errChan <- err
					}
				}()

				rst, err := fn()
				if err != nil {
					errChan <- err
					return
				}
				pair.Rst = rst
				pairChan <- pair
			})
		}(fnList[i])
	}

	rstList := make([]interface{}, n, n)
	i := 0
	for {
		var err error
		select {
		case pair := <-pairChan:
			rstList[pair.Index] = pair.Rst
			i++
		case err = <-errChan:

		}
		if err != nil {
			return nil, err
		}
		if i == n {
			return rstList, nil
		}
	}
}
