package rocket

import (
	"github.com/gone-io/emitter"
	"github.com/gone-io/gone"
)

func Priest(cemetery gone.Cemetery) error {
	_ = emitter.Priest(cemetery)

	if nil == cemetery.GetTomById(emitter.IdGoneEmitterMq) {
		cemetery.Bury(NewRocket())
	}
	return nil
}
