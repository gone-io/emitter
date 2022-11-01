package rocket

import (
	"github.com/gone-io/emitter"
	"github.com/gone-io/gone"
	"github.com/gone-io/gone/goner"
)

func Priest(cemetery gone.Cemetery) error {
	_ = goner.BasePriest(cemetery)
	_ = emitter.Priest(cemetery)

	if nil == cemetery.GetTomById(emitter.IdGoneEmitterMq) {
		cemetery.Bury(NewAliyunV4())
	}
	return nil
}
