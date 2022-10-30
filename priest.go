package emitter

import (
	"github.com/gone-io/gone"
	"github.com/gone-io/gone/goner"
)

const (
	IdGoneEmitter     = "gone-emitter"
	IdGoneEmitterMq   = "gone-emitter-mq"
	IdGoneEmitterTool = "gone-emitter-tool"
)

const (
	MsgHeaderHasNotType = 11001 + iota
	MsgTypeIsNotAString
	NotConsumeTheEvent
	EventDecodeError
	HeadersMustWithEventType
	SendError
	SendRstError
)

func Priest(cemetery gone.Cemetery) error {
	_ = goner.BasePriest(cemetery)

	if nil == cemetery.GetTomById(IdGoneEmitterTool) {
		cemetery.Bury(NewAllCompleter())
	}

	if nil == cemetery.GetTomById(IdGoneEmitter) {
		cemetery.Bury(NewEmitter())
	}
	return nil
}

func LocalMQPriest(cemetery gone.Cemetery) error {
	_ = Priest(cemetery)

	if nil == cemetery.GetTomById(IdGoneEmitterMq) {
		cemetery.Bury(NewLocalMQ())
	}
	return nil
}
