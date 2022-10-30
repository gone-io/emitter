package emitter

import (
	"github.com/gone-io/gone"
	"github.com/gone-io/gone/goner/tracer"
	"strconv"
)

func NewLocalMQ() (gone.Angel, gone.GonerId) {
	return &localMq{}, IdGoneEmitterMq
}

type msgWrap struct {
	id      int64
	content []byte
	headers Headers
}

type localMq struct {
	gone.Flag
	gone.Logger `gone:"gone-logger"`
	tracer      tracer.Tracer `gone:"gone-tracer"`
	buf         chan msgWrap
	sub         Subscriber

	incr       int64
	stopSignal chan struct{}
}

func (q *localMq) Send(msg MQMsg) (msgIds []string, err error) {
	q.incr++
	q.buf <- msgWrap{
		id:      q.incr,
		content: msg.GetBody(),
		headers: msg.GetHeaders(),
	}
	msgIds = append(msgIds, strconv.FormatInt(q.incr, 10))
	return
}

func (q *localMq) Consumer(sub Subscriber) {
	q.sub = sub
}

func (q *localMq) receiveMsg() {
	q.tracer.RecoverSetTraceId("", func() {
		for {
			select {
			case m := <-q.buf:
				m.headers[MsgId] = strconv.FormatInt(m.id, 10)
				err := q.sub(NewMQMsg(m.content, m.headers))

				if err != nil {
					q.Warnf("msg deal failed for err: %v", err)
				}
			case <-q.stopSignal:
				return
			}
		}
	})
}

func (q *localMq) Start(gone.Cemetery) error {
	q.buf = make(chan msgWrap, 8)
	q.stopSignal = make(chan struct{})
	go q.receiveMsg()
	return nil
}

func (q *localMq) Stop(gone.Cemetery) error {
	close(q.stopSignal)
	return nil
}
