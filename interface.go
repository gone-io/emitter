package emitter

import (
	"github.com/gone-io/gone"
	"time"
)

type DomainEvent interface{}

type Error error
type Decoder interface {
	Decode([]byte) Error
}

type Encoder interface {
	Encode() ([]byte, Error)
}

type OnEvent func(handler interface{})

type Sender interface {
	Send(event DomainEvent, duration ...time.Duration) error
}

type Consumer interface {
	Consume(on OnEvent)
}

type Emitter interface {
	Sender

	AfterRevive() gone.AfterReviveError

	GetConsumeEventTypes() []string
}

type Headers map[string]interface{}

type MQMsg interface {
	GetHeaders() Headers
	GetBody() []byte
}

type Subscriber func(MQMsg) error

type MQ interface {
	//Consumer 订阅消息
	Consumer(Subscriber)

	//Send 发送消息
	Send(MQMsg) (msgIds []string, err error)
}
