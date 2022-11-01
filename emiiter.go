package emitter

import (
	"encoding/json"
	"github.com/gone-io/gone"
	"github.com/gone-io/gone/goner/logrus"
	"github.com/gone-io/gone/goner/tracer"
	"reflect"
	"strings"
	"time"
)

const (
	TagEventType = "event-type"
	TagTrace     = "trace-id"
	TagDelay     = "delay"
	MsgId        = "msg-id"
)

func NewEmitter() (gone.Goner, gone.GonerId) {
	return &emitter{
		eventHandlerMap: make(map[string]*eventHandler),
	}, IdGoneEmitter
}

type DomainEventHandler func(event DomainEvent) error
type Decode func([]byte) (DomainEvent, Error)

type eventHandler struct {
	decode  Decode
	handles []DomainEventHandler
}

type emitter struct {
	gone.Flag
	logrus.Logger `gone:"gone-logger"`
	mq            MQ            `gone:"gone-emitter-mq"`
	tracer        tracer.Tracer `gone:"gone-tracer"`
	tool          AllCompleter  `gone:"gone-emitter-tool"`
	consumers     []Consumer    `gone:"*"`

	eventHandlerMap map[string]*eventHandler
}

func (r *emitter) Send(event DomainEvent, duration ...time.Duration) error {
	t := reflect.TypeOf(event)
	r.checkEventType(t)
	eventType := GetStructTypeString(t)

	headers := make(Headers)
	headers[TagTrace] = r.tracer.GetTraceId()
	headers[TagEventType] = eventType
	if len(duration) > 0 {
		headers[TagDelay] = duration[0]
	}

	encoder, ok := event.(Encoder)
	var msg []byte
	var err error
	if ok {
		msg, err = encoder.Encode()
		if err != nil {
			return err
		}
	} else {
		msg, err = json.Marshal(event)
		if err != nil {
			return err
		}
	}
	ids, err := r.mq.Send(NewMQMsg(msg, headers))
	id := strings.Join(ids, ",")

	r.Infof("send-message(Id=%s, type=%s) %s", id, eventType, msg)
	return err
}

func (r *emitter) AfterRevive() gone.AfterReviveError {
	r.consume()
	r.mq.Consumer(r.routeMsg)
	return nil
}

func (r *emitter) GetConsumeEventTypes() []string {
	types := make([]string, 0, len(r.eventHandlerMap))
	for t := range r.eventHandlerMap {
		types = append(types, t)
	}
	return types
}

// 消息路由
func (r *emitter) routeMsg(msg MQMsg) (err error) {
	headers := msg.GetHeaders()
	r.Infof("receive-message(Id=%s, type=%s) %s", headers[MsgId], headers[TagEventType], msg.GetBody())

	t, ok := headers[TagEventType]
	if !ok {
		err = gone.NewInnerError(MsgHeaderHasNotType, "msg header do not has type")
		return
	}

	msgType, ok := t.(string)
	if !ok {
		err = gone.NewInnerError(MsgTypeIsNotAString, "msg type is not a string")
		return
	}

	handle, ok := r.eventHandlerMap[msgType]
	if !ok {
		err = gone.NewInnerError(NotConsumeTheEvent, "do not consume the event")
		return
	}

	var event DomainEvent
	event, err = handle.decode(msg.GetBody())
	if err != nil {
		err = gone.NewInnerError(EventDecodeError, "event decode error")
		return
	}

	handleFns := make([]AllCompleteFunc, 0)
	for _, fn := range handle.handles {
		handleFns = append(handleFns, r.proxyDomainEventHandler(event, fn))
	}

	_, err = r.tool.AllComplete(handleFns...)
	return err
}

func (r *emitter) proxyDomainEventHandler(msg DomainEvent, fn DomainEventHandler) AllCompleteFunc {
	return func() (interface{}, error) {
		err := fn(msg)
		if err != nil {
			r.Errorf("%s deal msg(%v) err: %v", getFuncName(fn), msg, err)
		}
		return nil, err
	}
}

func (r *emitter) on(handler interface{}) {
	fv, pt := r.checkHandler(handler)
	r.addToMap(fv, pt)
}

var errPtr *error
var errType = reflect.TypeOf(errPtr).Elem()

func (r *emitter) checkHandler(handler interface{}) (fv reflect.Value, pt reflect.Type) {
	fv = reflect.ValueOf(handler)
	if fv.Kind() != reflect.Func {
		panic("event handler must be a func.")
	}
	ft := fv.Type()

	if ft.NumIn() != 1 {
		panic("handler function should be like func(*event) error, witch accept one parameter")
	}
	pt = ft.In(0)

	if ft.NumOut() != 1 || !ft.Out(0).Implements(errType) {
		panic("handler function should be like func(*event) error, witch output parameter must be error")
	}

	r.checkEventType(pt)
	return
}

func (r *emitter) checkEventType(evtType reflect.Type) {
	switch evtType.Kind() {
	case reflect.Struct:
		return
	case reflect.Pointer:
		if evtType.Elem().Kind() == reflect.Struct {
			return
		}
		fallthrough
	default:
		panic("handler parameter only support struct or struct pointer")
	}
}

// 处理订阅关系
func (r *emitter) consume() {
	for _, consumer := range r.consumers {
		consumer.Consume(r.on)
	}
}

func (r *emitter) addToMap(fv reflect.Value, pt reflect.Type) {
	t := GetStructTypeString(pt)
	handler := r.eventHandlerMap[t]
	if handler == nil {
		handler = &eventHandler{}

		handler.decode = r.proxyDecoder(pt)
		r.eventHandlerMap[t] = handler
	}
	handler.handles = append(handler.handles, r.proxyFn(fv, pt))
}

var decoderPtr *error
var decoderType = reflect.TypeOf(decoderPtr).Elem()

func (r *emitter) proxyDecoder(pt reflect.Type) Decode {
	if pt.Implements(decoderType) {
		return func(bytes []byte) (DomainEvent, Error) {
			value := reflect.New(pt)
			decoder := value.Convert(errType).Interface().(Decoder)
			return value, decoder.Decode(bytes)
		}
	} else {
		if reflect.Pointer == pt.Kind() {
			pt = pt.Elem()
		}
		return func(bytes []byte) (DomainEvent, Error) {
			ptr := reflect.New(pt)
			err := json.Unmarshal(bytes, ptr.Interface())
			return ptr.Interface(), err
		}

	}
}

func (r *emitter) proxyFn(fv reflect.Value, pt reflect.Type) DomainEventHandler {
	if reflect.Pointer == pt.Kind() {
		return func(event DomainEvent) error {
			ret := fv.Call([]reflect.Value{reflect.ValueOf(event)})
			err := ret[0].Interface()
			if err != nil {
				return err.(error)
			}
			return nil
		}
	}
	return func(event DomainEvent) error {
		ret := fv.Call([]reflect.Value{reflect.ValueOf(event).Elem()})
		err := ret[0].Interface()
		if err != nil {
			return err.(error)
		}
		return nil
	}
}
