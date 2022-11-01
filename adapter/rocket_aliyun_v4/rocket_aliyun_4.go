package rocket

import (
	"crypto/md5"
	"encoding/hex"
	rocket "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/gogap/errors"
	"github.com/gone-io/emitter"
	"github.com/gone-io/gone"
	"github.com/gone-io/gone/goner/logrus"
	"github.com/gone-io/gone/goner/tracer"
	"sort"
	"strings"
	"time"
)

func NewAliyunV4() (gone.Goner, gone.GonerId) {
	return &rocketMq{}, emitter.IdGoneEmitterMq
}

type rocketMq struct {
	gone.Flag

	client    rocket.MQClient
	producers []rocket.MQProducer

	logrus.Logger `gone:"gone-logger"`
	tracer        tracer.Tracer   `gone:"gone-tracer"`
	emitter       emitter.Emitter `gone:"gone-emitter"`

	endPoint   string `gone:"config,domain.event.rocketMq.endpoint"`
	instanceId string `gone:"config,domain.event.rocketMq.instanceId"`
	accessKey  string `gone:"config,domain.event.rocketMq.accessKey"`
	secretKey  string `gone:"config,domain.event.rocketMq.secretKey"`
	topic      string `gone:"config,domain.event.rocketMq.topic"`
	groupId    string `gone:"config,domain.event.rocketMq.groupId"`
	debug      string `gone:"config,domain.event.rocketMq.debug"`

	countOnceRead     int   `gone:"config,domain.event.rocketMq.connect.onceRead,default=8"`
	connectWaitSecond int64 `gone:"config,domain.event.rocketMq.connect.waitSecond,default=10"`

	endSignal  chan struct{}
	endFlag    bool
	subscriber emitter.Subscriber
}

func (r *rocketMq) Send(msg emitter.MQMsg) (msgIds []string, err error) {
	headers, event := msg.GetHeaders(), msg.GetBody()

	var msgTag string
	m := make(map[string]string)
	for k := range headers {
		v, ok := headers[k].(string)
		if ok {
			m[k] = v
			if k == emitter.TagEventType {
				msgTag = r.getStructTypeHash(v) //使用消息的类型Hash做messageTag
			}
		}
	}

	mqMsg := rocket.PublishMessageRequest{
		MessageBody: string(event),
		MessageTag:  msgTag,
		Properties:  m,
	}

	duration, ok := headers[emitter.TagDelay].(time.Duration)
	if ok && duration > 0 {
		delayTime := time.Now().Add(duration)
		mqMsg.StartDeliverTime = delayTime.UnixMilli()
	}
	return r.sendMsg(mqMsg)
}

func (r *rocketMq) sendMsg(mqMsg rocket.PublishMessageRequest) (msgIds []string, err error) {
	for _, producer := range r.producers {
		var ret rocket.PublishMessageResponse
		ret, err = producer.PublishMessage(mqMsg)
		if err != nil {
			r.Errorf("send event:%v err:%v", mqMsg, err)
			return
		}
		msgIds = append(msgIds, ret.MessageId)
	}
	return
}

func (r *rocketMq) Start(gone.Cemetery) (err error) {
	r.endFlag = false
	r.endSignal = make(chan struct{})
	r.client = rocket.NewAliyunMQClient(r.endPoint, r.accessKey, r.secretKey, "")
	r.producers = r.getProducers()
	r.startReadLoops()
	r.Infof("emitter rocket adapter started")
	return nil
}

func (r *rocketMq) Consumer(s emitter.Subscriber) {
	r.subscriber = s
}

func (r *rocketMq) Stop(gone.Cemetery) (err error) {
	r.endFlag = true
	close(r.endSignal)
	return nil
}

func (r *rocketMq) consumerLoop(mqConsumer rocket.MQConsumer) {
	rocketResChan := make(chan rocket.ConsumeMessageResponse)
	rocketErrChan := make(chan error)

	//消息消费
	go func() {
		for {
			select {
			case res := <-rocketResChan:
				for i := range res.Messages {
					go r.consumeMqMsg(res.Messages[i], mqConsumer)
				}
			case err := <-rocketErrChan: //处理消息拉取错误
				if !strings.Contains(err.(errors.ErrCode).Error(), "MessageNotExist") {
					r.Errorf("consume msg err:%v", err)
				}
			case <-r.endSignal:
				break
			}
		}
	}()

	//消息读取
	go func() {
		for {
			if r.endFlag {
				break
			}
			mqConsumer.ConsumeMessage(rocketResChan, rocketErrChan, int32(r.countOnceRead), r.connectWaitSecond)
		}
	}()
}
func (r *rocketMq) hasConsumerEvent() bool {
	return len(r.emitter.GetConsumeEventTypes()) > 0
}
func (r *rocketMq) startReadLoops() {
	if r.hasConsumerEvent() {
		for _, consumer := range r.getConsumers() {
			go r.consumerLoop(consumer)
		}
	} else {
		r.Warnf("no consume event, consumer was not start")
	}
}

func (r *rocketMq) genEvtListHash(eventTypes []string) string {
	arr := make([]string, 0, len(eventTypes))
	for i := range eventTypes {
		arr = append(arr, r.getStructTypeHash(eventTypes[i]))
	}

	sort.Sort(sort.StringSlice(arr))
	return strings.Join(arr, "||")
}

func (r *rocketMq) getStructTypeHash(t string) string {
	return Md5(r.debug + t)[:5]
}

func (r *rocketMq) getProducers() (producers []rocket.MQProducer) {
	if r.topic == "" {
		panic("topic cannot be empty")
	}
	topics := strings.Split(r.topic, ",")

	for _, topic := range topics {
		producer := r.client.GetProducer(r.instanceId, topic)
		producers = append(producers, producer)
	}
	return
}

func (r *rocketMq) getConsumers() (consumers []rocket.MQConsumer) {
	if r.topic == "" {
		panic("topic cannot be empty")
	}
	topics := strings.Split(r.topic, ",")
	for _, topic := range topics {
		eventTypes := r.emitter.GetConsumeEventTypes()
		tagStr := r.genEvtListHash(eventTypes)
		consumer := r.client.GetConsumer(r.instanceId, topic, r.groupId, tagStr)
		consumers = append(consumers, consumer)
	}
	return
}

func (r *rocketMq) consumeMqMsg(msg rocket.ConsumeMessageEntry, mqConsumer rocket.MQConsumer) {
	traceId := msg.Properties[emitter.TagTrace]
	r.tracer.RecoverSetTraceId(traceId, func() {
		headers := make(emitter.Headers)
		for k, v := range msg.Properties {
			headers[k] = v
		}

		headers[emitter.MsgId] = msg.MessageId
		err := r.subscriber(emitter.NewMQMsg([]byte(msg.MessageBody), headers))

		if err != nil {
			r.Errorf("consume msg error: %v", err)
			return
		}
		err = mqConsumer.AckMessage([]string{msg.ReceiptHandle})
		if err != nil {
			r.Errorf("ack msg error: %v", err)
		}
	})
}

func Md5(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}
