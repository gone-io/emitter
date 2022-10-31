package rocket

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	mq "github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
	"github.com/gone-io/emitter"
	"github.com/gone-io/gone"
	"github.com/gone-io/gone/goner/logrus"
	"github.com/gone-io/gone/goner/tracer"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func NewRocket() (gone.Angel, gone.GonerId) {
	return &rocket{}, emitter.IdGoneEmitterMq
}

type rocket struct {
	gone.Flag
	logrus.Logger `gone:"gone-logger"`
	tracer        tracer.Tracer   `gone:"gone-tracer"`
	emitter       emitter.Emitter `gone:"gone-emitter"`
	consumer      mq.SimpleConsumer
	producer      mq.Producer
	subscriber    emitter.Subscriber

	endPoint   string `gone:"config,domain.event.rocketMq.endpoint"`
	instanceId string `gone:"config,domain.event.rocketMq.instanceId"`
	accessKey  string `gone:"config,domain.event.rocketMq.accessKey"`
	secretKey  string `gone:"config,domain.event.rocketMq.secretKey"`
	topics     string `gone:"config,domain.event.rocketMq.topic"`
	groupId    string `gone:"config,domain.event.rocketMq.groupId"`
	debug      string `gone:"config,domain.event.rocketMq.debug"`

	awaitDuration     time.Duration `gone:"config,domain.event.rocketMq.await,default=5s"`
	maxMessageNum     int           `gone:"config,domain.event.rocketMq.connect.onceRead,default=8"`
	invisibleDuration time.Duration `gone:"config,domain.event.rocketMq.connect.invisible,default=20s"`
	consoleLog        bool          `gone:"config,domain.event.rocketMq.log.console,default=true"`
	logLevel          string        `gone:"config,domain.event.rocketMq.log.level,default=error"`
	logRoot           string        `gone:"config,domain.event.rocketMq.log.root,default=/var/logs/rocket"`
	logFilename       string        `gone:"config,domain.event.rocketMq.log.filename,default=rocket.log"`
	logFileMaxIndex   string        `gone:"config,domain.event.rocketMq.log.fileMaxIndex"`
	logFileMaxSize    string        `gone:"config,domain.event.rocketMq.log.FileMaxSize"`
	onlyConsume       bool          `gone:"config,domain.event.rocketMq.only-consume,default=false"`

	_topics []string
	once    sync.Once
}

func (r *rocket) getTopics() []string {
	r.once.Do(func() {
		r._topics = strings.Split(r.topics, ",")
		if len(r._topics) == 0 {
			panic("config,domain.event.rocketMq.topic cannot be empty")
		}
	})
	return r._topics
}

// Send 发送消息
func (r *rocket) Send(msg emitter.MQMsg) (msgIds []string, err error) {
	message := mq.Message{
		Body: msg.GetBody(),
	}

	headers := msg.GetHeaders()

	for k, v := range headers {
		s, ok := v.(string)
		if ok {
			message.AddProperty(k, s)
		}
	}

	d, ok := headers[emitter.TagDelay]
	if ok {
		duration, ok := d.(time.Duration)
		if ok {
			message.SetDelayTimestamp(time.Now().Add(duration))
		}
	}

	t, ok := headers[emitter.TagEventType]
	if !ok {
		err = gone.NewInnerError(emitter.HeadersMustWithEventType, "send(emitter.MQMsg) msg headers must with event type")
		return
	}
	msgType, ok := t.(string)
	if !ok {
		err = gone.NewInnerError(emitter.HeadersMustWithEventType, "send(emitter.MQMsg) msg headers must with event type")
		return
	}

	tag := r.getStructTypeHash(msgType)
	r.Debugf("send-msg's tag:%s", tag)
	message.SetTag(tag)

	for _, topic := range r.getTopics() {
		message.Topic = topic
		var sends []*mq.SendReceipt
		sends, err = r.producer.Send(context.TODO(), &message)
		if err != nil {
			return msgIds, gone.NewInnerError(emitter.SendError, err.Error())
		}
		if len(sends) < 1 {
			err = gone.NewInnerError(emitter.SendRstError, "r.producer.Send(context.TODO(), &message) rst len is 0")
			return
		}
		msgIds = append(msgIds, sends[0].MessageID)
	}
	return
}

func (r *rocket) getStructTypeHash(t string) string {
	return Md5(r.debug + t)[:5]
}

func Md5(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}

func (r *rocket) Consumer(s emitter.Subscriber) {
	r.subscriber = s
}

func (r *rocket) startConsumer() (err error) {
	r.consumer, err = mq.NewSimpleConsumer(&mq.Config{
		Endpoint: r.endPoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    r.accessKey,
			AccessSecret: r.secretKey,
		},
		ConsumerGroup: r.groupId,
	},
		mq.WithAwaitDuration(r.awaitDuration),
		mq.WithSubscriptionExpressions(r.getSubscriptionExpressions()),
	)

	if err != nil {
		return
	}

	err = r.consumer.Start()
	if err != nil {
		return
	}

	go r.msgReadLoop()
	return
}
func (r *rocket) startProducer() (err error) {
	r.producer, err = mq.NewProducer(&mq.Config{
		Endpoint: r.endPoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    r.accessKey,
			AccessSecret: r.secretKey,
		},
	}, mq.WithTopics(r.getTopics()...))

	if err != nil {
		return
	}
	return r.producer.Start()
}

func (r *rocket) Start(gone.Cemetery) (err error) {
	err = r.initLog()
	if err != nil {
		return
	}

	if r.hasConsumerEvent() {
		err = r.startConsumer()
		if err != nil {
			return
		}
	} else {
		r.Warnf("no consume event, consumer was not start")
	}

	if !r.onlyConsume {
		err = r.startProducer()
		if err != nil {
			return
		}
	} else {
		r.Warnf("no event sender, producer was not start")
	}
	return
}
func (r *rocket) Stop(gone.Cemetery) (err error) {
	if r.hasConsumerEvent() {
		err = r.consumer.GracefulStop()
		if err != nil {
			r.Errorf("consumer.GracefulStop err:%v", err)
		}
	}
	if !r.onlyConsume {
		err = r.producer.GracefulStop()
		if err != nil {
			r.Errorf("producer.GracefulStop err:%v", err)
		}
	}
	return
}

func (r *rocket) hasConsumerEvent() bool {
	return len(r.emitter.GetConsumeEventTypes()) > 0
}

func (r *rocket) getExpressions() string {
	eventTypes := r.emitter.GetConsumeEventTypes()
	arr := make([]string, 0, len(eventTypes))
	for i := range eventTypes {
		arr = append(arr, r.getStructTypeHash(eventTypes[i]))
	}
	sort.Sort(sort.StringSlice(arr))
	return strings.Join(arr, "||")
}

func (r *rocket) getSubscriptionExpressions() map[string]*mq.FilterExpression {
	topics := r.getTopics()
	m := make(map[string]*mq.FilterExpression)
	expressions := mq.NewFilterExpression(r.getExpressions())
	for _, topic := range topics {
		m[topic] = expressions
	}
	return m
}

func (r *rocket) msgReadLoop() {
	r.tracer.RecoverSetTraceId("", func() {
		for true {
			mvs, err := r.consumer.Receive(context.TODO(), int32(r.maxMessageNum), r.invisibleDuration)
			if err != nil {
				status, ok := err.(*mq.ErrRpcStatus)
				if ok && status.Code == 40401 {
					continue
				}
				r.Errorf("consumer.Receive err: %v", err)
			}

			for _, mv := range mvs {
				go r.dealMsg(mv)
			}
		}
	})
}

func (r *rocket) dealMsg(mv *mq.MessageView) {
	traceId := r.getMsgTraceId(mv)
	r.tracer.RecoverSetTraceId(traceId, func() {
		r.Debugf("receive-msg's tag:%s", mv.GetTag())
		err := r.subscriber(emitter.NewMQMsg(mv.GetBody(), r.getHeaders(mv)))
		if err != nil {
			r.Errorf("consumer msg err:%v", err)
		}

		// ack message
		err = r.consumer.Ack(context.TODO(), mv)
		if err != nil {
			r.Errorf("consumer ack err:%v", err)
		}
	})
}

func (r *rocket) getHeaders(mv *mq.MessageView) emitter.Headers {
	headers := make(emitter.Headers)
	properties := mv.GetProperties()
	if properties == nil {
		return headers
	}
	for k, v := range properties {
		headers[k] = v
	}
	headers[emitter.MsgId] = mv.GetMessageId()
	return headers
}

func (r *rocket) getMsgTraceId(mv *mq.MessageView) string {
	properties := mv.GetProperties()
	if properties == nil {
		return ""
	}
	return properties[emitter.TagTrace]
}

func (r *rocket) initLog() (err error) {
	err = os.Setenv(mq.ENABLE_CONSOLE_APPENDER, strconv.FormatBool(r.consoleLog))
	if err != nil {
		return
	}

	err = os.Setenv(mq.CLIENT_LOG_LEVEL, r.logLevel)
	if err != nil {
		return
	}

	err = os.Setenv(mq.CLIENT_LOG_ROOT, r.logRoot)
	if err != nil {
		return
	}

	err = os.Setenv(mq.CLIENT_LOG_ROOT, r.logRoot)
	if err != nil {
		return
	}

	err = os.Setenv(mq.CLIENT_LOG_FILENAME, r.logFilename)
	if err != nil {
		return
	}

	err = os.Setenv(mq.CLIENT_LOG_MAXINDEX, r.logFileMaxIndex)
	if err != nil {
		return
	}

	err = os.Setenv(mq.CLIENT_LOG_FILESIZE, r.logFileMaxSize)
	if err != nil {
		return
	}

	mq.ResetLogger()
	return
}
