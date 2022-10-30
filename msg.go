package emitter

func NewMQMsg(body []byte, headers Headers) MQMsg {
	return &mqMsg{
		h: headers,
		b: body,
	}
}

type mqMsg struct {
	h Headers
	b []byte
}

func (m *mqMsg) GetHeaders() Headers {
	return m.h
}

func (m *mqMsg) GetBody() []byte {
	return m.b
}
