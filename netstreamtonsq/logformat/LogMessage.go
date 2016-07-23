// automatically generated, do not modify

package logformat

import (
	flatbuffers "github.com/google/flatbuffers/go"
)
type LogMessage struct {
	_tab flatbuffers.Table
}

func GetRootAsLogMessage(buf []byte, offset flatbuffers.UOffsetT) *LogMessage {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &LogMessage{}
	x.Init(buf, n + offset)
	return x
}

func (rcv *LogMessage) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *LogMessage) From() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *LogMessage) RawMsg() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func LogMessageStart(builder *flatbuffers.Builder) { builder.StartObject(2) }
func LogMessageAddFrom(builder *flatbuffers.Builder, from flatbuffers.UOffsetT) { builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(from), 0) }
func LogMessageAddRawMsg(builder *flatbuffers.Builder, rawMsg flatbuffers.UOffsetT) { builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(rawMsg), 0) }
func LogMessageEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
