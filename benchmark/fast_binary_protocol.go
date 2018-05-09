package benchmark

import (
	"fmt"
	"code.byted.org/gopkg/thrift"
	"errors"
	"unsafe"
	"reflect"
	"io"
	//"runtime"
)

var (
	limitReadBytes      = thrift.SAFE_BUFFER_SIZE_LIM
	invalidDataLength   = thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, errors.New("Invalid data length"))
)


// 保证buffer不被reuse，为了较少拷贝，string, byte字段直接使用buffer
type TFastBinaryProtocol struct {
	// 底层实现会是最终的TSocket->TCPConn，直接fd.Read,fd.Write，存在系统调用
	t             thrift.TTransport
	strictRead    bool
	strictWrite   bool
	rBufSize      int
	wBufSize      int
	rBuf          *FrameBuffer
	wBuf          *FrameBuffer
	reader        io.Reader
	writer        io.Writer
}

type TFastFrameBinaryProtocolFactory struct {
	strictRead  bool
	strictWrite bool
	rBufSize    int
	wBufSize    int
	rBuff       *FrameBuffer
	wBuff       *FrameBuffer
}

func NewTFastFrameBinaryProtocolTransport(t thrift.TTransport, rBufSize, wBufSize int) *TFastBinaryProtocol {
	return NewTFastFrameBinaryProtocol(t, false, true, rBufSize, wBufSize)
}

func NewTFastFrameBinaryProtocol(t thrift.TTransport, strictRead, strictWrite bool, rBufSize, wBufSize int) *TFastBinaryProtocol {
	p := &TFastBinaryProtocol{t: t, strictRead: strictRead, strictWrite: strictWrite, rBufSize: rBufSize, wBufSize: wBufSize}
	p.rBuf = &FrameBuffer{
		b:      make([]byte, rBufSize),
		rwIdx:  0,
	}
	p.wBuf = &FrameBuffer{
		b:      make([]byte, wBufSize + 4),
		rwIdx:  4,
	}
	return p
}

func NewTFastFrameBinaryProtocol2(t thrift.TTransport, strictRead, strictWrite bool, pRBuf, pWBuf *[]byte) *TFastBinaryProtocol {
	p := &TFastBinaryProtocol{t: t, strictRead: strictRead, strictWrite: strictWrite}
	p.rBuf = &FrameBuffer{
		b:      *pRBuf,
		rwIdx:  0,
	}
	p.wBuf = &FrameBuffer{
		b:      *pWBuf,
		rwIdx:  4,
	}
	return p
}

func NewTFastFrameBinaryProtocolFactoryDefault(bufSize int) *TFastFrameBinaryProtocolFactory {
	return NewTFastFrameBinaryProtocolFactory(false, true, bufSize, bufSize)
}

func NewTFastFrameBinaryProtocolFactory(strictRead, strictWrite bool, rBufSize, wBufSize int) *TFastFrameBinaryProtocolFactory {
	return &TFastFrameBinaryProtocolFactory{strictRead: strictRead, strictWrite: strictWrite, rBufSize: rBufSize, wBufSize: wBufSize}
}

func NewTFastFrameBinaryProtocolFactory2(pRBuf, pWBuf *[]byte) *TFastFrameBinaryProtocolFactory {
	p := &TFastFrameBinaryProtocolFactory{strictRead: false, strictWrite: true, }
	p.rBuff = &FrameBuffer{
		b:      *pRBuf,
		rwIdx:  0,
	}
	p.wBuff = &FrameBuffer{
		b:      *pWBuf,
		rwIdx:  4,
	}
	return p
}

func (p *TFastFrameBinaryProtocolFactory) GetProtocol(t thrift.TTransport) thrift.TProtocol {
	if p.rBuff == nil {
		return NewTFastFrameBinaryProtocol(t, p.strictRead, p.strictWrite, p.rBufSize, p.wBufSize)
	} else {
		return NewTFastFrameBinaryProtocol2(t, p.strictRead, p.strictWrite, &p.rBuff.b, &p.wBuff.b)
	}

}

/**
 * Writing Methods；该方法仅掉用一次，对于thrift通信协议本身来说是必调用；Write->WriteStructBegin
 */

func (p *TFastBinaryProtocol) WriteMessageBegin(name string, typeId thrift.TMessageType, seqId int32) error {
	if p.strictWrite {
		version := uint32(thrift.VERSION_1) | uint32(typeId)
		p.WriteI32(int32(version))
		p.WriteString(name)
		p.WriteI32(seqId)
	} else {
		p.WriteString(name)
		p.WriteByte(byte(typeId))
		p.WriteI32(seqId)
	}
	return nil
}

func (p *TFastBinaryProtocol) WriteMessageEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) WriteStructBegin(name string) error {
	return nil
}

func (p *TFastBinaryProtocol) WriteStructEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) WriteFieldBegin(name string, typeId thrift.TType, id int16) error {
	/*
	p.WriteByte(byte(typeId))
	p.WriteI16(id)
	*/
	buffer := p.wBuf
	rwIdx := buffer.rwIdx
	valSize := GetTTypeSize(typeId)
	if rwIdx+3+valSize > cap(buffer.b) {
		grow(&buffer.b, 3+valSize)
	}
	buffer.b[rwIdx] = byte(typeId)
	buffer.b[rwIdx + 1] =  byte(id >> 8)
	buffer.b[rwIdx + 2] = byte(id)
	buffer.rwIdx += 3
	return nil
}

func (p *TFastBinaryProtocol) WriteFieldEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) WriteFieldStop() error {
	p.WriteByte(thrift.STOP)
	return nil
}

func (p *TFastBinaryProtocol) WriteMapBegin(keyType thrift.TType, valueType thrift.TType, size int) error {
	/*
	p.WriteByte(byte(keyType))
	p.WriteByte(byte(valueType))
	p.WriteI32(int32(size))
	*/
	buffer := p.wBuf
	rwIdx := buffer.rwIdx
	// 提前预估map可能的大小，提前分配好
	valSize := (GetTTypeSize(keyType) + GetTTypeSize(valueType)) * size
	if rwIdx+6+valSize > cap(buffer.b) {
		grow(&buffer.b, 6+valSize)
	}

	buffer.b[rwIdx] = byte(keyType)
	buffer.b[rwIdx + 1] = byte(valueType)
	val := uint32(size)
	buffer.b[rwIdx + 2] = byte(val >> 24)
	buffer.b[rwIdx + 3] =  byte(val >> 16)
	buffer.b[rwIdx + 4] =  byte(val >> 8)
	buffer.b[rwIdx + 5] = byte(val)
	buffer.rwIdx += 6
	return nil
}

func (p *TFastBinaryProtocol) WriteMapEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) WriteListBegin(elemType thrift.TType, size int) error {
	/*
	p.WriteByte(byte(elemType))
	p.WriteI32(int32(size))
	*/
	buffer := p.wBuf
	rwIdx := buffer.rwIdx
	// 提前预估list可能的大小，提前分配好
	valSize := GetTTypeSize(elemType) * size
	if rwIdx+5+valSize > cap(buffer.b) {
		grow(&buffer.b, 5+valSize)
	}
	buffer.b[rwIdx] = byte(elemType)
	val := uint32(size)
	buffer.b[rwIdx + 1] = byte(val >> 24)
	buffer.b[rwIdx + 2] =  byte(val >> 16)
	buffer.b[rwIdx + 3] =  byte(val >> 8)
	buffer.b[rwIdx + 4] = byte(val)
	buffer.rwIdx += 5
	return nil
}

func (p *TFastBinaryProtocol) WriteListEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) WriteSetBegin(elemType thrift.TType, size int) error {
	/*
	p.WriteByte(byte(elemType))
	p.WriteI32(int32(size))
	*/
	p.WriteListBegin(elemType, size)
	return nil
}

func (p *TFastBinaryProtocol) WriteSetEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) WriteBool(value bool) error {
	if value {
		return p.WriteByte(1)
	}
	return p.WriteByte(0)
}

func (p *TFastBinaryProtocol) WriteByte(value byte) error {
	buffer := p.wBuf
	rwIdx := buffer.rwIdx

	if rwIdx+1 > cap(buffer.b) {
		grow(&buffer.b, 1)
	}

	buffer.b[rwIdx] = value
	buffer.rwIdx += 1
	return nil
}

func (p *TFastBinaryProtocol) WriteI16(value int16) error {
	buffer := p.wBuf
	rwIdx := buffer.rwIdx
	/*
	if rwIdx+2 > cap(buffer.b) {
		grow(&buffer.b, 2)
	}
	*/
	val := uint16(value)
	buffer.b[rwIdx] = byte(val >> 8)
	buffer.b[rwIdx + 1] = byte(val)
	buffer.rwIdx += 2
	return nil
}

func (p *TFastBinaryProtocol) WriteI32(value int32) error {
	buffer := p.wBuf
	rwIdx := buffer.rwIdx
	/*
	if rwIdx+4 > cap(buffer.b) {
		grow(&buffer.b, 4)
	}
	*/
	val := uint32(value)
	buffer.b[rwIdx] = byte(val >> 24)
	buffer.b[rwIdx + 1] =  byte(val >> 16)
	buffer.b[rwIdx + 2] =  byte(val >> 8)
	buffer.b[rwIdx + 3] = byte(val)
	buffer.rwIdx += 4
	return nil
}

func (p *TFastBinaryProtocol) WriteI64(value int64) error {
	buffer := p.wBuf
	rwIdx := buffer.rwIdx
	/*
	if rwIdx+8 > cap(buffer.b) {
		grow(&buffer.b, 8)
	}
	*/
	val := uint64(value)
	buffer.b[rwIdx] = byte(val >> 56)
	buffer.b[rwIdx + 1] =  byte(val >> 48)
	buffer.b[rwIdx + 2] =  byte(val >> 40)
	buffer.b[rwIdx + 3] = byte(val >> 32)
	buffer.b[rwIdx + 4] = byte(val >> 24)
	buffer.b[rwIdx + 5] =  byte(val >> 16)
	buffer.b[rwIdx + 6] =  byte(val >> 8)
	buffer.b[rwIdx + 7] = byte(val)
	buffer.rwIdx += 8
	return nil
}

func (p *TFastBinaryProtocol) WriteDouble(value float64) error {
	return p.WriteI64(int64(*(*uint64)(unsafe.Pointer(&value))))
}

func (p *TFastBinaryProtocol) WriteString(value string) error {
	buffer := p.wBuf
	/*
	p.WriteI32(int32(len(value)))
	if rwIdx+len(value) > cap(buffer.b) {
		grow(&buffer.b, len(value))
	}
	*/
	rwIdx := buffer.rwIdx
	if rwIdx+len(value)+4 > cap(buffer.b) {
		grow(&buffer.b, len(value)+4)
	}
	val := uint32(len(value))
	buffer.b[rwIdx] = byte(val >> 24)
	buffer.b[rwIdx + 1] =  byte(val >> 16)
	buffer.b[rwIdx + 2] =  byte(val >> 8)
	buffer.b[rwIdx + 3] = byte(val)
	buffer.rwIdx += copy(buffer.b[rwIdx+4:], value) + 4
	return nil
}

func (p *TFastBinaryProtocol) WriteBytes(b []byte) {
	buffer := p.wBuf

	if buffer.rwIdx+len(b) > cap(buffer.b) {
		grow(&buffer.b, len(b))
	}
	buffer.rwIdx += copy(buffer.b[buffer.rwIdx:], b)
}

func (p *TFastBinaryProtocol) WriteBinary(value []byte) error {
	/*
	p.WriteI32(int32(len(value)))
	p.WriteBytes(value)
	*/
	buffer := p.wBuf
	rwIdx := buffer.rwIdx
	if rwIdx+len(value)+4 > cap(buffer.b) {
		grow(&buffer.b, len(value)+4)
	}
	val := uint32(len(value))
	buffer.b[rwIdx] = byte(val >> 24)
	buffer.b[rwIdx + 1] =  byte(val >> 16)
	buffer.b[rwIdx + 2] =  byte(val >> 8)
	buffer.b[rwIdx + 3] = byte(val)
	buffer.rwIdx += copy(buffer.b[rwIdx+4:], value) + 4
	return nil
}

/**
 * Reading methods；该方法仅掉用一次，对于thrift通信协议本身来说是必调用；单独协议本身只会调用Read->ReadStructBegin
 */

func (p *TFastBinaryProtocol) ReadMessageBegin() (name string, typeId thrift.TMessageType, seqId int32, err error) {
	err = p.ReadFrame()
	if err != nil {
		return
	}
	size, _ := p.ReadI32()
	if size < 0 {
		typeId = thrift.TMessageType(size & 0x0ff)
		version := int64(size) & thrift.VERSION_MASK
		if version != thrift.VERSION_1 {
			return name, typeId, seqId, thrift.NewTProtocolExceptionWithType(thrift.BAD_VERSION, fmt.Errorf("Bad version in ReadMessageBegin"))
		}
		name, _ = p.ReadString()
		seqId, _ = p.ReadI32()
		return name, typeId, seqId, nil
	}
	if p.strictRead {
		return name, typeId, seqId, thrift.NewTProtocolExceptionWithType(thrift.BAD_VERSION, fmt.Errorf("Missing version in ReadMessageBegin"))
	}
	// TODO ?
	name, _ = p.readStringBody(int(size))
	b, _ := p.ReadByte()
	typeId = thrift.TMessageType(b)
	seqId, _ = p.ReadI32()
	return name, typeId, seqId, nil
}

func (p *TFastBinaryProtocol) ReadMessageEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) ReadStructBegin() (name string, err error) {
	err = p.ReadFrame()
	return
}

func (p *TFastBinaryProtocol) ReadStructEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) ReadFieldBegin() (name string, typeId thrift.TType, seqId int16, err error) {
	t, _ := p.ReadByte()
	if t != thrift.STOP {
		seqId, err = p.ReadI16()
	}
	typeId = thrift.TType(t)
	return name, typeId, seqId, err
}

func (p *TFastBinaryProtocol) ReadFieldEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) ReadMapBegin() (kType, vType thrift.TType, size int, err error) {
	k, _ := p.ReadByte()
	kType = thrift.TType(k)
	v, _ := p.ReadByte()
	vType = thrift.TType(v)
	size32, _ := p.ReadI32()
	if size32 < 0 {
		err = invalidDataLength
		return
	}
	if size32 > int32(limitReadBytes) {
		err = thrift.SafeBufferError
		return
	}
	size = int(size32)
	return kType, vType, size, nil
}

func (p *TFastBinaryProtocol) ReadMapEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) ReadListBegin() (elemType thrift.TType, size int, err error) {
	b, _ := p.ReadByte()
	elemType = thrift.TType(b)
	size32, _ := p.ReadI32()
	if size32 < 0 {
		err = invalidDataLength
		return
	}
	if size32 > int32(limitReadBytes) {
		err = thrift.SafeBufferError
		return
	}

	size = int(size32)

	return
}

func (p *TFastBinaryProtocol) ReadListEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) ReadSetBegin() (elemType thrift.TType, size int, err error) {
	b, _ := p.ReadByte()
	elemType = thrift.TType(b)
	size32, _ := p.ReadI32()
	if size32 < 0 {
		err = invalidDataLength
		return
	}
	if size32 > int32(limitReadBytes) {
		err = thrift.SafeBufferError
		return
	}
	size = int(size32)
	return elemType, size, nil
}

func (p *TFastBinaryProtocol) ReadSetEnd() error {
	return nil
}

func (p *TFastBinaryProtocol) ReadBool() (value bool, err error) {
	b, _ := p.ReadByte()
	value = (b == 1)
	return
}

func (p *TFastBinaryProtocol) ReadByte() (value byte, err error) {
	value = p.rBuf.b[p.rBuf.rwIdx]
	p.rBuf.rwIdx += 1
	return
}

func (p *TFastBinaryProtocol) ReadI16() (value int16, err error) {
	buffer := p.rBuf
	rwIdx := buffer.rwIdx
	value = int16(uint16(buffer.b[rwIdx+1]) | uint16(buffer.b[rwIdx])<<8)
	buffer.rwIdx += 2
	return
}

func (p *TFastBinaryProtocol) ReadI32() (value int32, err error) {
	buffer := p.rBuf
	rwIdx := buffer.rwIdx
	value = int32(uint32(buffer.b[rwIdx+3]) | uint32(buffer.b[rwIdx+2])<<8 | uint32(buffer.b[rwIdx+1])<<16 | uint32(buffer.b[rwIdx])<<24)
	buffer.rwIdx += 4
	return
}

func (p *TFastBinaryProtocol) ReadI64() (value int64, err error) {
	buffer := p.rBuf
	rwIdx := buffer.rwIdx
	value = int64(uint64(buffer.b[rwIdx+7]) | uint64(buffer.b[rwIdx+6])<<8 | uint64(buffer.b[rwIdx+5])<<16 | uint64(buffer.b[rwIdx+4])<<24 | uint64(buffer.b[rwIdx+3])<<32 | uint64(buffer.b[rwIdx+2])<<40 | uint64(buffer.b[rwIdx+1])<<48 | uint64(buffer.b[rwIdx])<<56)
	buffer.rwIdx += 8
	return
}

func (p *TFastBinaryProtocol) ReadDouble() (value float64, err error) {
	buffer := p.rBuf
	rwIdx := buffer.rwIdx
	valUint64 := uint64(buffer.b[rwIdx+7]) | uint64(buffer.b[rwIdx+6])<<8 | uint64(buffer.b[rwIdx+5])<<16 | uint64(buffer.b[rwIdx+4])<<24 | uint64(buffer.b[rwIdx+3])<<32 | uint64(buffer.b[rwIdx+2])<<40 | uint64(buffer.b[rwIdx+1])<<48 | uint64(buffer.b[rwIdx])<<56
	buffer.rwIdx += 8
	value = *(*float64)(unsafe.Pointer(&valUint64))
	return
}

func (p *TFastBinaryProtocol) ReadString() (value string, err error) {
	var size int32
	buffer := p.rBuf
	size, _ = p.ReadI32()
	// 注意这里，不再make，业务层保证buf不会reuse；大内存buf不需要reuse
	rwIdx := buffer.rwIdx
	value = bytes2str(buffer.b[rwIdx:rwIdx+int(size)])
	buffer.rwIdx += int(size)
	return
}

func (p *TFastBinaryProtocol) ReadBinary() (value []byte, err error) {
	var size int32
	size, _ = p.ReadI32()
	if size < 0 {
		return nil, invalidDataLength
	}
	if size > int32(limitReadBytes) {
		return nil, thrift.SafeBufferError
	}
	// 注意这里，不再make，业务层保证buf不会reuse；大内存buf不需要reuse
	buffer := p.rBuf
	rwIdx := buffer.rwIdx
	value = buffer.b[rwIdx:rwIdx+int(size)]
	buffer.rwIdx += int(size)
	return
}

func (p *TFastBinaryProtocol) Flush() (err error) {
	buffer := p.wBuf
	frameSize := buffer.rwIdx - 4
	buffer.frameSize = frameSize
	buffer.b[0] = byte(frameSize >> 24)
	buffer.b[1] = byte(frameSize >> 16)
	buffer.b[2] = byte(frameSize >> 8)
	buffer.b[3] = byte(frameSize)
	wPos := 0
	i := 0

	for err == nil && wPos < buffer.rwIdx {
		i, err = p.t.Write(buffer.b[wPos:buffer.rwIdx])
		wPos += i
		/*
		if wPos < buffer.rwIdx {
			runtime.Gosched()
		}
		*/
	}

	p.t.Flush()
	p.ResetWriter()

	return
}

func (p *TFastBinaryProtocol) Skip(fieldType thrift.TType) (err error) {
	return thrift.SkipDefaultDepth(p, fieldType)
}

func (p *TFastBinaryProtocol) Transport() thrift.TTransport {
	return p.t
}

func (p *TFastBinaryProtocol) readStringBody(size int) (value string, err error) {
	if size < 0 {
		return "", nil
	}
	if size > int(limitReadBytes) {
		return "", thrift.SafeBufferError
	}
	buffer := p.rBuf
	rwIdx := buffer.rwIdx
	value = bytes2str(buffer.b[rwIdx:rwIdx+size])
	buffer.rwIdx += size
	return
}

// 只会在ReadStructBegin和ReadMessageBegin处调用；一次性读完
func (p *TFastBinaryProtocol) ReadFrame() (err error) {
	// 已经读取数据，不在从transport读
	buffer := p.rBuf
	if buffer.rwIdx < buffer.frameSize {
		return
	}
	buffer.rwIdx = 0
	buffer.frameSize = 0
	// step 1: 先读4字节header
	// step 2: 读取body
	rPos := 0
	rPos_1 := 0
	for err == nil && rPos < 4 {
		rPos_1, err = p.t.Read(buffer.b[:4])
		rPos += rPos_1
	}
	if rPos != 4 {
		return
	}
	frameSize := int(uint32(buffer.b[3]) | uint32(buffer.b[2]) << 8 | uint32(buffer.b[1]) << 16 | uint32(buffer.b[0]) << 24)
	if frameSize > thrift.DEFAULT_MAX_LENGTH {
		err = fmt.Errorf("Incorrect frame size")
	}
	buffer.frameSize = frameSize
	if cap(buffer.b) < frameSize {
		buffer.b = make([]byte, frameSize + 4)
		buffer.b[0] = byte(frameSize >> 24)
		buffer.b[1] = byte(frameSize >> 16)
		buffer.b[2] = byte(frameSize >> 8)
		buffer.b[3] = byte(frameSize)
	}
	buffer.rwIdx = 4
	for err == nil && rPos < buffer.frameSize + 4 {
		rPos_1, err = p.t.Read(buffer.b[rPos:])
		rPos += rPos_1
		/*
		if rPos < buffer.frameSize + 4 {
			runtime.Gosched()
		}
		*/
	}
	if err == io.EOF || rPos == buffer.frameSize + 4 {
		return nil
	}

	return
}

func (p *TFastBinaryProtocol) ResetWriter() {
	const limit_buf_size_2M = 1024 * 1024 * 2
	const reset_to_1M = 1024 * 1024 * 1
	if cap(p.wBuf.b) > limit_buf_size_2M {
		//oldC := cap(p.wBuf.b)
		p.wBuf.b = make([]byte, reset_to_1M)
		//fmt.Printf("[TFastFrameBuffer] Reduce-Cap oldC: %dKB newC: %dKB\n", oldC/1024, p.wBuf.iSize/1024)
	}
	p.wBuf.rwIdx = 4
}

func (p *TFastBinaryProtocol) ResetReader() {
	const limit_buf_size_2M = 1024 * 1024 * 2
	const reset_to_1M = 1024 * 1024 * 1
	if cap(p.rBuf.b) > limit_buf_size_2M {
		//oldC := cap(p.rBuf.b)
		p.rBuf.b = make([]byte, reset_to_1M)
		//fmt.Printf("[TFastFrameBuffer] Reduce-Cap oldC: %dKB newC: %dKB\n", oldC/1024, p.rBuf.iSize/1024)
	}
	p.rBuf.rwIdx = 0
	p.rBuf.frameSize = 0
}

func (p *TFastBinaryProtocol) Reset() {
	p.ResetWriter()
	p.ResetReader()
}

func str2bytes(s string) []byte {
	x := (*reflect.StringHeader)(unsafe.Pointer(&s))
	h := reflect.SliceHeader{x.Data, x.Len, x.Len}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func bytes2str(b []byte) string{
	x := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	h := reflect.StringHeader{x.Data, x.Len}
	return *(*string)(unsafe.Pointer(&h))
}

func newSlice(old []byte, cap int) []byte {
	newB := make([]byte, cap)
	copy(newB, old)
	return newB
}


/*
 大于8K，增长因子1.5；小于8K增长因子2
 */
func grow(pBuf *[]byte, n int) {
	cap := cap(*pBuf)
	newCap := cap << 1
	if newCap > 8192 {
		newCap -= cap >> 1
	}
	*pBuf = newSlice(*pBuf, newCap+n)
}
