package benchmark

import (
	"fmt"
	"io"
	"unsafe"
	"code.byted.org/ad/union_common/gooptlib"

	"code.byted.org/gopkg/thrift"
)

// 保证buffer不被reuse，为了较少拷贝，string, byte字段直接使用buffer
type TFastBufferedBinaryProtocol struct {
	// 底层实现会是最终的TSocket->TCPConn，直接fd.Read,fd.Write，存在系统调用
	t           thrift.TTransport
	strictRead  bool
	strictWrite bool
	// 大段n内存结构，直接write，不再copy
	wBigData    []byte
	wBigDataPos int
	// 对于Read，如果thrift字段存在比较大的string/binary类型，rBuf初始化尽量小些
	rBuf        *BinaryBuffer
	wBuf        *BinaryBuffer
}

type TFastBufferedBinaryProtocolFactory struct {
	strictRead  bool
	strictWrite bool
	rBufSize    int
	wBufSize    int
	rBuf        *BinaryBuffer
	wBuf        *BinaryBuffer
}

func NewTFastBufferedBinaryProtocolTransport(t thrift.TTransport, rBufSize, wBufSize int) *TFastBufferedBinaryProtocol {
	return NewTFastBufferedBinaryProtocol(t, false, true, rBufSize, wBufSize)
}

func NewTFastBufferedBinaryProtocol(t thrift.TTransport, strictRead, strictWrite bool, rBufSize, wBufSize int) *TFastBufferedBinaryProtocol {
	p := &TFastBufferedBinaryProtocol{t: t, strictRead: strictRead, strictWrite: strictWrite}
	p.rBuf = &BinaryBuffer{
		b:     make([]byte, rBufSize),
		r:     0,
		w:     0,
	}
	p.wBuf = &BinaryBuffer{
		b:     make([]byte, wBufSize),
		r:     0,
		w:     0,
	}
	return p
}

func NewTFastBufferedBinaryProtocol2(t thrift.TTransport, strictRead, strictWrite bool, pRBuf, pWBuf *BinaryBuffer) *TFastBufferedBinaryProtocol {
	p := &TFastBufferedBinaryProtocol{t: t, strictRead: strictRead, strictWrite: strictWrite}
	pRBuf.Clear()
	pWBuf.Clear()
	p.rBuf = pRBuf
	p.wBuf = pWBuf
	return p
}

func NewTFastBufferedBinaryProtocolFactoryDefault(bufSize int) *TFastBufferedBinaryProtocolFactory {
	return NewTFastBufferedBinaryProtocolFactory(false, true, bufSize, bufSize)
}

func NewTFastBufferedBinaryProtocolFactory(strictRead, strictWrite bool, rBufSize, wBufSize int) *TFastBufferedBinaryProtocolFactory {
	return &TFastBufferedBinaryProtocolFactory{strictRead: strictRead, strictWrite: strictWrite, rBufSize: rBufSize, wBufSize: wBufSize}
}

func NewTFastBufferedBinaryProtocolFactory2(pRBuf, pWBuf *BinaryBuffer) *TFastBufferedBinaryProtocolFactory {
	p := &TFastBufferedBinaryProtocolFactory{strictRead: false, strictWrite: true}
	pRBuf.Clear()
	pWBuf.Clear()
	p.rBuf = pRBuf
	p.wBuf = pWBuf
	return p
}

func (p *TFastBufferedBinaryProtocolFactory) GetProtocol(t thrift.TTransport) thrift.TProtocol {
	if p.rBuf == nil {
		return NewTFastBufferedBinaryProtocol(t, p.strictRead, p.strictWrite, p.rBufSize, p.wBufSize)
	} else {
		return NewTFastBufferedBinaryProtocol2(t, p.strictRead, p.strictWrite, p.rBuf, p.wBuf)
	}
}

func (p *TFastBufferedBinaryProtocolFactory) GetFrameBuffer() (*BinaryBuffer, *BinaryBuffer) {
	return p.rBuf, p.wBuf
}

/**
 * Writing Methods；该方法仅掉用一次，对于thrift通信协议本身来说是必调用；Write->WriteStructBegin
 */

func (p *TFastBufferedBinaryProtocol) WriteMessageBegin(name string, typeId thrift.TMessageType, seqId int32) error {
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

func (p *TFastBufferedBinaryProtocol) WriteMessageEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteStructBegin(name string) error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteStructEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteFieldBegin(name string, typeId thrift.TType, id int16) error {
	buffer := p.wBuf
	rwIdx := buffer.w
	valSize := GetTTypeSize(typeId)
	if rwIdx+3+valSize > cap(buffer.b) {
		gooptlib.GrowSlice(&buffer.b, 3+valSize)
	}
	buffer.b[rwIdx] = byte(typeId)
	buffer.b[rwIdx+1] = byte(id >> 8)
	buffer.b[rwIdx+2] = byte(id)
	buffer.w += 3
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteFieldEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteFieldStop() error {
	p.WriteByte(thrift.STOP)
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteMapBegin(keyType thrift.TType, valueType thrift.TType, size int) error {
	buffer := p.wBuf
	rwIdx := buffer.w
	// 提前预估map可能的大小，提前分配好
	valSize := (GetTTypeSize(keyType) + GetTTypeSize(valueType)) * size
	if rwIdx+6+valSize > cap(buffer.b) {
		gooptlib.GrowSlice(&buffer.b, 6+valSize)
	}

	buffer.b[rwIdx] = byte(keyType)
	buffer.b[rwIdx+1] = byte(valueType)
	val := uint32(size)
	buffer.b[rwIdx+2] = byte(val >> 24)
	buffer.b[rwIdx+3] = byte(val >> 16)
	buffer.b[rwIdx+4] = byte(val >> 8)
	buffer.b[rwIdx+5] = byte(val)
	buffer.w += 6
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteMapEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteListBegin(elemType thrift.TType, size int) error {
	buffer := p.wBuf
	rwIdx := buffer.w
	// 提前预估list可能的大小，提前分配好
	valSize := GetTTypeSize(elemType) * size
	if rwIdx+5+valSize > cap(buffer.b) {
		gooptlib.GrowSlice(&buffer.b, 5+valSize)
	}
	buffer.b[rwIdx] = byte(elemType)
	val := uint32(size)
	buffer.b[rwIdx+1] = byte(val >> 24)
	buffer.b[rwIdx+2] = byte(val >> 16)
	buffer.b[rwIdx+3] = byte(val >> 8)
	buffer.b[rwIdx+4] = byte(val)
	buffer.w += 5
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteListEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteSetBegin(elemType thrift.TType, size int) error {
	p.WriteListBegin(elemType, size)
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteSetEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteBool(value bool) error {
	return p.WriteByte(gooptlib.Bool2Byte(value))
}

func (p *TFastBufferedBinaryProtocol) WriteByte(value byte) error {
	buffer := p.wBuf
	rwIdx := buffer.w
	buffer.b[rwIdx] = value
	buffer.w += 1
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteI16(value int16) error {
	buffer := p.wBuf
	rwIdx := buffer.w
	val := uint16(value)
	buffer.b[rwIdx] = byte(val >> 8)
	buffer.b[rwIdx+1] = byte(val)
	buffer.w += 2
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteI32(value int32) error {
	buffer := p.wBuf
	rwIdx := buffer.w
	val := uint32(value)
	buffer.b[rwIdx] = byte(val >> 24)
	buffer.b[rwIdx+1] = byte(val >> 16)
	buffer.b[rwIdx+2] = byte(val >> 8)
	buffer.b[rwIdx+3] = byte(val)
	buffer.w += 4
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteI64(value int64) error {
	buffer := p.wBuf
	rwIdx := buffer.w
	val := uint64(value)
	buffer.b[rwIdx] = byte(val >> 56)
	buffer.b[rwIdx+1] = byte(val >> 48)
	buffer.b[rwIdx+2] = byte(val >> 40)
	buffer.b[rwIdx+3] = byte(val >> 32)
	buffer.b[rwIdx+4] = byte(val >> 24)
	buffer.b[rwIdx+5] = byte(val >> 16)
	buffer.b[rwIdx+6] = byte(val >> 8)
	buffer.b[rwIdx+7] = byte(val)
	buffer.w += 8
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteDouble(value float64) error {
	return p.WriteI64(int64(*(*uint64)(unsafe.Pointer(&value))))
}

func (p *TFastBufferedBinaryProtocol) WriteString(value string) error {
	buffer := p.wBuf
	rwIdx := buffer.w
	isBigData := true
	growLen := 4
	if len(value) < minBigDataLen || p.wBigDataPos > 0 {
		isBigData = false
		growLen += len(value)
	}
	if rwIdx+growLen > cap(buffer.b) {
		gooptlib.GrowSlice(&buffer.b, growLen)
	}
	val := uint32(len(value))
	buffer.b[rwIdx] = byte(val >> 24)
	buffer.b[rwIdx+1] = byte(val >> 16)
	buffer.b[rwIdx+2] = byte(val >> 8)
	buffer.b[rwIdx+3] = byte(val)
	buffer.w += 4
	if isBigData {
		p.wBigData = gooptlib.Str2Bytes(value)
		p.wBigDataPos = buffer.w
	} else {
		buffer.w += copy(buffer.b[rwIdx + 4:], value)
	}
	return nil
}

func (p *TFastBufferedBinaryProtocol) WriteBinary(value []byte) error {
	buffer := p.wBuf
	rwIdx := buffer.w
	isBigData := true
	growLen := 4
	if len(value) < minBigDataLen || p.wBigDataPos > 0 {
		isBigData = false
		growLen += len(value)
	}
	if rwIdx+growLen > cap(buffer.b) {
		gooptlib.GrowSlice(&buffer.b, growLen)
	}
	val := uint32(len(value))
	buffer.b[rwIdx] = byte(val >> 24)
	buffer.b[rwIdx+1] = byte(val >> 16)
	buffer.b[rwIdx+2] = byte(val >> 8)
	buffer.b[rwIdx+3] = byte(val)
	buffer.w += 4
	if isBigData {
		p.wBigData = value
		p.wBigDataPos = buffer.w
	} else {
		buffer.w += copy(buffer.b[rwIdx + 4:], value)
	}
	return nil
}

/**
 * Reading methods；该方法仅掉用一次，对于thrift通信协议本身来说是必调用；单独协议本身只会调用Read->ReadStructBegin
 */

func (p *TFastBufferedBinaryProtocol) ReadMessageBegin() (name string, typeId thrift.TMessageType, seqId int32, err error) {
	var size int32
	size, err = p.ReadI32()
	if err != nil {
		return
	}
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
	name, err = p.readStringBody(int(size))
	if err != nil {
		return
	}
	b, err := p.ReadByte()
	if err != nil {
		return
	}
	typeId = thrift.TMessageType(b)
	seqId, err = p.ReadI32()
	if err != nil {
		return
	}
	return name, typeId, seqId, nil
}

func (p *TFastBufferedBinaryProtocol) ReadMessageEnd() error {
	p.ResetReader()
	return nil
}

func (p *TFastBufferedBinaryProtocol) ReadStructBegin() (name string, err error) {
	return
}

func (p *TFastBufferedBinaryProtocol) ReadStructEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) ReadFieldBegin() (name string, typeId thrift.TType, seqId int16, err error) {
	var t byte
	t, err = p.ReadByte()
	if err != nil {
		return
	}
	if t != thrift.STOP {
		seqId, err = p.ReadI16()
	}
	typeId = thrift.TType(t)
	return name, typeId, seqId, err
}

func (p *TFastBufferedBinaryProtocol) ReadFieldEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) ReadMapBegin() (kType, vType thrift.TType, size int, err error) {
	// 分别读取kType，vType，size共6个字节
	var dat []byte
	dat, err = p.ReadAtLeastN(6)
	if err != nil {
		return
	}
	
	kType = thrift.TType(dat[0])
	vType = thrift.TType(dat[1])
	size32 := int32(uint32(dat[5]) | uint32(dat[4])<<8 | uint32(dat[3])<<16 | uint32(dat[2])<<24)
	if size32 < 0 || size32 > int32(limitReadBytes) {
		err = invalidDataLength
		return
	}
	size = int(size32)
	return
}

func (p *TFastBufferedBinaryProtocol) ReadMapEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) ReadListBegin() (elemType thrift.TType, size int, err error) {
	var dat []byte
	dat, err = p.ReadAtLeastN(5)
	if err != nil {
		return
	}
	
	elemType = thrift.TType(dat[0])
	size32 := int32(uint32(dat[4]) | uint32(dat[3])<<8 | uint32(dat[2])<<16 | uint32(dat[1])<<24)
	if size32 < 0 || size32 > int32(limitReadBytes) {
		err = invalidDataLength
		return
	}

	size = int(size32)

	return
}

func (p *TFastBufferedBinaryProtocol) ReadListEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) ReadSetBegin() (elemType thrift.TType, size int, err error) {
	return p.ReadListBegin()
}

func (p *TFastBufferedBinaryProtocol) ReadSetEnd() error {
	return nil
}

func (p *TFastBufferedBinaryProtocol) ReadBool() (value bool, err error) {
	b, _ := p.ReadByte()
	value = (b == 1)
	return
}

func (p *TFastBufferedBinaryProtocol) ReadByte() (value byte, err error) {
	var dat []byte
	dat, err = p.ReadAtLeastN(1)
	if err != nil {
		return
	}
	value = dat[0]
	return
}

func (p *TFastBufferedBinaryProtocol) ReadI16() (value int16, err error) {
	var dat []byte
	dat, err = p.ReadAtLeastN(2)
	if err != nil {
		return
	}

	value = int16(uint16(dat[1]) | uint16(dat[0])<<8)
	return
}

func (p *TFastBufferedBinaryProtocol) ReadI32() (value int32, err error) {
	var dat []byte
	dat, err = p.ReadAtLeastN(4)
	if err != nil {
		return
	}

	value = int32(uint32(dat[3]) | uint32(dat[2])<<8 | uint32(dat[1])<<16 | uint32(dat[0])<<24)
	return
}

func (p *TFastBufferedBinaryProtocol) ReadI64() (value int64, err error) {
	var dat []byte
	dat, err = p.ReadAtLeastN(8)
	if err != nil {
		return
	}

	value = int64(uint64(dat[7]) | uint64(dat[6])<<8 | uint64(dat[5])<<16 | uint64(dat[4])<<24 | uint64(dat[3])<<32 | uint64(dat[2])<<40 | uint64(dat[1])<<48 | uint64(dat[0])<<56)
	return
}

func (p *TFastBufferedBinaryProtocol) ReadDouble() (value float64, err error) {
	var dat []byte
	dat, err = p.ReadAtLeastN(8)
	if err != nil {
		return
	}

	valUint64 := uint64(dat[7]) | uint64(dat[6])<<8 | uint64(dat[5])<<16 | uint64(dat[4])<<24 | uint64(dat[3])<<32 | uint64(dat[2])<<40 | uint64(dat[1])<<48 | uint64(dat[0])<<56
	value = *(*float64)(unsafe.Pointer(&valUint64))
	return
}

func (p *TFastBufferedBinaryProtocol) ReadString() (value string, err error) {
	var size int32
	size, err = p.ReadI32()
	if err != nil {
		return
	}
	if size < 0 || size > int32(limitReadBytes) {
		return "", invalidDataLength
	}
	// 注意这里，不再make，业务层保证buf不会reuse；大内存buf不需要reuse
	dat := make([]byte, size)
	err = p.ReadAll(dat)
	if err != nil {
		return
	}
	value = gooptlib.Bytes2Str(dat)
	return
}

func (p *TFastBufferedBinaryProtocol) ReadBinary() (value []byte, err error) {
	var size int32
	size, err = p.ReadI32()
	if err != nil {
		return
	}
	if size < 0 || size > int32(limitReadBytes) {
		return nil, invalidDataLength
	}
	value = make([]byte, size)
	err = p.ReadAll(value)
	if err != nil {
		return
	}
	return
}

func (p *TFastBufferedBinaryProtocol) Flush() (err error) {
	buffer := p.wBuf
	if p.wBigDataPos == 0 {
		_, err = p.t.Write(buffer.b[0:buffer.w])
	} else {
		_, err = p.t.Write(buffer.b[0:p.wBigDataPos])
		if err != nil {
			_, err = p.t.Write(p.wBigData[0:])
			if err != nil {
				_, err = p.t.Write(buffer.b[p.wBigDataPos:buffer.w])
			}
		}
	}

	p.t.Flush()
	p.ResetWriter()
	return
}

func (p *TFastBufferedBinaryProtocol) Skip(fieldType thrift.TType) (err error) {
	return thrift.SkipDefaultDepth(p, fieldType)
}

func (p *TFastBufferedBinaryProtocol) Transport() thrift.TTransport {
	return p.t
}

func (p *TFastBufferedBinaryProtocol) readStringBody(size int) (value string, err error) {
	if size < 0 || size > limitReadBytes {
		return "", invalidDataLength
	}
	dat := make([]byte, size)
	err = p.ReadAll(dat)
	if err != nil {
		return
	}
	value = gooptlib.Bytes2Str(dat)
	return
}

func (p *TFastBufferedBinaryProtocol) ResetWriter() {
	const limit_buf_size_2M = 1024 * 1024 * 2
	const reset_to_1M = 1024 * 1024 * 1
	if cap(p.wBuf.b) > limit_buf_size_2M {
		p.wBuf.b = make([]byte, reset_to_1M)
	}
	p.wBuf.Clear()
	p.wBigData = nil
	p.wBigDataPos = 0
}

func (p *TFastBufferedBinaryProtocol) ResetReader() {
	const limit_buf_size_2M = 1024 * 1024 * 2
	const reset_to_1M = 1024 * 1024 * 1
	if cap(p.rBuf.b) > limit_buf_size_2M {
		p.rBuf.b = make([]byte, reset_to_1M)
	}
	p.rBuf.Clear()
}

func (p *TFastBufferedBinaryProtocol) Reset() {
	p.ResetWriter()
	p.ResetReader()
}

/*
 read可以考虑
 1: 用多buffer来做, 避免string, binary申请拷贝; 存在跨buffer情况
 2: 也可以用ring buffer; 需要上层处理边界(求模)
 这里直接使用简单处理方式, 上层不关心r,w位置;
 */
// for bool, int8, int16, int32, int64
func (p *TFastBufferedBinaryProtocol) ReadAtLeastN(size int) (dat []byte, err error ){
	rBuf := p.rBuf
	remain := rBuf.w - rBuf.r
	if remain >= size {
		dat = rBuf.b[rBuf.r:]
		rBuf.r += size
		return dat, nil
	}
	if p.rBuf.err != io.EOF {
		copy(rBuf.b[:remain], rBuf.b[rBuf.r:rBuf.w])
		wPos := remain
		nn := size - remain
		count := 5
		i := 0
		for nn > 0 && err == nil && count > 0 {
			i, err = p.t.Read(rBuf.b[wPos:])
			nn -= i
			wPos += i
			count--
		}
		rBuf.r = size
		rBuf.w = wPos
		p.rBuf.err = err
		if (err == io.EOF || err == nil) && nn <= 0 {
			return rBuf.b[0:size], nil
		}
		if err != nil {
			//return rBuf[0:size-nn], err
			return nil, err
		}
		//return rBuf[0:size-nn] , rwCountError
		return nil, rwCountError
	}
	//return rBuf[0:remain], unexpectedEof
	return nil, unexpectedEof
}

// for string, []byte
func (p *TFastBufferedBinaryProtocol) ReadAll(buf []byte) (err error ){
	rBuf := p.rBuf
	remain := rBuf.w - rBuf.r
	if remain >= len(buf) {
		copy(buf[0:], rBuf.b[rBuf.r:rBuf.w])
		rBuf.r += len(buf)
		return nil
	}
	if p.rBuf.err != io.EOF {
		copy(buf[0:], rBuf.b[rBuf.r:rBuf.w])
		tmpBuf := buf[remain:]
		needCopy := false
		wPos := remain
		if (len(rBuf.b) >> 1) > len(buf) && len(buf) < minBigDataLen {
			needCopy = true
			tmpBuf = rBuf.b[0:]
			wPos = 0
			rBuf.r = 0
			rBuf.w = 0
		}
		nn := len(buf) - remain
		count := gooptlib.Max((nn >> 15) + 15, 200)
		i := 0
		for err == nil && nn > 0 && count > 0 {
			i, err = p.t.Read(tmpBuf[wPos:])
			nn -= i
			wPos += i
			count--
		}
		p.rBuf.err = err
		if needCopy {
			rBuf.w = wPos
		}
		if (err == io.EOF || err == nil) && nn <= 0 {
			if needCopy {
				copy(buf[remain:], rBuf.b[0:len(buf) - remain])
				rBuf.r = len(buf) - remain
			}
			return nil
		}
		if err != nil {
			return err
		}
		return rwCountError
	}
	return unexpectedEof
}