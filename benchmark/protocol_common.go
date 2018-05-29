package benchmark

import (
	"code.byted.org/gopkg/thrift"
	"errors"
)

var (
	limitReadBytes    = thrift.SAFE_BUFFER_SIZE_LIM
	minBigDataLen     = 30000
	minBufferLen      = 64
	maxBufferLen      = 32000000
	invalidDataLength = thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, errors.New("Invalid data length"))
	rwCountError      = errors.New("Read or Write socket count error")
	invalidFrameSize  = errors.New("Read frame size error or timeout")
	unexpectedEof     = errors.New("Read unexpected EOF")
)
