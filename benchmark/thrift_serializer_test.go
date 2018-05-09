package benchmark

// thrift -out . -r --gen go:thrift_import=code.byted.org/gopkg/thrift echo.thrift
// go version go1.9.2 darwin/amd64

import (
	"code.byted.org/gopkg/thrift"
	//"github.com/stretchr/testify/assert"
	"testing"
	"github.com/zhiyu-he/go_performance/benchmark/echo"
	//"fmt"
	"fmt"
	"github.com/stretchr/testify/assert"
)

var (
	normal *thrift.TSerializer
	opt    *thrift.TSerializer
	opt1    *thrift.TSerializer

	normalD *thrift.TDeserializer
	optD    *thrift.TDeserializer
	optD1    *thrift.TDeserializer

	req = &echo.EchoReq{SeqId: 20171208, StrDat: "echo2323", MDat: map[string]float64{"ctr":0.123, "cvr": 0.567,"sdfsf":232,"32232":34.034}, T16: 123, T64:2323234, Li32:[]int32{12,534,45,4554,45,65,544}}

	byteReq []byte
)

func init() {
	t := thrift.NewTMemoryBufferLen(512)
	transport := thrift.NewTFramedTransport(t)
	p := thrift.NewTBinaryProtocolFactoryDefault().GetProtocol(transport)

	normal = &thrift.TSerializer{
		Transport: t,
		Protocol:  p,
	}

	t2 := thrift.NewTMemoryBufferLen(512)
	p2 := thrift.NewTFastFrameBinaryProtocolFactoryDefault(512).GetProtocol(t2)
	opt = &thrift.TSerializer{
		Transport: t2,
		Protocol:  p2,
	}

	t3 := thrift.NewTMemoryBufferLen(512)
	p3 := NewTFastFrameBinaryProtocolFactoryDefault(512).GetProtocol(t3)


	opt1 = &thrift.TSerializer{
		Transport: t3,
		Protocol:  p3,
	}

	normalD = &thrift.TDeserializer{
		Transport: t,
		Protocol:  p,
	}

	optD = &thrift.TDeserializer{
		Transport: t2,
		Protocol:  p2,
	}
	optD1 = &thrift.TDeserializer{
		Transport: t3,
		Protocol:  p3,
	}

	byteReq, _ = normal.Write(req)

}

func TestEqual(t *testing.T) {
	// test serializer equal
	dat, _:= opt.Write(req)
	//assert.Equal(t, byteReq, dat)

	dat1, _:= opt1.Write(req)
	//assert.Equal(t, byteReq, dat1)
	fmt.Printf("%#v\n%#v\n%#v\n", byteReq, dat, dat1)

	//test de-serializer equal
	reqNormal := &echo.EchoReq{}
	reqOPT := &echo.EchoReq{}
	reqOPT1 := &echo.EchoReq{}

	optD.Read(reqOPT, byteReq)
	normalD.Read(reqNormal, byteReq)

	optD1.Read(reqOPT1, byteReq)


	assert.EqualValues(t, reqNormal, reqOPT)
	assert.EqualValues(t, reqNormal, reqOPT1)

}

func apacheThriftWrite() {
	normal.Write(req)
}

func optThriftWrite() {
	opt.Write(req)
}

func opt1111ThriftWrite() {
	opt1.Write(req)
}

func apacheThriftRead(dat []byte) {
	normalD.Read(req, dat)
}

func optThriftRead(dat []byte) {
	optD.Read(req, dat)
}

func opt1111ThriftRead(dat []byte) {
	optD1.Read(req, dat)
}

func BenchmarkApacheThrift(b *testing.B) {
	for i := 0; i < b.N; i++ {
		apacheThriftWrite()
	}
}

func BenchmarkOPTThrift(b *testing.B) {
	for i := 0; i < b.N; i++ {
		optThriftWrite()
	}
}

func BenchmarkOPT11111Thrift(b *testing.B) {
	for i := 0; i < b.N; i++ {
		opt1111ThriftWrite()
	}
}
func BenchmarkApacheThriftRead(b *testing.B) {
	for i := 0; i < b.N; i++ {
		apacheThriftRead(byteReq)
	}
}

func BenchmarkOPTThriftRead(b *testing.B) {
	for i := 0; i < b.N; i++ {
		optThriftRead(byteReq)
	}
}

func BenchmarkOPT11111ThriftRead(b *testing.B) {
	for i := 0; i < b.N; i++ {
		opt1111ThriftRead(byteReq)
	}
}
