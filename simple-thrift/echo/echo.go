// Autogenerated by Thrift Compiler (0.10.0)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

package echo

import (
	"bytes"
	"fmt"
	"github.com/ThoseFlowers/thrift/lib/go/thrift"
)

// (needed to ensure safety because of naive import list construction.)
var _ = thrift.ZERO
var _ = fmt.Printf
var _ = bytes.Equal

// Attributes:
//  - SeqID
//  - StrDat
//  - BinDat
type EchoReq struct {
  SeqID int32 `thrift:"seq_id,1" db:"seq_id" json:"seq_id"`
  StrDat string `thrift:"str_dat,2" db:"str_dat" json:"str_dat"`
  BinDat []byte `thrift:"bin_dat,3" db:"bin_dat" json:"bin_dat"`
}

func NewEchoReq() *EchoReq {
  return &EchoReq{}
}


func (p *EchoReq) GetSeqID() int32 {
  return p.SeqID
}

func (p *EchoReq) GetStrDat() string {
  return p.StrDat
}

func (p *EchoReq) GetBinDat() []byte {
  return p.BinDat
}
func (p *EchoReq) Read(iprot thrift.TProtocol) error {
  if _, err := iprot.ReadStructBegin(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
  }


  for {
    _, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
    if err != nil {
      return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
    }
    if fieldTypeId == thrift.STOP { break; }
    switch fieldId {
    case 1:
      if err := p.ReadField1(iprot); err != nil {
        return err
      }
    case 2:
      if err := p.ReadField2(iprot); err != nil {
        return err
      }
    case 3:
      if err := p.ReadField3(iprot); err != nil {
        return err
      }
    default:
      if err := iprot.Skip(fieldTypeId); err != nil {
        return err
      }
    }
    if err := iprot.ReadFieldEnd(); err != nil {
      return err
    }
  }
  if err := iprot.ReadStructEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
  }
  return nil
}

func (p *EchoReq)  ReadField1(iprot thrift.TProtocol) error {
  if v, err := iprot.ReadI32(); err != nil {
  return thrift.PrependError("error reading field 1: ", err)
} else {
  p.SeqID = v
}
  return nil
}

func (p *EchoReq)  ReadField2(iprot thrift.TProtocol) error {
  if v, err := iprot.ReadString(); err != nil {
  return thrift.PrependError("error reading field 2: ", err)
} else {
  p.StrDat = v
}
  return nil
}

func (p *EchoReq)  ReadField3(iprot thrift.TProtocol) error {
  if v, err := iprot.ReadBinary(); err != nil {
  return thrift.PrependError("error reading field 3: ", err)
} else {
  p.BinDat = v
}
  return nil
}

func (p *EchoReq) Write(oprot thrift.TProtocol) error {
  if err := oprot.WriteStructBegin("EchoReq"); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
  if p != nil {
    if err := p.writeField1(oprot); err != nil { return err }
    if err := p.writeField2(oprot); err != nil { return err }
    if err := p.writeField3(oprot); err != nil { return err }
  }
  if err := oprot.WriteFieldStop(); err != nil {
    return thrift.PrependError("write field stop error: ", err) }
  if err := oprot.WriteStructEnd(); err != nil {
    return thrift.PrependError("write struct stop error: ", err) }
  return nil
}

func (p *EchoReq) writeField1(oprot thrift.TProtocol) (err error) {
  if err := oprot.WriteFieldBegin("seq_id", thrift.I32, 1); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:seq_id: ", p), err) }
  if err := oprot.WriteI32(int32(p.SeqID)); err != nil {
  return thrift.PrependError(fmt.Sprintf("%T.seq_id (1) field write error: ", p), err) }
  if err := oprot.WriteFieldEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field end error 1:seq_id: ", p), err) }
  return err
}

func (p *EchoReq) writeField2(oprot thrift.TProtocol) (err error) {
  if err := oprot.WriteFieldBegin("str_dat", thrift.STRING, 2); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:str_dat: ", p), err) }
  if err := oprot.WriteString(string(p.StrDat)); err != nil {
  return thrift.PrependError(fmt.Sprintf("%T.str_dat (2) field write error: ", p), err) }
  if err := oprot.WriteFieldEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field end error 2:str_dat: ", p), err) }
  return err
}

func (p *EchoReq) writeField3(oprot thrift.TProtocol) (err error) {
  if err := oprot.WriteFieldBegin("bin_dat", thrift.STRING, 3); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field begin error 3:bin_dat: ", p), err) }
  if err := oprot.WriteBinary(p.BinDat); err != nil {
  return thrift.PrependError(fmt.Sprintf("%T.bin_dat (3) field write error: ", p), err) }
  if err := oprot.WriteFieldEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field end error 3:bin_dat: ", p), err) }
  return err
}

func (p *EchoReq) String() string {
  if p == nil {
    return "<nil>"
  }
  return fmt.Sprintf("EchoReq(%+v)", *p)
}

// Attributes:
//  - Status
//  - Msg
type EchoRsp struct {
  Status int32 `thrift:"status,1" db:"status" json:"status"`
  Msg string `thrift:"msg,2" db:"msg" json:"msg"`
}

func NewEchoRsp() *EchoRsp {
  return &EchoRsp{}
}


func (p *EchoRsp) GetStatus() int32 {
  return p.Status
}

func (p *EchoRsp) GetMsg() string {
  return p.Msg
}
func (p *EchoRsp) Read(iprot thrift.TProtocol) error {
  if _, err := iprot.ReadStructBegin(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
  }


  for {
    _, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
    if err != nil {
      return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
    }
    if fieldTypeId == thrift.STOP { break; }
    switch fieldId {
    case 1:
      if err := p.ReadField1(iprot); err != nil {
        return err
      }
    case 2:
      if err := p.ReadField2(iprot); err != nil {
        return err
      }
    default:
      if err := iprot.Skip(fieldTypeId); err != nil {
        return err
      }
    }
    if err := iprot.ReadFieldEnd(); err != nil {
      return err
    }
  }
  if err := iprot.ReadStructEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
  }
  return nil
}

func (p *EchoRsp)  ReadField1(iprot thrift.TProtocol) error {
  if v, err := iprot.ReadI32(); err != nil {
  return thrift.PrependError("error reading field 1: ", err)
} else {
  p.Status = v
}
  return nil
}

func (p *EchoRsp)  ReadField2(iprot thrift.TProtocol) error {
  if v, err := iprot.ReadString(); err != nil {
  return thrift.PrependError("error reading field 2: ", err)
} else {
  p.Msg = v
}
  return nil
}

func (p *EchoRsp) Write(oprot thrift.TProtocol) error {
  if err := oprot.WriteStructBegin("EchoRsp"); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
  if p != nil {
    if err := p.writeField1(oprot); err != nil { return err }
    if err := p.writeField2(oprot); err != nil { return err }
  }
  if err := oprot.WriteFieldStop(); err != nil {
    return thrift.PrependError("write field stop error: ", err) }
  if err := oprot.WriteStructEnd(); err != nil {
    return thrift.PrependError("write struct stop error: ", err) }
  return nil
}

func (p *EchoRsp) writeField1(oprot thrift.TProtocol) (err error) {
  if err := oprot.WriteFieldBegin("status", thrift.I32, 1); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:status: ", p), err) }
  if err := oprot.WriteI32(int32(p.Status)); err != nil {
  return thrift.PrependError(fmt.Sprintf("%T.status (1) field write error: ", p), err) }
  if err := oprot.WriteFieldEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field end error 1:status: ", p), err) }
  return err
}

func (p *EchoRsp) writeField2(oprot thrift.TProtocol) (err error) {
  if err := oprot.WriteFieldBegin("msg", thrift.STRING, 2); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:msg: ", p), err) }
  if err := oprot.WriteString(string(p.Msg)); err != nil {
  return thrift.PrependError(fmt.Sprintf("%T.msg (2) field write error: ", p), err) }
  if err := oprot.WriteFieldEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field end error 2:msg: ", p), err) }
  return err
}

func (p *EchoRsp) String() string {
  if p == nil {
    return "<nil>"
  }
  return fmt.Sprintf("EchoRsp(%+v)", *p)
}

type EchoService interface {
  Hi() (err error)
  // Parameters:
  //  - Req
  Do(req *EchoReq) (r *EchoRsp, err error)
}

type EchoServiceClient struct {
  Transport thrift.TTransport
  ProtocolFactory thrift.TProtocolFactory
  InputProtocol thrift.TProtocol
  OutputProtocol thrift.TProtocol
  SeqId int32
}

func NewEchoServiceClientFactory(t thrift.TTransport, f thrift.TProtocolFactory) *EchoServiceClient {
  return &EchoServiceClient{Transport: t,
    ProtocolFactory: f,
    InputProtocol: f.GetProtocol(t),
    OutputProtocol: f.GetProtocol(t),
    SeqId: 0,
  }
}

func NewEchoServiceClientProtocol(t thrift.TTransport, iprot thrift.TProtocol, oprot thrift.TProtocol) *EchoServiceClient {
  return &EchoServiceClient{Transport: t,
    ProtocolFactory: nil,
    InputProtocol: iprot,
    OutputProtocol: oprot,
    SeqId: 0,
  }
}

func (p *EchoServiceClient) Hi() (err error) {
  if err = p.sendHi(); err != nil { return }
  return p.recvHi()
}

func (p *EchoServiceClient) sendHi()(err error) {
  oprot := p.OutputProtocol
  if oprot == nil {
    oprot = p.ProtocolFactory.GetProtocol(p.Transport)
    p.OutputProtocol = oprot
  }
  p.SeqId++
  if err = oprot.WriteMessageBegin("Hi", thrift.CALL, p.SeqId); err != nil {
      return
  }
  args := EchoServiceHiArgs{
  }
  if err = args.Write(oprot); err != nil {
      return
  }
  if err = oprot.WriteMessageEnd(); err != nil {
      return
  }
  return oprot.Flush()
}


func (p *EchoServiceClient) recvHi() (err error) {
  iprot := p.InputProtocol
  if iprot == nil {
    iprot = p.ProtocolFactory.GetProtocol(p.Transport)
    p.InputProtocol = iprot
  }
  method, mTypeId, seqId, err := iprot.ReadMessageBegin()
  if err != nil {
    return
  }
  if method != "Hi" {
    err = thrift.NewTApplicationException(thrift.WRONG_METHOD_NAME, "Hi failed: wrong method name")
    return
  }
  if p.SeqId != seqId {
    err = thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "Hi failed: out of sequence response")
    return
  }
  if mTypeId == thrift.EXCEPTION {
    error0 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
    var error1 error
    error1, err = error0.Read(iprot)
    if err != nil {
      return
    }
    if err = iprot.ReadMessageEnd(); err != nil {
      return
    }
    err = error1
    return
  }
  if mTypeId != thrift.REPLY {
    err = thrift.NewTApplicationException(thrift.INVALID_MESSAGE_TYPE_EXCEPTION, "Hi failed: invalid message type")
    return
  }
  result := EchoServiceHiResult{}
  if err = result.Read(iprot); err != nil {
    return
  }
  if err = iprot.ReadMessageEnd(); err != nil {
    return
  }
  return
}

// Parameters:
//  - Req
func (p *EchoServiceClient) Do(req *EchoReq) (r *EchoRsp, err error) {
  if err = p.sendDo(req); err != nil { return }
  return p.recvDo()
}

func (p *EchoServiceClient) sendDo(req *EchoReq)(err error) {
  oprot := p.OutputProtocol
  if oprot == nil {
    oprot = p.ProtocolFactory.GetProtocol(p.Transport)
    p.OutputProtocol = oprot
  }
  p.SeqId++
  if err = oprot.WriteMessageBegin("Do", thrift.CALL, p.SeqId); err != nil {
      return
  }
  args := EchoServiceDoArgs{
  Req : req,
  }
  if err = args.Write(oprot); err != nil {
      return
  }
  if err = oprot.WriteMessageEnd(); err != nil {
      return
  }
  return oprot.Flush()
}


func (p *EchoServiceClient) recvDo() (value *EchoRsp, err error) {
  iprot := p.InputProtocol
  if iprot == nil {
    iprot = p.ProtocolFactory.GetProtocol(p.Transport)
    p.InputProtocol = iprot
  }
  method, mTypeId, seqId, err := iprot.ReadMessageBegin()
  if err != nil {
    return
  }
  if method != "Do" {
    err = thrift.NewTApplicationException(thrift.WRONG_METHOD_NAME, "Do failed: wrong method name")
    return
  }
  if p.SeqId != seqId {
    err = thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "Do failed: out of sequence response")
    return
  }
  if mTypeId == thrift.EXCEPTION {
    error2 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
    var error3 error
    error3, err = error2.Read(iprot)
    if err != nil {
      return
    }
    if err = iprot.ReadMessageEnd(); err != nil {
      return
    }
    err = error3
    return
  }
  if mTypeId != thrift.REPLY {
    err = thrift.NewTApplicationException(thrift.INVALID_MESSAGE_TYPE_EXCEPTION, "Do failed: invalid message type")
    return
  }
  result := EchoServiceDoResult{}
  if err = result.Read(iprot); err != nil {
    return
  }
  if err = iprot.ReadMessageEnd(); err != nil {
    return
  }
  value = result.GetSuccess()
  return
}


type EchoServiceProcessor struct {
  processorMap map[string]thrift.TProcessorFunction
  handler EchoService
}

func (p *EchoServiceProcessor) AddToProcessorMap(key string, processor thrift.TProcessorFunction) {
  p.processorMap[key] = processor
}

func (p *EchoServiceProcessor) GetProcessorFunction(key string) (processor thrift.TProcessorFunction, ok bool) {
  processor, ok = p.processorMap[key]
  return processor, ok
}

func (p *EchoServiceProcessor) ProcessorMap() map[string]thrift.TProcessorFunction {
  return p.processorMap
}

func NewEchoServiceProcessor(handler EchoService) *EchoServiceProcessor {

  self4 := &EchoServiceProcessor{handler:handler, processorMap:make(map[string]thrift.TProcessorFunction)}
  self4.processorMap["Hi"] = &echoServiceProcessorHi{handler:handler}
  self4.processorMap["Do"] = &echoServiceProcessorDo{handler:handler}
return self4
}

func (p *EchoServiceProcessor) Process(iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
  name, _, seqId, err := iprot.ReadMessageBegin()
  if err != nil { return false, err }
  if processor, ok := p.GetProcessorFunction(name); ok {
    return processor.Process(seqId, iprot, oprot)
  }
  iprot.Skip(thrift.STRUCT)
  iprot.ReadMessageEnd()
  x5 := thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "Unknown function " + name)
  oprot.WriteMessageBegin(name, thrift.EXCEPTION, seqId)
  x5.Write(oprot)
  oprot.WriteMessageEnd()
  oprot.Flush()
  return false, x5

}

type echoServiceProcessorHi struct {
  handler EchoService
}

func (p *echoServiceProcessorHi) Process(seqId int32, iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
  args := EchoServiceHiArgs{}
  if err = args.Read(iprot); err != nil {
    iprot.ReadMessageEnd()
    x := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, err.Error())
    oprot.WriteMessageBegin("Hi", thrift.EXCEPTION, seqId)
    x.Write(oprot)
    oprot.WriteMessageEnd()
    oprot.Flush()
    return false, err
  }

  iprot.ReadMessageEnd()
  result := EchoServiceHiResult{}
  var err2 error
  if err2 = p.handler.Hi(); err2 != nil {
    x := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "Internal error processing Hi: " + err2.Error())
    oprot.WriteMessageBegin("Hi", thrift.EXCEPTION, seqId)
    x.Write(oprot)
    oprot.WriteMessageEnd()
    oprot.Flush()
    return true, err2
  }
  if err2 = oprot.WriteMessageBegin("Hi", thrift.REPLY, seqId); err2 != nil {
    err = err2
  }
  if err2 = result.Write(oprot); err == nil && err2 != nil {
    err = err2
  }
  if err2 = oprot.WriteMessageEnd(); err == nil && err2 != nil {
    err = err2
  }
  if err2 = oprot.Flush(); err == nil && err2 != nil {
    err = err2
  }
  if err != nil {
    return
  }
  return true, err
}

type echoServiceProcessorDo struct {
  handler EchoService
}

func (p *echoServiceProcessorDo) Process(seqId int32, iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
  args := EchoServiceDoArgs{}
  if err = args.Read(iprot); err != nil {
    iprot.ReadMessageEnd()
    x := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, err.Error())
    oprot.WriteMessageBegin("Do", thrift.EXCEPTION, seqId)
    x.Write(oprot)
    oprot.WriteMessageEnd()
    oprot.Flush()
    return false, err
  }

  iprot.ReadMessageEnd()
  result := EchoServiceDoResult{}
var retval *EchoRsp
  var err2 error
  if retval, err2 = p.handler.Do(args.Req); err2 != nil {
    x := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "Internal error processing Do: " + err2.Error())
    oprot.WriteMessageBegin("Do", thrift.EXCEPTION, seqId)
    x.Write(oprot)
    oprot.WriteMessageEnd()
    oprot.Flush()
    return true, err2
  } else {
    result.Success = retval
}
  if err2 = oprot.WriteMessageBegin("Do", thrift.REPLY, seqId); err2 != nil {
    err = err2
  }
  if err2 = result.Write(oprot); err == nil && err2 != nil {
    err = err2
  }
  if err2 = oprot.WriteMessageEnd(); err == nil && err2 != nil {
    err = err2
  }
  if err2 = oprot.Flush(); err == nil && err2 != nil {
    err = err2
  }
  if err != nil {
    return
  }
  return true, err
}


// HELPER FUNCTIONS AND STRUCTURES

type EchoServiceHiArgs struct {
}

func NewEchoServiceHiArgs() *EchoServiceHiArgs {
  return &EchoServiceHiArgs{}
}

func (p *EchoServiceHiArgs) Read(iprot thrift.TProtocol) error {
  if _, err := iprot.ReadStructBegin(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
  }


  for {
    _, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
    if err != nil {
      return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
    }
    if fieldTypeId == thrift.STOP { break; }
    if err := iprot.Skip(fieldTypeId); err != nil {
      return err
    }
    if err := iprot.ReadFieldEnd(); err != nil {
      return err
    }
  }
  if err := iprot.ReadStructEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
  }
  return nil
}

func (p *EchoServiceHiArgs) Write(oprot thrift.TProtocol) error {
  if err := oprot.WriteStructBegin("Hi_args"); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
  if p != nil {
  }
  if err := oprot.WriteFieldStop(); err != nil {
    return thrift.PrependError("write field stop error: ", err) }
  if err := oprot.WriteStructEnd(); err != nil {
    return thrift.PrependError("write struct stop error: ", err) }
  return nil
}

func (p *EchoServiceHiArgs) String() string {
  if p == nil {
    return "<nil>"
  }
  return fmt.Sprintf("EchoServiceHiArgs(%+v)", *p)
}

type EchoServiceHiResult struct {
}

func NewEchoServiceHiResult() *EchoServiceHiResult {
  return &EchoServiceHiResult{}
}

func (p *EchoServiceHiResult) Read(iprot thrift.TProtocol) error {
  if _, err := iprot.ReadStructBegin(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
  }


  for {
    _, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
    if err != nil {
      return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
    }
    if fieldTypeId == thrift.STOP { break; }
    if err := iprot.Skip(fieldTypeId); err != nil {
      return err
    }
    if err := iprot.ReadFieldEnd(); err != nil {
      return err
    }
  }
  if err := iprot.ReadStructEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
  }
  return nil
}

func (p *EchoServiceHiResult) Write(oprot thrift.TProtocol) error {
  if err := oprot.WriteStructBegin("Hi_result"); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
  if p != nil {
  }
  if err := oprot.WriteFieldStop(); err != nil {
    return thrift.PrependError("write field stop error: ", err) }
  if err := oprot.WriteStructEnd(); err != nil {
    return thrift.PrependError("write struct stop error: ", err) }
  return nil
}

func (p *EchoServiceHiResult) String() string {
  if p == nil {
    return "<nil>"
  }
  return fmt.Sprintf("EchoServiceHiResult(%+v)", *p)
}

// Attributes:
//  - Req
type EchoServiceDoArgs struct {
  Req *EchoReq `thrift:"req,1" db:"req" json:"req"`
}

func NewEchoServiceDoArgs() *EchoServiceDoArgs {
  return &EchoServiceDoArgs{}
}

var EchoServiceDoArgs_Req_DEFAULT *EchoReq
func (p *EchoServiceDoArgs) GetReq() *EchoReq {
  if !p.IsSetReq() {
    return EchoServiceDoArgs_Req_DEFAULT
  }
return p.Req
}
func (p *EchoServiceDoArgs) IsSetReq() bool {
  return p.Req != nil
}

func (p *EchoServiceDoArgs) Read(iprot thrift.TProtocol) error {
  if _, err := iprot.ReadStructBegin(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
  }


  for {
    _, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
    if err != nil {
      return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
    }
    if fieldTypeId == thrift.STOP { break; }
    switch fieldId {
    case 1:
      if err := p.ReadField1(iprot); err != nil {
        return err
      }
    default:
      if err := iprot.Skip(fieldTypeId); err != nil {
        return err
      }
    }
    if err := iprot.ReadFieldEnd(); err != nil {
      return err
    }
  }
  if err := iprot.ReadStructEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
  }
  return nil
}

func (p *EchoServiceDoArgs)  ReadField1(iprot thrift.TProtocol) error {
  p.Req = &EchoReq{}
  if err := p.Req.Read(iprot); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Req), err)
  }
  return nil
}

func (p *EchoServiceDoArgs) Write(oprot thrift.TProtocol) error {
  if err := oprot.WriteStructBegin("Do_args"); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
  if p != nil {
    if err := p.writeField1(oprot); err != nil { return err }
  }
  if err := oprot.WriteFieldStop(); err != nil {
    return thrift.PrependError("write field stop error: ", err) }
  if err := oprot.WriteStructEnd(); err != nil {
    return thrift.PrependError("write struct stop error: ", err) }
  return nil
}

func (p *EchoServiceDoArgs) writeField1(oprot thrift.TProtocol) (err error) {
  if err := oprot.WriteFieldBegin("req", thrift.STRUCT, 1); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:req: ", p), err) }
  if err := p.Req.Write(oprot); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Req), err)
  }
  if err := oprot.WriteFieldEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write field end error 1:req: ", p), err) }
  return err
}

func (p *EchoServiceDoArgs) String() string {
  if p == nil {
    return "<nil>"
  }
  return fmt.Sprintf("EchoServiceDoArgs(%+v)", *p)
}

// Attributes:
//  - Success
type EchoServiceDoResult struct {
  Success *EchoRsp `thrift:"success,0" db:"success" json:"success,omitempty"`
}

func NewEchoServiceDoResult() *EchoServiceDoResult {
  return &EchoServiceDoResult{}
}

var EchoServiceDoResult_Success_DEFAULT *EchoRsp
func (p *EchoServiceDoResult) GetSuccess() *EchoRsp {
  if !p.IsSetSuccess() {
    return EchoServiceDoResult_Success_DEFAULT
  }
return p.Success
}
func (p *EchoServiceDoResult) IsSetSuccess() bool {
  return p.Success != nil
}

func (p *EchoServiceDoResult) Read(iprot thrift.TProtocol) error {
  if _, err := iprot.ReadStructBegin(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
  }


  for {
    _, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
    if err != nil {
      return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
    }
    if fieldTypeId == thrift.STOP { break; }
    switch fieldId {
    case 0:
      if err := p.ReadField0(iprot); err != nil {
        return err
      }
    default:
      if err := iprot.Skip(fieldTypeId); err != nil {
        return err
      }
    }
    if err := iprot.ReadFieldEnd(); err != nil {
      return err
    }
  }
  if err := iprot.ReadStructEnd(); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
  }
  return nil
}

func (p *EchoServiceDoResult)  ReadField0(iprot thrift.TProtocol) error {
  p.Success = &EchoRsp{}
  if err := p.Success.Read(iprot); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Success), err)
  }
  return nil
}

func (p *EchoServiceDoResult) Write(oprot thrift.TProtocol) error {
  if err := oprot.WriteStructBegin("Do_result"); err != nil {
    return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err) }
  if p != nil {
    if err := p.writeField0(oprot); err != nil { return err }
  }
  if err := oprot.WriteFieldStop(); err != nil {
    return thrift.PrependError("write field stop error: ", err) }
  if err := oprot.WriteStructEnd(); err != nil {
    return thrift.PrependError("write struct stop error: ", err) }
  return nil
}

func (p *EchoServiceDoResult) writeField0(oprot thrift.TProtocol) (err error) {
  if p.IsSetSuccess() {
    if err := oprot.WriteFieldBegin("success", thrift.STRUCT, 0); err != nil {
      return thrift.PrependError(fmt.Sprintf("%T write field begin error 0:success: ", p), err) }
    if err := p.Success.Write(oprot); err != nil {
      return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Success), err)
    }
    if err := oprot.WriteFieldEnd(); err != nil {
      return thrift.PrependError(fmt.Sprintf("%T write field end error 0:success: ", p), err) }
  }
  return err
}

func (p *EchoServiceDoResult) String() string {
  if p == nil {
    return "<nil>"
  }
  return fmt.Sprintf("EchoServiceDoResult(%+v)", *p)
}


