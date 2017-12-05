


struct EchoReq {
    1: i32 seq_id;
    2: string str_dat;
    3: binary bin_dat;
    4: map<string,double> m_dat;
}

struct EchoRsp {
    1: i32 status;
    2: string msg;
}

service EchoService {
    void Hi();
    EchoRsp Do(1: EchoReq req)
}