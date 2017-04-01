import fsp_proto_pb2 as fsp_pb
import as_proto_pb2 as as_pb
import struct


def hton(num):
    return struct.pack('i', num)
# def partbuf():
#     return buf[type_hton,sep_hton]

# NC login AS
def NCLoginAS(user_name,user_passwd,seq=0):
    NCLAS=as_pb.LoginReq()
    NCLAS.user_name=user_name
    NCLAS.user_passwd=user_passwd

    NCLAS_msg=NCLAS.SerializeToString()
    type=as_pb.InterfaceType.Value('TypeLoginReq')
    # return buf
    fmt="!2i%ds"%len(NCLAS_msg)
    buf=struct.pack(fmt,type,seq,NCLAS_msg)

    ltype, seq, msg = struct.unpack(fmt, buf)

    print ltype, seq, msg

    return  buf

# NC apply stream from as
def NCApplyStreamFromAS(stream_type,stream_property,seq=0):
    NCASFAS=as_pb.ApplyForStreamReq()
    NCASFAS.stream_type=stream_type
    NCASFAS.stream_property=stream_property

    NCASFAS_msg=NCASFAS.SerializeToString()

    type=as_pb.InterfaceType.Value('TypeApplyForStreamReq')

    seq_hton = hton(seq)
    type_hton = hton(type)
    buf = [type_hton, seq_hton, NCASFAS_msg]

    return buf

def NCGetProxy(proxy_type,exception_proxy):
    list=[]
    return list

# NC notify that stream had been published
def NCNotifyStreamPublished(stream_id,seq=0):
    NCNSP=as_pb.UpwardNotifyStreamPublishedReq()
    NCNSP.stream_id=stream_id

    NCNSP_msg=NCNSP.SerializeToString()

    type=as_pb.InterfaceType.Value('TypeUpwardNotifyStreamPublishedReq')
    seq_hton = hton(seq)
    type_hton = hton(type)
    buf = [type_hton, seq_hton, NCNSP_msg]

    return buf


# AS notify (NC2)that NC(1) had been had been published stream

def ASNotifyStreamPublished(stream_id,stream_type,stream_subscribe_token,seq=0):
    ASNSP=as_pb.DownwardNotifyStreamPublishedReq()
    ASNSP.stream_id=stream_id
    ASNSP.stream_type=stream_type
    ASNSP.stream_subscribe_token=stream_subscribe_token

    ASNSP_msg=ASNSP.SerializeToString()
    type=as_pb.InterfaceType.Value('TypeDownwardNotifyStreamPublishedReq')

    seq_hton = hton(seq)
    type_hton = hton(type)
    buf = [type_hton, seq_hton, ASNSP_msg]

    return buf

#NC login CP
def LoginCP(client_token,seq= 0):
    nclogin = fsp_pb.LoginCP()
    nclogin.client_token = client_token
    token_serialize = nclogin.SerializeToString()
    # token_serialize=token_serialize[1:]
    login_type = fsp_pb.ProtoDictionary.Value('Enum2LoginCP')
    # sep_hton= hton(seq)
    # type_hton = hton(login_type)
    # buf = [type_hton,sep_hton,token_serialize]
    # print buf
    # -------------try first time start
    # lotingtype=struct.pack('!i',login_type)
    # logseq=struct.pack('!i',seq)
    #
    # tklen=len(token_serialize)
    # buf=bytearray(tklen+8)
    # buf[0:4]=lotingtype
    # print 'login type',buf[0:4]
    # buf[4:8]=logseq
    # print 'seq',buf[4:8]
    # buf[8:]=token_serialize
    #
    # print'buf type is ',type(buf)
    # return buf
    #-----------------------try first time end


    # second time try start
    buf=struct.pack("!2i10s",login_type,seq,token_serialize)

    return  buf

    #second time try end




def LogoutCP(client_token,seq=0):
    nclogout=fsp_pb.LogoutCP()
    nclogout.client_token=client_token

    nclogout_msg=nclogout.SerializeToString()

    type=fsp_pb.ProtoDictionary.Value('Enum2LogoutCP')

    seq_hton=hton(seq)
    type_hton=hton(type)

    buf=[type_hton,seq_hton,nclogout_msg]
    return buf


def NCApplyPublishStream(stream_id,stream_public_token,seq = 0):

    NCPS=fsp_pb.PublishStream()
    NCPS.stream_id=stream_id
    NCPS.stream_public_token=stream_public_token

    NCPS_msg=NCPS.SerializeToString()

    type=fsp_pb.ProtoDictionary.Value('Enum2PublishStream')


    seq_hton=hton(seq)

    type_hton=hton(type)

    buf=[type_hton,seq_hton,NCPS_msg]
    return buf

def NCUnPublishStream(stream_id,seq=0):
    NCUPS=fsp_pb.UnPublishStream()
    NCUPS.stream_id =stream_id

    NCUPS_msg=NCUPS.SerializeToString()

    type=fsp_pb.ProtoDictionary.Value('Enum2UnPublishStream')

    seq_hton=hton(seq)
    type_hton=hton(type)

    buf=[type_hton,seq_hton,NCUPS_msg]
    return  buf



def NCGetStreamServers(stream_id,stream_subscribe_token,exception_proxies=["3"],seq=0):

    NCGSS=fsp_pb.GetStreamServers()
    NCGSS.stream_id=stream_id
    NCGSS.stream_subscribe_token=stream_subscribe_token
    NCGSS.exception_proxies.extend(exception_proxies)

    NCGSS_msg=NCGSS.SerializeToString()

    type=fsp_pb.ProtoDictionary.Value('Enum2GetStreamServers')

    seq_hton=hton(seq)
    type_hton=hton(type)

    buf=[type_hton,seq_hton,NCGSS_msg]
    # print buf
    return buf

def NCStreamSendingStart(stream_id,seq=0):
    NCSSS=fsp_pb.StreamSendingStartNC()
    NCSSS.stream_id=stream_id

    NCSSS_msg=NCSSS.SerializeToString()

    type=fsp_pb.ProtoDictionary.Value('Enum2StreamSendingStartNC')

    seq_hton=hton(seq)
    type_hton=hton(type)
    buf=[type_hton, seq_hton, NCSSS_msg]
    return buf

def NCStreamSendingStop(stream_id,recv_client_id,seq=0):

    NCSSStop=fsp_pb.StreamSendingStop()
    NCSSStop.stream_id=stream_id
    NCSSStop.recv_client_id=recv_client_id

    NCSSStop_msg=NCSSStop.SerializeToString()

    type=fsp_pb.ProtoDictionary.Value('Enum2StreamSendingStop')

    seq_hton=hton(seq)
    type_hton=hton(type)

    buf=[type_hton, seq_hton, NCSSStop_msg]

    return buf

def NCNotifyStreamPublished(stream_id,seq=0):

    NCNSP=fsp_pb.NotifyStreamPublished()
    NCNSP.stream_id=stream_id

    NCNSP_msg=NCNSP.SerializeToString()

    type=fsp_pb.ProtoDictionary.Value('Enum2NotifyStreamPublished')

    seq_hton=hton(seq)
    type_hton=hton(type)
    buf = [type_hton, seq_hton, NCNSP_msg]

    return buf


def NCLoginSendingChannel(stream_id,stream_publish_token,client_token,seq=0):
    NCLSC=fsp_pb.LoginSendingChannel()
    NCLSC.stream_id=stream_id
    NCLSC.stream_publish_token=stream_publish_token
    NCLSC.client_token=client_token

    LSC_msg=NCLSC.SerializeToString()

    type=fsp_pb.ProtoDictionary.Value('Enum2LoginSendingChannel')

    seq_hton=hton(seq)
    type_hton=hton(type)
    buf = [type_hton, seq_hton, LSC_msg]

    return buf

def NCLoginReceivingChannel(stream_id,stream_subscribe_token,client_token,seq=0):
    NCLRC=fsp_pb.LoginReceivingChannel()
    NCLRC.stream_id=stream_id
    NCLRC.stream_subscribe_token=stream_subscribe_token
    NCLRC.client_token=client_token

    NCLRC_msg=NCLRC.SerializeToString()

    type=fsp_pb.ProtoDictionary.Value('Enum2LoginReceivingChannel')

    seq_hton=hton(seq)
    type_hton=hton(type)

    buf = [type_hton, seq_hton, NCLRC_msg]

    return buf


# aa=LoginCP("2")
NCGetStreamServers("22","678")