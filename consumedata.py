from kafka import KafkaConsumer
import time,struct
import fsp_proto_pb2 as fsp_pb

def consumeSC(topic):
    time.sleep(1)

    # consumer=KafkaConsumer("ablert_test",bootstrap_servers="192.168.7.62:9092",auto_offset_reset="earliest")
    consumer=KafkaConsumer(topic,bootstrap_servers="192.168.7.60:9092,192.168.7.61:9092,192.168.7.62:9092")

    for msg in consumer:
        # import pdb
        # pdb.set_trace()
        a=msg.value
        # print "a value is: ",a  please check a value is what??????
        cpbuf=a

        totalsize=len(a)
        fmt = "!bqb%dsi%ds" % (len("albert_test"), (len(cpbuf) - 14 - len("albert_test")))
        aa = struct.unpack(fmt, cpbuf)
        kk = fsp_pb.ClientConnected()
        kk.ParseFromString(aa[5])
        print kk.app_id
        print kk.client_id

consumeSC("albert_test")
