from kafka import KafkaConsumer
import time,struct
import fsp_proto_pb2 as fsp_pb

def consumeSC(topic):
    time.sleep(1)
    # kafkaCluster="192.168.7.60:9092,192.168.7.61:9092,192.168.7.62:9092"
    kafkaCluster="192.168.7.63:9092,192.168.7.64:9092,192.168.7.65:9092"
    # consumer=KafkaConsumer("ablert_test",bootstrap_servers="192.168.7.62:9092",auto_offset_reset="earliest")
    consumer=KafkaConsumer(topic,bootstrap_servers=kafkaCluster)

    for msg in consumer:
        a=msg.value
        # print "a value is: ",a  please check a value is what??????
        cpbuf=a
        # import pdb
        # pdb.set_trace()
        fmt = "!bqb%dsi%ds" % (len("cp_test1"), (len(cpbuf) - 14 - len("cp_test1")))
        aa = struct.unpack(fmt, cpbuf)
        kk = fsp_pb.ClientConnected()
        kk.ParseFromString(aa[5])


def clientRep(consume_topic,msg_topic,kafkaCluster):
    '''
    This function is using for consume data which produce by SC
    After SC finished consume the data which produce by CP, SC will produce data as CP's respone

    :param consume_topic: this topic used for this program to comsume data
    :param msg_topic: this topic used for unserial message in 'fmt'
    :param kafkaCluster:
    :return:
    '''
    time.sleep(1)
    kafkaCluster =  kafkaCluster
    consumer = KafkaConsumer(consume_topic,bootstrap_servers=kafkaCluster)

    for msg in consumer:
        cpbuf=msg.value
        fmt = "!bqb%dsi%ds" % (len(msg_topic), (len(cpbuf) - 14 - len(msg_topic)))
        aa = struct.unpack(fmt, cpbuf)
        kk = fsp_pb.ClientConnectedRsp()
        kk.ParseFromString(aa[5])
        print kk

#here  sc_test_instance103 is the topic that producer used to send message
clientRep("cp_test1","sc_test_instance103","192.168.7.63:9092,192.168.7.64:9092,192.168.7.65:9092" )