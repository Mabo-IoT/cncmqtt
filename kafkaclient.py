import yaml
import logger
import json
from pykafka import KafkaClient
import traceback
import queue
import time


class CncKafka:
    kafkahosts = None
    kafkatopic = None
    consumergroup = None
    consumerid = None
    kafkaclient = None

    def __init__(self):
        '''
        初始化，从配置文件读取服务器信息
        '''
        try:
            f = open("config.yaml","r+",encoding="utf-8")
            fstream = f.read()
            configobj = yaml.safe_load(fstream)
            self.kafkahosts = configobj['kafka']['server']
            self.kafkatopic = configobj['kafka']['topic']
            self.consumergroup = configobj['kafka']['consumergroup']
            self.consumerid = configobj['kafka']['consumerid']
            self.connect()
        except:
            errstr = traceback.format_exc()
            logger.writeLog("Kafka客户端实例化失败:" + errstr)
        
    def connect(self):
        '''
        连接Kafka服务器
        '''
        try:
            self.kafkaclient = KafkaClient(hosts = self.kafkahosts)
        except:
            errstr = traceback.format_exc()
            logger.writeLog("Kafka服务器连接错误:" + errstr, "kafka.log")


    def sendmsg(self, msg):
        '''
        作为生产者发送一条数据
        '''
        topic = self.kafkaclient.topics[self.kafkatopic.encode('utf-8')]#选择一个topic
        producer = topic.get_producer(sync=False,linger_ms=0)
        strtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(strtime + ':kafka写入:'+ msg)
        try:
            producer.produce(msg.encode('utf-8'))
        except:
            errstr = traceback.format_exc()
            logger.writeLog("Kafka发布数据失败:"+ errstr + msg, "kafka.log")

    def getconsumer(self):
        '''
        返回一个消费者对象
        '''
        try:
            topic = self.kafkaclient.topics[self.kafkatopic.encode('utf-8')]#选择一个topic
            consumer = topic.get_simple_consumer(consumer_group=b'test_group', 
                             auto_commit_enable=True, 
                             auto_commit_interval_ms=1, 
                             consumer_id=b'test_id')
            strtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            print(strtime + " :Kafka Consumer初始化成功!")
            return consumer
        except:
            errstr = traceback.format_exc()
            logger.writeLog("Kafka消费者实例化失败:" + errstr, "kafka.log")



if __name__ == "__main__":
    kclient = CncKafka()
    kclient.connect()
    # kclient.sendmsg(json.dumps({"niumoo":"bbb"}))
    consumer  = kclient.getconsumer()
    for message in consumer:
        if message is not None:
            print(message.offset, message.value.decode('utf-8'))