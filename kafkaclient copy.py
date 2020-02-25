import yaml
# import logger
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
import sys
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG)


if __name__ == "__main__":
    # 读取配置文件
    # try:
        f = open("config.yaml","r+",encoding="utf-8")
        fstream = f.read()
        configobj = yaml.safe_load(fstream)
        servers = configobj['kafka']['server']
        topic = configobj['kafka']['topic']
        producer = KafkaProducer(bootstrap_servers=servers,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                )
        future = producer.send(topic = 'cncmqtt', value = {"aaa": 'jmeimoumou'})
        producer.flush()
        

        # consumer = KafkaConsumer('__consumer_offsets', 
        #                          bootstrap_servers=servers)
        # for msg in consumer:
        #     consumer.commit()
        #     print(msg)
        
    # except:
        # logger.writeLog("读取配置文件失败!")



# from pykafka import KafkaClient
# host = '127.0.0.1:9092'
# client = KafkaClient(hosts = host)
# topic = client.topics["cncmqtt".encode()]
# # 将产生kafka同步消息，这个调用仅仅在我们已经确认消息已经发送到集群之后
# with topic.get_sync_producer() as producer:
#     for i in range(5):
#         producer.produce(('test message ' + str(i ** 2)).encode())