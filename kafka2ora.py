import time
import yaml
import queue
import json
import logger
import traceback
from cncparsing import CNCParsing
from kafkaclient import CncKafka
        

if __name__ == "__main__":
    try:
        # 初始化Kafka
        kclient = CncKafka()
        kclient.connect()
        consumer  = kclient.getconsumer()
        for message in consumer:
            if message is not None:
                print('kafka2ora接收到数据')
                 # 解析并存入Oracle数据
                resmsg =json.loads(message.value.decode('utf-8'))
                cncparse = CNCParsing(resmsg['topic'], json.loads(resmsg['msg']))
                cncparse.parse()
    except:
        errstr = traceback.format_exc()
        logger.writeLog("Kafka消费数据写入库错误:" + errstr, "kafka2ora.log")