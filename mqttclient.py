import paho.mqtt.client as mqtt
import threading
import time
import yaml
import queue
import json
import logger
from cncparsing import CNCParsing

# 全局变量定义需要订阅的主题
subtopic = []

# 定义队列OBJ对象
queobj = {
    "topic" : None,
    "msg": None
}

# 定义队列接搜订阅值
subqueue = queue.Queue()

#MQTT连接回调函数
def on_connect(client, userdata, flags, rc):
    logger.writeLog("成功连接MQTT服务器: "+str(rc))
    for topic in subtopic:
        client.subscribe(topic)

#MQTT订阅接收回调函数
def on_message(client, userdata, msg):
    # print(msg.topic+" "+str(msg.payload))
    queobj['topic'] = str(msg.topic)
    queobj['msg'] = json.loads(str(msg.payload,'utf-8'))
    subqueue.put(json.dumps(queobj))


def dealInfo():
    while 1:
        if not subqueue.empty() :
            obj =json.loads(subqueue.get())
            cncparse = CNCParsing(obj['topic'], obj['msg'])
            cncparse.parse()
        # time.sleep(0.1)

if __name__ == "__main__":
    # 读取配置文件
    try:
        f = open("config.yaml","r+",encoding="utf-8")
        fstream = f.read()
        configobj = yaml.safe_load(fstream)
        mqttserverurl = configobj['mqtt']['server']
        mqttserverport = configobj['mqtt']['port']
        mqttkeepalive = configobj['mqtt']['keepalive']
        subtopic = configobj['mqtt']['subtopics']
        print("即将订阅的主题为：" + subtopic)
        mqttclient = mqtt.Client()
        mqttclient.on_connect = on_connect
        mqttclient.on_message = on_message
        mqttclient.connect(mqttserverurl, mqttserverport, mqttkeepalive)
        
        # 开启写oracle数据库线程
        th = threading.Thread(target=dealInfo)
        th.start()
        while True:
            # 循环运行，保持MQTT连接
            mqttclient.loop()
    except:
        logger.writeLog("读取配置文件失败!")