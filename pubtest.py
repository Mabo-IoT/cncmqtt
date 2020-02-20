import paho.mqtt.client as mqtt
import threading
import time

#MQTT连接回调函数
def on_connect(client, userdata, flags, rc):
    print("连接服务器: "+str(rc))

#MQTT订阅接收回调函数
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def pubinfo(client):
    while True:
        client.publish('100/TXST001/ping', '{"CncId": "TXST001", "PingStr": "MDC is living"}')
        strtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(strtime + ":发布成功!")
        time.sleep(5)

        client.publish('100/TXST001/status', '{"CncId": "TXST001", "RunStatus": "1","Alarm":"0" ,"Time":"2019-09-04 14:15:13"}')
        strtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(strtime + ":发布成功!")
        time.sleep(5)

        client.publish('100/TXST001/x_vibration', '{"CncId": "TXST001", "XVibration": "0.93", "Time": "2019-09-04 14:15:13"}')
        strtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(strtime + ":发布成功!")
        time.sleep(5)

        client.publish('100/TXST001/y_vibration', '{"CncId": "TXST001", "YVibration": "0.93", "Time": "2019-09-04 14:15:13"}')
        strtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(strtime + ":发布成功!")
        time.sleep(5)

        client.publish('100/TXST001/z_vibration', '{"CncId": "TXST001", "ZVibration": "0.93", "Time": "2019-09-04 14:15:13"}')
        strtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(strtime + ":发布成功!")
        time.sleep(5)

        client.publish('100/TXST001/spindleTemp', '{"CncId": "TXST001", "SpindleTemp": "0.93","Time": "2019-09-04 14:15:13"}')
        strtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(strtime + ":发布成功!")
        time.sleep(5)

        client.publish('100/TXST001/envTemp', '{"CncId": "TXST001", "EnvTemp": "25.89", "Time": "2019-09-04 14:15:13"}')
        strtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(strtime + ":发布成功!")
        time.sleep(5)

        client.publish('100/TXST001/abrPower', '{"CncId": "TXST001","EnvTemp": "25.89","Time": "2019-09-04 14:15:13","AbrPower": {"ToolNo": "1","Msgl": "2.34","mssx": "2.00","msxx": "1.00"}}')
        strtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(strtime + ":发布成功!")
        time.sleep(5)


if __name__ == "__main__":
    mqttclient = mqtt.Client()
    mqttclient.on_connect = on_connect
    mqttclient.on_message = on_message
    mqttclient.connect('127.0.0.1', 1883, 30)
    th = threading.Thread(target=pubinfo,args=(mqttclient,)) 
    th.start()
    while True:
        mqttclient.loop()