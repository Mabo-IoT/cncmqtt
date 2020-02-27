# cncmqtt
通过mqtt客户端订阅CNC设备信息并将信息存入数据库

程序运行    python mqttclient.py

​                    python kafka2ora.py



程序结构及说明

mqttclient.py -------------------------接收mqtt消息并存入kafka

kafkaclient.py-------------------------实现kafka生产者及消费者功能

kafka2ora.py--------------------------通过使用kafkaclient，把kafka数据                        存入数据库

database.py---------------------------封装了数据库的基本操作

cncparsing.py-------------------------解析数据，根据数据生成不同的SQL语句，并将数据存入数据库

config.yaml-----------------------------程序配置文件