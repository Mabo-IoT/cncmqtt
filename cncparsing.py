import json
import logger
from database import DatabaseAdapter

class CNCParsing:
    topic = '' # 主题
    jsonobj = None # 内容
    oradb = None #oracle数据库操作对象

    def __init__(self,topic,jsonobj):
        self.topic = topic
        self.jsonobj = jsonobj
        self.oradb = DatabaseAdapter()

    # 根据主题及内容使用不同的方法处理数据
    def parse(self):
        print(self.topic)
        print(self.jsonobj)
        #--------------------------机床部分-----------------------------
        #机床基础信息
        if self.topic.find('Basic') != -1:
            '''
            {"Cncld":"1",
             "PingStr":"MDCisliving",
             "RunStatus":"0",
             "PoweronStatus":"0",
             "Alarm":"报警信息",
             "Time":"2020-01-22 22:53:14",
             "SpindleTemp":23.567,
             "EnvTemp":23.678,
             "CutfluTemp":23.789,
             "SliderTemp":23.891,
             "Coordinate":{"X":12.123,"Y":23.234,"Z":34.345}
            }
            '''
            logger.writeLog('接收到机床基础信息->' + json.dumps(self.jsonobj))
            # 给部分可选值设置默认值
            if 'Alarm' not in self.jsonobj.keys():
                self.jsonobj['Alarm'] = ''
            if 'Coordinate' not in self.jsonobj.keys():
                self.jsonobj['Coordinate'] = {}
            #生成插入的sql语句
            sqlstr = """
            insert into BASIC_MACHINE (cncid, pingstr, runstatus, poweronstatus, 
                                       alarm, time, spindletemp, envtemp, 
                                       cutflutemp, slidertemp, coordinate)
            values (:cncid, :pingstr, :runstatus, 
                    :poweronstatus, :alarm, to_date(:time, 'YYYY-MM-DD HH24:MI:SS'), 
                    :spindletemp, :envtemp, :cutflutemp, :slidertemp, :coordinate)
            """
            parameters = {'cncid':self.jsonobj['Cncld'], 'pingstr':self.jsonobj['PingStr'], 
                          'runstatus':self.jsonobj['RunStatus'], 'poweronstatus':self.jsonobj['PoweronStatus'], 
                          'alarm':self.jsonobj['Alarm'], 'time':self.jsonobj['Time'], 
                          'spindletemp':self.jsonobj['SpindleTemp'], 'envtemp':self.jsonobj['EnvTemp'], 
                          'cutflutemp':self.jsonobj['CutfluTemp'], 'slidertemp':self.jsonobj['SliderTemp'], 
                          'coordinate':json.dumps(self.jsonobj['Coordinate'])}
            # 插入数据库   
            self.oradb.insert(sqlstr, parameters) 
        # 主轴三向震动
        elif self.topic.find('vibration') != -1:
            '''
            {"Cncld":"1",
             "Xvibration":1.123,
             "Yvibration":1.123,
             "Zvibration":1.123,
             "XvibrationP":1.123,
             "YvibrationP":1.123,
             "ZvibrationP":1.123,
             "Time":"2020-01-22 22:53:14"
            }
            '''
            logger.writeLog('主轴三向震动->' + json.dumps(self.jsonobj))
            # 全是必须值无省略值
            
            #生成插入的sql语句
            sqlstr = """
            insert into MACHINE_VIBRATION (cncid, xvibration, yvibration, 
                                           zvibration, xvibrationp, yvibrationp,
                                           zvibrationp, time)
            values (:cncid, :xvibration, :yvibration, :zvibration, 
                    :xvibrationp, :yvibrationp, :zvibrationp,
                    to_date(:time, 'YYYY-MM-DD HH24:MI:SS'))
            """
            parameters = {'cncid':self.jsonobj['Cncld'], 'xvibration':self.jsonobj['Xvibration'], 
                          'yvibration':self.jsonobj['Yvibration'], 'zvibration':self.jsonobj['Zvibration'], 
                          'xvibrationp':self.jsonobj['XvibrationP'], 'yvibrationp':self.jsonobj['YvibrationP'],
                          'zvibrationp':self.jsonobj['ZvibrationP'],
                          'time':self.jsonobj['Time']}
            # 插入数据库   
            self.oradb.insert(sqlstr, parameters) 
        #刀具功率磨损值
        elif self.topic.find('Abrpower') != -1: 
            '''
            {"Cncld":"1",
             "AbrPower":{"ToolNo":1,
                         "Msgl":23.234,
                         "Mssx":34.345,
                         "Msxx":20.222},
             "Time":"2020-01-22 22:53:14"
            }
            '''
            logger.writeLog('接收到刀具功率磨损值->' + json.dumps(self.jsonobj))
        #主轴X方向加速度振动磨损值接口
        elif self.topic.find('XAbracceleration') != -1:
            '''
            {"Cncld":"1",
             "AbrAcceleration":{"ToolNo":1,
                                "Mszd":23.234,
                                "Mssx":34.345,
                                "Msxx":20.222},
             "Time":"2020-01-22 22:53:14"
            }
            '''
            logger.writeLog('主轴X方向加速度振动磨损值接口->' + json.dumps(self.jsonobj))
        #主轴Y方向加速度振动磨损值接口
        elif self.topic.find('YAbracceleration') != -1:
            '''
            {"Cncld":"1",
            "AbrAcceleration":{"ToolNo":1,
                               "Mszd":23.234,
                               "Mssx":34.345,
                               "Msxx":20.222},
            "Time":"2020-01-22 22:53:14"
            }
            '''
            logger.writeLog('主轴Y方向加速度振动磨损值接口->' + json.dumps(self.jsonobj))
        #主轴Z方向加速度振动磨损值接口
        elif self.topic.find('ZAbracceleration') != -1:
            '''
            {"Cncld":"1",
            "AbrAcceleration":{"ToolNo":1,
                                "Mszd":23.234,
                                "Mssx":34.345,
                                "Msxx":20.222},
            "Time":"2020-01-22 22:53:14"}
            '''
            logger.writeLog('主轴Z方向加速度振动磨损值接口->' + json.dumps(self.jsonobj))
        #主轴X方向速度振动磨损值接口
        elif self.topic.find('XAbrvelocity') != -1:
            '''
            {"Cncld":"1",
            "AbrVelocity":{"ToolNo":1,
                            "Mszd":23.234,
                            "Mssx":34.345,
                            "Msxx":20.222},
            "Time":"2020-01-22 22:53:14"}
            '''
            logger.writeLog('主轴X方向速度振动磨损值接口->' + json.dumps(self.jsonobj))
        #主轴Y方向速度振动磨损值接口
        elif self.topic.find('YAbrvelocity') != -1:
            '''
            {"Cncld":"1",
            "AbrVelocity":{"ToolNo":1,
                            "Mszd":23.234,
                            "Mssx":34.345,
                            "Msxx":20.222},
            "Time":"2020-01-22 22:53:14"}
            '''
            logger.writeLog('主轴Y方向速度振动磨损值接口->' + json.dumps(self.jsonobj)
        #主轴Z方向速度振动磨损值接口
        elif self.topic.find('ZAbrvelocity') != -1:
            '''
            {"Cncld":"1",
            "AbrVelocity":{"ToolNo":1,
                           "Mszd":23.234,
                           "Mssx":34.345,
                           "Msxx":20.222},
            "Time":"2020-01-22 22:53:14"}
            '''
            logger.writeLog('主轴Z方向速度振动磨损值接口->' + json.dumps(self.jsonobj))
        #热机时加速度有效值接口
        elif self.topic.find('Machineheat') != -1:
            '''
            {"Cncld":"1",
            "HeatNO":"111",
            "MSAvePower":23.234,"XSAvePower":34.345,"YSAvePower":20.222,
            "ZSAvePower":21.234,"BSAvePower":21.234,"VSAvePower":21.234,
            "MSStdPower":1,"XStdPower":1,"YStdPower":1,
            "ZStdPower":1,"BStdPower":1,"VStdPower":1,
            "MSXAccelerationMax":1,"MSYAccelerationMax":1,"MSZAccelerationMax":1,
            "MSBAccelerationMax":1,"XSXAccelerationMax":1,"YSYAccelerationMax":1,
            "MSXVelocityRMS":1,"MSYVelocityRMS":1,"MSZVelocityRMS":1,
            "MSBVelocityRMS":1,"XSXVelocityRMS":1,"YSYVelocityRMS":1,
            "Time":"2020-01-22 22:53:14"}
            '''
            logger.writeLog('热机时加速度有效值接口->' + json.dumps(self.jsonobj))
       
        #--------------------------机械手部分-----------------
        #热机时加速度有效值接口
        elif self.topic.find('ZAbrvelocity') != -1:
            '''
            {"Cncld":"1",
            "AbrVelocity":{"ToolNo":1,
                           "Mszd":23.234,
                           "Mssx":34.345,
                           "Msxx":20.222},
            "Time":"2020-01-22 22:53:14"}
            '''
            logger.writeLog('主轴Z方向速度振动磨损值接口->' + json.dumps(self.jsonobj))
        #热机时加速度有效值接口
        elif self.topic.find('ZAbrvelocity') != -1:
            '''
            {"Cncld":"1",
            "AbrVelocity":{"ToolNo":1,
                           "Mszd":23.234,
                           "Mssx":34.345,
                           "Msxx":20.222},
            "Time":"2020-01-22 22:53:14"}
            '''
            logger.writeLog('主轴Z方向速度振动磨损值接口->' + json.dumps(self.jsonobj))
        else:
            print("传入值异常，未找到匹配项!")
