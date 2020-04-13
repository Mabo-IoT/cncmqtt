import json
import logger
from database import DatabaseAdapter
import traceback

class CNCParsing:
    topic = '' # 主题
    jsonobj = None # 内容
    oradb = None #oracle数据库操作对象

    def __init__(self):
        try:
            self.oradb = DatabaseAdapter()
            self.oradb.oraconnect()
        except:
            errstr = traceback.format_exc()
            logger.writeLog("CNC字段解析程序初始化失败:" + errstr)

    # 根据主题及内容使用不同的方法处理数据
    def parse(self, topic,jsonobj):
        self.topic = topic
        self.jsonobj = jsonobj
        try:
            #--------------------------机床部分-----------------------------
            #机床基础信息
            if self.topic == 'Basic':
                '''
                {"CncId":"1",
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
                parameters = {'cncid':self.jsonobj['CncId'], 'pingstr':self.jsonobj['PingStr'], 
                            'runstatus':self.jsonobj['RunStatus'], 'poweronstatus':self.jsonobj['PoweronStatus'], 
                            'alarm':self.jsonobj['Alarm'], 'time':self.jsonobj['Time'], 
                            'spindletemp':self.jsonobj['SpindleTemp'], 'envtemp':self.jsonobj['EnvTemp'], 
                            'cutflutemp':self.jsonobj['CutfluTemp'], 'slidertemp':self.jsonobj['SliderTemp'], 
                            'coordinate':json.dumps(self.jsonobj['Coordinate'])}
                # 插入数据库  
                self.oradb.insert(sqlstr, parameters)
                # print("机床基础信息写入数据库:" + json.dumps(self.jsonobj))
                # logger.writeLog('机床基础信息写入数据库->' + json.dumps(self.jsonobj), "database.log")
            # 主轴三向震动
            elif self.topic == 'vibration':
                '''
                {"CncId":"1",
                "Xvibration":1.123,
                "Yvibration":1.123,
                "Zvibration":1.123,
                "XvibrationP":1.123,
                "YvibrationP":1.123,
                "ZvibrationP":1.123,
                "Time":"2020-01-22 22:53:14"
                }
                '''
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
                parameters = {'cncid':self.jsonobj['CncId'], 'xvibration':self.jsonobj['Xvibration'], 
                            'yvibration':self.jsonobj['Yvibration'], 'zvibration':self.jsonobj['Zvibration'], 
                            'xvibrationp':self.jsonobj['XvibrationP'], 'yvibrationp':self.jsonobj['YvibrationP'],
                            'zvibrationp':self.jsonobj['ZvibrationP'],
                            'time':self.jsonobj['Time']}
                # 插入数据库   
                self.oradb.insert(sqlstr, parameters)
                # print("主轴三向震动写入数据库:" + json.dumps(self.jsonobj)) 
                # logger.writeLog('主轴三向震动写入数据库->' + json.dumps(self.jsonobj), "database.log")
            #刀具功率磨损值
            elif self.topic == 'Abrpower': 
                '''
                {"CncId":"1",
                "AbrPower":{"ToolNo":1,
                            "Msgl":23.234,
                            "Mssx":34.345,
                            "Msxx":20.222},
                "Time":"2020-01-22 22:53:14"
                }
                '''
                #全是必须值无省略值
                #生成插入的sql语句
                sqlstr = """
                insert into MACHINE_POWER (cncid, time, abrpower,
                                        toolno, msgl, mssx, msxx)
                values (:cncid, to_date(:time, 'YYYY-MM-DD HH24:MI:SS'),
                        :abrpower, :toolno, :msgl, :mssx, :msxx)
                """
                parameters = {'cncid':self.jsonobj['CncId'], 'time':self.jsonobj['Time'],
                            'abrpower':json.dumps(self.jsonobj['AbrPower']),
                            'toolno':self.jsonobj['AbrPower']['ToolNo'],
                            'msgl':self.jsonobj['AbrPower']['Msgl'],
                            'mssx':self.jsonobj['AbrPower']['Mssx'],
                            'msxx':self.jsonobj['AbrPower']['Msxx']}
                # 插入数据库
                self.oradb.insert(sqlstr, parameters)
                # print("刀具功率磨损值写入数据库:" + json.dumps(self.jsonobj))
                # logger.writeLog('刀具功率磨损值写入数据库->' + json.dumps(self.jsonobj), "database.log")
            #主轴方向加速度振动磨损值接口
            elif self.topic == 'Abracceleration':
                '''
                    {"CncId":"1",
                    "AbrAcceleration":{"ToolNo":1,
                                    "Mszd":23.234,
                                    "Mssx":34.345,
                                    "Msxx":20.222},
                    "Time":"2020-01-22 22:53:14"
                    }
                '''
                #主轴X方向加速度振动磨损值接口
                if self.topic.find('XAbracceleration') != -1:
                    self.jsonobj['direction'] = 'X'
                #主轴Y方向加速度振动磨损值接口
                elif self.topic.find('YAbracceleration') != -1:
                    self.jsonobj['direction'] = 'Y'
                #主轴Z方向加速度振动磨损值接口
                elif self.topic.find('ZAbracceleration') != -1:
                    self.jsonobj['direction'] = 'Z'
                else:
                    logger.writeLog('未发现正确的主轴方向加速度->' + json.dumps(self.jsonobj))
                    self.jsonobj['direction'] = ''
                #全是必须值无省略值
                #生成插入的sql语句
                sqlstr = """
                insert into ABRACCELERATION (cncid, time, toolno,
                                            direction, mszd, mssx,
                                            msxx)
                values (:cncid, to_date(:time, 'YYYY-MM-DD HH24:MI:SS'),
                        :toolno, :direction, :mszd, :mssx, :msxx)
                """
                parameters = {'cncid':self.jsonobj['CncId'], 'time':self.jsonobj['Time'],
                            'toolno':self.jsonobj['AbrAcceleration']['ToolNo'],
                            'direction':self.jsonobj['direction'],
                            'mszd':self.jsonobj['AbrAcceleration']['Mszd'],
                            'mssx':self.jsonobj['AbrAcceleration']['Mssx'],
                            'msxx':self.jsonobj['AbrAcceleration']['Msxx']}
                # 插入数据库
                self.oradb.insert(sqlstr, parameters)
                # print("主轴方向加速度振动磨损写入数据库:" + json.dumps(self.jsonobj))
                # logger.writeLog('主轴方向加速度振动磨损写入数据库->' + json.dumps(self.jsonobj), "database.log")

            #主轴方向速度振动磨损值接口
            elif self.topic == 'Abrvelocity':
                '''
                {"CncId":"1",
                "AbrVelocity":{"ToolNo":1,
                                "Mszd":23.234,
                                "Mssx":34.345,
                                "Msxx":20.222},
                "Time":"2020-01-22 22:53:14"}
                '''
                #主轴X方向速度振动磨损值接口
                if self.topic.find('XAbrvelocity') != -1:
                    self.jsonobj['direction'] = 'X'
                #主轴Y方向速度振动磨损值接口
                elif self.topic.find('YAbrvelocity') != -1:
                    self.jsonobj['direction'] = 'Y'
                #主轴Z方向速度振动磨损值接口
                elif self.topic.find('ZAbrvelocity') != -1:
                    self.jsonobj['direction'] = 'Z'
                else:
                    logger.writeLog('未发现正确的主轴方向速度振动磨损值->' + json.dumps(self.jsonobj))
                    self.jsonobj['direction'] = ''
                #全是必须值无省略值
                #生成插入的sql语句
                sqlstr = """
                insert into ABRVELOCITY (cncid, time, toolno,
                                            direction, mszd, mssx,
                                            msxx)
                values (:cncid, to_date(:time, 'YYYY-MM-DD HH24:MI:SS'),
                        :toolno, :direction, :mszd, :mssx, :msxx)
                """
                parameters = {'cncid':self.jsonobj['CncId'], 'time':self.jsonobj['Time'],
                            'toolno':self.jsonobj['AbrVelocity']['ToolNo'],
                            'direction':self.jsonobj['direction'],
                            'mszd':self.jsonobj['AbrVelocity']['Mszd'],
                            'mssx':self.jsonobj['AbrVelocity']['Mssx'],
                            'msxx':self.jsonobj['AbrVelocity']['Msxx']}
                # 插入数据库
                self.oradb.insert(sqlstr, parameters)
                # print("主轴方向速度振动磨损写入数据库:" + json.dumps(self.jsonobj))
                # logger.writeLog('主轴方向速度振动磨损写入数据库->' + json.dumps(self.jsonobj), "database.log")
            #热机时加速度有效值接口
            elif self.topic == 'Machineheat':
                '''
                {"CncId":"1",
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
                #全是必须值无省略值
                #生成插入的sql语句
                sqlstr = """
                insert into MACHINEHEAT (cncid, time, heatno, msavepower, xsavepower, ysavepower,
                                        zsavepower, bsavepower, vsavepower, msstdpower,
                                        xstdpower, ystdpower, zstdpower, bstdpower, vstdpower,
                                        msxaccelerationmax, msyaccelerationmax, mszaccelerationmax,
                                        msbaccelerationmax, xsxaccelerationmax, ysyaccelerationmax,
                                        msxvelocityrms, msyvelocityrms, mszvelocityrms, msbvelocityrms,
                                        xsxvelocityrms, ysyvelocityrms)
                values (:cncid, to_date(:time, 'YYYY-MM-DD HH24:MI:SS'),
                        :heatno, :msavepower, :xsavepower, :ysavepower,
                        :zsavepower, :bsavepower, :vsavepower, :msstdpower,
                        :xstdpower, :ystdpower, :zstdpower, :bstdpower, :vstdpower,
                        :msxaccelerationmax, :msyaccelerationmax, :mszaccelerationmax,
                        :msbaccelerationmax, :xsxaccelerationmax, :ysyaccelerationmax,
                        :msxvelocityrms, :msyvelocityrms, :mszvelocityrms, :msbvelocityrms,
                        :xsxvelocityrms, :ysyvelocityrms)
                """
                parameters = {'cncid':self.jsonobj['CncId'], 'time':self.jsonobj['Time'],
                            'heatno':self.jsonobj['HeatNO'], 'msavepower':self.jsonobj['MSAvePower'],
                            'xsavepower':self.jsonobj['XSAvePower'], 'ysavepower':self.jsonobj['YSAvePower'],
                            'zsavepower':self.jsonobj['ZSAvePower'], 'bsavepower':self.jsonobj['BSAvePower'],
                            'vsavepower':self.jsonobj['VSAvePower'], 'msstdpower':self.jsonobj['MSStdPower'],
                            'xstdpower':self.jsonobj['XStdPower'], 'ystdpower':self.jsonobj['YStdPower'],
                            'zstdpower':self.jsonobj['ZStdPower'], 'bstdpower':self.jsonobj['BStdPower'],
                            'vstdpower':self.jsonobj['VStdPower'], 'msxaccelerationmax':self.jsonobj['MSXAccelerationMax'],
                            'msyaccelerationmax':self.jsonobj['MSYAccelerationMax'], 'mszaccelerationmax':self.jsonobj['MSZAccelerationMax'],
                            'msbaccelerationmax':self.jsonobj['MSBAccelerationMax'], 'xsxaccelerationmax':self.jsonobj['XSXAccelerationMax'],
                            'ysyaccelerationmax':self.jsonobj['YSYAccelerationMax'], 'msxvelocityrms':self.jsonobj['MSXVelocityRMS'],
                            'msyvelocityrms':self.jsonobj['MSYVelocityRMS'], 'mszvelocityrms':self.jsonobj['MSZVelocityRMS'],
                            'msbvelocityrms':self.jsonobj['MSBVelocityRMS'], 'xsxvelocityrms':self.jsonobj['XSXVelocityRMS'],
                            'ysyvelocityrms':self.jsonobj['YSYVelocityRMS']}
                # 插入数据库
                self.oradb.insert(sqlstr, parameters)
                # print("热机时加速度有效值写入数据库:" + json.dumps(self.jsonobj))
                # logger.writeLog('热机时加速度有效值写入数据库->' + json.dumps(self.jsonobj), "database.log")
        
            #--------------------------机械手部分-----------------
            #机械手基础信息
            elif self.topic == 'JxsBasic':
                '''
                {"CncId":"1","CncNo":"111","PartNo":"222",
                "Coordinate":{"X":12.123,"Y":23.234,"Z":34.345},
                "Time":"2020-01-22 22:53:14"}
                '''
                # 给部分可选值设置默认值
                if 'Coordinate' not in self.jsonobj.keys():
                    self.jsonobj['Coordinate'] = {}
                #生成插入的sql语句
                sqlstr = """
                insert into BASIC_MACHINE_HAND (cncid, cncno, partno, x_axis, y_axis, z_axis, time)
                values (:cncid, :cncno, :partno, :x_axis, :y_axis, :z_axis, to_date(:time, 'YYYY-MM-DD HH24:MI:SS'))
                """
                parameters = {'cncid':self.jsonobj['CncId'], 'cncno':self.jsonobj['CncNo'],
                              'partno':self.jsonobj['PartNo'], 'x_axis':self.jsonobj['Coordinate']['X'],
                              'y_axis':self.jsonobj['Coordinate']['Y'],'z_axis':self.jsonobj['Coordinate']['Z'],
                              'time':self.jsonobj['Time']}
                # 插入数据库  
                self.oradb.insert(sqlstr, parameters)
                # print("机械手基础信息写入数据库:" + json.dumps(self.jsonobj))
                # logger.writeLog('机械手基础信息写入数据库->' + json.dumps(self.jsonobj), "database.log")
            
            #机械手振动接口
            elif self.topic == 'Jxsvibration':
                '''
               {"CncId":"1","LY+vibrationP":1.123,"LY-vibrationP":1.123,
                "RY+vibrationP":1.123,"RY-vibrationP":1.123,"LY+vibration":1.123,
                "LY-vibration":1.123,"RY+vibration":1.123,"RY-vibration":1.123,
                "Time":"2020-01-22 22:53:14"}
                '''
                #给部分可选值设置默认值
                
                #生成插入的sql语句
                sqlstr = """
                insert into machine_hand_vibration (cncid, ly_upper_vibrationp, ly_down_vibrationp,
                                          ry_upper_vibrationp, ry_down_vibrationp, 
                                          ly_upper_vibration, ly_down_vibration,
                                          ry_upper_vibration, ry_down_vibration, time)
                values (:cncid, :ly_upper_vibrationp, :ly_down_vibrationp,
                        :ry_upper_vibrationp, :ry_down_vibrationp, 
                        :ly_upper_vibration, :ly_down_vibration,
                        :ry_upper_vibration, :ry_down_vibration,
                        to_date(:time, 'YYYY-MM-DD HH24:MI:SS'))
                """
                parameters = {'cncid':self.jsonobj['CncId'],
                              'ly_upper_vibrationp': self.jsonobj['LY+vibrationP'],'ly_down_vibrationp': self.jsonobj['LY-vibrationP'],
                              'ry_upper_vibrationp': self.jsonobj['RY+vibrationP'],'ry_down_vibrationp': self.jsonobj['RY-vibrationP'],
                              'ly_upper_vibration': self.jsonobj['LY+vibration'],'ly_down_vibration': self.jsonobj['LY-vibration'],
                              'ry_upper_vibration': self.jsonobj['RY+vibration'],'ry_down_vibration': self.jsonobj['RY-vibration'],
                              'time':self.jsonobj['Time']}
                # 插入数据库  
                self.oradb.insert(sqlstr, parameters)
                # print("机械手震动信息写入数据库:" + json.dumps(self.jsonobj))
                # logger.writeLog('机械手震动信息写入数据库->' + json.dumps(self.jsonobj), "database.log")
            
            #机械手自检上传接口
            elif self.topic == 'JxsSelftest':
                '''
                {"CncId":"1","XAvePower":1,"Z1AvePower":1,"Z2AvePower":1,
                 "A1AvePower":1,"A2AvePower":1,"XStdPower":1,"Z1StdPower":1,
                 "Z2StdPower":1,"A1StdPower":1,"A2StdPower":1,
                 "Time":"2020-01-22 22:53:14"}
                '''
                #生成插入的sql语句
                sqlstr = """
                insert into MACHINE_HAND_SELFTEST (cncid, xavepower, z1avepower, z2avepower, 
                                        a1avepower, a2avepower, xstdpower, z1stdpower, 
                                        z2stdpower, a1stdpower, a2stdpower, time)
                values (:cncid, :xavepower, :z1avepower, :z2avepower, 
                        :a1avepower, :a2avepower, :xstdpower, :z1stdpower, 
                        :z2stdpower, :a1stdpower, :a2stdpower, 
                        to_date(:time, 'YYYY-MM-DD HH24:MI:SS'))
                """
                parameters = {'cncid':self.jsonobj['CncId'], 
                              'xavepower':self.jsonobj['XAvePower'],  
                              'z1avepower':self.jsonobj['Z1AvePower'],
                              'z2avepower':self.jsonobj['Z2AvePower'],
                              'a1avepower':self.jsonobj['A1AvePower'],
                              'a2avepower':self.jsonobj['A2AvePower'],
                              'xstdpower':self.jsonobj['XStdPower'],
                              'z1stdpower':self.jsonobj['Z1StdPower'],
                              'z2stdpower':self.jsonobj['Z2StdPower'],
                              'a1stdpower':self.jsonobj['A1StdPower'],
                              'a2stdpower':self.jsonobj['A2StdPower'],
                              'time':self.jsonobj['Time']}
                # 插入数据库  
                self.oradb.insert(sqlstr, parameters)
                # print("机械手自检写入数据库:" + json.dumps(self.jsonobj))
                # logger.writeLog('机械手自检写入数据库->' + json.dumps(self.jsonobj), "database.log")

            #--------------------------其他Transfer机床数据上传--------------------------
            elif self.topic == 'Transferdata':
                '''
                {"machineID":"1","vibration1":23.234,"vibration2":23.234,
                "vibration3":23.234,"vibration4":23.234,"vibration5":23.234,
                "vibration6":23.234,"Temp1":30.03,"Temp2":30.03,"Temp3":30.03,
                "Temp4":30.03,"Temp5":30.03,"Temp6":30.03,"Time":"2020-01-22 22:53:14"}
                '''
                # 给部分可选值设置默认值
                if 'vibration1' not in self.jsonobj.keys():
                    self.jsonobj['vibration1'] = 0.0
                if 'vibration2' not in self.jsonobj.keys():
                    self.jsonobj['vibration2'] = 0.0
                if 'vibration3' not in self.jsonobj.keys():
                    self.jsonobj['vibration3'] = 0.0
                if 'vibration4' not in self.jsonobj.keys():
                    self.jsonobj['vibration4'] = 0.0
                if 'vibration5' not in self.jsonobj.keys():
                    self.jsonobj['vibration5'] = 0.0
                if 'vibration6' not in self.jsonobj.keys():
                    self.jsonobj['vibration6'] = 0.0
                if 'Temp1' not in self.jsonobj.keys():
                    self.jsonobj['Temp1'] = 0.0
                if 'Temp2' not in self.jsonobj.keys():
                    self.jsonobj['Temp2'] = 0.0
                if 'Temp3' not in self.jsonobj.keys():
                    self.jsonobj['Temp3'] = 0.0
                if 'Temp4' not in self.jsonobj.keys():
                    self.jsonobj['Temp4'] = 0.0
                if 'Temp5' not in self.jsonobj.keys():
                    self.jsonobj['Temp5'] = 0.0
                if 'Temp6' not in self.jsonobj.keys():
                    self.jsonobj['Temp6'] = 0.0
                #生成插入的sql语句
                sqlstr = """
                insert into BASIC_OTHER_MACHINE (machine_id, time, vibration1, vibration2,
                                                vibration3, vibration4, vibration5,
                                                vibration6, temp1, temp2, temp3, temp4,
                                                temp5, temp6)
                values (:machine_id, to_date(:time, 'YYYY-MM-DD HH24:MI:SS'),
                        :vibration1, :vibration2, :vibration3, :vibration4,
                        :vibration5, :vibration6, :temp1, :temp2, :temp3, :temp4,
                        :temp5, :temp6)
                """
                parameters = {'machine_id':self.jsonobj['machineID'], 'time':self.jsonobj['Time'],
                            'vibration1':self.jsonobj['vibration1'],
                            'vibration2':self.jsonobj['vibration2'],
                            'vibration3':self.jsonobj['vibration3'],
                            'vibration4':self.jsonobj['vibration4'],
                            'vibration5':self.jsonobj['vibration5'],
                            'vibration6':self.jsonobj['vibration6'],
                            'temp1':self.jsonobj['Temp1'], 'temp2':self.jsonobj['Temp2'],
                            'temp3':self.jsonobj['Temp3'], 'temp4':self.jsonobj['Temp4'],
                            'temp5':self.jsonobj['Temp5'], 'temp6':self.jsonobj['Temp6'],
                            }
                # 插入数据库
                self.oradb.insert(sqlstr, parameters)
                # print("其他Transfer机床数据写入数据库:" + json.dumps(self.jsonobj))
                # logger.writeLog('其他Transfer机床数据写入数据库->' + json.dumps(self.jsonobj), "database.log")
            else:
                logger.writeLog("传入值异常，未找到匹配项!")
        except:
            errstr = traceback.format_exc()
            logger.writeLog("CNC字段解析程序失败:" + errstr + json.dumps(self.jsonobj))
        

