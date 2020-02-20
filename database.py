import json
import logger
import cx_Oracle as oracle
import yaml
import traceback


class DatabaseAdapter:
    orahost = None
    oraport= 1521
    oraservicename= None
    orauser= None
    orapassword= None
    connectionstr = None
    conn = None
    cursor = None

    def __init__(self):
        '''
        初始化，从配置文件读取服务器信息
        '''
        try:
            f = open("config.yaml","r+",encoding="utf-8")
            fstream = f.read()
            configobj = yaml.safe_load(fstream)
            self.orahost = configobj['oracle']['server']
            self.oraport = configobj['oracle']['port']
            self.oraservicename = configobj['oracle']['servicename']
            self.orauser = configobj['oracle']['user']
            self.orapassword = configobj['oracle']['password']
        except:
            logger.writeLog("读取数据库配置文件失败!")
    
    def oraconnect(self):
        '''
        连接数据库方法
        '''
        self.connectionstr = "%s/%s@%s/%s"%(self.orauser, self.orapassword, self.orahost, self.oraservicename)
        try:
            self.conn = oracle.connect(self.connectionstr,encoding="UTF-8")
            self.cursor = self.conn.cursor()
            return self.conn,self.cursor
        except:
            errstr = traceback.format_exc()
            logger.writeLog("Oracle数据库连接错误:" + errstr)

    def insert(self, sqlstr, para):
        '''
        数据库插入
        需要采用绑定变量的方式进行，否则会有安全问题
        '''
        # para = { dept_id=280, dept_name="Facility" }
        # cursor.execute("""
        # insert into departments (department_id, department_name)
        # values (:dept_id, :dept_name)""", data)
        try:
            if self.cursor:
                self.cursor.execute(sqlstr, para)
                self.conn.commit()
            else:
                # 可以进行重连尝试，未实现
                logger.writeLog("Oracle数据库插入失败:" + sqlstr + json.dumps(para))
        except:
            errstr = traceback.format_exc()
            logger.writeLog("Oracle数据库插入失败:" + errstr + sqlstr + json.dumps(para))
            

    def search(self, sqlstr, para=None):
        '''
        数据库查询
        '''
        try:
            if para == None:
                self.cursor.execute(sqlstr)
            else:
                self.cursor.execute(sqlstr,para)
            rows = self.cursor.fetchall()
            return rows
        except:
            logger.writeLog("Oracle数据库查询失败:" + sqlstr)
            return False

    def closeconn(self):
        '''
        关闭数据库连接
        '''
        try:
            self.cursor.close()
            self.conn.close()
        except:
            errstr = traceback.format_exc()
            logger.writeLog("Oracle数据库连接关闭错误:" + errstr)



if __name__ == "__main__":
    db = DatabaseAdapter()
    db.oraconnect()
    # jsonobj = {"Cncld":"1",
    #          "PingStr":"MDCisliving",
    #          "RunStatus":"0",
    #          "PoweronStatus":"0",
    #          "Alarm":"报警信息",
    #          "Time":"2020-02-19 16:00:14",
    #          "SpindleTemp":23.567,
    #          "EnvTemp":23.678,
    #          "CutfluTemp":23.789,
    #          "SliderTemp":23.891,
    #          "Coordinate":{"X":12.123,"Y":23.234,"Z":34.345}
    #         }
    # sqlstr = """
    #         insert into BASIC_MACHINE (cncid, pingstr, runstatus, poweronstatus, 
    #                                    alarm, time, spindletemp, envtemp, 
    #                                    cutflutemp, slidertemp, coordinate)
    #         values (:cncid, :pingstr, :runstatus, 
    #                 :poweronstatus, :alarm, to_date(:time, 'YYYY-MM-DD HH24:MI:SS'), 
    #                 :spindletemp, :envtemp, :cutflutemp, :slidertemp, :coordinate)
    #         """
    # parameters = {'cncid':jsonobj['Cncld'], 'pingstr':jsonobj['PingStr'], 
    #                       'runstatus':jsonobj['RunStatus'], 'poweronstatus':jsonobj['PoweronStatus'], 
    #                       'alarm':jsonobj['Alarm'], 'time':jsonobj['Time'], 
    #                       'spindletemp':jsonobj['SpindleTemp'], 'envtemp':jsonobj['EnvTemp'], 
    #                       'cutflutemp':jsonobj['CutfluTemp'], 'slidertemp':jsonobj['SliderTemp'], 
    #                       'coordinate':json.dumps(jsonobj['Coordinate'])}
    jsonobj = {"Cncld":"1",
             "Xvibration":1.123,
             "Yvibration":1.123,
             "Zvibration":1.123,
             "XvibrationP":1.123,
             "YvibrationP":1.123,
             "ZvibrationP":1.123,
             "Time":"2020-02-19 17:05:14"
            }
    sqlstr = """
            insert into MACHINE_VIBRATION (cncid, xvibration, yvibration, 
                                           zvibration, xvibrationp, yvibrationp,
                                           zvibrationp, time)
            values (:cncid, :xvibration, :yvibration, :zvibration, 
                    :xvibrationp, :yvibrationp, :zvibrationp,
                    to_date(:time, 'YYYY-MM-DD HH24:MI:SS'))
            """

    parameters = {'cncid':jsonobj['Cncld'], 'xvibration':jsonobj['Xvibration'], 
                          'yvibration':jsonobj['Yvibration'], 'zvibration':jsonobj['Zvibration'], 
                          'xvibrationp':jsonobj['XvibrationP'], 'yvibrationp':jsonobj['YvibrationP'],
                          'zvibrationp':jsonobj['ZvibrationP'],
                          'time':jsonobj['Time']}
    db.insert(sqlstr, parameters)

    sql = "SELECT * FROM MACHINE_VIBRATION"
    res = db.search(sql)
    print(res)
    db.closeconn()
