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
                #进行重连
                logger.writeLog("Oracle数据库尝试重新连接","insertfail.log") 
                self.oraconnect()
                if self.cursor:
                    self.cursor.execute(sqlstr, para)
                    self.conn.commit()
                else:
                    logger.writeLog("Oracle数据库重连插入失败:" + sqlstr + json.dumps(para),"insertfail.log")
        except:
            errstr = traceback.format_exc()
            logger.writeLog("Oracle数据库插入失败:" + errstr + sqlstr + json.dumps(para),"insertfail.log")
            

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
    # db.oraconnect()
    # 测试验证
    jsonobj = {"machineID":"1","vibration1":23.234,"vibration2":23.234,
             "vibration3":23.234,"vibration4":23.234,"vibration5":23.234,
             "vibration6":23.234,"Temp1":30.03,"Temp2":30.03,"Temp3":30.03,
             "Temp4":30.03,"Temp5":30.03,"Temp6":30.03,"Time":"2020-02-20 15:53:14"}
    
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
    parameters = {'machine_id':jsonobj['machineID'], 'time':jsonobj['Time'], 
                          'vibration1':jsonobj['vibration1'],
                          'vibration2':jsonobj['vibration2'],
                          'vibration3':jsonobj['vibration3'], 
                          'vibration4':jsonobj['vibration4'],
                          'vibration5':jsonobj['vibration5'],
                          'vibration6':jsonobj['vibration6'],
                          'temp1':jsonobj['Temp1'], 'temp2':jsonobj['Temp2'],
                          'temp3':jsonobj['Temp3'], 'temp4':jsonobj['Temp4'],
                          'temp5':jsonobj['Temp5'], 'temp6':jsonobj['Temp6'],
                          }
    db.insert(sqlstr,parameters)

    sql = "SELECT * FROM BASIC_OTHER_MACHINE"
    res = db.search(sql)
    print(res)
    db.closeconn()
