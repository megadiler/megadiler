import sys
import pandas as pd
import cx_Oracle
import time
import warnings
from sqlalchemy import create_engine
from datetime import datetime

warnings.filterwarnings("ignore")

def getConnectionToSandbox():
    # соединение с БД 
    connection = cx_Oracle.connect('KBB_MNG_BUS', 'F0z4Db', cx_Oracle.makedsn('msk-ds01dwh.fc.uralsibbank.ru', 1521, None, 'sandbox.prod.msk.usb'))
    if connection == None :
        myPrint('Проблема. Не удалось установить соединение c БД Sandbox')
        sys.exit()
    else :
        myPrint('Cоединение c БД Sandbox установлено')
    return connection    

def getData(query, connect):
    data = pd.read_sql(query, con = connect)    
    return data

def sendDataFrameToDB(input_df, table_name, time_data, df_size = 64, chunksize_ = 128):
    dsn_tns = cx_Oracle.makedsn('msk-ds01dwh', '1521', service_name='sandbox.prod.msk.usb') #настройки TNS
    cstr = 'oracle://{user}:{password}@{sid}'.format(user='KBB_MNG_BUS', password='F0z4Db', sid=dsn_tns) #настройки подключения к базе
    engine =  create_engine(cstr, convert_unicode=False, pool_recycle=10, pool_size=50, echo=False, hide_parameters=True) #настройки подключения к базе

    row_num = input_df.shape[0]
    #row_num = 25
    size = df_size
    start = 0
    run_write = True

    while run_write:
        end = start + size
        if end >= row_num:
            end = row_num
            run_write = False
        tmp_df = input_df.iloc[start : end]
        res = tmp_df.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize = chunksize_)
        delta_time = round( (time.time() - time_data)/60, 2)
        if 0 == start % (100*size):
            print('t= {:.2f}, start= {}, end= {}, res = {}'.format(delta_time, start, end, res))
        start += size
        
        #TODO
        # wlimit = 50000
        # if start > wlimit: 
        #     myPrint('Write limit = {}'.format(wlimit))
        #     break
        
def DropTable(dbcon, tablename): 
    dbcur = dbcon.cursor()
    try: 
        dbcur.execute('SELECT * FROM {}'.format(tablename))
        dbcur.execute('drop table {}'.format(tablename))
        print('Table dropped')
        return True
    except cx_Oracle.DatabaseError as e:
        x = e.args[0]
        if x.code == 942: 
            return False
        else:
            raise e
    finally: 
        dbcur.close()                

def IsDayOff():
    return datetime.today().weekday() in [6, 7]

def myPrint(text, path_to_write = 'c:\\work\\output.log'):
    dt = datetime.now().strftime('[%d.%m.%Y %H:%M:%S]')
    output_str = '{}: {}'.format(dt, text)
    print(output_str)
    with open(path_to_write, 'a')  as output_f:
        print(output_str, file = output_f)
