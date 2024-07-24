'''
https://www.nalog.gov.ru/opendata/7707329152-revexp/
'''

import pandas as pd
import xml.etree.ElementTree as ET
import cx_Oracle, sys, time, os
from datetime import datetime
from sqlalchemy import create_engine
import oracledb as odb

time_data = time.time()

print('скрипт парсер XML -> DB')

def ParseFile(file_path):
    #file_path = '''c:\\work\\xmlprc\\VO_OTKRDAN_5_9965_9965_20231225_0455d691-7a8b-47fe-ba0c-1e29e7412e08.xml'''
    print('ParseFile begin' )
    xml_data = ''
    with open( file_path, 'r', encoding='utf8') as f:
        xml_data = f.read()
    print(len(xml_data))
    root = ET.fromstring(xml_data)
    print('root', root)
    rt = root.tag
    print('rt', rt)

    count = 0
    big_list = []
    for child in root:
        #print('child', child)
        #print('child.tag', child.tag)
        #print('child.attrib.get(ДатаСост)=', child.attrib.get('ДатаСост'))
        datasost = None
        if child.attrib.get('ДатаСост') is not None:
            datasost = datetime.strptime( child.attrib.get('ДатаСост'), '%d.%m.%Y')
        inn_ = None
        title_ = ''
        dohod_ = 0.0
        rashod_ = None
        for child2 in child:
            #print('child2', child2)
            #print('child2.tag', child2.tag)
            if child2.tag == 'СведНП':
                inn_ = child2.attrib.get('ИННЮЛ')
                title_ = child2.attrib.get('НаимОрг')
            if child2.tag == 'СведДохРасх':            
                dohod_ = int(float( child2.attrib.get('СумДоход') ) )       
                rashod_ = int( float( child2.attrib.get('СумРасход') ) )
                count += 1
        new_row = (datasost, inn_, title_, dohod_, rashod_)
        big_list.append(new_row)

    delta_time = round(time.time() - time_data, 1)/60
    #print('count= {}, d(t)={}'.format(count, delta_time))      

    with conn_chd.cursor() as cursor:
        cursor.executemany('insert into vvo_sved_dohod_rashod values (:1, :2, :3, :4, :5)', big_list)
    conn_chd.commit()
    print('ParseFile end' )    
    pass

odb.init_oracle_client()
User = 'SB_DMAS'
Psw = 'getaKod#240124'
Params = odb.ConnectParams(host='e30-scan.fc.uralsibbank.ru', port='1522', service_name = 'cdw.work')
conn_chd = odb.connect(user=User, password=Psw, params = Params)

directory = '''c:\\work\\xmlprc'''

for filename in os.listdir(directory):
    print(filename)
    file_extension = os.path.splitext(filename)[1]
    if file_extension != '.xml': continue
    filepath = os.path.join(directory, filename)
    if os.path.isfile(filepath):
        print(os.path.join(directory, filename))
        ParseFile(filepath)

conn_chd.close()        
        