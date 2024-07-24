'''
скрипт для скачивания token
'''
import sys, os
import pandas 
import requests
import urllib3
import xml.etree.ElementTree as ET
import oracledb as odb
#import sqlalchemy 
import time
from common_utils import myPrint

# from time import sleep
from datetime import datetime, date, timedelta

urllib3.disable_warnings()

PATH_TO_WRITE = 'c:\\work\\m_bfo_update\\output.log'

insert_query = '''insert into bfo_reports( "ИННЮЛ", "КПП", "ОКПО", "ДатаДок", "Период", "ОтчетГод", "ИдФайл", "НомКорр", \
"Баланс_Пассив_ДолгосрОбяз_СумОтч", \
"Баланс_Пассив_ДолгосрОбяз_ЗаемСредств_СумОтч", \
"Баланс_Пассив_ДолгосрОбяз_ОтложНалОбяз_СумОтч", \
"Баланс_Пассив_ДолгосрОбяз_ОценОбяз_СумОтч", \
"Баланс_Пассив_ДолгосрОбяз_ПрочОбяз_СумОтч", \
"Баланс_Пассив_КраткосрОбяз_СумОтч", \
"Баланс_Пассив_КраткосрОбяз_ЗаемСредств_СумОтч", \
"Баланс_Пассив_КраткосрОбяз_КредитЗадолж_СумОтч", \
"Баланс_Пассив_КраткосрОбяз_ДоходБудущ_СумОтч", \
"Баланс_Пассив_КраткосрОбяз_ОценОбяз_СумОтч", \
"Баланс_Пассив_КраткосрОбяз_ПрочОбяз_СумОтч", \

"Баланс_Актив_ВнеОбА_СумОтч", \
"Баланс_Актив_ОбА_СумОтч", \
"Баланс_Актив_ВнеОбА_ФинВлож_СумОтч",\
"Баланс_Актив_ОбА_ДенежнСр_СумОтч",\

"ФинРез_СебестПрод_СумОтч", \
"ФинРез_ВаловаяПрибыль_СумОтч", \
"ФинРез_Выруч_СумОтч", \
"ФинРез_Выруч_СумПред", \
"ФинРез_КомРасход_СумОтч", \
"ФинРез_РасхОбДеят_СумОтч", \
"ФинРез_УпрРасход_СумОтч", \
"ФинРез_ПроцПолуч_СумОтч", \
"ФинРез_ПроцУпл_СумОтч", \

"ДвижениеДен_ТекОпер_Платеж_НалогПриб_СумОтч", \
"ДвижениеДен_ТекОпер_Поступ_СумОтч", \
"ДвижениеДен_ФинОпер_Платеж_СумОтч", \
"ДвижениеДен_ФинОпер_СальдоФин_СумОтч", \
"ОтчетИзмКап_ДвиженКап_ОтчетГод_Кап31дек_Итог") \

values ( :ИННЮЛ, :КПП, :ОКПО, :ДатаДок, :Период, :ОтчетГод, :ИдФайл, :НомКорр, \
:Баланс_Пассив_ДолгосрОбяз_СумОтч, \
:Баланс_Пассив_ДолгосрОбяз_ЗаемСредств_СумОтч, \
:Баланс_Пассив_ДолгосрОбяз_ОтложНалОбяз_СумОтч, \
:Баланс_Пассив_ДолгосрОбяз_ОценОбяз_СумОтч, \
:Баланс_Пассив_ДолгосрОбяз_ПрочОбяз_СумОтч, \
:Баланс_Пассив_КраткосрОбяз_СумОтч, \
:Баланс_Пассив_КраткосрОбяз_ЗаемСредств_СумОтч, \
:Баланс_Пассив_КраткосрОбяз_КредитЗадолж_СумОтч, \
:Баланс_Пассив_КраткосрОбяз_ДоходБудущ_СумОтч, \
:Баланс_Пассив_КраткосрОбяз_ОценОбяз_СумОтч, \
:Баланс_Пассив_КраткосрОбяз_ПрочОбяз_СумОтч, \
:Баланс_Актив_ВнеОбА_СумОтч, \
:Баланс_Актив_ОбА_СумОтч, \
:Баланс_Актив_ВнеОбА_ФинВлож_СумОтч, \
:Баланс_Актив_ОбА_ДенежнСр_СумОтч, \
:ФинРез_СебестПрод_СумОтч, \
:ФинРез_ВаловаяПрибыль_СумОтч, \
:ФинРез_Выруч_СумОтч, \
:ФинРез_Выруч_СумПред, \
:ФинРез_КомРасход_СумОтч, \
:ФинРез_РасхОбДеят_СумОтч, \
:ФинРез_УпрРасход_СумОтч, \
:ФинРез_ПроцПолуч_СумОтч, \
:ФинРез_ПроцУпл_СумОтч, \
:ДвижениеДен_ТекОпер_Платеж_НалогПриб_СумОтч, \
:ДвижениеДен_ТекОпер_Поступ_СумОтч, \
:ДвижениеДен_ФинОпер_Платеж_СумОтч, \
:ДвижениеДен_ФинОпер_СальдоФин_СумОтч,\
:ОтчетИзмКап_ДвиженКап_ОтчетГод_Кап31дек_Итог)'''

error_amount = 0 
success_amount = 0
empty_amount = 0
size_amount = 0
inn_errors = []
getFile_Error = 0 #ошибка получения файла XML по токену
getFile_OK = 0 # успешное скачивание XML
insert_error = 0 #ошибка вставки данных в bfo_reports
insert_OK = 0 
ODB_error = 0 

def getConnDB(schema):
    ''' SB_DMAS / SB_DKA'''
    odb.init_oracle_client()
    if schema == 'SB_DMAS':
        User = 'SB_DMAS'
        Psw = 'getaKod#240124'
    elif schema == 'SB_DKA':
        User = 'OzhgibesovVV[SB_DKA]'
        Psw = 'powerBI'#os.getenv('pswchd')
    else: 
        return None    
    Params = odb.ConnectParams(host='e30-scan.fc.uralsibbank.ru', port='1522', service_name = 'cdw.work')
    conn_db = odb.connect(user=User, password=Psw, params = Params)
    myPrint(f'conn_db: {conn_db}', PATH_TO_WRITE)
    return conn_db

def get_token():
    url_main = 'https://api-bo.nalog.ru/oauth/token' #ссылка на получение токена авторизации
    header_main = {'Accept': '*/*', 'Authorization': 'Basic YXBpOjEyMzQ1Njc4OTA=', 'User-Agent': 'curl/7.82.0'} #настройки для запроса авторизации
    #data_main={'grant_type':'password','username':'otstavnovav@uralsib.ru','password':'Ddu!48Az'} #данные для авторизации    
    data_main={'grant_type':'password','username':'dementievmo@uralsib.ru','password':'Ddu!48Az'} #данные для авторизации        
    resp_main = requests.post(url_main, verify=False, headers=header_main, data = data_main) #запрос на получение токена авторизации
    tokenT0 = time.time()    
    token = None
    header = ''
    if resp_main.status_code != requests.codes.ok:
        myPrint(f'ошибка получения token', PATH_TO_WRITE)
    else:    
        token = resp_main.json()['access_token'] #получаем токен из ответа
        expires_in = resp_main.json()['expires_in']
        header = {'Authorization': 'Bearer '+token, 'User-Agent': 'curl/7.37.1'} #настройки для запроса списка файлов
    myPrint(f'token: {token}', PATH_TO_WRITE)
    myPrint(f'expires_in: {expires_in}', PATH_TO_WRITE)
    myPrint(f'tokenT0: {datetime.now().time()}', PATH_TO_WRITE)
    return (token, expires_in, tokenT0, header)    

def get_page_amount(report_year, token, size, header):
    url = 'https://api-bo.nalog.ru/api/v1/files/?period={}'.format(str(report_year)) #ссылка для получения списка файлов    
    data={'period': str(report_year), 'fileType': 'BFO', 'reportType': 'BFO_TKS', 'size': str(size)} 
    resp = requests.get(url, verify=False, headers=header, params = data) #запрос с проверкой на ошибку        
    totalPages = None    
    status = resp.status_code
    if status == requests.codes.ok:
        totalPages = resp.json()['totalPages']
    return (status, totalPages)          

def get_file_tokens(report_year, token, page, size, header):
    url = 'https://api-bo.nalog.ru/api/v1/files/?period={}'.format(str(report_year)) #ссылка для получения списка файлов    
    data = {'period': str(report_year), 'fileType': 'BFO', 'reportType': 'BFO_TKS', 'size': str(size), 'sort': ('uploadDate', 'DESC'), 'page': str(page)} #данные для запроса списка файлов    
    resp = requests.get(url, verify=False, headers=header, params = data) #запрос с проверкой на ошибку        
    return (resp.status_code, resp.json())          

def getFile(token, header):
    resp_file = ''
    try: 
        resp_file = requests.get('https://api-bo.nalog.ru/api/v1/files/' + token, verify=False, headers=header) #запрос на получение файла
        if resp_file.status_code == requests.codes.ok:
            if resp_file.text:
                if resp_file.apparent_encoding != 'windows-1251':
                    if resp_file.apparent_encoding == 'MacCyrillic':
                        resp_file.encoding = 'windows-1251'
                    if resp_file.apparent_encoding == 'KOI8-R':
                        resp_file.encoding = 'windows-1251'
                    if resp_file.apparent_encoding == 'mac_greek':
                        resp_file.encoding = 'windows-1251'
    except:
        myPrint(f'ошибка получения файла', PATH_TO_WRITE)
    return (resp_file.status_code, resp_file)    

### 
def SavePage2db(data, page):
    global error_amount, success_amount, empty_amount, size_amount, T0, conn_db
    # по всем токенам страницы
    for indx, iter in enumerate(data['content']): 
        id = iter['id']
        inn = iter['inn']        
        fileName = iter['fileName']        
        fileType = iter['fileType']        
        reportType = iter['reportType']        
        period = int(iter['period'])
        uploadDate = datetime.strptime( iter['uploadDate'], '%Y-%m-%d')
        token = iter['token']
        status = 'Токен получен'
        with conn_db.cursor() as cursor:
            success_amount += 1
            row = [id, inn, fileName, fileType, reportType, period, uploadDate, token, page, date.today(), status]                        
            cursor.execute('insert into bfo_tokens values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)', row)
            conn_db.commit()
### 

#
def get_NumKorr_byINN(cursor, inn: str, period: int)->int:
    cursor.execute('''select "НомКорр" from sb_dmas.bfo_reports where "ИННЮЛ"=:inn_param and "ОтчетГод"=:period_param order by 1 desc''', inn_param = inn, period_param = period)
    res = cursor.fetchone()
    if res:
        return res[0]
    else:
        return None    

# 
def parse_page(data, page, last_actual_date) :
    global error_amount, success_amount, empty_amount, size_amount, T0, conn_db, inn_errors
    global getFile_Error, getFile_OK, insert_error, insert_OK, ODB_error
    # по всем токенам страницы
    for indx, iter in enumerate(data['content']): 
        id = iter['id']
        inn = iter['inn']        
        fileName = iter['fileName']
        fileType = iter['fileType']        
        reportType = iter['reportType']        
        period = int(iter['period'])
        uploadDate = datetime.strptime( iter['uploadDate'], '%Y-%m-%d')
        file_token = iter['token']
        status = 'Токен получен'
        #my Print( 'inn={}, uploadDate={}'.format(inn, uploadDate) )

        # ищем старый скачанный токен 
        with conn_db.cursor() as cursor:
            uploadDate_old = getUploadDate(cursor, inn, period)
            myPrint( f'inn={inn}, uploadDate={uploadDate}, uploadDate_old={uploadDate_old}', PATH_TO_WRITE )
            if (not uploadDate_old) or (uploadDate_old < uploadDate):
                # загрузка нового XML 
                getFileStatus, fileXML = getFile(file_token, header)
                if getFileStatus != requests.codes.ok:  
                    getFile_Error += 1
                    myPrint(f'ошибка получения файла XML по file_token', PATH_TO_WRITE)                    
                    continue
                else:    
                    getFile_OK += 1
 
                # парсинг нового XML 
                XMLtext = (inn, period, fileXML.text)
                try:   
                    values = getDataFromXMLstring( XMLtext )
                except:
                    values = None        
                if not values:
                    myPrint(f'ошибка парсинга XML', PATH_TO_WRITE)
                else:    
                    try: 
                        # вставка новой записи
                        cursor.execute(insert_query, values)
                        conn_db.commit()
                        insert_OK += 1
                        myPrint(f'{inn} вставка новой записи в bfo_reports', PATH_TO_WRITE)    
                        date_doc_new = values['ДатаДок'] #date_doc_to_del = values['ДатаДок']                    
                        num_corr_new = values['НомКорр']
                    except:
                        myPrint(f'Ошибка вставки данных в bfo_reports. inn={inn}', PATH_TO_WRITE)
                        inn_errors.append(inn)
                        insert_error += 1
                        conn_db = getConnDB('SB_DMAS')
                        cursor = conn_db.cursor()
                    else:
                        try:
                            if uploadDate_old:
                                # удаление старого token 
                                # cursor.execute('delete from sb_dka.bfo_tokens where inn={} and period={} and uploadDate={}'.format(inn, period, uploadDate_old))                    
                                cursor.execute('''delete from sb_dka.bfo_tokens where inn=:inn_param and period=:period_param and uploadDate=:uploadDate_param''', inn_param=inn, period_param=period, uploadDate_param=uploadDate_old)
                                myPrint(f'{inn} удаление старого token ', PATH_TO_WRITE)

                            # вставка нового token
                            row = [id, inn, fileName, fileType, reportType, period, uploadDate, token, page, date.today(), status]                        
                            cursor.execute('insert into sb_dka.bfo_tokens values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)', row)

                            #if uploadDate_old:
                                # удаление старого bfo_report
                            #    cursor.execute('''delete from sb_dmas.bfo_reports where "ИННЮЛ"=:inn_param and "ОтчетГод"=:period_param and "НомКорр"!=:num_corr_param''', inn_param = inn, period_param = period, num_corr_param = num_corr_new )
                            #    myPrint(f'{inn} удаление старого bfo_reports', PATH_TO_WRITE)
                            success_amount += 1
                        finally:
                            conn_db.commit()

                    # if insert_error >= 50: break
# parse_page(data, page):                    

def INT(input: str) -> int:
    '''str -> int'''
    if input is not None:
        return int(input)
    else:
        return None    

def INT1000(input: str, mt=1000) -> int:
    '''str -> int'''
    if input is not None:
        return int(input)*mt
    else:
        return None    

def CreateEmptyDict() -> dict:
    '''создание пустого словаря'''
    res_dict = {}
    res_dict['ИННЮЛ'] = None #1 
    res_dict['ОтчетГод'] = None #2
    res_dict['КПП'] = None #3
    res_dict['ОКПО'] = None #4
    res_dict['ДатаДок'] = None #5
    res_dict['Период'] = None #6
    res_dict['ИдФайл'] =  None #7
    res_dict['НомКорр'] = None #8
    res_dict['Баланс_Пассив_ДолгосрОбяз_СумОтч'] = None #9
    res_dict['Баланс_Пассив_ДолгосрОбяз_ЗаемСредств_СумОтч'] = None  #10
    res_dict['Баланс_Пассив_ДолгосрОбяз_ОтложНалОбяз_СумОтч'] = None #11
    res_dict['Баланс_Пассив_ДолгосрОбяз_ОценОбяз_СумОтч'] = None #12
    res_dict['Баланс_Пассив_ДолгосрОбяз_ПрочОбяз_СумОтч'] = None #13
    res_dict['Баланс_Пассив_КраткосрОбяз_СумОтч'] = None #14
    res_dict['Баланс_Пассив_КраткосрОбяз_ЗаемСредств_СумОтч'] = None #15
    res_dict['Баланс_Пассив_КраткосрОбяз_КредитЗадолж_СумОтч'] = None #16
    res_dict['Баланс_Пассив_КраткосрОбяз_ДоходБудущ_СумОтч'] = None #17
    res_dict['Баланс_Пассив_КраткосрОбяз_ОценОбяз_СумОтч'] = None #18
    res_dict['Баланс_Пассив_КраткосрОбяз_ПрочОбяз_СумОтч'] = None #19
    res_dict['Баланс_Актив_ВнеОбА_СумОтч'] = None #20
    res_dict['Баланс_Актив_ОбА_СумОтч'] = None #21
    res_dict['Баланс_Актив_ВнеОбА_ФинВлож_СумОтч'] = None #22
    res_dict['Баланс_Актив_ОбА_ДенежнСр_СумОтч'] = None #23
    res_dict['ФинРез_СебестПрод_СумОтч'] = None #24
    res_dict['ФинРез_ВаловаяПрибыль_СумОтч'] = None #25
    res_dict['ФинРез_Выруч_СумОтч'] = None #26
    res_dict['ФинРез_Выруч_СумПред'] = None #27
    res_dict['ФинРез_КомРасход_СумОтч'] = None #28
    res_dict['ФинРез_РасхОбДеят_СумОтч'] = None #29
    res_dict['ФинРез_УпрРасход_СумОтч'] = None #30
    res_dict['ФинРез_ПроцПолуч_СумОтч'] = None #31
    res_dict['ФинРез_ПроцУпл_СумОтч'] = None #32
    res_dict['ДвижениеДен_ТекОпер_Платеж_НалогПриб_СумОтч'] = None #33
    res_dict['ДвижениеДен_ТекОпер_Поступ_СумОтч'] = None #34
    res_dict['ДвижениеДен_ФинОпер_Платеж_СумОтч'] = None #35
    res_dict['ДвижениеДен_ФинОпер_СальдоФин_СумОтч'] = None #36
    res_dict['ОтчетИзмКап_ДвиженКап_ОтчетГод_Кап31дек_Итог'] = None #37
    return res_dict

def getDataFromXMLstring(input_row):
    if not input_row:
        return None
    if len(input_row) < 2:
        return None
    try: 
        root = ET.fromstring(str(input_row[2]))
        res_dict = CreateEmptyDict()

        res_dict['ИННЮЛ'] = str(input_row[0])
        res_dict['ОтчетГод'] = INT(input_row[1]) #int(root[0].attrib['ОтчетГод'])

        if root.find('./Документ/СвНП/НПЮЛ') is not None:
            res_dict['КПП'] = root.find('./Документ/СвНП/НПЮЛ').get('КПП')
        res_dict['ОКПО'] = root.find('./Документ/СвНП').get('ОКПО')

        # datasost = datetime.strptime( child.attrib.get('ДатаСост'), '%d.%m.%Y')
        res_dict['ДатаДок'] = datetime.strptime( root.find('./Документ').get('ДатаДок'), '%d.%m.%Y')
        res_dict['Период'] = INT(root.find('./Документ').get('Период'))

        res_dict['ИдФайл'] =  root.attrib['ИдФайл']
        res_dict['НомКорр'] = root[0].attrib['НомКорр']
        
        if root.find('./Документ/Баланс/Пассив') is not None:
            if root.find('./Документ/Баланс/Пассив/ДолгосрОбяз') is not None:
                res_dict['Баланс_Пассив_ДолгосрОбяз_СумОтч'] = INT1000(root.find('./Документ/Баланс/Пассив/ДолгосрОбяз').get('СумОтч'))
                if root.find('./Документ/Баланс/Пассив/ДолгосрОбяз/ЗаемСредств') is not None:                    
                    res_dict['Баланс_Пассив_ДолгосрОбяз_ЗаемСредств_СумОтч'] =  INT1000(root.find('./Документ/Баланс/Пассив/ДолгосрОбяз/ЗаемСредств').get('СумОтч'))
                if root.find('./Документ/Баланс/Пассив/ДолгосрОбяз/ОтложНалОбяз') is not None:                    
                    res_dict['Баланс_Пассив_ДолгосрОбяз_ОтложНалОбяз_СумОтч'] = INT1000(root.find('./Документ/Баланс/Пассив/ДолгосрОбяз/ОтложНалОбяз').get('СумОтч'))
                if root.find('./Документ/Баланс/Пассив/ДолгосрОбяз/ОценОбяз') is not None:                
                    res_dict['Баланс_Пассив_ДолгосрОбяз_ОценОбяз_СумОтч'] = INT1000(root.find('./Документ/Баланс/Пассив/ДолгосрОбяз/ОценОбяз').get('СумОтч'))
                if root.find('./Документ/Баланс/Пассив/ДолгосрОбяз/ПрочОбяз') is not None:                
                    res_dict['Баланс_Пассив_ДолгосрОбяз_ПрочОбяз_СумОтч'] = INT1000(root.find('./Документ/Баланс/Пассив/ДолгосрОбяз/ПрочОбяз').get('СумОтч'))
            if root.find('./Документ/Баланс/Пассив/КраткосрОбяз') is not None:
                res_dict['Баланс_Пассив_КраткосрОбяз_СумОтч'] = INT1000(root.find('./Документ/Баланс/Пассив/КраткосрОбяз').get('СумОтч'))
                if root.find('./Документ/Баланс/Пассив/КраткосрОбяз/ЗаемСредств') is not None:                
                    res_dict['Баланс_Пассив_КраткосрОбяз_ЗаемСредств_СумОтч'] = INT1000(root.find('./Документ/Баланс/Пассив/КраткосрОбяз/ЗаемСредств').get('СумОтч'))
                if root.find('./Документ/Баланс/Пассив/КраткосрОбяз/КредитЗадолж') is not None:                
                    res_dict['Баланс_Пассив_КраткосрОбяз_КредитЗадолж_СумОтч'] = INT1000(root.find('./Документ/Баланс/Пассив/КраткосрОбяз/КредитЗадолж').get('СумОтч'))
                if root.find('./Документ/Баланс/Пассив/КраткосрОбяз/ДоходБудущ') is not None:                
                    res_dict['Баланс_Пассив_КраткосрОбяз_ДоходБудущ_СумОтч'] = INT1000(root.find('./Документ/Баланс/Пассив/КраткосрОбяз/ДоходБудущ').get('СумОтч'))
                if root.find('./Документ/Баланс/Пассив/КраткосрОбяз/ОценОбяз') is not None:                
                    res_dict['Баланс_Пассив_КраткосрОбяз_ОценОбяз_СумОтч'] = INT1000(root.find('./Документ/Баланс/Пассив/КраткосрОбяз/ОценОбяз').get('СумОтч'))
                if root.find('./Документ/Баланс/Пассив/КраткосрОбяз/ПрочОбяз') is not None:                
                    res_dict['Баланс_Пассив_КраткосрОбяз_ПрочОбяз_СумОтч'] = INT1000(root.find('./Документ/Баланс/Пассив/КраткосрОбяз/ПрочОбяз').get('СумОтч'))   

        if root.find('./Документ/Баланс/Актив') is not None:
            if root.find('./Документ/Баланс/Актив/ВнеОбА') is not None:
                res_dict['Баланс_Актив_ВнеОбА_СумОтч'] = INT1000(root.find('./Документ/Баланс/Актив/ВнеОбА').get('СумОтч'))     
            if root.find('./Документ/Баланс/Актив/ОбА') is not None:               
                res_dict['Баланс_Актив_ОбА_СумОтч'] = INT1000(root.find('./Документ/Баланс/Актив/ОбА').get('СумОтч'))   
            if root.find('./Документ/Баланс/Актив/ВнеОбА/ФинВлож') is not None:
                res_dict['Баланс_Актив_ВнеОбА_ФинВлож_СумОтч'] = INT1000(root.find('./Документ/Баланс/Актив/ВнеОбА/ФинВлож').get('СумОтч'))
            if root.find('./Документ/Баланс/Актив/ОбА/ДенежнСр') is not None:
                res_dict['Баланс_Актив_ОбА_ДенежнСр_СумОтч'] = INT1000(root.find('./Документ/Баланс/Актив//ОбА/ДенежнСр').get('СумОтч'))

        if root.find('./Документ/ФинРез') is not None:
            if root.find('./Документ/ФинРез/СебестПрод'):
                res_dict['ФинРез_СебестПрод_СумОтч'] = INT1000(root.find('./Документ/ФинРез/СебестПрод').get('СумОтч'))
            if root.find('./Документ/ФинРез/ВаловаяПрибыль') is not None:                
                res_dict['ФинРез_ВаловаяПрибыль_СумОтч'] = INT1000(root.find('./Документ/ФинРез/ВаловаяПрибыль').get('СумОтч'))
            if root.find('./Документ/ФинРез/Выруч') is not None:                
                res_dict['ФинРез_Выруч_СумОтч'] = INT1000(root.find('./Документ/ФинРез/Выруч').get('СумОтч') )
            if root.find('./Документ/ФинРез/Выруч') is not None:                
                res_dict['ФинРез_Выруч_СумПред'] = INT1000(root.find('./Документ/ФинРез/Выруч').get('СумПред'))
            if root.find('./Документ/ФинРез/КомРасход') is not None:
                res_dict['ФинРез_КомРасход_СумОтч'] = INT1000(root.find('./Документ/ФинРез/КомРасход').get('СумОтч'))
            if root.find('./Документ/ФинРез/РасхОбДеят') is not None:
                res_dict['ФинРез_РасхОбДеят_СумОтч'] = INT1000(root.find('./Документ/ФинРез/РасхОбДеят').get('СумОтч'))
            if root.find('./Документ/ФинРез/УпрРасход') is not None:
                res_dict['ФинРез_УпрРасход_СумОтч'] = INT1000(root.find('./Документ/ФинРез/УпрРасход').get('СумОтч'))
            if root.find('./Документ/ФинРез/ПроцПолуч') is not None:
                res_dict['ФинРез_ПроцПолуч_СумОтч'] = INT1000(root.find('./Документ/ФинРез/ПроцПолуч').get('СумОтч'))
            if root.find('./Документ/ФинРез/ПроцУпл') is not None:
                res_dict['ФинРез_ПроцУпл_СумОтч'] = INT1000(root.find('./Документ/ФинРез/ПроцУпл').get('СумОтч'))     

        if root.find('./Документ/ДвижениеДен') is not None:
            if root.find('./Документ/ДвижениеДен/ТекОпер/Платеж/НалогПриб') is not None:
                res_dict['ДвижениеДен_ТекОпер_Платеж_НалогПриб_СумОтч'] = INT1000(root.find('./Документ/ДвижениеДен/ТекОпер/Платеж/НалогПриб').get('СумОтч'))
            if root.find('./Документ/ДвижениеДен/ТекОпер/Поступ') is not None:               
                res_dict['ДвижениеДен_ТекОпер_Поступ_СумОтч'] = INT1000(root.find('./Документ/ДвижениеДен/ТекОпер/Поступ').get('СумОтч'))
            if root.find('./Документ/ДвижениеДен/ФинОпер/Платеж') is not None:
                res_dict['ДвижениеДен_ФинОпер_Платеж_СумОтч'] = INT1000(root.find('./Документ/ДвижениеДен/ФинОпер/Платеж').get('СумОтч'))
            if root.find('./Документ/ДвижениеДен/ФинОпер/СальдоФин') is not None:
                res_dict['ДвижениеДен_ФинОпер_СальдоФин_СумОтч'] = INT1000(root.find('./Документ/ДвижениеДен/ФинОпер/СальдоФин').get('СумОтч'))

        if root.find('./Документ/ОтчетИзмКап/ДвиженКап/ОтчетГод/Кап31дек') is not None:
            res_dict['ОтчетИзмКап_ДвиженКап_ОтчетГод_Кап31дек_Итог'] = INT1000(root.find('./Документ/ОтчетИзмКап/ДвиженКап/ОтчетГод/Кап31дек').get('Итог'))
   
    except Exception: 
        myPrint(f'Ошибка парсинга ИНН', PATH_TO_WRITE)        
        res_dict = None        
    return res_dict     
#getDataFromXMLstring

def getMaxDate(cursor):
    global REPORT_YEAR
    cursor.execute('select max(uploadDate) from sb_dka.bfo_tokens where period = {}'.format(REPORT_YEAR))
    res = cursor.fetchone()
    myPrint(f'max(uploadDate)={res[0]}', PATH_TO_WRITE)
    return res[0]

def getNumDublicates(cursor)->int:
    global REPORT_YEAR
    query = '''select count(*) from  (select "ИННЮЛ" from sb_dmas.bfo_reports where "ОтчетГод" = 2023 group by "ИННЮЛ" having count("ИННЮЛ") > 1)'''
    cursor.execute(query)
    res = cursor.fetchone()
    #myPrint(f'Number of Dublicates = {res[0]}')
    return int( res[0] )

def getUploadDate(cursor, inn, period):
    # cursor.execute('select max(uploadDate) from sb_dka.bfo_tokens  where period = {}'.format(REPORT_YEAR))
    try:
        cursor.execute('select uploadDate from sb_dka.bfo_tokens where  inn={} and period={} '.format(inn, period))    
        res = cursor.fetchone()
    except:
        myPrint(f'Ошибка в функции getUploadDate', PATH_TO_WRITE)
        myPrint(f'inn={inn}, uploadDate={res[0]}', PATH_TO_WRITE)        
        ODB_error += 1
        conn_db = getConnDB('SB_DMAS')
        cursor = conn_db.cursor()
        res = None
    finally:
        if res:
            # myPrint(f'inn={inn}, uploadDate={res[0]}', PATH_TO_WRITE)
            return res[0]
        else:
            return None    
                                 
# получение токена авторизации
#time_start = datetime.now().strftime('%H:%M')
token, expires, tokenT0, header = get_token()

REPORT_YEAR = 2023
SIZE = 5

# кол-во страниц 
status, totalPages = get_page_amount( REPORT_YEAR, token, SIZE, header)
if (status != requests.codes.ok) or (totalPages is None):
    myPrint(f'не удалось вычислить размер данных', PATH_TO_WRITE)
    pass
else:
    myPrint(f'totalPages: {totalPages}', PATH_TO_WRITE)

T0 = time.time()

odb.init_oracle_client()
User = 'SB_DMAS'
Psw = 'getaKod#240124'#
#User = 'OzhgibesovVV[SB_DKA]'
#Psw = os.getenv('pswchd')
Params = odb.ConnectParams(host='e30-scan.fc.uralsibbank.ru', port='1522', service_name = 'cdw.work')
conn_db = odb.connect(user=User, password=Psw, params = Params)
cursor = conn_db.cursor()

last_actual_date = getMaxDate(cursor)
if not last_actual_date:
    last_actual_date = date.today() - timedelta(days=30)
myPrint(f'last_actual_date={last_actual_date}', PATH_TO_WRITE)

# for page in range(1, totalPages+1):
try: 
    for page_number  in range( totalPages, totalPages-1000, -1 ):
        myPrint(f'page={page_number}', PATH_TO_WRITE)
        delta = time.time() - tokenT0
        if delta >= expires:
            (token, expires, tokenT0, header) = get_token()
        status, page_data  = get_file_tokens(REPORT_YEAR, token, page_number, SIZE, header) 
        delta_time = round((time.time() - T0)/60, 1)    
        myPrint(f'D(t)= {delta_time}, st.= {status}, page= {page_number}, errors {error_amount}', PATH_TO_WRITE)    
        if status == requests.codes.ok:
            parse_page( page_data, page_number, last_actual_date )        
finally:
    num_dublicates = getNumDublicates(cursor)
    myPrint(f'Кол-во дубликатов до удаления:{num_dublicates}', PATH_TO_WRITE )
    try:    
        cursor.execute('''delete from sb_dmas.bfo_reports where "ОтчетГод"=:par_1 and rowid in (select rowid from (select rowid, row_number() over (partition by "ИННЮЛ" order by "ДатаДок" desc, "НомКорр" desc ) as rn from sb_dmas.bfo_reports where "ОтчетГод"=:par_2) where rn > 1)''', par_1 = 2023, par_2 = 2023)
        myPrint(f'Дубликаты в bfo_reports удалены', PATH_TO_WRITE )    
        num_dublicates = getNumDublicates(cursor)
        myPrint(f'Кол-во дубликатов после удаления:{num_dublicates}', PATH_TO_WRITE )
    except:
        myPrint(f'Ошибка удаления дубликатов', PATH_TO_WRITE )
        #conn_db = getConnDB('SB_DMAS')
        #cursor = conn_db.cursor()
    finally: 
        conn_db.commit()    

delta_time = round((time.time() - T0)/60, 1)
myPrint( f'D(t)= {delta_time}', PATH_TO_WRITE )
time_now = datetime.now().strftime('%H:%M')
myPrint( f'время: {time_now}', PATH_TO_WRITE )
myPrint( f'success_amount= {success_amount}', PATH_TO_WRITE )
myPrint( f'GetFile_OK= {getFile_OK}',  PATH_TO_WRITE)
myPrint( f'GetFile_Error {getFile_Error}', PATH_TO_WRITE )
myPrint( f'insert_error= {insert_error}', PATH_TO_WRITE )
myPrint( f'insert_OK = {insert_OK}', PATH_TO_WRITE )
myPrint( f'ODB_error= {ODB_error}', PATH_TO_WRITE )
