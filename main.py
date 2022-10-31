
import json
import os
import opendatasets as od
from sqlalchemy import create_engine
import pandas as pd
import datetime
from datetime import timedelta, date, datetime
from sqlalchemy import inspect

today = date.today()

URL = "https://www.kaggle.com/noaa/hurricane-database"
name_file = 'atlantic.csv'
table_name = 'cyclones'
table_report = 'cyclones_history'
path = 'csv_file'

statys = {
        'TD':'Tropical cyclone of tropical depression intensity (< 34 knots)',
        'TS':'Tropical cyclone of tropical storm intensity (34-63 knots)',
        'HU':'Tropical cyclone of hurricane intensity (> 64 knots)',
        'EX':'Extratropical cyclone (of any intensity)',
        'SD':'Subtropical cyclone of subtropical depression intensity (< 34 knots)',
        'SS':'Subtropical cyclone of subtropical storm intensity (> 34 knots)',
        'LO':'A low that is neither a tropical cyclone, a subtropical cyclone, nor an extratropical cyclone (of any intensity)',
        'WV':'Tropical Wave (of any intensity)',
        'DB':'Disturbance (of any intensity)',
    }

def generete_range(start_date, finish_date=today):
    '''генерируем лист дат'''
    all_date = list(
        pd.date_range(start_date,finish_date-timedelta(days=1),freq='d'))
    all_date = [str(date).split(' ')[0] for date in all_date]
    l = len(all_date)
    print(f'cформирован лист из {l} дат, между {start_date} - {finish_date}')
    return all_date

def clear_date(param_data):
    '''получение нужного формата даты для SQL'''
    return param_data.replace('-', '')

def clear_dir_upload_file(path):
    '''очистка содержимого папки для формирования файлов'''
    if os.path.isdir(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            os.remove(file_path)
    else:
        os.makedirs(path)
    return print(f'директория `{path}` готова для создания файлоф .csv')

def get_file_indir(path):
    '''возвращает лист с файлами из директории'''
    list_file = []
    if os.path.isdir(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            list_file.append(file_path)
    l = len(list_file)
    print(f'получено `{l}` файлов .csv')
    return list_file

def get_user_conf(name_file):
    '''fet user param to conect in db'''
    with open(name_file, 'r') as f:
        conf = json.load(f)
    print(f'получены данные для подключения к SQL')
    return conf

def connect_sql(conf):
    '''connect on db'''
    DB = conf['DB']
    LOGIN = conf['LOGIN']
    PASSWORD = conf['PASSWORD']
    HOST = conf['HOST']
    PORT = conf['PORT']
    con_string = f"postgresql+psycopg2://{LOGIN}:{PASSWORD}@{HOST}:{PORT}/{DB}"
    con = create_engine(con_string)
    print(f'подключились к кабе `{DB}`')
    return con

def insert_data_from_kaggle(conn, name_file, table_name):
    '''загрузка данныих из kaggle в базу данных'''
    try:
        atlantic = pd.read_csv(f'./hurricane-database/{name_file}', header = 0)
        atlantic.to_sql(table_name, conn, if_exists='replace', index=False)
        print('успешно загрузили файлы из kaggle')
    except:
        print('error')

def get_row_on_filter_from_sql(conn, param_data):
    param_data = clear_date(param_data)
    # stmt = f'select * from {table_name} where "Date" >= {param_data}'
    stmt = f'''
        select a."ID", a."Date", "Status" from public.{table_name} as a
            join (
                select "ID", max("Time") as "Time", "Date"  from
                public.{table_name} where "Date" = {param_data}
                group by "ID", "Date"
            ) as b on a."ID" = b."ID" and a."Time" = b."Time" and a."Date" = b."Date"
    '''
    data = conn.execute(stmt).fetchall()
    return data

def write_csv(path, array, param_data):
    '''создание файла .csv'''
    count = len(array)
    for rec in array:
        string_line = ','.join([str(i) for i in rec])
        file_name = f'cyclones_{clear_date(param_data)}.csv'

        with open(f'./{path}/{file_name}', 'w') as f:
            f.write(string_line)
            
    return count

# формируем файл для кажой из дат
def createte_files(path, all_date):
    '''формируем файлы для кажой из дат'''
    all_count = 0
    for date in all_date:
        
        array = get_row_on_filter_from_sql(conn, param_data=date)
        count = write_csv(path, array, param_data=date)
        all_count +=count

    return print(f"создано: {all_count} файлов")

def read_csv(file):
    '''conver_csv_to_df'''
    df = pd.read_csv(file, header=None)
    df.columns = ['id', 'date_from', 'status']
    df['date_end'] = None
    df = df[['date_from', 'date_end', 'id', 'status']]
    return df

def check_excist_table(conn, table_report):
    '''проверяем наличие таблицы в базе'''
    insp = inspect(conn)
    if not table_report in insp.get_table_names():
        print(f'таблицы {table_report} в базе нет')
        return True
    else:
        print(f'таблица {table_report} в базе есть')
        return False

def check_table(conn, table_report, df):
    # проверка наличия таблицы
    if check_excist_table(conn, table_report):
        df.to_sql(table_report, conn, if_exists='append', index=False)
        print(f'создаем таблицу {table_report}')

def write_history(conn, table_report, csv):
    ''' проверяем наличие схожей записи в базу данных
    в случае повторной загрузки файла
    если нет то создаем и возвращаем количеыство записей в базу
    '''
    def minus_one_day(date_from):
        '''возвращает минус один день от ткущей даты в формате строки'''
        date_time_obj = datetime.strptime(str(date_from), '%Y%m%d') + timedelta(days=-1)
        return date_time_obj.strftime('%Y%m%d')

    def check_id_in_db(conn, table_report, id_, date_from, statys, ):
        '''проверка наличия записи в базе
        '''
        strm_1 = f'''
            select * from {table_report}
                where "id" = '{id_}'
            '''
        sdf = conn.execute(strm_1).fetchall()
        if len(sdf) == 0:
            df.to_sql(table_report, conn, if_exists='append', index=False)

    def check_dubl_write(conn, table_report, id_, date_from, statys, ):
        '''логика проверки повторной записи
            если есть повторная запись то удаляем ее 
            и у пред последней записи меняем поле дата
        '''
        strm_1 = f'''
            select * from {table_report}
                where "id" = '{id_}' and
                    "date_from" = {date_from} and
                    "date_end" is null and
                    "status" = '{statys}'
            '''
        sdf = conn.execute(strm_1).fetchall()

        if len(sdf) != 0:
            # если такая запись уже есть то просто удаляем ее
            # и после этого обновляем поле date_end у предыдущей записи 
            strm = f'''
            delete from {table_report}
                where "id" = '{id_}' and
                    "date_from" = {date_from} and
                    "date_end" is null and
                    "status" = '{statys}'
            '''
            sdf = conn.execute(strm)
            print(f'удаляем последнюю запись, которая появилась повторно с id {id_}')
            strm = f'''
            update cyclones_history
                set "date_end" = Null
                where concat("id", "date_end") = (select concat("id", max("date_end"))
                    from cyclones_history
                    where "id" = '{id_}' and "date_end" is not null
                    group by "id") 
            '''
            sdf = conn.execute(strm)
            print(f'обновляем предпоследнуюю запись с id {id_}')

    df = read_csv(csv)
    id_ = df['id'].iloc[0]
    date_from = df['date_from'].iloc[0]
    statys = df['status'].iloc[0]
    check_table(conn, table_report, df)

    check_dubl_write(conn, table_report, id_, date_from, statys, )
    check_id_in_db(conn, table_report, id_, date_from, statys, )
    # находим последний статус не равный статусу из файла и без даты окончания
    strm_2 = f'''
        select * from {table_report}
            where "id" = '{id_}' and
                "date_end" is null and
                "status" != '{statys}'
        '''
    sdf = len(conn.execute(strm_2).fetchall())
    if sdf != 0:
        # если появился новый статус то
        # обновляем дату у последней записи минус один день
        date_from = minus_one_day(date_from)
        strm_2 = f'''
        update {table_report}
        set "date_end" = {date_from}
            where "id" = '{id_}' and
                "date_end" is null and
                "status" != '{statys}'
        '''
        sdf = conn.execute(strm_2)
        # и добавляем новую запись
        df.to_sql(table_report, conn, if_exists='append', index=False)
        print(f'делаем запись {table_report}')

# connect to sql
conf = get_user_conf('conf.json')
conn = connect_sql(conf=conf)

# upload file and create db on sql
od.download(URL, force=True)
insert_data_from_kaggle(conn, name_file, table_name)
# date = ['2013-01-01']
# получаем лист с датами по которым надо получить данный
all_date = generete_range('2013-01-01', finish_date=today)

clear_dir_upload_file(path)
createte_files(path, all_date)

for file in get_file_indir(path):
    write_history(conn, table_report, file)

# при тестировании для очистки от дубликатов запускать два раза
# conn.execute('''
#     delete from public.cyclones_history
# where concat(id, date_from, date_end, status) in (
# select concat(id, date_from, date_end, status) from  public.cyclones_history
# group by id, date_from, date_end, status
# having count(*) > 1)
# ''')