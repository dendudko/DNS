import psycopg2
from execution_time import get_execution_time
import os
from dotenv.main import load_dotenv
from concurrent import futures
import pandas as pd
import math

# Доступ к БД через переменные среды
load_dotenv()


# В качестве БД postgresql 16, развернутый на виртуалке ubuntu в docker-контейнере (код запускаю с винды).
# Вероятно, время выполнения кода при запуске с линукса, как с основной системы, значительно уменьшится.
def get_db_connection():
    host = os.environ.get("HOST")
    dbname = os.environ.get("DBNAME")
    user = os.environ.get("USER_NAME")
    password = os.environ.get("PASSWORD")
    connection = psycopg2.connect(f'host={host} dbname={dbname} user={user} password={password}')
    return connection, connection.cursor()


conn, cursor = get_db_connection()
conn.autocommit = True


# Метод для разбиения csv файла
@get_execution_time
def split_csv_file(file_name, chunk_size):
    print(f'Разбиение файла {file_name}...')
    # Дикий костыль - если кусок csv файла один, то записываем в файл мимо ProcessPoolExecutor-а (выигрыш пары секунд)
    number_of_chunks = math.ceil(sum(1 for _ in open(file_name, 'r', encoding='UTF-8')) / chunk_size)
    rows = pd.read_csv(file_name, chunksize=chunk_size)
    if number_of_chunks > 1:
        # Многопроцессорная запись csv файлов, работает быстрее, чем многопоточность
        with futures.ProcessPoolExecutor() as executor:
            [executor.submit(chunk.to_csv, f'./chunks/chunk_{i}_{file_name}', index=False, encoding='UTF-8',
                             header=False) for i, chunk in enumerate(rows)]
    else:
        for i, chunk in enumerate(rows):
            chunk.to_csv(f'./chunks/chunk_{i}_{file_name}', index=False, encoding='UTF-8', header=False)


def copy_expert_threading(cmd, file):
    local_conn, local_cursor = get_db_connection()
    with open(file, 'r', encoding='UTF-8') as f:
        local_cursor.copy_expert(cmd, f)
    local_conn.commit()
    local_conn.close()


@get_execution_time
def fill_table_from_file(file_name: str, one_file=False):
    table_name = file_name.split(sep='.')[0]
    print(f'Заполнение таблицы {table_name}...')
    # one_file - для обработки добавляемой таблицы calculation, для которой файл csv не разбивается
    if not one_file:
        files = ['./chunks/' + f for f in os.listdir('chunks') if file_name in f]
    else:
        files = [file_name]
    sql_cmd = f"copy {table_name} from stdin (format csv)"
    # Многопоточная загрузка данных в БД
    with futures.ThreadPoolExecutor() as executor:
        [executor.submit(copy_expert_threading, sql_cmd, file) for file in files]


# Создаю индексы для полей, которые могли бы быть внешними ключами.
# Создаются долго, зато запросы выполняются быстро :)
@get_execution_time
def create_indexes():
    print('Создание индексов...')
    cursor.execute('create index concurrently idx_t_sales_filial on t_sales("Филиал")')
    cursor.execute('create index concurrently idx_t_sales_nomenklatura on t_sales("Номенклатура")')
    cursor.execute('create index concurrently idx_t_branches_gorod on t_branches("Город")')


# Удаление доставки грузов (данных меньше, можно удалить условие из запросов -> скорость работы выше)
@get_execution_time
def delete_dostavka_gruzov():
    print('Удаление доставок грузов...')
    cursor.execute('''
        delete from t_sales s using t_products p where 
        s."Номенклатура" = p."Ссылка" 
        and (lower(p."Наименование") like '%грузов%'
        or lower(p."Наименование") like '%доставка%');
        delete from t_products where 
        lower("Наименование") like '%грузов%'
        or lower("Наименование") like '%доставка%';
        ''')


# Очистка или создание директории chunks
def clean_or_create_chunks_dir():
    folder_path = 'chunks'
    if os.path.exists(folder_path):
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
    else:
        os.mkdir('chunks')


@get_execution_time
def create_db():
    # Внешние ключи не создаю - сильно тормозят работу, целостность БД вроде не особо интересует
    cursor.execute('''
    drop table if exists t_branches;
    drop table if exists t_cities;
    drop table if exists t_products;
    drop table if exists t_sales;

    create table t_cities(
    id bigint,
    Ссылка text,
    Наименование text,
    primary key (Ссылка));
    
    create table t_products(
    id integer,
    Ссылка text,
    Наименование text,
    primary key (Ссылка));
    
    create table t_branches(
    id bigint,
    Ссылка text,
    Наименование text,
    Город text,
    КраткоеНаименование text,
    Регион text,
    primary key (Ссылка));

    create table t_sales(
    id bigint,
    Период timestamp,
    Филиал text,
    Номенклатура text,
    Количество float,
    Продажа float,
    primary key (id));
    ''')

    file_names = ['t_cities.csv', 't_products.csv', 't_branches.csv', 't_sales.csv']

    # 4 строки ниже необходимы при первом запуске или изменении размера chunks, иначе можно закомментировать
    clean_or_create_chunks_dir()
    for file_name in file_names:
        split_csv_file(file_name, 1000000)
    print()

    for file_name in file_names:
        fill_table_from_file(file_name)

    delete_dostavka_gruzov()
    create_indexes()
