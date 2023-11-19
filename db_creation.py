import psycopg2
from execution_time import get_execution_time
import os
from dotenv.main import load_dotenv

# Доступ к БД через переменные среды
load_dotenv()


# В качестве БД postgresql 16, развернутый на виртуалке ubuntu в docker-контейнере (код запускаю с винды).
# Вероятно, время выполнения кода при запуске с линукса, как с основной системы, значительно уменьшится.
def get_db_connection():
    host = os.environ.get("HOST")
    dbname = os.environ.get("DBNAME")
    user = os.environ.get("USER")
    password = os.environ.get("PASSWORD")
    connection = psycopg2.connect(f'host={host} dbname={dbname} user={user} password={password}')
    return connection, connection.cursor()


conn, cursor = get_db_connection()


@get_execution_time
def fill_table_from_file(file_name: str):
    table_name = file_name.split(sep='.')[0]
    print(f'Заполнение таблицы {table_name}...')
    with open(file_name, 'r', encoding='utf-8') as f:
        next(f)
        # COPY одной транзакцией работает быстрее всего.
        # Пытался реализовать многопоточность, но, вероятно, из-за расходов на создание файлов работает медленнее.
        cursor.copy_expert(f"copy {table_name} from stdin (format csv)", f)
        conn.commit()


@get_execution_time
def create_db():
    cursor.execute('''
    drop table if exists t_branches;
    drop table if exists t_cities;
    drop table if exists t_products;
    drop table if exists t_sales;

    create table t_branches(
    id integer primary key,
    Ссылка text,
    Наименование text,
    Город text,
    КраткоеНаименование text,
    Регион text);

    create table t_cities(
    id integer primary key,
    Ссылка text,
    Наименование text);

    create table t_products(
    id integer primary key,
    Ссылка text,
    Наименование text);

    create table t_sales(
    id bigint primary key,
    Период timestamp,
    Филиал text,
    Номенклатура text,
    Количество float,
    Продажа float);
    ''')
    conn.commit()

    fill_table_from_file('t_branches.csv')
    fill_table_from_file('t_cities.csv')
    fill_table_from_file('t_products.csv')
    fill_table_from_file('t_sales.csv')

    # С индексами не пошло, создаются долго, время выполнения остается прежним :(
