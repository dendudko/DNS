import pandas as pd
import warnings
from db_creation import get_db_connection, create_db, fill_table_from_file
from execution_time import get_execution_time
import matplotlib.pyplot as plt
import sys
import numpy as np

# pandas-у не нравится psycopg2, ругается и работает
warnings.filterwarnings("ignore")
# Немного увеличиваю максимальную ширину столбца выводимых датафреймов
pd.options.display.max_colwidth = 75

# Для замены числа на строку в выводе
days = {1: 'Понедельник', 2: 'Вторник', 3: 'Среда', 4: 'Четверг',
        5: 'Пятница', 6: 'Суббота', 7: 'Воскресенье'}

conn, cursor = get_db_connection()

# Очевидно, писал код с принтами, перенаправляю вывод в файл, чтобы много не переделывать :)
sys.stdout = open('output.txt', 'wt', encoding='UTF-8')


# Отбивка дефисами для output-а
def make_line():
    return '\n' + '-' * 100


# По всем заданиям вопрос - считаются ли различные "доставки грузов" за товары?
# TODO - вернуть доставки грузов?)

# 10 первых магазинов по количеству продаж
@get_execution_time
def analytics_1_1():
    return pd.read_sql('''
        select b."Наименование", sum(s."Количество") as "Количество продаж"
        from t_sales s, t_branches b, t_products p
        where s."Филиал"=b."Ссылка" and s."Номенклатура"=p."Ссылка"
        and lower(b."Наименование") not like '%склад%'
        and lower(b."КраткоеНаименование") not like '%склад%'
        and lower(p."Наименование") not like '%грузов%'
        and lower(p."Наименование") not like '%доставка%'
        group by b."Наименование"
        order by "Количество продаж" desc limit 10;
        ''', conn)


# 10 первых складов по количеству продаж
@get_execution_time
def analytics_1_2():
    return pd.read_sql('''
        select b."Наименование", sum(s."Количество") as "Количество продаж"
        from t_sales s, t_branches b, t_products p
        where s."Филиал"=b."Ссылка" and s."Номенклатура"=p."Ссылка"
        and (lower(b."Наименование") like '%склад%'
        or lower(b."КраткоеНаименование") like '%склад%')
        and lower(p."Наименование") not like '%грузов%'
        and lower(p."Наименование") not like '%доставка%'
        group by b."Наименование"
        order by "Количество продаж" desc limit 10;
        ''', conn)


# 10 самых продаваемых товаров по складам
@get_execution_time
def analytics_1_3():
    return pd.read_sql('''
    select p."Наименование", sum(s."Количество") as "Количество продаж"
    from t_sales s, t_branches b, t_products p
    where s."Филиал"=b."Ссылка" and s."Номенклатура"=p."Ссылка"
    and (lower(b."Наименование") like '%склад%'
    or lower(b."КраткоеНаименование") like '%склад%')
    and lower(p."Наименование") not like '%грузов%'
    and lower(p."Наименование") not like '%доставка%'
    group by p."Наименование"
    order by "Количество продаж" desc limit 10;
    ''', conn)


# 10 самых продаваемых товаров по магазинам
@get_execution_time
def analytics_1_4():
    return pd.read_sql('''
    select p."Наименование", sum(s."Количество") as "Количество продаж"
    from t_sales s, t_branches b, t_products p
    where s."Филиал"=b."Ссылка" and s."Номенклатура"=p."Ссылка"
    and lower(b."Наименование") not like '%склад%'
    and lower(b."КраткоеНаименование") not like '%склад%'
    and lower(p."Наименование") not like '%грузов%'
    and lower(p."Наименование") not like '%доставка%'
    group by p."Наименование"
    order by "Количество продаж" desc limit 10;
    ''', conn)


# 10 городов, в которых больше всего продавалось товаров
@get_execution_time
def analytics_1_5():
    return pd.read_sql('''
    select c."Наименование", sum(s."Количество") as "Количество продаж"
    from t_sales s, t_branches b, t_products p, t_cities c
    where s."Филиал"=b."Ссылка" and s."Номенклатура"=p."Ссылка" and b."Город"=c."Ссылка"
    and lower(p."Наименование") not like '%грузов%'
    and lower(p."Наименование") not like '%доставка%'
    group by c."Наименование"
    order by "Количество продаж" desc limit 10;
    ''', conn)


# В какие часы и в какой день недели происходит максимальное количество продаж.
# Вывожу всего по максимуму, код выглядит страшно :)
@get_execution_time
def analytics_2():
    analytics_2_df = pd.read_sql('''
    select extract(hour from s."Период")::integer as "Час",
    extract(isodow from s."Период")::integer as "День недели",
    sum(s."Количество") as "Количество продаж"
    from t_sales s, t_products p
    where s."Номенклатура"=p."Ссылка"
    and lower(p."Наименование") not like '%грузов%'
    and lower(p."Наименование") not like '%доставка%'
    group by "Час", "День недели"
    order by "Количество продаж" desc;
    ''', conn)
    analytics_2_df_copy = analytics_2_df.copy(deep=True)
    analytics_2_df['День недели'] = analytics_2_df['День недели'].map(days)
    return str('\nТоп 10 часов в днях недели по количеству продаж\n' + analytics_2_df.head(10).to_string() + '\n\n' +
               'Топ часов по количеству продаж\n' + analytics_2_df.groupby(by='Час', as_index=False).sum().
               sort_values(by='Количество продаж', ascending=False, ignore_index=True).drop(columns=['День недели']).
               to_string() + '\n\n' +
               'Топ дней по количеству продаж\n' + analytics_2_df.groupby(by='День недели', as_index=False).sum().
               sort_values(by='Количество продаж', ascending=False, ignore_index=True).drop(columns=['Час']).
               to_string()), analytics_2_df_copy


# Графики количества продаж в каждом часе, и количества продаж по дням недели.
# Вывожу всего по максимуму, код выглядит страшно :)
@get_execution_time
def analytics_3(analytics_3_df: pd.DataFrame):
    analytics_3_df_1 = analytics_3_df.groupby(by='Час', as_index=False).sum()
    analytics_3_df_2 = analytics_3_df.groupby(by='День недели', as_index=False).sum()

    analytics_3_df = analytics_3_df.sort_values(by=['День недели', 'Час'], ignore_index=True)
    analytics_3_df['День недели'] = analytics_3_df['День недели'].map(days)
    fig, ax = plt.subplots(figsize=(14, 7))
    for day in analytics_3_df['День недели'].unique():
        day_data = analytics_3_df[analytics_3_df['День недели'] == day]
        ax.plot(day_data['Час'], day_data['Количество продаж'], label=day, marker='o', markersize=3)
    ax.set_xlabel('Часы дня')
    ax.set_ylabel('Количество продаж')
    ax.set_title('Количество продаж по дням недели и часам')
    ax.legend()
    plt.xticks(range(0, 24))
    plt.savefig('analytics_3_0.png')

    plt.figure(figsize=(10, 5))
    plt.plot(analytics_3_df_1['Час'], analytics_3_df_1['Количество продаж'], marker='o')
    plt.title(label='Количество продаж по часам')
    plt.xticks(range(0, 24))
    plt.savefig('analytics_3_1.png')

    plt.figure(figsize=(10, 5))
    plt.plot(analytics_3_df_2['День недели'].map(days), analytics_3_df_2['Количество продаж'], marker='o')
    plt.title(label='Количество продаж по дням недели')
    plt.savefig('analytics_3_2.png')

    return 'Графики сохранены в директории проекта'


# Расчетная часть
@get_execution_time
def calculations():
    df = pd.read_sql('''
        select s."Номенклатура", sum(s."Количество") as "Количество продаж" 
        from t_sales s, t_products p
        where s."Номенклатура"=p."Ссылка"
        and lower(p."Наименование") not like '%грузов%'
        and lower(p."Наименование") not like '%доставка%'
        group by s."Номенклатура"; 
        ''', conn)
    quantiles = df['Количество продаж'].quantile([.3, .9]).values
    df['КлассТовара'] = np.where(df['Количество продаж'] < quantiles[0], 'Наименее продаваемый',
                                 np.where(df['Количество продаж'] <= quantiles[1],
                                          'Средне продаваемый', 'Наиболее продаваемый'))
    df = df.drop(columns='Количество продаж')

    # Сохраняю результат в файл и пишу из него в БД
    df.to_csv('calculations.csv')
    cursor.execute('''
    drop table if exists calculations;
    create table calculations(
    id integer primary key,
    Номенклатура text,
    КлассТовара text);
    ''')
    conn.commit()
    fill_table_from_file('calculations.csv')

    return 'Результат сохранен в файл в директории проекта и в базу данных postgres'


if __name__ == '__main__':
    create_db()
    print(make_line().replace('\n', ''))

    print(analytics_1_1(), make_line())
    print(analytics_1_2(), make_line())
    print(analytics_1_3(), make_line())
    print(analytics_1_4(), make_line())
    print(analytics_1_5(), make_line())

    # В пунктах 2, 3 не совсем понял задание - вывожу всего по максимуму :)
    analytics_2_output, analytics_2_df_main = analytics_2()
    print(analytics_2_output, make_line())

    # Пункт 3 работает на данных из пункта 2
    print(analytics_3(analytics_2_df_main), make_line())

    print(calculations())
