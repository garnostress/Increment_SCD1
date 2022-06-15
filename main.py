#!/usr/bin/python3

import pandas as pd
import os
import fnmatch as fn
import datetime as DT
import jaydebeapi
import time

# Рабочие директории
mainpath = '/home/demipt2/trof/'  # рабочая директория на сервере
archive = '/home/demipt2/trof/archive/' # архивная директория на сервере

# Подключение к БД

conn = jaydebeapi.connect(
"oracle.jdbc.driver.OracleDriver",
"jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle",
["demipt2","peregrintook"],
"d:\sqldeveloper\jdbc\lib\ojdbc8.jar",)
conn.jconn.setAutoCommit(False)
curs = conn.cursor()
print('Соединение с базой данных установлено')


# 1. Очистка стейджингов
curs.execute("delete from demipt2.trof_stg_fct_pssprt_blcklst")
curs.execute("delete from demipt2.trof_stg_fact_transactions")
curs.execute("delete from demipt2.trof_stg_dim_terminals_1")
curs.execute("delete from demipt2.trof_stg_dim_terminals_2")
curs.execute("delete from demipt2.trof_del_terminals")
curs.execute("delete from demipt2.trof_stg_dim_cards")
curs.execute("delete from demipt2.trof_del_cards")
curs.execute("delete from demipt2.trof_stg_dim_accounts")
curs.execute("delete from demipt2.trof_del_accounts")
curs.execute("delete from demipt2.trof_stg_dim_clients")
curs.execute("delete from demipt2.trof_del_clients")
print('Очистка таблиц завершена')

# 2. Обработка таблиц фактов
# 2.1 Загрузка таблиц с паспортами в стеджинг
files_dates = {}
for file_name in os.listdir(mainpath):
    if fn.fnmatch(file_name, 'passport_blacklist_*.xlsx'): # поиск файлов с паспортами по маске
        files_dates[file_name] = file_name[19:27]   # выделяем дату из имени файла
for file, date in sorted(files_dates.items()): # сортируем по дате
    df = pd.read_excel(mainpath+file, sheet_name='blacklist', header=0, index_col=None)
    df = df[df.date == pd.to_datetime(DT.datetime.strptime(date, '%d%m%Y').date())] # извлечение строк которые соответствуют дате в имени файла
    df['date'] = df['date'].astype(str) # преобразование даты в строку для загрузки в Oracle
    curs.executemany("insert into demipt2.trof_stg_fct_pssprt_blcklst (entry_dt, passport_num) values( to_date(?, 'YYYY-MM-DD' ), ?)", df.values.tolist())
    os.rename(mainpath+file, mainpath+file + '.backup') # переименование файла
    os.replace(mainpath+file + '.backup', archive+file + '.backup') # перемещение файла в папку с архивами
print("Паспортные данные загружены в стейджинг")

# 2.2 Загрузка таблиц с транзакциями в стейджинг
files_dates = {}
for file_name in os.listdir(mainpath):
    if fn.fnmatch(file_name, 'transactions_*.txt'): # поиск файлов с транзакциями по маске
        files_dates[file_name] = file_name
for file, date in sorted(files_dates.items()): # сортировка по дате
    df = pd.read_csv(mainpath + file, sep=';', header=0, index_col=None, decimal=",")
    df['transaction_date'] = df['transaction_date'].astype(str)
    curs.executemany("insert into demipt2.trof_stg_fact_transactions(trans_id, trans_date, amt, card_num, oper_type,  oper_result, terminal) values(?, to_date(?,  'YYYY-MM-DD HH24:MI:SS'), ?, ?, ?, ?, ?)", df.values.tolist() )
    os.rename(mainpath + file, mainpath + file + '.backup')  # переименование файла
    os.replace(mainpath + file + '.backup', archive + file + '.backup')  # перемещение в папку с архивами
print("Данные о транзакциях загружены в стейджинг")

# 2.3 Загрузка данных из стейджинга в таргет
curs.execute("""
            insert into demipt2.trof_dwh_fact_pssprt_blcklst ( passport_num, entry_dt)
            select passport_num, entry_dt from demipt2.trof_stg_fct_pssprt_blcklst
            """)
curs.execute("""
            insert into demipt2.trof_dwh_fact_transactions ( trans_id, trans_date, amt, card_num, oper_type,  oper_result, terminal)
            select trans_id, trans_date, amt, card_num, oper_type,  oper_result, terminal from demipt2.trof_stg_fact_transactions
            """)
# 3. Обработка таблиц измерений
# 3.1 Загрузка данных в стейджинг
# Загружаем из файла таблицу с терминалами в формате SCD1
files_dates = {}
for file_name in os.listdir(mainpath):
    if fn.fnmatch(file_name, 'terminals_*.xlsx'): # поиск файлов с терминалами по маске
        files_dates[file_name] = file_name[10:18]   # выделяем дату create_dt из имени файла
for file, date in sorted(files_dates.items()): # сортируем по дате, для соблюдения историчности
    df = pd.read_excel(mainpath + file, sheet_name='terminals', header=0, index_col=None)  # грузим
    df['create_dt'] = (date[4:8]+'-'+ date[2:4]+'-'+date[0:2])# добавляем create_dt из имени файла для перевода в scd1
    df['create_dt'] = df['create_dt'].astype(str)
    df['update_dt'] = None
    curs.executemany("insert into demipt2.trof_stg_dim_terminals_1(terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt) values(?, ?, ?, ?, to_date(?, 'YYYY-MM-DD' ), ?)",df.values.tolist())
    os.rename(mainpath + file, mainpath + file + '.backup')  # переименование файла
    os.replace(mainpath + file + '.backup', archive + file + '.backup')  # перемещение в папку с архивами
print("Данные с терминалами загружены в стейджинг")

# Загрузка данных о терминалах в промежуточный стейджинг с учетом мета-данных
curs.execute(""" insert into demipt2.trof_stg_dim_terminals_2(terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt)
                    select terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt from demipt2.trof_stg_dim_terminals_1
                    where coalesce(update_dt, create_dt) >= (
                        select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
                    from demipt2.trof_meta_dim
                      where table_db = 'BANK' and table_name = 'TERMINALS' )
                   """)
# Загрузка данных о картах в стейджинг
curs.execute("""
                insert into demipt2.trof_stg_dim_cards ( card_num,account_num,create_dt,update_dt )
                select card_num,account,create_dt,update_dt from BANK.cards 
                where coalesce (update_dt,create_dt) >= (
                    select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
                    from demipt2.trof_meta_dim where table_db = 'BANK' and table_name = 'CARDS' )
                """)
# Загрузка данных об аккаунтах в стейджинг
curs.execute("""
                insert into demipt2.trof_stg_dim_accounts ( account_num, valid_to, client, create_dt,update_dt )
                select account, valid_to, client, create_dt,update_dt from BANK.accounts 
                where coalesce (update_dt,create_dt) >= (
                    select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
                    from demipt2.trof_meta_dim where table_db = 'BANK' and table_name = 'ACCOUNTS' )
                """)
# Загрузка данных о клиентах в стейджинг
curs.execute("""
                insert into demipt2.trof_stg_dim_clients ( client_id, last_name, first_name, patronymic,
                                                        date_of_birth, passport_num, passport_valid_to,
                                                        phone, create_dt,update_dt )
                select client_id, last_name, first_name, patronymic,
                       date_of_birth, passport_num, passport_valid_to,
                       phone, create_dt,update_dt from BANK.clients
                where coalesce (update_dt,create_dt) >= (
                    select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
                    from demipt2.trof_meta_dim where table_db = 'BANK' and table_name = 'CLIENTS' )
                  """)
# 3.2 Выделяем "вставки" и "обновления"  и загружаем их в приемник
curs.execute("""
                merge into demipt2.trof_dwh_dim_terminals tgt
                using (
                    select
                        s.terminal_id,
                        s.terminal_type,
                        s.terminal_city,
                        s.terminal_address,
                        s.create_dt,
                        s.update_dt
                    from demipt2.trof_stg_dim_terminals_2 s
                    left join demipt2.trof_dwh_dim_terminals t
                    on s.terminal_id = t.terminal_id
                    where
                        t.terminal_id is null or
                        ( t.terminal_id is not null and ( 1=0
                            or t.terminal_type <> s.terminal_type or ( s.terminal_type is null and t.terminal_type is not null ) or ( s.terminal_type is not null and t.terminal_type is null )
                            or t.terminal_city <> s.terminal_city or ( s.terminal_city is null and t.terminal_city is not null ) or ( s.terminal_city is not null and t.terminal_city is null )
                            or t.terminal_address <> s.terminal_address or ( s.terminal_address is null and t.terminal_address is not null ) or ( s.terminal_address is not null and t.terminal_address is null )
                                )
                        )
                ) stg
                on ( tgt.terminal_id = stg.terminal_id )
                when not matched then insert( terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt ) values ( stg.terminal_id, stg.terminal_type, stg.terminal_city, stg.terminal_address,  stg.create_dt, stg.update_dt )
                when matched then update set terminal_type = stg.terminal_type, terminal_city = stg.terminal_city, terminal_address = stg.terminal_address,  create_dt = stg.create_dt, update_dt = stg.update_dt
                """)
curs.execute("""
                merge into demipt2.trof_dwh_dim_cards tgt
                using (
                    select
                        s.card_num,
                        s.account_num,
                        s.create_dt,
                        s.update_dt
                    from demipt2.trof_stg_dim_cards s
                    left join demipt2.trof_dwh_dim_cards t
                    on s.card_num = t.card_num
                    where
                        t.card_num is null or
                        ( t.card_num is not null and ( 1=0
                            or t.account_num <> s.account_num or ( s.account_num is null and t.account_num is not null ) or ( s.account_num is not null and t.account_num is null )
                                )
                        )
                ) stg
                on ( tgt.card_num = stg.card_num )
                when not matched then insert( card_num, account_num, create_dt,update_dt ) values ( stg.card_num, stg.account_num, stg.create_dt, stg.update_dt )
                when matched then update set account_num = stg.account_num, create_dt = stg.create_dt, update_dt = stg.update_dt
                """)
curs.execute("""
                merge into demipt2.trof_dwh_dim_accounts tgt
                using (
                    select
                        s.account_num,
                        s.valid_to,
                        s.client,
                        s.create_dt,
                        s.update_dt
                    from demipt2.trof_stg_dim_accounts s
                    left join demipt2.trof_dwh_dim_accounts t
                    on s.account_num = t.account_num
                    where
                        t.account_num is null or
                        ( t.account_num is not null and ( 1=0
                            or t.valid_to <> s.valid_to or ( s.valid_to is null and t.valid_to is not null ) or ( s.valid_to is not null and t.valid_to is null )
                            or t.client <> s.client or ( s.client is null and t.client is not null ) or ( s.client is not null and t.client is null )
                        )
                        )
                ) stg
                on ( tgt.account_num = stg.account_num )
                when not matched then insert( account_num, valid_to, client, create_dt, update_dt ) values ( stg.account_num, stg.valid_to, stg.client, stg.create_dt, stg.update_dt )
                when matched then update set valid_to = stg.valid_to, client = stg.client, create_dt = stg.create_dt, update_dt = stg.update_dt
                """)
curs.execute("""
                merge into demipt2.trof_dwh_dim_clients tgt
                using (
                    select
                        s.client_id,
                        s.last_name,
                        s.first_name,
                        s.patronymic,
                        s.date_of_birth,
                        s.passport_num,
                        s.passport_valid_to,
                        s.phone,
                        s.create_dt,
                        s.update_dt
                    from demipt2.trof_stg_dim_clients s
                    left join demipt2.trof_dwh_dim_clients t
                    on s.client_id = t.client_id
                    where
                        t.client_id is null or
                        ( t.client_id is not null and ( 1=0
                            or t.last_name <> s.last_name or ( s.last_name is null and t.last_name is not null ) or ( s.last_name is not null and t.last_name is null )
                            or t.first_name <> s.first_name or ( s.first_name is null and t.first_name is not null ) or ( s.first_name is not null and t.first_name is null )
                            or t.patronymic <> s.patronymic or ( s.patronymic is null and t.patronymic is not null ) or ( s.patronymic is not null and t.patronymic is null )
                            or t.date_of_birth <> s.date_of_birth or ( s.date_of_birth is null and t.date_of_birth is not null ) or ( s.date_of_birth is not null and t.date_of_birth is null )
                            or t.passport_num <> s.passport_num or ( s.passport_num is null and t.passport_num is not null ) or ( s.passport_num is not null and t.passport_num is null )
                            or t.passport_valid_to <> s.passport_valid_to or ( s.passport_valid_to is null and t.passport_valid_to is not null ) or ( s.passport_valid_to is not null and t.passport_valid_to is null )
                            or t.phone <> s.phone or ( s.phone is null and t.phone is not null ) or ( s.phone is not null and t.phone is null )
                            )
                        )
                ) stg
                on ( tgt.client_id = stg.client_id )
                when not matched then insert( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt ) values ( stg.client_id, stg.last_name, stg.first_name,  stg.patronymic, stg.date_of_birth, stg.passport_num, stg.passport_valid_to, stg.phone, stg.create_dt, stg.update_dt )
                when matched then update set last_name = stg.last_name, first_name = stg.first_name, patronymic = stg.patronymic, date_of_birth = stg.date_of_birth, passport_num = stg.passport_num, passport_valid_to = stg.passport_valid_to,  phone = stg.phone, create_dt = stg.create_dt, update_dt = stg.update_dt
                 """)
# 3.3  Захват ключей для вычисления удалений
curs.execute("""
                insert into demipt2.trof_del_terminals ( terminal_id )
                select terminal_id from trof_stg_dim_terminals_2
                """)
curs.execute("""
                insert into demipt2.trof_del_cards ( card_num )
                select card_num from BANK.cards
                """)
curs.execute("""
                insert into demipt2.trof_del_accounts ( account_num )
                select account from BANK.accounts
                """)
curs.execute("""
                insert into demipt2.trof_del_clients ( client_id )
                select client_id from BANK.clients
                """)

#  3.4  Удаляем из приемника удаленные записи

curs.execute("""
                delete from demipt2.trof_dwh_dim_terminals
                where terminal_id in (
                    select
                        t.terminal_id
                    from demipt2.trof_dwh_dim_terminals t
                    left join demipt2.trof_del_terminals s
                    on t.terminal_id = s.terminal_id
                    where s.terminal_id is null 
                )
                """)
time.sleep(2)

curs.execute("""
                delete from demipt2.trof_dwh_dim_cards
                where card_num in (
                    select
                        t.card_num
                    from demipt2.trof_dwh_dim_cards t
                    left join demipt2.trof_del_cards s
                    on t.card_num = s.card_num
                    where s.card_num is null
                )
                """)
time.sleep(2)

curs.execute("""
                delete from demipt2.trof_dwh_dim_accounts
                where account_num in (
                    select
                        t.account_num
                    from demipt2.trof_dwh_dim_accounts t
                    left join demipt2.trof_del_accounts s
                    on t.account_num = s.account_num
                    where s.account_num is null
                )
                """)
time.sleep(2)

curs.execute("""
                delete from demipt2.trof_dwh_dim_clients
                where client_id in (
                    select
                        t.client_id
                    from demipt2.trof_dwh_dim_clients t
                    left join demipt2.trof_del_clients s
                    on t.client_id = s.client_id
                    where s.client_id is null
                )
                 """)
time.sleep(2)
# 6. Обновляем метаданные

curs.execute(""" 
                update demipt2.trof_meta_dim
                set last_update_dt = ( select max(coalesce (update_dt,create_dt)) from demipt2.trof_stg_dim_terminals_2 )
                where table_db = 'BANK' and table_name = 'TERMINALS' and ( select max(coalesce (update_dt,create_dt)) 
                from demipt2.trof_stg_dim_terminals_2 ) is not null
                """)

curs.execute(""" 
                update demipt2.trof_meta_dim
                set last_update_dt = ( select max(coalesce (update_dt,create_dt)) from demipt2.trof_stg_dim_cards )
                where table_db = 'BANK' and table_name = 'CARDS' and ( select max(coalesce (update_dt,create_dt)) from demipt2.trof_stg_dim_cards ) is not null
                """)

curs.execute(""" 
                update demipt2.trof_meta_dim
                set last_update_dt = ( select max(coalesce (update_dt,create_dt)) from demipt2.trof_stg_dim_accounts )
                where table_db = 'BANK' and table_name = 'ACCOUNTS' and ( select max(coalesce (update_dt,create_dt)) from demipt2.trof_stg_dim_accounts ) is not null
                """)

curs.execute(""" 
                update demipt2.trof_meta_dim
                set last_update_dt = ( select max(coalesce (update_dt,create_dt)) from demipt2.trof_stg_dim_clients )
                where table_db = 'BANK' and table_name = 'CLIENTS' and ( select max(coalesce (update_dt,create_dt)) from demipt2.trof_stg_dim_clients ) is not null
                """)
# 7. Совершение операции при просроченном или заблокированном паспорте
# 7.1 создаем представление паспортов из черного списка и просроченных
curs.execute(""" 
                create view demipt2.trof_rep_fraud_view_p as
                with trof_temp_table as (
                    select 
                           cl.last_name,
                           cl.first_name,
                           cl.patronymic,
                           cl.phone,
                           cl.passport_num,
                           cl.passport_valid_to,
                           tr.trans_date,
                           ac.account_num,
                           cr.card_num,
                           tr.trans_id,
                           tr.oper_type,
                           tr.oper_result
                    from demipt2.trof_dwh_dim_clients cl
                             left join demipt2.trof_dwh_dim_accounts ac
                             on cl.client_id = ac.client
                             left join demipt2.trof_dwh_dim_cards cr
                             on ac.account_num = cr.account_num
                             left join demipt2.trof_dwh_fact_transactions tr
                             on rtrim(cr.card_num) = tr.card_num and tr.oper_type = 'PAYMENT' and oper_result = 'SUCCESS'
                    where cl.passport_valid_to < tr.trans_date
                    union
                    select 
                           cl.last_name,
                           cl.first_name,
                           cl.patronymic,
                           cl.phone,
                           cl.passport_num,
                           cl.passport_valid_to,
                           tr.trans_date,
                           ac.account_num,
                           cr.card_num,
                           tr.trans_id,
                           tr.oper_type,
                           tr.oper_result
                    from demipt2.trof_dwh_dim_clients cl
                             left join demipt2.trof_dwh_dim_accounts ac
                                       on cl.client_id = ac.client
                             left join demipt2.trof_dwh_dim_cards cr
                                       on ac.account_num = cr.account_num
                             left join demipt2.trof_dwh_fact_transactions tr
                                       on rtrim(cr.card_num) = tr.card_num and tr.oper_type = 'PAYMENT' and oper_result = 'SUCCESS'
                    where passport_num in (select passport_num from demipt2.trof_dwh_fact_pssprt_blcklst)
                )
                select 
                       trans_date as  event_dt,
                       passport_num as passport,
                       last_name || ' ' || first_name || ' ' || patronymic as fio,
                       phone,
                       '1' as event_type,
                       TO_DATE(TO_CHAR(trans_date, 'DD-MM-YYYY'), 'DD-MM-YYYY') + interval '1 0:13:00' day to second as report_dt
                from trof_temp_table
                """)
# Добавляем отчет в витрину данных
curs.execute("""
                insert into demipt2.trof_rep_fraud
                select * from demipt2.trof_rep_fraud_view_p
                """)
# Очистка представления
curs.execute("drop view demipt2.trof_rep_fraud_view_p ")
# 7.2 Совершение операции при недействующем договоре
curs.execute("""
                create view demipt2.trof_rep_fraud_view_acc as
                    with trof_temp_table2 as (
                        select
                               cl.last_name,
                               cl.first_name,
                               cl.patronymic,
                               cl.phone,
                               cl.passport_num,
                               ac.account_num,
                               ac.valid_to,
                               tr.trans_date,
                               tr.trans_id,
                               tr.oper_type,
                               tr.oper_result
                        from demipt2.trof_dwh_dim_clients cl
                                 left join demipt2.trof_dwh_dim_accounts ac
                                 on cl.client_id = ac.client
                                 left join demipt2.trof_dwh_dim_cards cr
                                 on ac.account_num = cr.account_num
                                 left join demipt2.trof_dwh_fact_transactions tr
                                 on rtrim(cr.card_num) = tr.card_num and tr.oper_type = 'PAYMENT' and oper_result = 'SUCCESS' 
                                    where ac.valid_to < tr.trans_date
                    )
                select 
                       trans_date as  event_dt,
                       passport_num as passport,
                       last_name || ' ' || first_name || ' ' || patronymic as fio,
                       phone,
                       '2' as event_type,
                       TO_DATE(TO_CHAR(trans_date, 'DD-MM-YYYY'), 'DD-MM-YYYY') + interval '1 0:13:00' day to second as report_dt
                from trof_temp_table2
                """)
#  Добавляем данные в витрину данных
curs.execute("""
                insert into demipt2.trof_rep_fraud
                select * from demipt2.trof_rep_fraud_view_acc
                """)
#  Очистка представления
curs.execute("drop view demipt2.trof_rep_fraud_view_acc")



#  Фиксация транзакции и закрытие соединения с базой данных
conn.commit()
curs.close()
conn.close()
print("Скрипт отработал успешно")
