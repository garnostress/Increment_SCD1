# Increment_SCD1
Инкрементальная загрузка данных в DWH хранилище.

исполняемый файл - main.py
директория src/ - директория с файлами для загрузки терминалов, транзакций, паспортов
директория archive/ - директория в которую перемещаются текстовые файлы после загрузки в БД
директория sql_scripts/ - директория с sql-файлом создания DDL-структуры БД. не связана с main.py запуск в ручном режиме.
директория py_scripts/ - директория с pyton-скриптом очистки всех таблиц и инициации мета-таблиц БД. не связана с main.py запуск в ручном режиме.

в БД загружены файлы за три дня 
