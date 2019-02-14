# Сборка и тестирование
Для сборки:
```
go build cmd/main.go
./main
```
По умолчанию запускается в 1 поток и с конфигом conf.json. Для изменения есть параметры:

Параметр                              | Описание
---                                   | ---
--threads                             | Указание количества потоков
--config                              | Указание конфигурационного файла (в формате json)

Пример:
```
go build cmd/main.go
./main -threads=8 -config=custom_conf.json
```

Предварительно можно сгенерировать случайные данные в БД.
В ``<dev-dir>/exporter/internal/fill/fill.go`` можно менять схему таблиц, количество данных.

# Задание

Написать экспортер данных из postgres в csv. В каждом csv-файле должен быть хэдер, содержащий названия колонок в postgres. Все поля формата timestamp должны быть преобразованы в unixtime (целое число).


На вход через параметры командной строки подается два параметра:
- config - имя файла с конфигом (json/yml/...).
- threads - количество потоков загрузчика.

Конфиг должен содержать следующие параметры:
- Данные для коннекта в БД.
- Папка, в которую запишется результат.
- Массив объектов для выгрузки, каждый объект содержит:
- Название таблицы.
- Sql-запрос, который используется для выгрузки.
- Максимальное количество строк в одном файле.

Пример конфига (необязательно использовать точно такой формат и названия полей):

```
{
  "conn": "host=localhost user=postgres port=5432 dbname=some_db",
  "output_dir": "./result",
  "tables": [
    {
      "name": "people",
      "query": "select name, lastname, birthday from people",
      "max_lines": 100
    },
    {
      "name": "cities",
      "query": "select name, country from cities",
      "max_lines": 1000
    }
  ]
}
```

В результате работы программы создаются csv-файлы:

```
./result/people/000.csv
./result/people/001.csv
./result/people/002.csv
./result/cities/000.csv
./result/cities/001.csv
```

* Экспортер универсален и не зависит от структуры таблиц

Схема БД (можно добавить еще таблицы):
```
create table people (
  id serial not null primary key,
  name varchar(256) not null,
  lastname varchar(256) not null,
  birthday date not null,
  some_flag integer not null,
  created timestamp(0) not null default current_timestamp
) ;

create table cities (
  id serial not null primary key,
  name varchar(256) not null,
  country_id integer not null,
  created timestamp(0) not null default current_timestamp
) ;

create table countries (
  id serial not null primary key,
  name varchar(256) not null,
  created timestamp(0) not null default current_timestamp
) ;
```
