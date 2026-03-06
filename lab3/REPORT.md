# Теоретический отчет: MPP-архитектура и моделирование данных в GreenPlum

## Оглавление

1. [Что такое MPP и зачем нужен GreenPlum](#1-что-такое-mpp-и-зачем-нужен-greenplum)
2. [Архитектура GreenPlum: Master и Segments](#2-архитектура-greenplum-master-и-segments)
3. [Docker-кластер: как устроен наш docker-compose](#3-docker-кластер-как-устроен-наш-docker-compose)
4. [Ключи дистрибьюции (Distribution Keys)](#4-ключи-дистрибьюции-distribution-keys)
5. [PXF: Platform eXtension Framework](#5-pxf-platform-extension-framework)
6. [Типы Motion (пересылка данных между сегментами)](#6-типы-motion-пересылка-данных-между-сегментами)
7. [EXPLAIN ANALYZE: чтение плана запроса](#7-explain-analyze-чтение-плана-запроса)
8. [gpfdist: параллельная загрузка из CSV](#8-gpfdist-параллельная-загрузка-из-csv)
9. [Датасет и ER-диаграмма](#9-датасет-и-er-диаграмма)
10. [Результаты анализа распределения](#10-результаты-анализа-распределения)
11. [Результаты запросов и планов](#11-результаты-запросов-и-планов)
12. [Вопросы для защиты](#12-вопросы-для-защиты)

---

## 1. Что такое MPP и зачем нужен GreenPlum

**MPP (Massively Parallel Processing)** -- архитектура, где данные и вычисления распределены между множеством независимых узлов (сегментов), работающих параллельно.

**Отличие от обычного PostgreSQL:**

| Характеристика | PostgreSQL | GreenPlum |
|---------------|-----------|-----------|
| Хранение данных | Один сервер | Распределено по сегментам |
| Выполнение запроса | Одно ядро/процесс | Параллельно на всех сегментах |
| Масштабирование | Вертикальное (мощнее сервер) | Горизонтальное (больше сегментов) |
| Оптимально для | OLTP (много мелких запросов) | OLAP (аналитика, большие данные) |

**GreenPlum** -- это форк PostgreSQL, расширенный до MPP. Он совместим с PostgreSQL по SQL-синтаксису, но под капотом распределяет данные и выполняет запросы параллельно.

**Shared-Nothing архитектура:** каждый сегмент имеет свой CPU, RAM, диск. Сегменты не разделяют ресурсы -- общаются только по сети. Это позволяет линейно масштабироваться.

---

## 2. Архитектура GreenPlum: Master и Segments

```
                    +------------------+
   Клиент (psql)-->|   MASTER NODE    |
                    |  (координатор)   |
                    |  - парсит SQL    |
                    |  - строит план   |
                    |  - НЕ хранит     |
                    |    пользов. данные|
                    +--------+---------+
                             |
                    Interconnect (сеть)
                             |
                +------------+------------+
                |                         |
        +-------v-------+        +-------v-------+
        |   SEGMENT 1   |        |   SEGMENT 2   |
        | - хранит часть |        | - хранит часть |
        |   данных       |        |   данных       |
        | - выполняет    |        | - выполняет    |
        |   свою часть   |        |   свою часть   |
        |   запроса      |        |   запроса      |
        +----------------+        +----------------+
```

### Master Node (координатор)
- Принимает SQL-запросы от клиентов
- Парсит SQL, строит план выполнения (Query Plan)
- Распределяет задачи по сегментам
- Собирает результаты (Gather Motion)
- **НЕ хранит пользовательские данные** (только системный каталог)

### Segment Nodes (рабочие узлы)
- Хранят свою **порцию** данных (определяется ключом дистрибьюции)
- Выполняют свою часть запроса параллельно
- Обмениваются данными при необходимости (Motion)

В нашем кластере: **1 master + 2 segments** (по заданию).

### Проверка кластера

```sql
SELECT * FROM gp_segment_configuration;
```

Результат:
```
 dbid | content | role | hostname | port | datadir
------+---------+------+----------+------+-------------------------
    1 |      -1 | p    | master   | 5432 | /data/master/gpseg-1     -- master
    2 |       0 | p    | segment1 | 6000 | /data/00/primary/gpseg0  -- segment 1
    3 |       1 | p    | segment2 | 6000 | /data/00/primary/gpseg1  -- segment 2
```

- `content = -1` -- это master
- `content = 0, 1` -- сегменты (нумерация с 0)
- `role = p` -- primary (основной)

---

## 3. Docker-кластер: как устроен наш docker-compose

### Контейнеры

| Контейнер | Роль | Образ | Порт |
|-----------|------|-------|------|
| master | Координатор GreenPlum | woblerr/greenplum:6.26.4 | 5432 |
| segment1 | Сегмент данных #0 | woblerr/greenplum:6.26.4 | -- |
| segment2 | Сегмент данных #1 | woblerr/greenplum:6.26.4 | -- |
| oracle | Доп. хранилище (по заданию) | gvenzl/oracle-xe:21-slim | 1521 |
| gpfdist | HTTP-сервер для CSV | woblerr/greenplum:6.26.4 | 8080 |

### SSH между нодами

GreenPlum использует SSH для:
- `gpssh-exkeys` -- обмен ключами при инициализации
- `gpinitsystem` -- создание сегментов
- `gpstart/gpstop` -- управление кластером
- `pxf cluster sync` -- синхронизация конфигурации

В docker-compose мы генерируем SSH-ключи и монтируем их:

```yaml
# Master: получает приватный ключ
volumes:
  - ./config/ssh/id_rsa:/tmp/id_rsa
  - ./config/ssh/id_rsa.pub:/tmp/id_rsa.pub

# Segments: получают authorized_keys (публичный ключ мастера)
volumes:
  - ./config/ssh/authorized_keys:/tmp/authorized_keys
```

Windows монтирует файлы с правами 777, а SSH требует 600 для приватного ключа. Поэтому мастер копирует ключи и исправляет права:

```yaml
entrypoint: ["/bin/bash", "-c",
  "cp /tmp/id_rsa /home/gpadmin/.ssh/id_rsa &&
   chmod 600 /home/gpadmin/.ssh/id_rsa &&
   exec /entrypoint.sh /start_gpdb.sh"]
```

### Инициализация кластера (gpinitsystem)

Файл `config/gpinitsystem_config`:

```
ARRAY_NAME="Greenplum"
MASTER_HOSTNAME=master        -- имя мастера
MASTER_PORT=5432              -- порт мастера
PORT_BASE=6000                -- стартовый порт сегментов
DATABASE_NAME=toystore        -- база данных по умолчанию
declare -a DATA_DIRECTORY=(/data/00/primary)  -- каталог данных на сегментах
MACHINE_LIST_FILE=/data/hostfile_gpinitsystem  -- файл со списком сегментов
```

Файл `config/hostfile_gpinitsystem`:
```
segment1
segment2
```

---

## 4. Ключи дистрибьюции (Distribution Keys)

### Что это

Ключ дистрибьюции -- колонка, по значению которой GreenPlum определяет, на каком сегменте хранить строку. Применяется **хеш-функция**: `hash(value) % число_сегментов`.

```sql
CREATE TABLE orders (
    order_id INT,
    website_session_id INT,
    ...
) DISTRIBUTED BY (website_session_id);
```

Здесь каждая строка orders попадает на сегмент `hash(website_session_id) % 2`.

### Виды дистрибьюции

| Тип | Синтаксис | Когда использовать |
|-----|-----------|-------------------|
| Hash | `DISTRIBUTED BY (col)` | Основной вариант. По значению колонки. |
| Random | `DISTRIBUTED RANDOMLY` | Для очень маленьких таблиц (нет смысла в хеше) |
| Replicated | `DISTRIBUTED REPLICATED` | Полная копия на каждом сегменте (GP 6+) |

### Критерии выбора ключа

1. **Высокая кардинальность** -- много уникальных значений -> равномерное распределение
2. **Частый JOIN** -- если две таблицы часто джойнятся, они должны быть на одном ключе
3. **Равномерность** -- значения должны быть распределены без перекоса

### Наш выбор ключей и обоснование

| Таблица | Записей | Ключ | Обоснование |
|---------|---------|------|-------------|
| products | 4 | RANDOMLY | Слишком мало строк для хеша. При 4 строках хеш даст перекос. |
| website_sessions | 472,871 | website_session_id | PK, максимальная кардинальность, участвует в JOIN с pageviews и orders |
| website_pageviews | 1,188,124 | website_session_id | **Co-located JOIN** с sessions. Данные одной сессии хранятся на одном сегменте. |
| orders | 32,313 | website_session_id | **Co-located JOIN** с sessions. Заказ и его сессия на одном сегменте. |
| order_items | 40,025 | order_id | **Co-located JOIN** с refunds. Альтернатива: website_session_id (но нет прямой связи). |
| order_item_refunds | 1,731 | order_id | **Co-located JOIN** с order_items по order_id. |

### Co-located JOIN -- ключевая оптимизация

Когда две таблицы распределены по одному и тому же ключу, и JOIN идет по этому ключу, данные **уже лежат на одном сегменте**. GreenPlum выполняет JOIN локально без пересылки -- это называется **co-located join**.

```
sessions (BY website_session_id)    orders (BY website_session_id)
+---Segment 0---+                   +---Segment 0---+
| session 1     |                   | order для     |
| session 3     |   JOIN локально   | session 1     |
| session 5     |<=================>| order для     |
|               |   БЕЗ пересылки  | session 3     |
+---------------+                   +---------------+

+---Segment 1---+                   +---Segment 1---+
| session 2     |                   | order для     |
| session 4     |   JOIN локально   | session 2     |
| session 6     |<=================>| order для     |
|               |   БЕЗ пересылки  | session 4     |
+---------------+                   +---------------+
```

Если бы orders был BY (order_id), то данные сессии 1 на segment 0, а заказ для сессии 1 мог бы быть на segment 1. Пришлось бы **перемещать данные** (Redistribute Motion) -- это дорого.

### Что будет если выбрать плохой ключ

В `explain_queries.py` мы меняем ключ orders с `website_session_id` на `order_id` и видим:

- **До**: co-located join, 599 мс
- **После**: появляется Redistribute Motion, 837 мс (+40%)

---

## 5. PXF: Platform eXtension Framework

### Что это

PXF -- Java-сервис, встроенный в GreenPlum, который позволяет читать и писать данные из внешних источников (Oracle, Hadoop, S3, и т.д.) через SQL.

### Как работает

```
  GreenPlum SQL-запрос
        |
        v
  +--MASTER--+
  | SELECT * |
  | FROM     |
  | ext_tbl  |
  +----+-----+
       |
  +----+------+------+
  |           |      |
  v           v      v
Seg0:PXF   Seg1:PXF    <-- PXF-агент на КАЖДОМ сегменте
  |           |
  |  JDBC     |  JDBC
  v           v
+------ORACLE------+       <-- Внешний источник
| TOYSTORE.ORDERS  |
+------------------+
```

Каждый сегмент запускает свой PXF-агент (Java). Агент подключается к Oracle по JDBC и тянет свою порцию данных.

### Наша реализация (3 файла)

**1. setup_pxf.py -- настройка PXF**

Что делает:
- Скачивает `ojdbc8.jar` (Oracle JDBC-драйвер) в `/data/pxf/lib/`
- Создает `jdbc-site.xml` с параметрами подключения к Oracle
- Синхронизирует конфиг на все сегменты (`pxf cluster sync`)
- Перезапускает PXF (`pxf cluster start`)

Конфигурация PXF-сервера `oracle` (`jdbc-site.xml`):

```xml
<configuration>
    <property>
        <name>jdbc.driver</name>
        <value>oracle.jdbc.driver.OracleDriver</value>
    </property>
    <property>
        <name>jdbc.url</name>
        <value>jdbc:oracle:thin:@oracle:1521/XEPDB1</value>
    </property>
    <property>
        <name>jdbc.user</name>
        <value>toystore</value>
    </property>
    <property>
        <name>jdbc.password</name>
        <value>toystore123</value>
    </property>
</configuration>
```

- `jdbc.driver` -- класс Java-драйвера Oracle
- `jdbc.url` -- строка подключения. `oracle` -- hostname контейнера в Docker-сети, `1521` -- порт, `XEPDB1` -- pluggable database
- `jdbc.user/password` -- учетные данные Oracle

**2. transfer_to_greenplum.py -- создание External Tables и перенос данных**

External Table -- "виртуальная" таблица. При SELECT данные читаются из Oracle на лету:

```sql
CREATE EXTERNAL TABLE ext_orders (
    order_id            INT,
    created_at          TIMESTAMP,
    website_session_id  INT,
    ...
) LOCATION ('pxf://TOYSTORE.ORDERS?PROFILE=Jdbc&SERVER=oracle')
  FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```

Разбор LOCATION:
- `pxf://` -- протокол PXF
- `TOYSTORE.ORDERS` -- схема.таблица в Oracle (имена в ВЕРХНЕМ регистре!)
- `?PROFILE=Jdbc` -- используем JDBC-профиль
- `&SERVER=oracle` -- имя PXF-сервера (из jdbc-site.xml)

Перенос данных -- просто INSERT ... SELECT:

```sql
INSERT INTO orders SELECT * FROM ext_orders;
```

GreenPlum читает данные из Oracle через PXF и вставляет в свою распределённую таблицу.

### Почему JDBC, а не прямое подключение

JDBC (Java Database Connectivity) -- стандартный Java-интерфейс для БД. PXF написан на Java, поэтому использует JDBC. Для каждой БД нужен свой драйвер:
- Oracle -> `ojdbc8.jar`
- PostgreSQL -> `postgresql.jar`
- MySQL -> `mysql-connector.jar`

---

## 6. Типы Motion (пересылка данных между сегментами)

Motion -- операция пересылки данных между сегментами в процессе выполнения запроса. Это **самая дорогая** операция в MPP, т.к. требует сетевого обмена.

### Gather Motion

```
Segment 0 --+
             +--> Master (собирает результаты)
Segment 1 --+
```

Сбор данных с сегментов на мастер. **Всегда присутствует** в финальном плане (результат нужно вернуть клиенту). Обозначение: `Gather Motion N:1` (N сегментов -> 1 мастер).

### Redistribute Motion

```
Segment 0 --+--> Segment 0
             X
Segment 1 --+--> Segment 1
```

Перехеширование данных между сегментами. Нужен когда **ключи JOIN не совпадают** с ключами дистрибьюции. Пример: sessions BY (website_session_id) JOIN orders BY (order_id) -- GreenPlum перехеширует orders по website_session_id.

Обозначение: `Redistribute Motion N:N` (каждый сегмент отправляет часть данных другим).

**Это ДОРОГАЯ операция** -- все данные передаются по сети.

### Broadcast Motion

```
Segment 0 --+--> Segment 0 (получает полную копию)
             |
             +--> Segment 1 (получает полную копию)
```

Копирование **всей** маленькой таблицы на каждый сегмент. Применяется когда одна таблица очень маленькая (products -- 4 строки). Дешевле скопировать 4 строки на все сегменты, чем перехешировать большую таблицу.

Обозначение: `Broadcast Motion N:N` (но фактически маленькая таблица рассылается всем).

### Co-located Join (отсутствие Motion)

Когда обе таблицы распределены по одному ключу и JOIN идет по этому ключу, пересылка **НЕ НУЖНА**. В плане запроса вы **не увидите** Motion между этими таблицами -- просто Hash Join.

Это **лучший сценарий** -- именно для этого мы выбираем ключи дистрибьюции!

### Сравнительная таблица

| Тип Motion | Когда возникает | Стоимость | Как избежать |
|-----------|-----------------|-----------|-------------|
| Gather | Всегда (финал) | Низкая | Невозможно |
| Redistribute | JOIN по != ключу дистрибьюции | Высокая | Co-located JOIN |
| Broadcast | JOIN с маленькой таблицей | Средняя | DISTRIBUTED REPLICATED |
| (нет Motion) | Co-located JOIN | Нулевая | Правильный выбор ключей |

---

## 7. EXPLAIN ANALYZE: чтение плана запроса

### Синтаксис

```sql
EXPLAIN ANALYZE SELECT ... ;
```

- `EXPLAIN` -- показывает план без выполнения
- `EXPLAIN ANALYZE` -- выполняет запрос и показывает реальное время

### Как читать план (снизу вверх)

```
Gather Motion 2:1  (actual time=586ms)      <-- 4. Собираем на мастер
  -> Sort                                    <-- 3. Сортируем
    -> Hash Join                             <-- 2. Джойним
      -> Seq Scan on sessions                <-- 1. Сканируем sessions
      -> Hash
        -> Seq Scan on orders                <-- 1. Сканируем orders
```

Каждый узел плана показывает:
- **Тип операции** (Seq Scan, Hash Join, Sort, Motion)
- **cost** -- оценка стоимости оптимизатором
- **actual time** -- реальное время (только с ANALYZE)
- **rows** -- количество строк

### Ключевые операции в плане

| Операция | Что делает |
|----------|-----------|
| Seq Scan | Последовательное чтение всей таблицы |
| Index Scan | Чтение по индексу |
| Hash Join | JOIN через хеш-таблицу (основной в GP) |
| Nested Loop | Вложенный цикл (для маленьких таблиц) |
| HashAggregate | GROUP BY через хеш |
| Sort | Сортировка (ORDER BY) |
| Redistribute Motion | Перехеширование данных |
| Broadcast Motion | Рассылка маленькой таблицы |
| Gather Motion | Сбор результатов на мастер |

### Slice в плане

GreenPlum разбивает план на **slice** (срезы). Каждый slice выполняется параллельно на сегментах:

```
(slice0) -- выполняется на мастере
(slice1; segments: 2) -- выполняется на 2 сегментах
(slice2; segments: 2) -- тоже на 2 сегментах
```

---

## 8. gpfdist: параллельная загрузка из CSV

### Что это

gpfdist -- HTTP-сервер от GreenPlum, который раздает CSV-файлы. Каждый сегмент GreenPlum параллельно тянет свою порцию файла. Это **самый быстрый способ загрузки** больших файлов.

### Как работает

```
                       +-- Segment 0 (тянет строки 1-50%)
CSV-файл --> gpfdist --+
             (HTTP)    +-- Segment 1 (тянет строки 51-100%)
```

gpfdist использует специальный протокол (`X-GP-PROTO` заголовок). Обычный браузер или curl не может скачать файл с gpfdist -- он работает только с GreenPlum.

### Наша реализация

В docker-compose.yml gpfdist-контейнер:

```yaml
gpfdist:
  image: woblerr/greenplum:6.26.4
  entrypoint: ["/bin/bash", "-c",
    "source /usr/local/greenplum-db/greenplum_path.sh &&
     gpfdist -d /data -p 8080 -l /data/gpfdist.log"]
  volumes:
    - ./data:/data    # CSV-файлы доступны gpfdist
```

External Table через gpfdist (в `gpfdist_load.py`):

```sql
CREATE EXTERNAL TABLE ext_gpfdist_sessions (
    website_session_id INT,
    created_at TIMESTAMP,
    ...
) LOCATION ('gpfdist://gpfdist:8080/website_sessions.csv')
  FORMAT 'CSV' (HEADER);
```

Разбор:
- `gpfdist://gpfdist:8080` -- hostname и порт контейнера gpfdist
- `/website_sessions.csv` -- имя файла в каталоге /data
- `FORMAT 'CSV' (HEADER)` -- формат CSV, первая строка - заголовок

### gpfdist vs PXF: сравнение скорости

| Метод | Время (472K строк) | Путь данных |
|-------|-------------------|-------------|
| gpfdist | ~1.6 сек | CSV -> HTTP -> Segments |
| PXF | ~2.8 сек | Oracle -> JDBC -> Java -> Segments |

gpfdist быстрее в ~1.8x, потому что:
1. Читает напрямую из файла (нет СУБД-посредника)
2. HTTP-протокол проще JDBC
3. Нет Java/JVM накладных расходов

---

## 9. Датасет и ER-диаграмма

### Maven Fuzzy Factory (Toy Store)

Интернет-магазин игрушек. 6 таблиц, ~1.7 млн строк.

### Связи между таблицами

```
WEBSITE_SESSIONS (472,871)
  |-- website_session_id (PK)
  |-- user_id
  |
  +--< WEBSITE_PAGEVIEWS (1,188,124)
  |     |-- website_session_id (FK)
  |     |-- pageview_url
  |
  +--< ORDERS (32,313)
        |-- website_session_id (FK)
        |-- user_id
        |-- primary_product_id (FK -> products)
        |
        +--< ORDER_ITEMS (40,025)
              |-- order_id (FK)
              |-- product_id (FK -> products)
              |
              +--< ORDER_ITEM_REFUNDS (1,731)
                    |-- order_item_id (FK)
                    |-- order_id (FK)

PRODUCTS (4)
  |-- product_id (PK)
  |-- product_name
```

Связи:
- `website_sessions` 1:N `website_pageviews` (по website_session_id)
- `website_sessions` 1:N `orders` (по website_session_id)
- `orders` 1:N `order_items` (по order_id)
- `order_items` 1:N `order_item_refunds` (по order_item_id)
- `products` 1:N `order_items` (по product_id)

ER-диаграмма: `public/mermaid-diagram.png`

---

## 10. Результаты анализа распределения

Скрипт `analyze_distribution.py` показывает, сколько строк каждой таблицы на каждом сегменте:

| Таблица | Segment 0 | Segment 1 | Skew |
|---------|-----------|-----------|------|
| products | 1 (25%) | 3 (75%) | 50% (SKEW -- но всего 4 строки, не критично) |
| website_sessions | 236,511 (50.0%) | 236,360 (50.0%) | 0.0% -- идеально |
| website_pageviews | 593,999 (50.0%) | 594,125 (50.0%) | 0.0% -- идеально |
| orders | 16,166 (50.0%) | 16,147 (50.0%) | 0.1% -- идеально |
| order_items | 19,955 (49.9%) | 20,070 (50.1%) | 0.3% -- отлично |
| order_item_refunds | 831 (48.0%) | 900 (52.0%) | 4.0% -- хорошо |

**Вывод:** ключи дистрибьюции выбраны правильно -- распределение равномерное (50/50).

### Как проверить распределение

```sql
SELECT gp_segment_id, COUNT(*)
FROM website_sessions
GROUP BY gp_segment_id
ORDER BY gp_segment_id;
```

`gp_segment_id` -- скрытая системная колонка, показывающая номер сегмента, на котором физически хранится строка.

---

## 11. Результаты запросов и планов

### Запрос 1: Конверсия сессий в заказы по источнику трафика

```sql
SELECT ws.utm_source,
       COUNT(DISTINCT ws.website_session_id) as sessions,
       COUNT(DISTINCT o.order_id) as orders,
       ROUND(COUNT(DISTINCT o.order_id)::numeric /
             COUNT(DISTINCT ws.website_session_id) * 100, 2) as conv_rate
FROM website_sessions ws
LEFT JOIN orders o ON ws.website_session_id = o.website_session_id
WHERE ws.utm_source IS NOT NULL
GROUP BY ws.utm_source
ORDER BY sessions DESC;
```

**Что в плане:** sessions и orders оба BY (website_session_id) -> **Hash Left Join** (co-located, без Redistribute). Redistribute Motion есть, но только для GROUP BY по utm_source (агрегация). Время: **599 мс**.

### Запрос 2: Среднее число просмотров на сессию по устройству

```sql
SELECT ws.device_type,
       COUNT(wp.website_pageview_id) as total_pageviews,
       COUNT(DISTINCT ws.website_session_id) as total_sessions,
       ROUND(COUNT(wp.website_pageview_id)::numeric /
             COUNT(DISTINCT ws.website_session_id), 2) as avg_pv
FROM website_sessions ws
JOIN website_pageviews wp ON ws.website_session_id = wp.website_session_id
GROUP BY ws.device_type;
```

**Что в плане:** sessions и pageviews оба BY (website_session_id) -> **Hash Join** (co-located). Redistribute для GROUP BY по device_type. Время: **882 мс**.

### Запрос 3: Выручка и возвраты по продуктам

```sql
SELECT p.product_name,
       COUNT(oi.order_item_id) as items_sold,
       SUM(oi.price_usd) as revenue,
       COALESCE(SUM(r.refund_amount_usd), 0) as refunds
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
LEFT JOIN order_item_refunds r
  ON oi.order_id = r.order_id AND oi.order_item_id = r.order_item_id
GROUP BY p.product_name
ORDER BY revenue DESC;
```

**Что в плане:**
- order_items + order_item_refunds: **Hash Left Join** (co-located по order_id)
- products (4 строки): **Broadcast Motion** (рассылка на все сегменты)
- Время: **30 мс** (маленькие таблицы)

### После смены ключа (orders BY order_id)

```sql
ALTER TABLE orders SET DISTRIBUTED BY (order_id);
```

Повторяем Запрос 1 -- появляется **Redistribute Motion** для orders:

```
-> Redistribute Motion 2:2
      Hash Key: o.website_session_id
      -> Seq Scan on orders o
```

GreenPlum вынужден перехешировать orders по website_session_id для JOIN. Время: **837 мс** (было 599 -- рост на 40%).

---

## 12. Вопросы для защиты

### Базовые вопросы

**Q: Что такое MPP?**
A: Massively Parallel Processing -- архитектура, где данные и вычисления распределены по множеству узлов, работающих параллельно. GreenPlum -- MPP-система на основе PostgreSQL.

**Q: Чем Master отличается от Segment?**
A: Master -- координатор: принимает SQL, строит план, распределяет задачи, собирает результаты. Не хранит пользовательские данные. Segments -- хранят данные и выполняют вычисления параллельно.

**Q: Что такое ключ дистрибьюции?**
A: Колонка, по хешу которой GreenPlum определяет, на какой сегмент попадает строка. Выбирается при CREATE TABLE: `DISTRIBUTED BY (col)`.

**Q: Как выбрать ключ дистрибьюции?**
A: 1) Высокая кардинальность (много уникальных значений). 2) Частые JOIN по этой колонке. 3) Равномерное распределение значений.

### Вопросы по Motion

**Q: Что такое Redistribute Motion?**
A: Перехеширование данных между сегментами. Возникает когда JOIN идет по колонке, отличной от ключа дистрибьюции. Самая дорогая операция.

**Q: Что такое Broadcast Motion?**
A: Копирование маленькой таблицы на все сегменты. Дешевле, чем Redistribute большой таблицы.

**Q: Что такое co-located join?**
A: JOIN без пересылки данных. Обе таблицы распределены по одному ключу, и JOIN идет по этому же ключу. Данные уже на одном сегменте.

**Q: Как избавиться от Redistribute Motion?**
A: Распределить обе таблицы по одному ключу, по которому идет JOIN.

### Вопросы по PXF

**Q: Что такое PXF?**
A: Platform eXtension Framework -- Java-сервис в GreenPlum для чтения данных из внешних источников (Oracle, Hadoop, S3) через SQL.

**Q: Как PXF подключается к Oracle?**
A: Через JDBC. Мы настраиваем jdbc-site.xml с драйвером, URL, логином/паролем. PXF на каждом сегменте запускает Java-агент, который подключается к Oracle.

**Q: Что такое External Table?**
A: "Виртуальная" таблица в GreenPlum. При SELECT данные читаются из внешнего источника на лету. Не хранит данные локально.

**Q: Зачем External Table, если можно сразу INSERT?**
A: External Table -- это абстракция. Она позволяет: 1) проверить данные перед загрузкой (SELECT), 2) трансформировать при загрузке (INSERT ... SELECT ... WHERE), 3) переиспользовать (загрузить в разные таблицы с разными ключами).

### Вопросы по gpfdist

**Q: Что такое gpfdist?**
A: HTTP-сервер от GreenPlum, раздающий CSV-файлы. Каждый сегмент параллельно тянет свою порцию файла. Самый быстрый способ загрузки.

**Q: Почему gpfdist быстрее PXF?**
A: gpfdist читает файл напрямую по HTTP, без посредника (СУБД). PXF идет через Java -> JDBC -> Oracle -> сеть -> Java. В нашем тесте gpfdist быстрее в ~1.8x.

**Q: Можно ли открыть gpfdist в браузере?**
A: Нет. gpfdist использует специальный протокол (заголовок X-GP-PROTO). Работает только с сегментами GreenPlum.

### Вопросы по архитектуре

**Q: Почему Shared-Nothing?**
A: Каждый сегмент имеет свой CPU, RAM, диск. Не разделяют ресурсы -- общаются только по сети. Это позволяет масштабироваться линейно: добавляем сегменты -- растет производительность.

**Q: Что будет если один сегмент упадет?**
A: Без mirror-сегментов -- данные на этом сегменте недоступны. В production используют mirror (реплику), но в лабе мы их не настраивали.

**Q: Почему GreenPlum не подходит для OLTP?**
A: Каждый запрос распределяется по всем сегментам. Для простого SELECT по PK это overhead. GreenPlum оптимален для аналитических запросов (OLAP) -- сканирование больших таблиц, агрегации, JOIN.
