# Лабораторная работа: MPP-архитектура и моделирование данных в Greenplum

## Датасет

**Maven Fuzzy Factory** (Toy Store) -- интернет-магазин игрушек, 6 таблиц, ~1.7 млн строк.
ER-диаграмма: `public/mermaid-diagram.png`

## Архитектура

```
  CSV-файлы (data/)
       |                         +------------------+
       |  load_to_oracle.py      |   GreenPlum      |
       +------------------------>|   Master         |<--- psql (порт 5432)
       |                         |   (координатор)  |
       |                         +--------+---------+
       |                                  |
       v                           +------+------+
  +---------+    PXF (JDBC)        |             |
  | Oracle  |<-------------------->| Segment 1   |  Segment 2   |
  | XE      |    transfer_to_gp   | (данные)    |  (данные)     |
  +---------+                      +-------------+  +------------+
                                          ^
  +---------+    gpfdist (HTTP)           |
  | gpfdist |-----------------------------+
  | сервер  |    gpfdist_load.py
  +---------+
```

## Требования

- Docker Desktop
- Python 3.10+
- `pip install oracledb` (устанавливается в venv)

## Быстрый запуск

```bash
# 1. Создать виртуальное окружение
py -m venv venv
venv\Scripts\activate          # Windows
pip install oracledb

# 2. Запустить кластер (master + 2 segments + Oracle + gpfdist)
docker compose up -d
# Подождать ~2 минуты пока GreenPlum и Oracle инициализируются

# 3. Загрузить данные в Oracle
py load_to_oracle.py

# 4. Настроить PXF (JDBC-подключение к Oracle)
py setup_pxf.py

# 5. Перенести данные из Oracle в GreenPlum через PXF
py transfer_to_greenplum.py

# 6. Анализ распределения данных по сегментам
py analyze_distribution.py

# 7. Запросы с EXPLAIN ANALYZE + смена ключей дистрибьюции
py explain_queries.py

# 8. Загрузка CSV через gpfdist (усложненный вариант)
py gpfdist_load.py
```

## Структура проекта

```
lab-greenplum-2/
|-- docker-compose.yml           # Кластер: 1 master + 2 segments + Oracle + gpfdist
|-- config/
|   |-- gpinitsystem_config      # Конфигурация инициализации GreenPlum
|   |-- hostfile_gpinitsystem    # Список сегментов (segment1, segment2)
|   |-- ssh/                     # SSH-ключи для связи master <-> segments
|-- data/                        # CSV-файлы датасета Maven Fuzzy Factory
|-- public/
|   |-- mermaid-diagram.png      # ER-диаграмма датасета
|-- load_to_oracle.py            # Шаг 2: CSV -> Oracle (oracledb)
|-- setup_pxf.py                 # Шаг 3.1: настройка PXF (JDBC-драйвер + конфиг)
|-- transfer_to_greenplum.py     # Шаг 3.2: Oracle -> GreenPlum через PXF
|-- analyze_distribution.py      # Шаг 4: распределение данных по сегментам
|-- explain_queries.py           # Шаг 5: EXPLAIN ANALYZE + смена ключей
|-- gpfdist_load.py              # Шаг 6: загрузка CSV через gpfdist
|-- REPORT.md                    # Теоретический отчет (подготовка к защите)
|-- README.md                    # Этот файл
```

## Подключение к базам

| Сервис | Хост | Порт | База | Пользователь | Пароль |
|--------|------|------|------|-------------|--------|
| GreenPlum | localhost | 5432 | toystore | gpadmin | gpadmin |
| Oracle | localhost | 1521 | XEPDB1 | toystore | toystore123 |

## Остановка

```bash
docker compose down       # Остановить без удаления данных
docker compose down -v    # Остановить и удалить все volumes
```
