"""
Скрипт загрузки CSV-данных Maven Fuzzy Factory в Oracle.
Шаг 2 лабораторной: загружаем данные из CSV в Oracle,
чтобы потом через PXF перенести их в GreenPlum.
"""
import csv
import oracledb
from datetime import datetime

# Подключение к Oracle (контейнер oracle из docker-compose)
ORACLE_DSN = "localhost:1521/XEPDB1"
ORACLE_USER = "toystore"
ORACLE_PASSWORD = "toystore123"

DATA_DIR = "data"
BATCH_SIZE = 5000

# DDL для создания таблиц в Oracle
CREATE_TABLES_SQL = [
    """CREATE TABLE products (
        product_id    NUMBER(10) PRIMARY KEY,
        created_at    TIMESTAMP,
        product_name  VARCHAR2(200)
    )""",
    """CREATE TABLE website_sessions (
        website_session_id  NUMBER(10) PRIMARY KEY,
        created_at          TIMESTAMP,
        user_id             NUMBER(10),
        is_repeat_session   NUMBER(1),
        utm_source          VARCHAR2(100),
        utm_campaign        VARCHAR2(100),
        utm_content         VARCHAR2(100),
        device_type         VARCHAR2(50),
        http_referer        VARCHAR2(200)
    )""",
    """CREATE TABLE website_pageviews (
        website_pageview_id  NUMBER(10) PRIMARY KEY,
        created_at           TIMESTAMP,
        website_session_id   NUMBER(10),
        pageview_url         VARCHAR2(200)
    )""",
    """CREATE TABLE orders (
        order_id            NUMBER(10) PRIMARY KEY,
        created_at          TIMESTAMP,
        website_session_id  NUMBER(10),
        user_id             NUMBER(10),
        primary_product_id  NUMBER(10),
        items_purchased     NUMBER(10),
        price_usd           NUMBER(10,2),
        cogs_usd            NUMBER(10,2)
    )""",
    """CREATE TABLE order_items (
        order_item_id   NUMBER(10) PRIMARY KEY,
        created_at      TIMESTAMP,
        order_id        NUMBER(10),
        product_id      NUMBER(10),
        is_primary_item NUMBER(1),
        price_usd       NUMBER(10,2),
        cogs_usd        NUMBER(10,2)
    )""",
    """CREATE TABLE order_item_refunds (
        order_item_refund_id  NUMBER(10) PRIMARY KEY,
        created_at            TIMESTAMP,
        order_item_id         NUMBER(10),
        order_id              NUMBER(10),
        refund_amount_usd     NUMBER(10,2)
    )""",
]

# Маппинг CSV-файл -> (таблица, список колонок, типы)
# Типы: 'int', 'float', 'timestamp', 'str'
TABLES = {
    "products.csv": {
        "table": "products",
        "columns": ["product_id", "created_at", "product_name"],
        "types": ["int", "timestamp", "str"],
    },
    "website_sessions.csv": {
        "table": "website_sessions",
        "columns": [
            "website_session_id", "created_at", "user_id", "is_repeat_session",
            "utm_source", "utm_campaign", "utm_content", "device_type", "http_referer",
        ],
        "types": ["int", "timestamp", "int", "int", "str", "str", "str", "str", "str"],
    },
    "website_pageviews.csv": {
        "table": "website_pageviews",
        "columns": ["website_pageview_id", "created_at", "website_session_id", "pageview_url"],
        "types": ["int", "timestamp", "int", "str"],
    },
    "orders.csv": {
        "table": "orders",
        "columns": [
            "order_id", "created_at", "website_session_id", "user_id",
            "primary_product_id", "items_purchased", "price_usd", "cogs_usd",
        ],
        "types": ["int", "timestamp", "int", "int", "int", "int", "float", "float"],
    },
    "order_items.csv": {
        "table": "order_items",
        "columns": [
            "order_item_id", "created_at", "order_id", "product_id",
            "is_primary_item", "price_usd", "cogs_usd",
        ],
        "types": ["int", "timestamp", "int", "int", "int", "float", "float"],
    },
    "order_item_refunds.csv": {
        "table": "order_item_refunds",
        "columns": [
            "order_item_refund_id", "created_at", "order_item_id",
            "order_id", "refund_amount_usd",
        ],
        "types": ["int", "timestamp", "int", "int", "float"],
    },
}


def parse_value(value, typ):
    """Конвертирует строковое значение из CSV в нужный Python-тип."""
    if value == "" or value is None:
        return None
    if typ == "int":
        return int(value)
    if typ == "float":
        return float(value)
    if typ == "timestamp":
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return value  # str


def main():
    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
    cursor = conn.cursor()

    # 1. Создаем таблицы (если уже есть - пересоздаем)
    for ddl in CREATE_TABLES_SQL:
        table_name = ddl.split("TABLE ")[1].split(" ")[0].split("(")[0]
        try:
            cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
            print(f"  Удалена старая таблица {table_name}")
        except oracledb.DatabaseError:
            pass
        cursor.execute(ddl)
        print(f"  Создана таблица {table_name}")
    conn.commit()

    # 2. Загружаем данные из CSV
    for csv_file, meta in TABLES.items():
        table = meta["table"]
        columns = meta["columns"]
        types = meta["types"]

        placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
        insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

        filepath = f"{DATA_DIR}/{csv_file}"
        row_count = 0
        batch = []

        print(f"\nЗагрузка {csv_file} -> {table}...")

        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader)  # пропускаем заголовок

            for row in reader:
                parsed = [parse_value(row[i], types[i]) for i in range(len(columns))]
                batch.append(parsed)
                row_count += 1

                if len(batch) >= BATCH_SIZE:
                    cursor.executemany(insert_sql, batch)
                    conn.commit()
                    batch = []
                    print(f"  ...загружено {row_count} строк", end="\r")

            if batch:
                cursor.executemany(insert_sql, batch)
                conn.commit()

        print(f"  Готово: {row_count} строк загружено в {table}")

    # 3. Проверяем количество записей
    print("\n=== Проверка загрузки ===")
    for meta in TABLES.values():
        cursor.execute(f"SELECT COUNT(*) FROM {meta['table']}")
        count = cursor.fetchone()[0]
        print(f"  {meta['table']}: {count} записей")

    cursor.close()
    conn.close()
    print("\nЗагрузка завершена!")


if __name__ == "__main__":
    main()
