"""
Шаг 3.2 - Перенос данных из Oracle в GreenPlum через PXF.

Что делает этот скрипт:
1. Создает External Tables (PXF) в GreenPlum
   - Это "виртуальные" таблицы: SELECT из них читает данные из Oracle на лету
   - Используют протокол pxf:// и сервер 'oracle' (настроенный в setup_pxf.py)
2. Создает обычные таблицы с ключами дистрибьюции (DISTRIBUTED BY)
   - Ключ дистрибьюции определяет, как данные распределяются между сегментами
   - Выбираем колонки с высокой кардинальностью и частыми JOIN
3. Переносит данные: INSERT INTO gp_table SELECT * FROM ext_table

Обоснование ключей дистрибьюции:
  - products: DISTRIBUTED RANDOMLY (всего 4 записи, смысла в хеше нет)
  - website_sessions: DISTRIBUTED BY (website_session_id)
    -> PK, высокая кардинальность, используется в JOIN с orders и pageviews
  - website_pageviews: DISTRIBUTED BY (website_session_id)
    -> co-located join с website_sessions (данные одной сессии на одном сегменте)
  - orders: DISTRIBUTED BY (website_session_id)
    -> co-located join с website_sessions
  - order_items: DISTRIBUTED BY (order_id)
    -> co-located join с orders по order_id
  - order_item_refunds: DISTRIBUTED BY (order_id)
    -> co-located join с order_items по order_id

Соответствует пунктам задания:
  - "Загрузить данные через PXF"
  - "Выбрать ключи дистрибьюции с обоснованием"
"""
import subprocess
import sys


def psql_exec(sql, database="toystore"):
    """Выполняет SQL-запрос в GreenPlum через psql."""
    result = subprocess.run(
        ["docker", "exec", "-u", "gpadmin", "master", "bash", "-c",
         f"source ~/.bashrc && psql -d {database} -c \"{sql}\""],
        capture_output=True, text=True, timeout=300,
    )
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        # Фильтруем NOTICE-сообщения
        for line in result.stderr.strip().split("\n"):
            if line and "NOTICE" not in line:
                print(line, file=sys.stderr)
    return result.returncode


# External Tables: читают данные из Oracle через PXF
# Формат: pxf://SCHEMA.TABLE?PROFILE=Jdbc&SERVER=oracle
EXTERNAL_TABLES = [
    """CREATE EXTERNAL TABLE ext_products (
        product_id    INT,
        created_at    TIMESTAMP,
        product_name  TEXT
    ) LOCATION ('pxf://TOYSTORE.PRODUCTS?PROFILE=Jdbc&SERVER=oracle')
    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')""",

    """CREATE EXTERNAL TABLE ext_website_sessions (
        website_session_id  INT,
        created_at          TIMESTAMP,
        user_id             INT,
        is_repeat_session   INT,
        utm_source          TEXT,
        utm_campaign        TEXT,
        utm_content         TEXT,
        device_type         TEXT,
        http_referer        TEXT
    ) LOCATION ('pxf://TOYSTORE.WEBSITE_SESSIONS?PROFILE=Jdbc&SERVER=oracle')
    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')""",

    """CREATE EXTERNAL TABLE ext_website_pageviews (
        website_pageview_id  INT,
        created_at           TIMESTAMP,
        website_session_id   INT,
        pageview_url         TEXT
    ) LOCATION ('pxf://TOYSTORE.WEBSITE_PAGEVIEWS?PROFILE=Jdbc&SERVER=oracle')
    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')""",

    """CREATE EXTERNAL TABLE ext_orders (
        order_id            INT,
        created_at          TIMESTAMP,
        website_session_id  INT,
        user_id             INT,
        primary_product_id  INT,
        items_purchased     INT,
        price_usd           NUMERIC(10,2),
        cogs_usd            NUMERIC(10,2)
    ) LOCATION ('pxf://TOYSTORE.ORDERS?PROFILE=Jdbc&SERVER=oracle')
    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')""",

    """CREATE EXTERNAL TABLE ext_order_items (
        order_item_id   INT,
        created_at      TIMESTAMP,
        order_id        INT,
        product_id      INT,
        is_primary_item INT,
        price_usd       NUMERIC(10,2),
        cogs_usd        NUMERIC(10,2)
    ) LOCATION ('pxf://TOYSTORE.ORDER_ITEMS?PROFILE=Jdbc&SERVER=oracle')
    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')""",

    """CREATE EXTERNAL TABLE ext_order_item_refunds (
        order_item_refund_id  INT,
        created_at            TIMESTAMP,
        order_item_id         INT,
        order_id              INT,
        refund_amount_usd     NUMERIC(10,2)
    ) LOCATION ('pxf://TOYSTORE.ORDER_ITEM_REFUNDS?PROFILE=Jdbc&SERVER=oracle')
    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')""",
]

# Обычные таблицы с ключами дистрибьюции
# ВАЖНО: в GreenPlum PRIMARY KEY должен включать колонку DISTRIBUTED BY.
# Мы осознанно убираем PK там, где ключ дистрибьюции != PK,
# чтобы оптимизировать co-located JOIN между таблицами.
GP_TABLES = [
    # products: 4 записи, DISTRIBUTED RANDOMLY (слишком мало строк для хеша)
    """CREATE TABLE products (
        product_id    INT,
        created_at    TIMESTAMP,
        product_name  TEXT
    ) DISTRIBUTED RANDOMLY""",

    # website_sessions: ключ = website_session_id (PK, высокая кардинальность)
    """CREATE TABLE website_sessions (
        website_session_id  INT,
        created_at          TIMESTAMP,
        user_id             INT,
        is_repeat_session   INT,
        utm_source          TEXT,
        utm_campaign        TEXT,
        utm_content         TEXT,
        device_type         TEXT,
        http_referer        TEXT
    ) DISTRIBUTED BY (website_session_id)""",

    # website_pageviews: ключ = website_session_id (co-located JOIN с sessions)
    """CREATE TABLE website_pageviews (
        website_pageview_id  INT,
        created_at           TIMESTAMP,
        website_session_id   INT,
        pageview_url         TEXT
    ) DISTRIBUTED BY (website_session_id)""",

    # orders: ключ = website_session_id (co-located JOIN с sessions)
    """CREATE TABLE orders (
        order_id            INT,
        created_at          TIMESTAMP,
        website_session_id  INT,
        user_id             INT,
        primary_product_id  INT,
        items_purchased     INT,
        price_usd           NUMERIC(10,2),
        cogs_usd            NUMERIC(10,2)
    ) DISTRIBUTED BY (website_session_id)""",

    # order_items: ключ = order_id (co-located JOIN с orders)
    """CREATE TABLE order_items (
        order_item_id   INT,
        created_at      TIMESTAMP,
        order_id        INT,
        product_id      INT,
        is_primary_item INT,
        price_usd       NUMERIC(10,2),
        cogs_usd        NUMERIC(10,2)
    ) DISTRIBUTED BY (order_id)""",

    # order_item_refunds: ключ = order_id (co-located JOIN с order_items)
    """CREATE TABLE order_item_refunds (
        order_item_refund_id  INT,
        created_at            TIMESTAMP,
        order_item_id         INT,
        order_id              INT,
        refund_amount_usd     NUMERIC(10,2)
    ) DISTRIBUTED BY (order_id)""",
]

# Порядок загрузки (имя ext таблицы -> имя GP таблицы)
TRANSFER_ORDER = [
    ("ext_products", "products"),
    ("ext_website_sessions", "website_sessions"),
    ("ext_website_pageviews", "website_pageviews"),
    ("ext_orders", "orders"),
    ("ext_order_items", "order_items"),
    ("ext_order_item_refunds", "order_item_refunds"),
]


def main():
    # 1. Удаляем старые таблицы если есть
    print("=== 1. Очистка старых таблиц ===")
    for ext_name, gp_name in TRANSFER_ORDER:
        psql_exec(f"DROP EXTERNAL TABLE IF EXISTS {ext_name} CASCADE;")
        psql_exec(f"DROP TABLE IF EXISTS {gp_name} CASCADE;")

    # 2. Создаем External Tables (PXF -> Oracle)
    print("\n=== 2. Создаем External Tables (читают из Oracle через PXF) ===")
    for ddl in EXTERNAL_TABLES:
        table_name = ddl.split("TABLE ")[1].split(" ")[0].split("(")[0]
        print(f"  Создаем {table_name}...")
        psql_exec(ddl.replace("\n", " "))

    # 3. Проверяем что External Tables работают
    print("\n=== 3. Проверяем подключение к Oracle через PXF ===")
    psql_exec("SELECT COUNT(*) as products_in_oracle FROM ext_products;")

    # 4. Создаем обычные GP-таблицы с ключами дистрибьюции
    print("\n=== 4. Создаем GP-таблицы с ключами дистрибьюции ===")
    for ddl in GP_TABLES:
        table_name = ddl.split("TABLE ")[1].split(" ")[0].split("(")[0]
        print(f"  Создаем {table_name}...")
        psql_exec(ddl.replace("\n", " "))

    # 5. Переносим данные: Oracle -> PXF -> GreenPlum
    print("\n=== 5. Переносим данные из Oracle в GreenPlum ===")
    for ext_name, gp_name in TRANSFER_ORDER:
        print(f"\n  {ext_name} -> {gp_name}...")
        psql_exec(f"INSERT INTO {gp_name} SELECT * FROM {ext_name};")
        psql_exec(f"SELECT COUNT(*) as rows FROM {gp_name};")

    # 6. Итоговая проверка
    print("\n=== 6. Итоговая проверка данных в GreenPlum ===")
    psql_exec("""
        SELECT 'products' as table_name, COUNT(*) as rows FROM products
        UNION ALL SELECT 'website_sessions', COUNT(*) FROM website_sessions
        UNION ALL SELECT 'website_pageviews', COUNT(*) FROM website_pageviews
        UNION ALL SELECT 'orders', COUNT(*) FROM orders
        UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
        UNION ALL SELECT 'order_item_refunds', COUNT(*) FROM order_item_refunds
        ORDER BY table_name;
    """.replace("\n", " "))

    print("\n=== Данные перенесены из Oracle в GreenPlum! ===")


if __name__ == "__main__":
    main()
