"""
Шаг 5 - Запросы с EXPLAIN ANALYZE и анализ Motion-типов.

Что делает этот скрипт:
1. Выполняет 3 аналитических запроса с EXPLAIN ANALYZE
   - Показывает план выполнения запроса: какие операции, на каких сегментах
2. Объясняет типы Motion (пересылки данных между сегментами):
   - Gather Motion: данные собираются с сегментов на мастер (финальная агрегация)
   - Redistribute Motion: данные перехешируются между сегментами (нужен для JOIN
     когда таблицы распределены по разным ключам)
   - Broadcast Motion: маленькая таблица копируется на все сегменты
   - Co-located Join: JOIN без пересылки (таблицы на одном ключе) - нет Motion!
3. Меняет ключи дистрибьюции и повторно анализирует планы

Соответствует пункту задания:
  "3+ запроса к нескольким таблицам с EXPLAIN ANALYZE,
   изменить ключи, повторный анализ, объяснить Motion"
"""
import subprocess
import sys


def psql_exec(sql, database="toystore"):
    """Выполняет SQL-запрос в GreenPlum и возвращает вывод."""
    result = subprocess.run(
        ["docker", "exec", "-u", "gpadmin", "master", "bash", "-c",
         f"source ~/.bashrc && psql -d {database} -c \"{sql}\""],
        capture_output=True, text=True, timeout=120,
    )
    output = ""
    if result.stdout:
        output += result.stdout
    if result.stderr:
        for line in result.stderr.strip().split("\n"):
            if line and "NOTICE" not in line and "WARNING" not in line:
                output += line + "\n"
    return output


def run_explain(title, sql, explanation):
    """Выполняет EXPLAIN ANALYZE и выводит результат с пояснением."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)
    print(f"\nSQL:\n{sql}\n")
    print("EXPLAIN ANALYZE:")
    output = psql_exec(f"EXPLAIN ANALYZE {sql}")
    print(output)
    print(f"Объяснение:\n{explanation}")


def main():
    # Сначала обновим статистику для оптимизатора
    print("Обновляем статистику (ANALYZE) для оптимизатора запросов...")
    for table in ["products", "website_sessions", "website_pageviews",
                   "orders", "order_items", "order_item_refunds"]:
        psql_exec(f"ANALYZE {table};")

    # =====================================================================
    # ЧАСТЬ 1: Запросы с текущими ключами дистрибьюции
    # (sessions, pageviews, orders по website_session_id - co-located)
    # =====================================================================
    print("\n" + "#" * 70)
    print("  ЧАСТЬ 1: Текущие ключи дистрибьюции")
    print("  sessions/pageviews/orders -> BY (website_session_id)")
    print("  order_items/refunds -> BY (order_id)")
    print("#" * 70)

    # Запрос 1: JOIN sessions + orders (co-located!)
    run_explain(
        "Запрос 1: Конверсия сессий в заказы по источнику трафика",
        "SELECT ws.utm_source, COUNT(DISTINCT ws.website_session_id) as sessions, "
        "COUNT(DISTINCT o.order_id) as orders, "
        "ROUND(COUNT(DISTINCT o.order_id)::numeric / COUNT(DISTINCT ws.website_session_id) * 100, 2) as conv_rate "
        "FROM website_sessions ws "
        "LEFT JOIN orders o ON ws.website_session_id = o.website_session_id "
        "WHERE ws.utm_source IS NOT NULL "
        "GROUP BY ws.utm_source ORDER BY sessions DESC;",
        "sessions и orders оба DISTRIBUTED BY (website_session_id).\n"
        "Ожидаем CO-LOCATED JOIN - нет Redistribute Motion!\n"
        "Только Gather Motion (сбор результатов на мастер)."
    )

    # Запрос 2: JOIN sessions + pageviews (co-located!)
    run_explain(
        "Запрос 2: Среднее количество просмотров на сессию по типу устройства",
        "SELECT ws.device_type, COUNT(wp.website_pageview_id) as total_pageviews, "
        "COUNT(DISTINCT ws.website_session_id) as total_sessions, "
        "ROUND(COUNT(wp.website_pageview_id)::numeric / COUNT(DISTINCT ws.website_session_id), 2) as avg_pv "
        "FROM website_sessions ws "
        "JOIN website_pageviews wp ON ws.website_session_id = wp.website_session_id "
        "GROUP BY ws.device_type;",
        "sessions и pageviews оба DISTRIBUTED BY (website_session_id).\n"
        "Ожидаем CO-LOCATED JOIN - нет Redistribute Motion!\n"
        "Данные одной сессии лежат на одном сегменте."
    )

    # Запрос 3: JOIN orders + order_items + products (order_items по order_id, products RANDOMLY)
    run_explain(
        "Запрос 3: Выручка и возвраты по продуктам",
        "SELECT p.product_name, COUNT(oi.order_item_id) as items_sold, "
        "SUM(oi.price_usd) as revenue, "
        "COALESCE(SUM(r.refund_amount_usd), 0) as refunds "
        "FROM order_items oi "
        "JOIN products p ON oi.product_id = p.product_id "
        "LEFT JOIN order_item_refunds r ON oi.order_id = r.order_id AND oi.order_item_id = r.order_item_id "
        "GROUP BY p.product_name ORDER BY revenue DESC;",
        "order_items и order_item_refunds оба DISTRIBUTED BY (order_id) -> co-located JOIN.\n"
        "products DISTRIBUTED RANDOMLY (4 строки) -> Broadcast Motion\n"
        "(маленькая таблица рассылается на все сегменты)."
    )

    # =====================================================================
    # ЧАСТЬ 2: Меняем ключи дистрибьюции и смотрим разницу
    # =====================================================================
    print("\n\n" + "#" * 70)
    print("  ЧАСТЬ 2: Меняем ключи дистрибьюции")
    print("  Перераспределяем orders BY (order_id) вместо (website_session_id)")
    print("  Это СЛОМАЕТ co-located join с sessions!")
    print("#" * 70)

    # Меняем распределение orders
    print("\nПерераспределяем таблицу orders...")
    psql_exec("ALTER TABLE orders SET DISTRIBUTED BY (order_id);")
    psql_exec("ANALYZE orders;")

    # Повторяем Запрос 1 - теперь будет Redistribute Motion!
    run_explain(
        "Запрос 1 (после изменения): Конверсия по источнику трафика",
        "SELECT ws.utm_source, COUNT(DISTINCT ws.website_session_id) as sessions, "
        "COUNT(DISTINCT o.order_id) as orders, "
        "ROUND(COUNT(DISTINCT o.order_id)::numeric / COUNT(DISTINCT ws.website_session_id) * 100, 2) as conv_rate "
        "FROM website_sessions ws "
        "LEFT JOIN orders o ON ws.website_session_id = o.website_session_id "
        "WHERE ws.utm_source IS NOT NULL "
        "GROUP BY ws.utm_source ORDER BY sessions DESC;",
        "ТЕПЕРЬ sessions BY (website_session_id), а orders BY (order_id).\n"
        "Ожидаем REDISTRIBUTE MOTION - GreenPlum вынужден перехешировать\n"
        "одну из таблиц для JOIN. Это дороже, чем co-located!"
    )

    # Восстанавливаем оригинальное распределение
    print("\nВосстанавливаем оригинальное распределение orders BY (website_session_id)...")
    psql_exec("ALTER TABLE orders SET DISTRIBUTED BY (website_session_id);")
    psql_exec("ANALYZE orders;")

    print("\n" + "=" * 70)
    print("  ИТОГО: Типы Motion в GreenPlum")
    print("=" * 70)
    print("""
  1. Gather Motion     - сбор данных с сегментов на мастер (всегда есть)
  2. Redistribute Motion - перехеширование для JOIN (когда ключи не совпадают)
  3. Broadcast Motion   - рассылка маленькой таблицы на все сегменты
  4. Co-located Join    - JOIN без пересылки (ключи совпадают) - ЛУЧШИЙ вариант!

  Вывод: правильный выбор ключа дистрибьюции позволяет избежать
  Redistribute Motion и ускорить JOIN-запросы.
""")


if __name__ == "__main__":
    main()
