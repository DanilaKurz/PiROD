"""
Шаг 4 - Анализ распределения данных по сегментам GreenPlum.

Что делает этот скрипт:
1. Для каждой таблицы запрашивает количество строк на каждом сегменте
   - Используем системную колонку gp_segment_id (есть в каждой строке)
   - Она показывает на каком сегменте физически хранится строка
2. Выводит таблицу распределения и процент перекоса (skew)
3. Строит столбчатую диаграмму распределения

Соответствует пункту задания:
  "Проанализировать распределение данных по сегментам, построить графики"

Хорошее распределение = примерно 50/50 между двумя сегментами.
Плохое распределение (skew) = одн сегмент перегружен.
"""
import subprocess
import sys
import json


TABLES = [
    "products", "website_sessions", "website_pageviews",
    "orders", "order_items", "order_item_refunds",
]


def psql_query(sql, database="toystore"):
    """Выполняет SQL и возвращает результат как текст."""
    result = subprocess.run(
        ["docker", "exec", "-u", "gpadmin", "master", "bash", "-c",
         f"source ~/.bashrc && psql -d {database} -t -A -c \"{sql}\""],
        capture_output=True, text=True, timeout=60,
    )
    return result.stdout.strip()


def main():
    print("=" * 65)
    print("  Распределение данных по сегментам GreenPlum")
    print("  (gp_segment_id: 0 = segment1, 1 = segment2)")
    print("=" * 65)

    all_data = {}

    for table in TABLES:
        # Запрашиваем распределение по сегментам
        sql = (
            f"SELECT gp_segment_id, COUNT(*) "
            f"FROM {table} "
            f"GROUP BY gp_segment_id "
            f"ORDER BY gp_segment_id"
        )
        raw = psql_query(sql)

        segments = {}
        total = 0
        for line in raw.strip().split("\n"):
            if "|" in line:
                seg_id, count = line.split("|")
                segments[int(seg_id)] = int(count)
                total += int(count)

        all_data[table] = {"segments": segments, "total": total}

        # Вычисляем перекос (skew)
        if total > 0 and len(segments) >= 2:
            max_seg = max(segments.values())
            min_seg = min(segments.values())
            skew_pct = ((max_seg - min_seg) / total) * 100
        else:
            skew_pct = 0

        # Выводим результат
        print(f"\n{table} (всего: {total:,}, skew: {skew_pct:.1f}%)")
        print(f"  Ключ: {get_dist_key(table)}")
        for seg_id in sorted(segments.keys()):
            count = segments[seg_id]
            pct = (count / total * 100) if total > 0 else 0
            bar = "#" * int(pct / 2)
            print(f"  Segment {seg_id}: {count:>10,} ({pct:5.1f}%) {bar}")

    # Строим ASCII-диаграмму
    print("\n" + "=" * 65)
    print("  Диаграмма распределения (% строк на каждом сегменте)")
    print("=" * 65)
    print(f"{'Таблица':<22} {'Seg0':>6} {'Seg1':>6}  Баланс")
    print("-" * 65)
    for table, data in all_data.items():
        segs = data["segments"]
        total = data["total"]
        if total == 0:
            continue
        s0 = segs.get(0, 0)
        s1 = segs.get(1, 0)
        p0 = s0 / total * 100
        p1 = s1 / total * 100
        bar0 = "#" * int(p0 / 5)
        bar1 = "#" * int(p1 / 5)
        balance = "OK" if abs(p0 - p1) < 10 else "SKEW!"
        print(f"{table:<22} {p0:5.1f}% {p1:5.1f}%  {bar0}|{bar1}  {balance}")

    print("\nOK = разница < 10%, SKEW = перекос > 10%")
    print("Идеальное распределение: 50%|50%")


def get_dist_key(table):
    """Возвращает описание ключа дистрибьюции для таблицы."""
    keys = {
        "products": "DISTRIBUTED RANDOMLY",
        "website_sessions": "DISTRIBUTED BY (website_session_id)",
        "website_pageviews": "DISTRIBUTED BY (website_session_id)",
        "orders": "DISTRIBUTED BY (website_session_id)",
        "order_items": "DISTRIBUTED BY (order_id)",
        "order_item_refunds": "DISTRIBUTED BY (order_id)",
    }
    return keys.get(table, "unknown")


if __name__ == "__main__":
    main()
