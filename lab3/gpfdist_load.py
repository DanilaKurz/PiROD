"""
Шаг 6 - Загрузка CSV через gpfdist (усложненный вариант).

Что такое gpfdist:
  HTTP-сервер от GreenPlum, раздает CSV-файлы. Каждый сегмент
  параллельно тянет свою порцию файла -> самый быстрый способ загрузки.

Схема:
  CSV-файл -> gpfdist (HTTP) -> Segment1 (параллельно)
                              -> Segment2 (параллельно)

Что делает этот скрипт:
1. Проверяет что gpfdist-контейнер работает и отдает файлы
2. Создает External Table через gpfdist:// протокол
3. Загружает данные в GP-таблицу (демонстрация на website_sessions)
4. Сравнивает время загрузки gpfdist vs PXF

Соответствует пункту задания:
  "Усложненный вариант: gpfdist Dockerfile + External Table для CSV"

Примечание: gpfdist-контейнер уже описан в docker-compose.yml
и использует образ woblerr/greenplum:6.26.4 с entrypoint:
  gpfdist -d /data -p 8080
"""
import subprocess
import sys
import time
import os

# Предотвращаем конвертацию путей в Git Bash на Windows
ENV = {**os.environ, "MSYS_NO_PATHCONV": "1"}


def psql_exec(sql, database="toystore"):
    """Выполняет SQL-запрос в GreenPlum и возвращает вывод."""
    result = subprocess.run(
        ["docker", "exec", "-u", "gpadmin", "master", "bash", "-c",
         f"source ~/.bashrc && psql -d {database} -c \"{sql}\""],
        capture_output=True, text=True, timeout=300, env=ENV,
    )
    output = ""
    if result.stdout:
        output += result.stdout
    if result.stderr:
        for line in result.stderr.strip().split("\n"):
            if line and "NOTICE" not in line:
                output += line + "\n"
    return output


def main():
    # 1. Проверяем что gpfdist работает
    print("=== 1. Проверяем gpfdist ===")
    # gpfdist - не обычный HTTP, работает только с GP-сегментами (нужен X-GP-PROTO)
    # Проверяем что контейнер запущен и файлы на месте
    result = subprocess.run(
        ["docker", "exec", "gpfdist", "bash", "-c",
         "ls /data/website_sessions.csv && echo OK"],
        capture_output=True, text=True, timeout=10, env=ENV,
    )
    if "OK" in result.stdout:
        print("  gpfdist контейнер работает, CSV-файлы доступны")
    else:
        print("  ОШИБКА: gpfdist не работает или файлы не найдены")
        return

    # 2. Создаем External Table через gpfdist
    print("\n=== 2. Создаем External Table через gpfdist ===")
    print("  Протокол gpfdist:// - сегменты параллельно тянут данные")

    psql_exec("DROP EXTERNAL TABLE IF EXISTS ext_gpfdist_sessions;")

    output = psql_exec(
        "CREATE EXTERNAL TABLE ext_gpfdist_sessions ("
        "  website_session_id INT,"
        "  created_at TIMESTAMP,"
        "  user_id INT,"
        "  is_repeat_session INT,"
        "  utm_source TEXT,"
        "  utm_campaign TEXT,"
        "  utm_content TEXT,"
        "  device_type TEXT,"
        "  http_referer TEXT"
        ") LOCATION ('gpfdist://gpfdist:8080/website_sessions.csv')"
        " FORMAT 'CSV' (HEADER);"
    )
    print(output)

    # 3. Проверяем что External Table читает данные
    print("=== 3. Проверяем чтение через gpfdist ===")
    output = psql_exec("SELECT COUNT(*) as rows_via_gpfdist FROM ext_gpfdist_sessions;")
    print(output)

    # 4. Загружаем в отдельную таблицу и замеряем время
    print("=== 4. Загрузка через gpfdist (замер времени) ===")
    psql_exec("DROP TABLE IF EXISTS sessions_via_gpfdist;")
    psql_exec(
        "CREATE TABLE sessions_via_gpfdist ("
        "  website_session_id INT,"
        "  created_at TIMESTAMP,"
        "  user_id INT,"
        "  is_repeat_session INT,"
        "  utm_source TEXT,"
        "  utm_campaign TEXT,"
        "  utm_content TEXT,"
        "  device_type TEXT,"
        "  http_referer TEXT"
        ") DISTRIBUTED BY (website_session_id);"
    )

    start = time.time()
    output = psql_exec("INSERT INTO sessions_via_gpfdist SELECT * FROM ext_gpfdist_sessions;")
    gpfdist_time = time.time() - start
    print(f"  gpfdist: {gpfdist_time:.2f} сек")
    print(output)

    # 5. Сравниваем с PXF
    print("=== 5. Сравнение: gpfdist vs PXF ===")
    psql_exec("DROP TABLE IF EXISTS sessions_via_pxf;")
    psql_exec(
        "CREATE TABLE sessions_via_pxf ("
        "  website_session_id INT,"
        "  created_at TIMESTAMP,"
        "  user_id INT,"
        "  is_repeat_session INT,"
        "  utm_source TEXT,"
        "  utm_campaign TEXT,"
        "  utm_content TEXT,"
        "  device_type TEXT,"
        "  http_referer TEXT"
        ") DISTRIBUTED BY (website_session_id);"
    )

    start = time.time()
    output = psql_exec("INSERT INTO sessions_via_pxf SELECT * FROM ext_website_sessions;")
    pxf_time = time.time() - start
    print(f"  PXF (Oracle): {pxf_time:.2f} сек")
    print(output)

    print(f"\n  gpfdist: {gpfdist_time:.2f} сек")
    print(f"  PXF:     {pxf_time:.2f} сек")
    if gpfdist_time < pxf_time:
        print(f"  gpfdist быстрее в {pxf_time/gpfdist_time:.1f}x раз!")
    else:
        print(f"  PXF быстрее в {gpfdist_time/pxf_time:.1f}x раз (Oracle кеширует)")

    print("\n  gpfdist быстрее т.к. читает CSV напрямую по HTTP,")
    print("  а PXF идет через Java -> JDBC -> Oracle -> сеть.")

    # 6. Проверяем количество
    print("\n=== 6. Проверка ===")
    output = psql_exec(
        "SELECT 'gpfdist' as method, COUNT(*) as rows FROM sessions_via_gpfdist "
        "UNION ALL "
        "SELECT 'pxf', COUNT(*) FROM sessions_via_pxf "
        "UNION ALL "
        "SELECT 'original', COUNT(*) FROM website_sessions;"
    )
    print(output)

    # Удаляем временные таблицы
    psql_exec("DROP TABLE IF EXISTS sessions_via_gpfdist;")
    psql_exec("DROP TABLE IF EXISTS sessions_via_pxf;")
    psql_exec("DROP EXTERNAL TABLE IF EXISTS ext_gpfdist_sessions;")

    print("=== Готово! ===")


if __name__ == "__main__":
    main()
