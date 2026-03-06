"""
Шаг 3.1 - Настройка PXF (Platform eXtension Framework) на GreenPlum master.

Что делает этот скрипт:
1. Скачивает Oracle JDBC-драйвер (ojdbc8.jar) в PXF
   - PXF написан на Java, ему нужен драйвер чтобы подключаться к Oracle
2. Создает конфигурацию PXF-сервера 'oracle' (jdbc-site.xml)
   - Указывает адрес Oracle, логин/пароль
3. Синхронизирует конфиг на все сегменты (pxf cluster sync)
   - PXF работает на каждом сегменте, поэтому конфиг нужен везде
4. Перезапускает PXF (pxf cluster restart)

Соответствует пункту задания:
  "Загрузить данные через PXF из дополнительного хранилища (Oracle)"
"""
import subprocess
import sys


def docker_exec(command, user="gpadmin"):
    """Выполняет команду внутри контейнера master от указанного пользователя."""
    result = subprocess.run(
        ["docker", "exec", "-u", user, "master", "bash", "-c", command],
        capture_output=True, text=True, timeout=120,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return result.returncode


def main():
    # 1. Скачиваем Oracle JDBC-драйвер
    print("=== 1. Скачиваем Oracle JDBC-драйвер (ojdbc8.jar) ===")
    print("PXF - Java-сервис, ему нужен JDBC-драйвер для подключения к Oracle")
    rc = docker_exec(
        "source ~/.bashrc && "
        "mkdir -p $PXF_BASE/lib && "
        "wget -q -O $PXF_BASE/lib/ojdbc8.jar "
        "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar && "
        "ls -lh $PXF_BASE/lib/ojdbc8.jar"
    )
    if rc != 0:
        print("ОШИБКА: не удалось скачать JDBC-драйвер")
        return

    # 2. Создаем конфигурацию PXF-сервера 'oracle'
    print("\n=== 2. Создаем конфигурацию PXF-сервера 'oracle' ===")
    print("Файл jdbc-site.xml указывает PXF как подключаться к Oracle:")
    print("  - Драйвер: oracle.jdbc.driver.OracleDriver")
    print("  - URL: jdbc:oracle:thin:@oracle:1521/XEPDB1")
    print("  - Пользователь: toystore")

    jdbc_site_xml = r"""<?xml version="1.0" encoding="UTF-8"?>
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
</configuration>"""

    rc = docker_exec(
        "source ~/.bashrc && "
        "mkdir -p $PXF_BASE/servers/oracle && "
        f"cat > $PXF_BASE/servers/oracle/jdbc-site.xml << 'XMLEOF'\n{jdbc_site_xml}\nXMLEOF"
    )
    if rc != 0:
        print("ОШИБКА: не удалось создать jdbc-site.xml")
        return
    print("jdbc-site.xml создан")

    # 3. Синхронизируем конфиг на все сегменты
    print("\n=== 3. Синхронизируем конфиг на сегменты ===")
    print("PXF работает на каждом сегменте - конфиг нужен везде")
    rc = docker_exec("source ~/.bashrc && pxf cluster sync")
    if rc != 0:
        print("ОШИБКА: pxf cluster sync не удался")
        return

    # 4. Перезапускаем PXF
    print("\n=== 4. Перезапускаем PXF ===")
    docker_exec("source ~/.bashrc && pxf cluster stop 2>/dev/null; pxf cluster start")

    print("\n=== PXF настроен! ===")
    print("Теперь GreenPlum может читать данные из Oracle через External Tables")


if __name__ == "__main__":
    main()
