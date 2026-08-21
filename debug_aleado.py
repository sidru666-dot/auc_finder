#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Диагностика подключения к платному фиду aleado (FTP + BZip2 + MySQL-дамп).

Проверяет весь путь по шагам и на каждом шаге печатает понятный результат
по-русски, чтобы было видно, где именно затык, если что-то не работает.

ПЕРЕД запуском задайте переменные окружения (см. README, раздел
"Платный фид aleado"):

    export ALEADO_FTP_HOST="..."       # адрес, который подтвердит поставщик
    export ALEADO_FTP_PORT="21"
    export ALEADO_FTP_USER="jmmoto_user"
    export ALEADO_FTP_PASSWORD="..."
    # если знаете точное имя файла на FTP - укажите, иначе бот попробует
    # найти *.bz2 сам:
    # export ALEADO_FTP_FILENAME="dump_20260820.sql.bz2"

    export MYSQL_HOST="127.0.0.1"
    export MYSQL_USER="aleado_feed"
    export MYSQL_PASSWORD="..."        # пароль от ЛОКАЛЬНОГО MySQL/MariaDB
                                        # (не от FTP!) — см. README про его
                                        # установку и создание пользователя

Запуск:
    python3 debug_aleado.py

Пришлите мне вывод целиком, если что-то не сработало.
"""
import os
import sys

import aleado_client as ac


def line():
    print("=" * 78)


def main():
    line()
    print("Шаг 1: подключение к FTP")
    print("Хост:", ac.FTP_HOST or "(НЕ ЗАДАН — задайте ALEADO_FTP_HOST)")
    print("Порт:", ac.FTP_PORT)
    print("Пользователь:", ac.FTP_USER or "(не задан)")
    print("Папка на FTP:", ac.FTP_DIR)
    print("Имя файла:", ac.FTP_FILENAME or "(не задано — ищу сам самый свежий *.bz2)")

    if not ac.FTP_HOST or not ac.FTP_USER:
        print()
        print("ОШИБКА: не заданы ALEADO_FTP_HOST / ALEADO_FTP_USER. Задайте переменные")
        print("окружения (см. шапку файла) и запустите заново.")
        sys.exit(1)

    tmp_dir = "/tmp/aleado_debug"
    os.makedirs(tmp_dir, exist_ok=True)

    try:
        bz2_path = ac.download_dump(tmp_dir)
    except Exception as e:
        print()
        print("ОШИБКА подключения/скачивания:", repr(e))
        print()
        print("Частые причины:")
        print("  - неверный хост/порт (DNS не резолвится, тайм-аут) — уточните у")
        print("    поставщика точный адрес;")
        print("  - неверный логин/пароль;")
        print("  - IP этого сервера не в whitelist'е у поставщика — спросите его,")
        print("    нужно ли сообщать им IP сервера;")
        print("  - файл называется не так, как мы угадали — задайте")
        print("    ALEADO_FTP_FILENAME точным именем (спросите у поставщика).")
        sys.exit(1)

    print("OK, скачано:", bz2_path)

    line()
    print("Шаг 2: распаковка BZip2")
    try:
        sql_path = ac.extract_bz2(bz2_path)
    except Exception as e:
        print("ОШИБКА распаковки:", repr(e))
        print("Возможно, файл на FTP на самом деле не .bz2, а что-то другое —")
        print("посмотрите первые байты файла (file", bz2_path, ") и пришлите мне.")
        sys.exit(1)
    print("OK, распаковано:", sql_path, "({} байт)".format(os.path.getsize(sql_path)))

    line()
    print("Шаг 3: импорт в локальный MySQL/MariaDB")
    print("MYSQL_HOST={} MYSQL_DB={} MYSQL_USER={}".format(ac.MYSQL_HOST, ac.MYSQL_DB, ac.MYSQL_USER))
    try:
        ac.import_dump(sql_path)
    except Exception as e:
        print("ОШИБКА импорта:", repr(e))
        print()
        print("Частые причины:")
        print("  - MySQL/MariaDB не установлен или не запущен на сервере;")
        print("  - неверные MYSQL_USER/MYSQL_PASSWORD, или у пользователя нет прав")
        print("    создавать базы/таблицы — см. README, раздел про создание")
        print("    пользователя MySQL для этого бота;")
        print("  - команда `mysql` не найдена в PATH.")
        sys.exit(1)
    print("OK, дамп импортирован в базу", ac.MYSQL_DB)

    line()
    print("Шаг 4: какие таблицы и колонки реально получились")
    for table in ac.TABLES:
        columns = ac._describe_columns(table)
        if not columns:
            print("Таблица {!r}: НЕ НАЙДЕНА в дампе после импорта.".format(table))
            print("  Проверьте ALEADO_TABLES — может, в дампе она называется иначе.")
            continue
        resolved, extra_grades = ac.resolve_columns(columns)
        print("Таблица {!r}: {} колонок: {}".format(table, len(columns), columns))
        print("  Сопоставление с полями бота:")
        for canon, col in resolved.items():
            mark = col if col else "?? НЕ НАЙДЕНО (проверьте FIELD_CANDIDATES в aleado_client.py)"
            print("    {:<16} -> {}".format(canon, mark))
        if extra_grades:
            print("  Похоже на доп. оценки узлов (покажем в уведомлении отдельно):", extra_grades)

        rows = ac._fetch_rows(table, columns)
        print("  Строк в таблице:", len(rows))
        if rows:
            print("  Пример первой строки:")
            for k, v in rows[0].items():
                print("    {}: {!r}".format(k, v))

    line()
    print("Готово. Если сопоставление полей (Шаг 4) выглядит правильно — можно")
    print("деплоить/перезапускать бота (см. README, раздел про Railway).")


if __name__ == "__main__":
    main()
