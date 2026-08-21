#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Клиент платного фида аукционных данных aleado (логин jmmoto_user).

Формат фида (уточнено у поставщика 20.08.2026): на FTP лежит BZip2-архив
с готовым MySQL-дампом (mysqldump). После распаковки — обычный .sql файл
с CREATE TABLE + INSERT INTO для таблицы(-иц) вида moto_lots_ns. Поля
(по словам поставщика): ID лота, дата, номер лота, аукцион, марка,
модель, год, тип кузова/рамы, пробег, объём двигателя, начальная и
конечная цена, цвет, оценки узлов, результат торгов, VIN, ссылки на
фотографии, описания на английском/русском.

Мы НЕ угадываем точные имена колонок — вместо этого:
  1. скачиваем .bz2 с FTP,
  2. распаковываем,
  3. импортируем .sql в локальную MySQL/MariaDB (это единственный
     по-настоящему надёжный способ разобрать дамп: он умеет всё, что
     умеет сам mysqldump/INSERT — многострочные значения, экранирование,
     BLOB/HEX, разные кодировки; свой парсер SQL текстом это всё
     воспроизводить ненадёжно),
  4. смотрим реальные имена колонок через DESCRIBE,
  5. сопоставляем их с "каноническими" полями (марка/модель/год/цена/...)
     по списку вероятных имён (см. FIELD_CANDIDATES) — так бот работает,
     даже если точные названия колонок окажутся не такими, как мы
     предполагаем, и не ломается, если поставщик когда-то добавит/уберёт
     колонку.

Требования к окружению:
  - клиент MySQL (пакет `mysql-client`/`mariadb-client`, команда `mysql`
    в PATH) — нужен ТОЛЬКО для самого импорта дампа (шаг "выполнить этот
    .sql файл целиком"), через subprocess; для этого шага PyMySQL не
    подходит — импорт мультистейтментного дампа с DDL надёжно делает
    именно родной клиент, а не библиотека;
  - после импорта колонки/строки читаются обратно через PyMySQL (см.
    db.py — та же функция подключения, что и для собственных таблиц
    бота);
  - MySQL/MariaDB-сервер, доступный по сети (на Railway — плагин MySQL,
    больше ничего ставить не нужно; см. README).

Настройка через переменные окружения (см. README):
  ALEADO_FTP_HOST, ALEADO_FTP_PORT, ALEADO_FTP_USER, ALEADO_FTP_PASSWORD
  ALEADO_FTP_DIR       — папка на FTP, где лежит архив (по умолчанию "/")
  ALEADO_FTP_FILENAME  — точное имя файла, если оно всегда одно и то же;
                          если не задано, бот сам возьмёт из списка файлов
                          в ALEADO_FTP_DIR тот, что с самой свежей датой
                          изменения (или последний по имени, если сервер
                          не отдаёт даты).
  ALEADO_TABLES        — какие таблицы использовать, через запятую
                          (по умолчанию "moto_lots_ns"). Когда узнаете от
                          поставщика названия таблиц для авто/спецтехники
                          — добавьте их сюда через запятую, код их
                          подхватит без изменений (правда, специфичные
                          для мотоциклов подписи в уведомлениях всё равно
                          стоит будет поправить).
  ALEADO_REFRESH_TTL_SECONDS — как часто перекачивать и переимпортировать
                          дамп (по умолчанию 1800 = 30 минут).
  MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
                        — доступ к ЛОКАЛЬНОМУ MySQL/MariaDB (не путать с
                          FTP-доступом к aleado!). MYSQL_DB по умолчанию
                          "aleado_feed" — база создаётся автоматически,
                          если её ещё нет.
"""

import os
import re
import bz2
import ftplib
import logging
import shutil
import subprocess
import tempfile
import threading
import time

import db

log = logging.getLogger("auction-watcher.aleado")

# --------------------------------------------------------------------------
# Настройки
# --------------------------------------------------------------------------

FTP_HOST = os.environ.get("ALEADO_FTP_HOST", "")
FTP_PORT = int(os.environ.get("ALEADO_FTP_PORT", "21"))
FTP_USER = os.environ.get("ALEADO_FTP_USER", "")
FTP_PASSWORD = os.environ.get("ALEADO_FTP_PASSWORD", "")
FTP_DIR = os.environ.get("ALEADO_FTP_DIR", "/")
FTP_FILENAME = os.environ.get("ALEADO_FTP_FILENAME", "")  # пусто = искать самому

TABLES = [t.strip() for t in os.environ.get("ALEADO_TABLES", "moto_lots_ns").split(",") if t.strip()]

REFRESH_TTL_SECONDS = int(os.environ.get("ALEADO_REFRESH_TTL_SECONDS", "1800"))

# Доступ к MySQL/MariaDB — те же переменные (и тот же фолбэк на
# Railway-style MYSQLHOST/... без подчёркивания), что и в db.py, чтобы
# не настраивать одно и то же дважды.
MYSQL_HOST = db.MYSQL_HOST
MYSQL_PORT = db.MYSQL_PORT
MYSQL_USER = db.MYSQL_USER
MYSQL_PASSWORD = db.MYSQL_PASSWORD
MYSQL_DB = db.MYSQL_DB

# Канонические поля бота -> список вероятных имён колонок в дампе
# (без учёта регистра, проверяется и точное совпадение, и вхождение
# подстроки — см. resolve_columns). Список специально с запасом: лучше
# несколько лишних кандидатов, чем не найти реальную колонку.
FIELD_CANDIDATES = {
    "lot_id": ["id", "lot_id", "id_lot", "lotid", "lot_id_ns"],
    "lot_number": ["lot_number", "lot_no", "lotno", "number", "lot"],
    "auction_date": ["date", "auction_date", "sale_date", "aukцион_date", "adate"],
    "auction_name": ["auction", "auction_name", "auction_house", "place"],
    "brand": ["make", "brand", "marka", "maker", "manufacturer"],
    "model": ["model", "model_name", "modelname"],
    "year": ["year", "model_year", "year_model", "yr"],
    "body_type": ["body_type", "frame_type", "body", "frame", "type"],
    "mileage": ["mileage", "odometer", "run", "millage"],
    "engine_volume": ["engine_volume", "displacement", "engine_cc", "cc", "volume"],
    "start_price": ["start_price", "starting_price", "price_start", "startprice"],
    "end_price": [
        "end_price", "final_price", "sold_price", "price_end",
        "result_price", "endprice", "price",
    ],
    "color": ["color", "colour"],
    "grade_overall": ["grade", "overall_grade", "score", "rank", "rating"],
    "result": ["result", "sale_result", "status", "torgi_result"],
    "vin": ["vin", "chassis", "chassis_no", "frame_no"],
    "photo_url": ["photo", "photo_url", "photos", "image", "img", "picture", "pic_url", "pics"],
    "description_en": ["description_en", "desc_en", "comment_en", "note_en"],
    "description_ru": ["description_ru", "desc_ru", "comment_ru", "note_ru", "description", "comment"],
}

# Колонки, которые дополнительно подсвечиваем в уведомлении, если их имя
# похоже на "ещё одну оценку узла" (например equipment_grade,
# engine_grade и т.п.), но они не попали ни в одно каноническое поле
# выше. Формируется автоматически в resolve_columns().
EXTRA_GRADE_HINTS = ("grade", "score", "балл", "оцен", "rating")

_state_lock = threading.Lock()
_cache = {
    "ts": 0.0,
    "tables": {},   # table_name -> {"columns": [...], "resolved": {...}, "rows": [dict,...]}
    "error": None,
}


# --------------------------------------------------------------------------
# Шаг 1: FTP — найти и скачать архив
# --------------------------------------------------------------------------


def _list_candidates(ftp):
    """Возвращает (все_имена_в_папке, отсортированный_список_bz2_кандидатов
    от самого свежего к самому старому). Логирует полный листинг папки —
    это единственный способ понять, что реально лежит на FTP и что из
    этого видно аккаунту, если потом какой-то конкретный файл откажется
    скачиваться (см. download_dump)."""
    names = ftp.nlst()
    log.info("aleado: листинг папки %r на FTP (%d файлов): %s", FTP_DIR, len(names), names)
    candidates = [n for n in names if n.lower().endswith((".bz2", ".sql.bz2"))]
    if not candidates:
        raise RuntimeError(
            "На FTP в папке {!r} не нашлось файлов *.bz2 (список файлов: {}). "
            "Если имя файла постоянное, задайте его явно через "
            "ALEADO_FTP_FILENAME.".format(FTP_DIR, names)
        )

    # Пробуем сортировать по дате изменения (MDTM), если сервер её отдаёт;
    # если нет — сортируем по имени (часто в имени есть дата: 20260820...).
    def mtime_key(name):
        try:
            resp = ftp.sendcmd("MDTM " + name)
            # "213 20260820120000"
            return resp.split()[-1]
        except Exception:
            return ""

    candidates.sort(key=lambda n: (mtime_key(n), n), reverse=True)
    return names, candidates


def download_dump(local_dir):
    """Скачивает архив с FTP в local_dir, возвращает путь к .bz2 файлу.

    Если конкретное имя не задано через ALEADO_FTP_FILENAME, перебираем
    все найденные *.bz2 от самого свежего к самому старому: некоторые
    файлы в папке могут оказаться недоступны конкретному аккаунту на
    чтение (сервер отвечает "550 ... Permission denied" на RETR при
    полностью успешном логине и листинге — такое бывает, если в одной
    папке лежат дампы разных тарифов/клиентов) — в этом случае просто
    пробуем следующий кандидат, а не падаем сразу."""
    if not FTP_HOST:
        raise RuntimeError("ALEADO_FTP_HOST не задан.")

    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=60)
    ftp.login(FTP_USER, FTP_PASSWORD)
    try:
        if FTP_DIR and FTP_DIR != "/":
            ftp.cwd(FTP_DIR)

        if FTP_FILENAME:
            remote_names = [FTP_FILENAME]
        else:
            _, remote_names = _list_candidates(ftp)

        errors = []
        for remote_name in remote_names:
            local_path = os.path.join(local_dir, os.path.basename(remote_name))
            log.info("aleado: скачиваю %s ...", remote_name)
            try:
                with open(local_path, "wb") as f:
                    ftp.retrbinary("RETR " + remote_name, f.write)
            except ftplib.error_perm as e:
                log.warning(
                    "aleado: файл %s недоступен для скачивания этому аккаунту (%s) — "
                    "пробую следующий кандидат, если есть",
                    remote_name, e,
                )
                errors.append("{}: {}".format(remote_name, e))
                try:
                    os.remove(local_path)
                except OSError:
                    pass
                continue
            log.info("aleado: скачано %s (%d байт)", local_path, os.path.getsize(local_path))
            return local_path

        raise RuntimeError(
            "Ни один из найденных файлов *.bz2 не скачался: {}. "
            "Похоже, аккаунту {!r} не разрешено читать (RETR) ни один из этих файлов на "
            "FTP, хотя логин и листинг папки прошли успешно — уточните у поставщика "
            "точное имя файла для этого аккаунта.".format("; ".join(errors), FTP_USER)
        )
    finally:
        try:
            ftp.quit()
        except Exception:
            ftp.close()


def extract_bz2(bz2_path):
    """Распаковывает .bz2 в .sql рядом, возвращает путь к .sql."""
    sql_path = bz2_path
    for suffix in (".bz2",):
        if sql_path.endswith(suffix):
            sql_path = sql_path[: -len(suffix)]
    if sql_path == bz2_path:
        sql_path = bz2_path + ".sql"

    with bz2.open(bz2_path, "rb") as src, open(sql_path, "wb") as dst:
        while True:
            chunk = src.read(1024 * 1024)
            if not chunk:
                break
            dst.write(chunk)
    log.info("aleado: распаковано в %s (%d байт)", sql_path, os.path.getsize(sql_path))
    return sql_path


# --------------------------------------------------------------------------
# Шаг 2: импорт .sql в локальный MySQL/MariaDB через клиент `mysql`
# --------------------------------------------------------------------------


def _mysql_defaults_file():
    """Временный --defaults-extra-file с паролем, чтобы не светить пароль
    в списке процессов (ps aux) и не спрашивать его интерактивно.

    ssl-mode=DISABLED — потому что MySQL-плагин на Railway отдаёт
    самоподписанный сертификат, а клиент `mysql` (в отличие от PyMySQL,
    который по умолчанию SSL не требует) по умолчанию пытается его
    проверить и падает с "TLS/SSL error: self-signed certificate in
    certificate chain". Соединение и так идёт по приватной сети внутри
    одного проекта Railway (MYSQL_HOST=*.railway.internal, наружу не
    торчит), так что отключать проверку сертификата здесь безопасно."""
    fd, path = tempfile.mkstemp(prefix="aleado_mysql_", suffix=".cnf")
    with os.fdopen(fd, "w") as f:
        f.write("[client]\n")
        f.write("host={}\n".format(MYSQL_HOST))
        f.write("port={}\n".format(MYSQL_PORT))
        f.write("user={}\n".format(MYSQL_USER))
        f.write("password={}\n".format(MYSQL_PASSWORD))
        f.write("ssl-mode=DISABLED\n")
    os.chmod(path, 0o600)
    return path


def _run_mysql(sql_text=None, sql_file=None, database=None, defaults_file=None):
    cmd = ["mysql", "--defaults-extra-file=" + defaults_file]
    if database:
        cmd.append(database)
    stdin_data = None
    if sql_file:
        cmd_stdin = open(sql_file, "rb")
    else:
        cmd_stdin = subprocess.PIPE
        stdin_data = (sql_text or "").encode("utf-8")

    proc = subprocess.Popen(cmd, stdin=cmd_stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate(input=stdin_data)
    if sql_file:
        cmd_stdin.close()
    if proc.returncode != 0:
        raise RuntimeError("mysql завершился с ошибкой: {}".format(err.decode("utf-8", "replace")))
    return out.decode("utf-8", "replace")


def import_dump(sql_path):
    """Создаёт базу MYSQL_DB (если её нет) и импортирует туда дамп.
    Дампы mysqldump обычно сами делают DROP TABLE IF EXISTS + CREATE
    TABLE, так что повторный импорт при каждом обновлении — это просто
    полная замена данных на свежие, а не накопление дублей."""
    defaults_file = _mysql_defaults_file()
    try:
        _run_mysql(
            sql_text="CREATE DATABASE IF NOT EXISTS `{}` CHARACTER SET utf8mb4;".format(MYSQL_DB),
            defaults_file=defaults_file,
        )
        _run_mysql(sql_file=sql_path, database=MYSQL_DB, defaults_file=defaults_file)
    finally:
        os.unlink(defaults_file)


# --------------------------------------------------------------------------
# Шаг 3: читаем таблицы обратно как список словарей
# --------------------------------------------------------------------------


def _describe_columns(table):
    """Реальный список колонок таблицы после импорта — через PyMySQL
    (параметризованный запрос, без склейки имени таблицы прямо в SQL
    для самого условия WHERE; имя таблицы попадает в SQL только как
    значение параметра здесь, не как часть текста запроса)."""
    conn = db._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s "
                "ORDER BY ORDINAL_POSITION",
                (table,),
            )
            return [row["COLUMN_NAME"] for row in cur.fetchall()]
    finally:
        conn.close()


def resolve_columns(available_columns):
    """Сопоставляет канонические поля бота с реальными именами колонок.
    Возвращает (resolved, extra_grade_columns)."""
    lower_map = {c.lower(): c for c in available_columns}
    resolved = {}
    used = set()

    for canon, candidates in FIELD_CANDIDATES.items():
        found = None
        for cand in candidates:
            if cand in lower_map:
                found = lower_map[cand]
                break
        if not found:
            for col_lower, col_orig in lower_map.items():
                if col_orig in used:
                    continue
                if any(cand in col_lower for cand in candidates):
                    found = col_orig
                    break
        resolved[canon] = found
        if found:
            used.add(found)

    extra_grades = []
    for col_lower, col_orig in lower_map.items():
        if col_orig in used:
            continue
        if any(hint in col_lower for hint in EXTRA_GRADE_HINTS):
            extra_grades.append(col_orig)

    return resolved, extra_grades


def _fetch_rows(table, columns):
    """SELECT всех строк таблицы через PyMySQL, возвращает список
    словарей {column: value}. Имя таблицы у нас всегда из ALEADO_TABLES
    (настройка процесса, не пользовательский ввод) и уже проверено через
    _describe_columns (иначе сюда бы не дошли) — но на всякий случай
    ограничиваем его только буквами/цифрами/подчёркиванием, раз оно всё
    равно попадает в текст запроса как идентификатор (для идентификаторов
    таблиц/колонок параметризация в SQL невозможна в принципе — только
    значения можно параметризовать)."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", table):
        raise ValueError("Подозрительное имя таблицы, отказываюсь: {!r}".format(table))
    col_list = ", ".join("`{}`".format(c) for c in columns if re.fullmatch(r"[A-Za-z0-9_]+", c))
    conn = db._connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT {} FROM `{}`".format(col_list, table))
            return cur.fetchall()
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Публичный интерфейс: обновление кэша по таймеру + доступ к лотам
# --------------------------------------------------------------------------


def _refresh_is_fresh():
    return _cache["ts"] and (time.time() - _cache["ts"]) < REFRESH_TTL_SECONDS


def _do_refresh():
    tmp_dir = tempfile.mkdtemp(prefix="aleado_dump_")
    try:
        bz2_path = download_dump(tmp_dir)
        sql_path = extract_bz2(bz2_path)
        import_dump(sql_path)

        new_tables = {}
        for table in TABLES:
            columns = _describe_columns(table)
            if not columns:
                log.warning("aleado: таблица %s не найдена в дампе (после импорта её нет в БД)", table)
                continue
            resolved, extra_grades = resolve_columns(columns)
            rows = _fetch_rows(table, columns)
            new_tables[table] = {
                "columns": columns,
                "resolved": resolved,
                "extra_grades": extra_grades,
                "rows": rows,
            }
            missing = [k for k, v in resolved.items() if not v]
            log.info(
                "aleado: таблица %s — %d строк, %d колонок%s",
                table, len(rows), len(columns),
                ", не нашлось соответствия для: {}".format(missing) if missing else "",
            )

        with _state_lock:
            _cache["tables"] = new_tables
            _cache["ts"] = time.time()
            _cache["error"] = None
    except Exception as e:
        log.exception("aleado: обновление фида не удалось")
        with _state_lock:
            _cache["error"] = str(e)
        raise
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def ensure_fresh():
    """Обновляет кэш, если он устарел (или его ещё не было). Возвращает
    True, если после вызова в кэше есть данные (пусть и не самые
    свежие — при ошибке обновления сохраняем последние удачные)."""
    if _refresh_is_fresh():
        return True
    with _state_lock:
        if _refresh_is_fresh():
            return True
    try:
        _do_refresh()
        return True
    except Exception:
        return bool(_cache["tables"])


def last_error():
    return _cache["error"]


def get_lots(table=None):
    """Список лотов (список словарей с каноническими ключами, см.
    FIELD_CANDIDATES) из одной или всех настроенных таблиц."""
    ensure_fresh()
    tables = [table] if table else TABLES
    out = []
    for t in tables:
        info = _cache["tables"].get(t)
        if not info:
            continue
        resolved = info["resolved"]
        for raw in info["rows"]:
            lot = {"_table": t, "_raw": raw}
            for canon, col in resolved.items():
                lot[canon] = raw.get(col) if col else None
            lot["_extra_grades"] = {
                col: raw.get(col) for col in info["extra_grades"] if raw.get(col)
            }
            out.append(lot)
    return out
