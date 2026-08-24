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
    # "bid" — реальное имя в дампе aleado для номера лота (напр. "0260",
    # используется и в ссылках на фото); ничего общего со ставкой на
    # торгах, несмотря на название.
    "lot_number": ["lot_number", "lot_no", "lotno", "number", "lot", "bid"],
    "auction_date": ["date", "auction_date", "sale_date", "aukцион_date", "adate"],
    "auction_name": ["auction", "auction_name", "auction_house", "place"],
    # "company_en" — реальное имя колонки с маркой в дампе aleado
    # (значения вида "Honda"/"Yamaha"), подтверждено по примеру строки.
    "brand": ["make", "brand", "marka", "maker", "manufacturer", "company_en"],
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
    # "scores_en" — реальная общая оценка лота (значения вида "6"); идёт
    # первым, чтобы точный матч по имени колонки перебил менее точное
    # совпадение по подстроке "grade" с "model_grade_en" (которая у
    # aleado почти всегда пустая — это отдельное текстовое поле, не
    # общая оценка).
    "grade_overall": ["scores_en", "grade", "overall_grade", "score", "rank", "rating"],
    "result": ["result", "sale_result", "status", "torgi_result"],
    "vin": ["vin", "chassis", "chassis_no", "frame_no"],
    # "pictures" — реальное имя колонки в дампе aleado со ссылками на
    # фото лота (подтверждено по примеру строки): значение — список
    # ссылок вида https://p3.aleado.com/p3/MotoPicGetter?...&npic=N&url=,
    # склеенных через "#" (не запятую!) — см. lot_photo() в bot_server.py,
    # которая это разбирает. Указано первым явным кандидатом, чтобы точный
    # матч по имени колонки не зависел от порядка колонок в таблице (без
    # этого разрешение подстрокой могло в теории зацепить не ту колонку).
    "photo_url": [
        "pictures", "photo", "photo_url", "photos", "image", "img",
        "picture", "pic_url", "pics",
    ],
    # "parsed_data_en"/"parsed_data_ru" — реальные поля aleado с разбором
    # состояния лота (структурированный текст: пояснения по узлам,
    # ссылки на видео, история цен и т.п.), английская и русская версии.
    "description_en": ["description_en", "desc_en", "comment_en", "note_en", "parsed_data_en"],
    "description_ru": [
        "description_ru", "desc_ru", "comment_ru", "note_ru", "description", "comment",
        "parsed_data_ru",
    ],
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
    "lots": [],     # готовые словари лотов (канонические поля), посчитано один раз за обновление
    "brand_models": {},  # brand.lower() -> [model, ...] отсортировано по алфавиту, посчитано один раз за обновление
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


# MySQL-плагин на Railway отдаёт самоподписанный сертификат, а клиент
# `mysql` (в отличие от PyMySQL, который по умолчанию SSL не требует)
# по умолчанию пытается его проверить и падает с "TLS/SSL error:
# self-signed certificate in certificate chain". Соединение и так идёт
# по приватной сети внутри одного проекта Railway (MYSQL_HOST=
# *.railway.internal, наружу не торчит), так что отключать проверку
# сертификата тут безопасно — но конкретное имя опции для этого
# отличается между MySQL-клиентом и разными версиями MariaDB-клиента
# (какой именно окажется в образе — заранее не известно, `default-
# mysql-client` в apt на разных дистрибутивах резолвится по-разному), и
# неверное имя опции — это фатальная ошибка запуска ("unknown
# variable"), а не мягкое предупреждение. Поэтому пробуем по очереди
# несколько вариантов и останавливаемся на первом, который клиент
# принял (после этого либо соединение уже сработало, либо упало по
# другой причине, не связанной с именем опции — тогда дальше пробовать
# бессмысленно).
_SSL_DISABLE_VARIANTS = (
    "ssl-mode=DISABLED",       # MySQL 8.x клиент, MariaDB-клиент 10.4+
    "ssl=0",                   # старый универсальный булевский флаг
    "ssl-verify-server-cert=0",  # старый MariaDB-клиент без ssl-mode
    "",                        # совсем без опции — как было изначально
)


def _mysql_defaults_file(ssl_line=""):
    """Временный --defaults-extra-file с паролем, чтобы не светить пароль
    в списке процессов (ps aux) и не спрашивать его интерактивно."""
    fd, path = tempfile.mkstemp(prefix="aleado_mysql_", suffix=".cnf")
    with os.fdopen(fd, "w") as f:
        f.write("[client]\n")
        f.write("host={}\n".format(MYSQL_HOST))
        f.write("port={}\n".format(MYSQL_PORT))
        f.write("user={}\n".format(MYSQL_USER))
        f.write("password={}\n".format(MYSQL_PASSWORD))
        if ssl_line:
            f.write(ssl_line + "\n")
    os.chmod(path, 0o600)
    return path


# Жёсткий потолок на импорт дампа через клиент mysql (subprocess). Без
# этого таймаута зависший клиент (например, ждёт metadata lock на
# таблице от чужого/старого соединения — такое бывает буквально в
# несколько секунд перехлёста при редеплое, когда старый контейнер ещё
# не до конца отключился от MySQL) блокировал бы _do_refresh() НАВСЕГДА,
# а вместе с ним — весь бот (см. комментарий у _state_lock: пока
# ensure_fresh() держит lock, ЛЮБОЙ клик в Telegram, которому нужны
# данные фида, тоже виснет навечно). Лучше через несколько минут явно
# провалить это обновление (bot.get_lots() и т.п. просто отдадут
# последний удачный кэш) и попробовать снова на следующем цикле, чем
# подвесить бота целиком без возможности восстановиться самому.
MYSQL_IMPORT_TIMEOUT_SECONDS = 240


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
    try:
        out, err = proc.communicate(input=stdin_data, timeout=MYSQL_IMPORT_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        raise RuntimeError(
            "mysql не ответил за {} сек. (похоже, завис на блокировке таблицы) — прервано".format(
                MYSQL_IMPORT_TIMEOUT_SECONDS
            )
        )
    finally:
        if sql_file:
            cmd_stdin.close()
    if proc.returncode != 0:
        raise RuntimeError("mysql завершился с ошибкой: {}".format(err.decode("utf-8", "replace")))
    return out.decode("utf-8", "replace")


# Кэш подобранного варианта на процесс, чтобы не перебирать заново на
# каждое обновление фида — как только один вариант сработал (или мы
# перебрали все и остался последний), используем его дальше.
_ssl_variant_cache = {"value": None}


def _run_mysql_ssl_aware(**kwargs):
    """То же самое, что и последовательность _mysql_defaults_file +
    _run_mysql, но с перебором _SSL_DISABLE_VARIANTS при ошибке вида
    "unknown variable" (клиент не знает такое имя опции — пробуем
    следующий вариант). Любая другая ошибка (неверный пароль, нет сети
    и т.п.) сразу пробрасывается дальше, перебор не имеет смысла."""
    variants = (
        [_ssl_variant_cache["value"]]
        if _ssl_variant_cache["value"] is not None
        else list(_SSL_DISABLE_VARIANTS)
    )
    last_exc = None
    for i, ssl_line in enumerate(variants):
        defaults_file = _mysql_defaults_file(ssl_line)
        try:
            result = _run_mysql(defaults_file=defaults_file, **kwargs)
            _ssl_variant_cache["value"] = ssl_line
            return result
        except RuntimeError as e:
            last_exc = e
            is_last = i == len(variants) - 1
            if "unknown variable" in str(e).lower() and not is_last:
                continue
            raise
        finally:
            os.unlink(defaults_file)
    raise last_exc


def import_dump(sql_path):
    """Создаёт базу MYSQL_DB (если её нет) и импортирует туда дамп.
    Дампы mysqldump обычно сами делают DROP TABLE IF EXISTS + CREATE
    TABLE, так что повторный импорт при каждом обновлении — это просто
    полная замена данных на свежие, а не накопление дублей."""
    _run_mysql_ssl_aware(
        sql_text="CREATE DATABASE IF NOT EXISTS `{}` CHARACTER SET utf8mb4;".format(MYSQL_DB),
    )
    _run_mysql_ssl_aware(sql_file=sql_path, database=MYSQL_DB)


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


def _build_lot(table, resolved, extra_grade_cols, raw):
    lot = {"_table": table, "_raw": raw}
    for canon, col in resolved.items():
        lot[canon] = raw.get(col) if col else None
    lot["_extra_grades"] = {col: raw.get(col) for col in extra_grade_cols if raw.get(col)}
    return lot


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
            if missing:
                # Печатаем реальный список колонок и первую строку целиком —
                # без этого не понять, как называется, например, марка в
                # дампе конкретного поставщика, чтобы дописать её в
                # FIELD_CANDIDATES.
                log.info("aleado: таблица %s — все колонки: %s", table, columns)
                if rows:
                    log.info("aleado: таблица %s — первая строка: %s", table, rows[0])

        # Разбираем "сырые" строки в канонические словари лотов И считаем
        # индекс марка -> модели ОДИН РАЗ здесь, при обновлении фида — а
        # не при каждом обращении пользователя. Раньше get_lots()
        # пересобирал все 14000+ строк заново на КАЖДЫЙ вызов (а его
        # вызывает и /watch при выборе марки, и фоновая проверка, и
        # предпросмотр), и то же самое отдельно повторял
        # fetch_model_candidates() в bot_server.py, чтобы выбрать модели
        # под марку — то есть один клик по кнопке марки мог означать
        # двойной проход по всем строкам. Теперь и список лотов, и
        # готовый индекс по маркам считаются один раз за обновление и
        # переиспользуются без пересчёта.
        all_lots = []
        brand_model_counts = {}  # brand_lower -> {model_name: count}
        for table, info in new_tables.items():
            resolved = info["resolved"]
            extra_grade_cols = info["extra_grades"]
            for raw in info["rows"]:
                lot = _build_lot(table, resolved, extra_grade_cols, raw)
                all_lots.append(lot)
                brand = (lot.get("brand") or "").strip()
                model = (lot.get("model") or "").strip()
                if not brand or not model:
                    continue
                counts = brand_model_counts.setdefault(brand.lower(), {})
                counts[model] = counts.get(model, 0) + 1

        # --- ВРЕМЕННО: диагностика бага "цена в боте ×1000 меньше
        # реальной" (жалоба: Yamaha MT-07-3, VIN RM48J-000372 /
        # RM50J-000427 — бот показал ¥600 вместо ¥600 000). Гипотеза:
        # разные аукционные дома в дампе aleado указывают цену в разных
        # единицах (например, в тысячах иен вместо иен) — тогда медиана
        # цены по конкретному auction_name будет в ~1000 раз меньше, чем
        # у остальных. Логируем медиану/мин/макс цены по каждому
        # auction_name (топ-30 по количеству строк) + сырые данные для
        # конкретных проблемных VIN — уберём после того, как найдём
        # причину.
        try:
            _price_by_auction = {}
            _target_vins = {"rm48j-000372", "rm50j-000427"}
            for lot in all_lots:
                raw_price = lot.get("end_price") or lot.get("start_price")
                try:
                    val = float(str(raw_price).replace(",", "").strip())
                except (TypeError, ValueError):
                    val = None
                auction = (lot.get("auction_name") or "?").strip() or "?"
                if val is not None:
                    _price_by_auction.setdefault(auction, []).append(val)
                vin = (lot.get("vin") or "").strip().lower()
                if vin in _target_vins:
                    log.info(
                        "DEBUG_PRICE: найден лот VIN=%r таблица=%r auction_name=%r "
                        "start_price=%r end_price=%r result=%r сырая_строка=%r",
                        lot.get("vin"), lot.get("_table"), lot.get("auction_name"),
                        lot.get("start_price"), lot.get("end_price"), lot.get("result"),
                        lot.get("_raw"),
                    )
            for auction, vals in sorted(
                _price_by_auction.items(), key=lambda kv: -len(kv[1])
            )[:30]:
                vals_sorted = sorted(vals)
                n = len(vals_sorted)
                log.info(
                    "DEBUG_PRICE: auction_name=%r n=%d min=%.0f median=%.0f max=%.0f",
                    auction, n, vals_sorted[0], vals_sorted[n // 2], vals_sorted[-1],
                )
        except Exception:
            log.exception("DEBUG_PRICE: диагностика упала")
        # --- конец временной диагностики

        # Раньше модели сортировались по частоте встречаемости (самые
        # ходовые — первыми). Живой пользователь попросил алфавитный
        # порядок — "если знаешь что хочешь XSR, листаешь в конец" —
        # по частоте так не сориентироваться, а по алфавиту это
        # предсказуемо. Сортируем по НОРМАЛИЗОВАННОМУ виду (пробелы
        # схлопнуты, повторяющаяся марка спереди срезана), а не по
        # сырой строке из фида — так порядок совпадает с тем, что
        # реально показано на кнопках (см. model_button_label в
        # bot_server.py), а не с сырым текстом вроде " TRIUMPH  TIGER
        # 900 RALLY PRO" (там алфавитный порядок по сырой строке был бы
        # бессмысленным — она вся начиналась бы на "TRIUMPH").
        def _model_sort_key(model, brand_lower):
            m = re.sub(r"\s+", " ", (model or "").strip())
            if m.upper().startswith(brand_lower.upper()):
                rest = m[len(brand_lower):].lstrip(" -_/")
                if rest:
                    m = rest
            return m.upper()

        brand_models = {
            brand_lower: sorted(counts.keys(), key=lambda m: _model_sort_key(m, brand_lower))
            for brand_lower, counts in brand_model_counts.items()
        }

        # ВАЖНО: _do_refresh() всегда вызывается из ensure_fresh() уже
        # ВНУТРИ "with _state_lock:" (см. ниже) — тот же самый поток тут
        # повторно брать _state_lock НЕ должен. threading.Lock() не
        # реентрантный: повторный "with _state_lock:" тем же потоком —
        # это не "подождать своей же очереди", а самозаклинивание навечно
        # (поток ждёт снятия блокировки, которую сам же держит и снять не
        # может, пока не выйдет из этого with — а выйти не может, пока не
        # дождётся). Именно это и произошло в проде: лог "таблица ...
        # строк" успевал напечататься (он ДО этого места), а дальше поток
        # зависал тут насмерть — _cache["ts"] так и не проставлялся,
        # ensure_fresh() у всех остальных (в т.ч. по клику "Марка: ...")
        # висел бы вечно в очереди на тот же lock. Раньше здесь стоял
        # "with _state_lock:" — это и была причина зависаний "Ищу
        # модели..." на много минут. Пишем в кэш без повторного захвата.
        _cache["tables"] = new_tables
        _cache["lots"] = all_lots
        _cache["brand_models"] = brand_models
        _cache["ts"] = time.time()
        _cache["error"] = None
    except Exception as e:
        log.exception("aleado: обновление фида не удалось")
        _cache["error"] = str(e)
        raise
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def ensure_fresh():
    """Обновляет кэш, если он устарел (или его ещё не было). Возвращает
    True, если после вызова в кэше есть данные (пусть и не самые
    свежие — при ошибке обновления сохраняем последние удачные).

    ВАЖНО: сам _do_refresh() выполняется ВНУТРИ блокировки _state_lock,
    а не после её отпускания — раньше здесь была ошибка, из-за которой
    два потока, одновременно увидевшие устаревший кэш (например,
    фоновая проверка по таймеру и одновременно чей-то клик по кнопке в
    Telegram), запускали ДВА параллельных обновления: каждый качал дамп
    с FTP и импортировал его в MySQL сам по себе, и эти два импорта
    мешали друг другу (гонка за одни и те же таблицы), из-за чего
    рядовое обновление, которое должно занимать секунды-десятки секунд,
    растягивалось на несколько минут — именно это выглядело как
    "зависание" бота. Теперь при устаревшем кэше все потоки встают в
    очередь на один и тот же lock и просто ждут результата ОДНОГО
    обновления, а не начинают каждый своё."""
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
    FIELD_CANDIDATES) из одной или всех настроенных таблиц.

    Список уже готов в кэше (посчитан один раз в _do_refresh() при
    каждом обновлении фида) — здесь только отдаём его, без повторной
    пересборки на каждый вызов."""
    ensure_fresh()
    if table is None:
        return _cache["lots"]
    return [lot for lot in _cache["lots"] if lot.get("_table") == table]


def get_brand_models(brand, limit=None):
    """Названия моделей марки brand, отсортированные по алфавиту (по
    нормализованному виду — см. _model_sort_key в _do_refresh) — из
    готового индекса, посчитанного один раз при обновлении фида (см.
    _do_refresh), вместо повторного прохода по всем лотам на каждый
    клик. Раньше сортировка была по частоте встречаемости, но так
    сложно ориентироваться, зная точное название модели — алфавитный
    порядок предсказуемее."""
    ensure_fresh()
    models = _cache["brand_models"].get(brand.strip().lower(), [])
    return models[:limit] if limit else list(models)
