#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Собственное состояние бота (подписки/фильтры и уже отправленные лоты) —
в той же MySQL/MariaDB, куда aleado_client.py импортирует дамп фида. Не
в JSON-файлах на диске: на Railway (и вообще в любом контейнерном
хостинге без подключённого постоянного тома) файловая система
перезапускается вместе с деплоем — значит, JSON-файлы с подписчиками
после каждого редеплоя обнулялись бы. База переживает передеплой сама
по себе, это ровно то, что нужно.

Использует PyMySQL (чистый Python, без компиляции — в отличие от
mysqlclient/mysql-connector, ставится одной командой pip на любой
платформе) и параметризованные запросы — то есть без ручной склейки SQL
из пользовательского текста (маркой/моделью пользователь может ввести
что угодно вручную кнопкой "Указать текстом", это внешний ввод).

Настройки — те же переменные окружения, что и у aleado_client.py
(MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DB), с фолбэком
на переменные, которые Railway сам прописывает при подключении плагина
MySQL (MYSQLHOST/MYSQLPORT/MYSQLUSER/MYSQLPASSWORD/MYSQLDATABASE) — так
после подключения плагина в Railway ничего вручную прописывать не
нужно, бот сам их подхватит.
"""

import os
import threading

import pymysql
import pymysql.cursors

MYSQL_HOST = os.environ.get("MYSQL_HOST") or os.environ.get("MYSQLHOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT") or os.environ.get("MYSQLPORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER") or os.environ.get("MYSQLUSER", "aleado_feed")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD") or os.environ.get("MYSQLPASSWORD", "")
MYSQL_DB = os.environ.get("MYSQL_DB") or os.environ.get("MYSQLDATABASE", "aleado_feed")

_schema_lock = threading.Lock()
_schema_ready = False

# Доп. поля фильтра (аналог полей формы поиска на jmmoto.ru — пробег,
# объём двигателя, оценка состояния, номер лота, VIN, свободный текст).
# Добавляются через ALTER TABLE к уже существующей bot_watches (см.
# _migrate_watch_columns), а не только в CREATE TABLE — иначе на уже
# развёрнутой базе (где таблица создана раньше, без этих колонок) бот
# упал бы при первом же INSERT с "Unknown column".
_WATCH_EXTRA_COLUMNS = {
    "mileage_from": "INT NULL",
    "mileage_to": "INT NULL",
    "engine_from": "INT NULL",
    "engine_to": "INT NULL",
    "grade_from": "DOUBLE NULL",
    "grade_to": "DOUBLE NULL",
    "lot_number": "VARCHAR(64) NULL",
    "vin": "VARCHAR(64) NULL",
    "free_text": "VARCHAR(191) NULL",
}


def _connect():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=15,
    )


def init_schema():
    """CREATE TABLE IF NOT EXISTS для собственных таблиц бота. Не трогает
    таблицы фида aleado (moto_lots_ns и т.п.) — те живут своей жизнью,
    их создаёт/пересоздаёт импорт дампа (aleado_client.import_dump)."""
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bot_watches (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        chat_id BIGINT NOT NULL,
                        brand VARCHAR(191) NOT NULL,
                        model VARCHAR(191) NULL,
                        year_from INT NULL,
                        year_to INT NULL,
                        max_price DOUBLE NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_chat_id (chat_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bot_seen_lots (
                        lot_key VARCHAR(191) NOT NULL PRIMARY KEY,
                        seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )
                # bot_known_models — накопленный НАВСЕГДА список (марка,
                # модель), которые хоть раз встречались в фиде aleado.
                # Нужен, потому что сам фид — это "текущий срез" (что
                # ещё идёт/недавно появилось), а не архив: модель, у
                # которой сейчас нет ни одного лота, полностью пропадает
                # из кнопок выбора модели, хотя раньше (и, возможно,
                # снова в будущем) лоты по ней были — живая жалоба:
                # BMW R1250GS ADVENTURE есть у поставщика данных, но не
                # находится в кнопках бота, потому что в текущем срезе
                # по ней временно нет лотов. С этой таблицей кнопка,
                # once появившись, остаётся навсегда — список моделей
                # только растёт со временем.
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bot_known_models (
                        brand VARCHAR(191) NOT NULL,
                        model VARCHAR(191) NOT NULL,
                        first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (brand, model)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )
                # bot_lot_history — своя копия каждого лота, виденного в
                # фиде, плюс отметка closed_at, когда лот пропадает из
                # текущего среза (единственный доступный признак
                # "торги завершились" — сам фид aleado не хранит явный
                # результат продажи, лот просто исчезает из выдачи).
                # Нужна на будущее для раздела "Статистика"/"похожие
                # проданные лоты" — диагностика показала, что в самом
                # текущем срезе фида завершённых лотов не бывает вообще
                # (aleado отдаёт только то, что ещё идёт), поэтому без
                # своей накопленной копии этот раздел показывать
                # нечего. lot_id — тот же "id" из дампа aleado (он же
                # используется в ссылках вида jmmoto.ru/<id>), стабилен
                # между обновлениями фида, пока лот жив.
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bot_lot_history (
                        lot_id VARCHAR(64) NOT NULL PRIMARY KEY,
                        vin VARCHAR(64) NULL,
                        brand VARCHAR(191) NOT NULL,
                        model VARCHAR(191) NOT NULL,
                        year VARCHAR(16) NULL,
                        auction_name VARCHAR(191) NULL,
                        auction_date VARCHAR(32) NULL,
                        mileage_raw VARCHAR(64) NULL,
                        engine_volume VARCHAR(64) NULL,
                        grade_overall VARCHAR(32) NULL,
                        price_raw VARCHAR(64) NULL,
                        result VARCHAR(64) NULL,
                        first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        closed_at TIMESTAMP NULL,
                        INDEX idx_brand_model (brand, model),
                        INDEX idx_closed (closed_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )
                _migrate_watch_columns(cur)
        finally:
            conn.close()
        _schema_ready = True


def _migrate_watch_columns(cur):
    """Добавляет в bot_watches колонки из _WATCH_EXTRA_COLUMNS, которых
    там ещё нет (на новой базе CREATE TABLE их пока не создаёт — проще
    держать один список и здесь, и добавлять миграцией, чем дублировать
    его в CREATE TABLE и следить, чтобы они не разошлись)."""
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='bot_watches'"
    )
    existing = {row["COLUMN_NAME"] for row in cur.fetchall()}
    for col, ddl in _WATCH_EXTRA_COLUMNS.items():
        if col not in existing:
            cur.execute("ALTER TABLE bot_watches ADD COLUMN `{}` {}".format(col, ddl))


# --------------------------------------------------------------------------
# Фильтры пользователей (bot_watches) — замена SUBS/subscribers.json
# --------------------------------------------------------------------------


def list_watches(chat_id=None):
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            if chat_id is None:
                cur.execute("SELECT * FROM bot_watches ORDER BY id")
            else:
                cur.execute("SELECT * FROM bot_watches WHERE chat_id=%s ORDER BY id", (chat_id,))
            return cur.fetchall()
    finally:
        conn.close()


def get_watch(watch_id, chat_id):
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM bot_watches WHERE id=%s AND chat_id=%s",
                (watch_id, chat_id),
            )
            return cur.fetchone()
    finally:
        conn.close()


def add_watch(
    chat_id, brand, model, year_from, year_to, max_price,
    mileage_from=None, mileage_to=None, engine_from=None, engine_to=None,
    grade_from=None, grade_to=None, lot_number=None, vin=None, free_text=None,
):
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO bot_watches "
                "(chat_id, brand, model, year_from, year_to, max_price, "
                "mileage_from, mileage_to, engine_from, engine_to, "
                "grade_from, grade_to, lot_number, vin, free_text) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    chat_id, brand, model, year_from, year_to, max_price,
                    mileage_from, mileage_to, engine_from, engine_to,
                    grade_from, grade_to, lot_number, vin, free_text,
                ),
            )
            return cur.lastrowid
    finally:
        conn.close()


def delete_watch(watch_id, chat_id):
    """Возвращает True, если строка реально была удалена."""
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM bot_watches WHERE id=%s AND chat_id=%s",
                (watch_id, chat_id),
            )
            return cur.rowcount > 0
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Уже отправленные лоты (bot_seen_lots) — замена STATE/seen_lots.json
# --------------------------------------------------------------------------


def is_seen(lot_key):
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM bot_seen_lots WHERE lot_key=%s", (lot_key,))
            return cur.fetchone() is not None
    finally:
        conn.close()


def mark_seen(lot_key):
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # INSERT IGNORE: если два потока одновременно пометят один и
            # тот же лот — не упадём на дублирующемся первичном ключе.
            cur.execute("INSERT IGNORE INTO bot_seen_lots (lot_key) VALUES (%s)", (lot_key,))
    finally:
        conn.close()


def list_seen_keys():
    """Все уже отправленные ключи разом, одним запросом — используется в
    фоновой проверке (check_all), чтобы не делать по отдельному SELECT на
    каждую пару лот×фильтр (при сотнях лотов и нескольких подписках это
    были бы тысячи соединений с базой за один проход)."""
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT lot_key FROM bot_seen_lots")
            return {row["lot_key"] for row in cur.fetchall()}
    finally:
        conn.close()


def mark_seen_bulk(lot_keys):
    """Одним запросом помечает сразу много ключей — пара с
    list_seen_keys() для того же соображения (меньше круговых походов в
    базу за один проход фоновой проверки)."""
    lot_keys = list(lot_keys)
    if not lot_keys:
        return
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany("INSERT IGNORE INTO bot_seen_lots (lot_key) VALUES (%s)", [(k,) for k in lot_keys])
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Накопленный список моделей (bot_known_models) — растущий каталог
# "марка/модель", которые хоть раз встречались в фиде. См. подробный
# разбор причины в init_schema() у CREATE TABLE bot_known_models.
# --------------------------------------------------------------------------


def upsert_known_models(pairs):
    """pairs — итерируемое из (brand, model). Добавляет ещё не виденные
    пары в bot_known_models и обновляет last_seen_at у уже известных.
    Список только растёт — записи отсюда никогда не удаляются."""
    seen = set()
    clean = []
    for brand, model in pairs:
        brand = (brand or "").strip()
        model = (model or "").strip()
        if not brand or not model:
            continue
        key = (brand.lower(), model.lower())
        if key in seen:
            continue
        seen.add(key)
        clean.append((brand, model))
    if not clean:
        return
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO bot_known_models (brand, model) VALUES (%s, %s) "
                "ON DUPLICATE KEY UPDATE last_seen_at = CURRENT_TIMESTAMP",
                clean,
            )
    finally:
        conn.close()


def known_models_for_brand(brand):
    """Все модели марки brand, когда-либо встречавшиеся в фиде (не
    только те, что есть в ТЕКУЩЕМ срезе) — сравнение марки без учёта
    регистра, раз в самом фиде написание могло не совпасть буква в
    букву."""
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT model FROM bot_known_models WHERE LOWER(brand) = LOWER(%s)",
                (brand,),
            )
            return [row["model"] for row in cur.fetchall()]
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Своя история лотов (bot_lot_history) — см. подробный разбор причины в
# init_schema() у CREATE TABLE bot_lot_history. Задел на будущее для
# раздела "Статистика"/"похожие проданные лоты": сам текущий срез фида
# завершённых лотов не содержит вообще (подтверждено диагностикой), так
# что без своей копии этому разделу опираться не на что.
# --------------------------------------------------------------------------


def sync_lot_history(lots):
    """Синхронизирует bot_lot_history с ТЕКУЩИМ срезом фида: заводит
    новые строки / обновляет last_seen_at и изменяемые поля у уже
    известных лотов, а те лоты, что раньше считались "ещё живыми"
    (closed_at IS NULL), но пропали из текущего среза — помечает
    closed_at=NOW() (единственный доступный признак завершения торгов,
    см. комментарий у CREATE TABLE)."""
    rows = []
    current_ids = set()
    for lot in lots:
        lot_id = lot.get("lot_id")
        brand = (lot.get("brand") or "").strip()
        model = (lot.get("model") or "").strip()
        if not lot_id or not brand or not model:
            continue
        lot_id = str(lot_id)
        current_ids.add(lot_id)
        rows.append((
            lot_id,
            lot.get("vin"),
            brand,
            model,
            str(lot.get("year") or "") or None,
            lot.get("auction_name"),
            str(lot.get("auction_date") or "") or None,
            str(lot.get("mileage") or "") or None,
            str(lot.get("engine_volume") or "") or None,
            str(lot.get("grade_overall") or "") or None,
            str(lot.get("end_price") or lot.get("start_price") or "") or None,
            str(lot.get("result") or "") or None,
        ))

    if not rows:
        return

    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            # Кто числится "ещё живым" ДО этого обновления — чтобы
            # затем понять, кто из них пропал из текущего среза.
            cur.execute("SELECT lot_id FROM bot_lot_history WHERE closed_at IS NULL")
            previously_open = {row["lot_id"] for row in cur.fetchall()}

            cur.executemany(
                """
                INSERT INTO bot_lot_history
                    (lot_id, vin, brand, model, year, auction_name, auction_date,
                     mileage_raw, engine_volume, grade_overall, price_raw, result)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    last_seen_at = CURRENT_TIMESTAMP,
                    closed_at = NULL,
                    vin = VALUES(vin),
                    mileage_raw = VALUES(mileage_raw),
                    engine_volume = VALUES(engine_volume),
                    grade_overall = VALUES(grade_overall),
                    price_raw = VALUES(price_raw),
                    result = VALUES(result)
                """,
                rows,
            )

            gone = previously_open - current_ids
            if gone:
                cur.executemany(
                    "UPDATE bot_lot_history SET closed_at = CURRENT_TIMESTAMP WHERE lot_id = %s",
                    [(lid,) for lid in gone],
                )
    finally:
        conn.close()


def closed_lot_history(brand, model=None, limit=10):
    """Последние `limit` лотов марки (и, если задана, модели), которые
    пропали из текущего среза фида (closed_at IS NOT NULL — наша
    приближённая замена "проданных/завершённых торгов", раз сам фид
    aleado эту информацию не хранит). model=None — любая модель этой
    марки (аналог пункта "Любая модель" в разделе Статистика). Сравнение
    марки/модели без учёта регистра и лишних пробелов на всякий случай."""
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            if model:
                cur.execute(
                    "SELECT * FROM bot_lot_history "
                    "WHERE LOWER(brand) = LOWER(%s) AND LOWER(model) = LOWER(%s) "
                    "AND closed_at IS NOT NULL "
                    "ORDER BY closed_at DESC LIMIT %s",
                    (brand.strip(), model.strip(), limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM bot_lot_history "
                    "WHERE LOWER(brand) = LOWER(%s) "
                    "AND closed_at IS NOT NULL "
                    "ORDER BY closed_at DESC LIMIT %s",
                    (brand.strip(), limit),
                )
            return cur.fetchall()
    finally:
        conn.close()
