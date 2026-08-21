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
