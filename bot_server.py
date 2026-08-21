#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram-бот для отслеживания мотоциклетных лотов на японских аукционах.

ПОСТОЯННО РАБОТАЮЩИЙ процесс (не задача по расписанию). Запускается один
раз (например, через systemd) и живёт, пока не остановите. Поэтому:
  - на нажатия кнопок бот отвечает мгновенно;
  - все настройки (марка, модель, годы, цена) выбираются кнопками, а
    список моделей под каждую марку подтягивается ЖИВЬЁМ из данных
    аукционов — это реальные названия из текущих лотов, а не выдуманный
    список, поэтому не нужно гадать, как правильно написать модель;
  - годы "от"/"до" всегда включают текущий и следующий год, плюс кнопку
    "Ввести вручную" (можно и просто "26" вместо "2026");
  - в конце настройки фильтра можно сразу посмотреть, что подходит на
    аукционах ПРЯМО СЕЙЧАС, не дожидаясь фонового обхода;
  - у каждого сохранённого фильтра в /mywatches есть кнопка "Проверить"
    для такого же мгновенного показа текущих лотов по нему;
  - фоновая проверка новых лотов всё равно идёт по таймеру
    (CHECK_INTERVAL_SECONDS), потому что аукционы сами никого не
    оповещают — это неизбежно в любой архитектуре.

Использует python-telegram-bot версии 13.5 (синхронный API) — версии
13.6+ используют typing.Generic в классе Updater, а в Python 3.6 это
натыкается на баг интерпретатора (TypeError: __dict__ slot disallowed).
13.5 — последняя версия до этого изменения, полностью рабочая.

Источник данных:
  - Платный фид aleado (логин jmmoto_user) — BZip2-архив с готовым
    MySQL-дампом (таблица moto_lots_ns и, при необходимости, другие —
    см. ALEADO_TABLES), который бот скачивает по FTP, распаковывает и
    импортирует в MySQL/MariaDB, откуда и берёт лоты. Логика
    скачивания/импорта/чтения — в aleado_client.py, там же подробное
    описание формата и настройки (ALEADO_FTP_*, MYSQL_*).
  - Прежние источники (bid.cars — США Copart/IAAI, и прямой разбор
    jmmoto.ru) убраны совсем: bid.cars стоял за Cloudflare и требовал
    отдельный Docker-контейнер FlareSolverr только чтобы обойти защиту,
    а прямой разбор jmmoto.ru заменён на официальный платный фид от того
    же поставщика данных — это надёжнее и не зависит от того, не
    поменяет ли сайт вёрстку/защиту.
  - Цены в лотах — в йенах. Порог цены в фильтре задаётся в рублях (это
    удобнее пользователю), пересчёт йена→рубль берётся по АКТУАЛЬНОМУ
    курсу ЦБ РФ (см. get_jpy_to_rub ниже), не по зафиксированной когда-то
    константе.

Собственное состояние бота (список фильтров/подписок и уже отправленные
лоты) — тоже в MySQL (db.py), а не в JSON-файлах на диске: контейнерный
хостинг вроде Railway пересоздаёт файловую систему при каждом редеплое,
JSON-файлы бы обнулялись, база — нет.

Запуск (локально):
    export TELEGRAM_BOT_TOKEN="ваш_токен_от_BotFather"
    python3 bot_server.py

Развёртывание на Railway (рекомендуемый способ) — см. README.

Обязательно: одновременно должен работать только ОДИН экземпляр этого
бота на одном токене (иначе Telegram будет присылать ошибку "Conflict").
"""

import os
import re
import html
import time
import logging
import datetime
import threading

import requests

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    Filters,
    CallbackContext,
)

import db
import aleado_client as aleado

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("auction-watcher")

# --------------------------------------------------------------------------
# Настройки
# --------------------------------------------------------------------------

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_TOKEN:
    raise SystemExit(
        "Не задана переменная окружения TELEGRAM_BOT_TOKEN. "
        "Пример: export TELEGRAM_BOT_TOKEN=123456:AA...  и запустите заново."
    )

# Как часто ПРОВЕРЯТЬ (по фильтрам) уже загруженные лоты на новые (в
# секундах). Не путать с ALEADO_REFRESH_TTL_SECONDS (как часто сам фид
# перекачивается и переимпортируется с нуля) — эта проверка может
# запускаться чаще, тогда она просто будет сверяться с тем же самым
# кэшем, пока он не устареет.
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", "300"))

# Сколько лотов показывать за раз в мгновенном просмотре ("Показать сейчас").
PREVIEW_LIMIT = 15

# Сколько разных моделей показывать кнопками под маркой.
MODEL_BUTTON_LIMIT = 18

# --------------------------------------------------------------------------
# Курс йена -> рубль (актуальный, по данным ЦБ РФ)
# --------------------------------------------------------------------------

CBR_RATE_URL = "https://www.cbr-xml-daily.ru/daily_json.js"
CBR_RATE_TTL_SECONDS = int(os.environ.get("CBR_RATE_TTL_SECONDS", "21600"))  # 6 часов
# Запасной курс, если ЦБ недоступен И ещё ни разу не удавалось получить
# настоящий курс с начала работы процесса. Поправьте, если сильно уйдёт.
FALLBACK_JPY_TO_RUB = float(os.environ.get("FALLBACK_JPY_TO_RUB", "0.68"))

_rate_lock = threading.Lock()
_rate_cache = {"value": FALLBACK_JPY_TO_RUB, "ts": 0.0}


def get_jpy_to_rub():
    """1 йена в рублях, по курсу ЦБ РФ (в их API JPY котируется за 100
    йен, отсюда деление на Nominal). Кэшируется на CBR_RATE_TTL_SECONDS.
    При ошибке — используем последний удачно полученный курс (или
    FALLBACK_JPY_TO_RUB, если удачных ещё не было)."""
    if _rate_cache["ts"] and (time.time() - _rate_cache["ts"]) < CBR_RATE_TTL_SECONDS:
        return _rate_cache["value"]
    with _rate_lock:
        if _rate_cache["ts"] and (time.time() - _rate_cache["ts"]) < CBR_RATE_TTL_SECONDS:
            return _rate_cache["value"]
        try:
            resp = requests.get(CBR_RATE_URL, timeout=10)
            resp.raise_for_status()
            jpy = resp.json()["Valute"]["JPY"]
            rate = float(jpy["Value"]) / float(jpy["Nominal"])
            _rate_cache["value"] = rate
            log.info("Курс ЦБ обновлён: 1 JPY = %.4f RUB", rate)
        except Exception as e:
            log.warning(
                "Не удалось получить курс ЦБ, использую прошлый/запасной (%.4f): %s",
                _rate_cache["value"], e,
            )
        _rate_cache["ts"] = time.time()
        return _rate_cache["value"]


# Марки в кнопках. "Другая марка" даёт возможность ввести любую вручную.
BRANDS = [
    "Honda", "Yamaha", "Suzuki", "Kawasaki",
    "BMW", "Harley-Davidson", "Ducati", "Aprilia",
    "Triumph", "KTM", "Indian",
]
OTHER_BRAND = "__other__"

HELP_TEXT = (
    "Я слежу за мотолотами на японских аукционах (данные — платный фид "
    "aleado) и присылаю новые лоты по вашим фильтрам, с фото и оценкой "
    "состояния, если они есть в данных лота.\n\n"
    "Команды:\n"
    "/start или /help — это сообщение и меню\n"
    "/watch — добавить фильтр (марка/модель/годы/цена кнопками)\n"
    "/mywatches — список своих фильтров: можно мгновенно проверить или удалить\n"
    "/cancel — отменить текущую настройку фильтра\n\n"
    "Модели в кнопках — реальные названия из текущих лотов, поэтому не "
    "нужно гадать написание. Годы и цену можно как выбрать кнопкой, так "
    "и напечатать вручную. В конце настройки фильтра можно сразу "
    "посмотреть, что подходит прямо сейчас, не дожидаясь рассылки.\n\n"
    "Порог цены задаётся в рублях. В самих лотах цена в йенах — рядом "
    "показывается пересчёт в рубли по АКТУАЛЬНОМУ курсу ЦБ РФ на момент "
    "показа."
)

# --------------------------------------------------------------------------
# Состояние (подписчики/фильтры + уже отправленные лоты) — в MySQL, не в
# JSON-файлах на диске (см. db.py: на Railway и вообще в контейнерном
# хостинге без подключённого тома файлы бы обнулялись при каждом
# редеплое, а база — нет). Все обращения идут через db.*, здесь своего
# состояния в памяти процесса для этого больше нет.
# --------------------------------------------------------------------------

# Черновик текущего диалога настройки фильтра, храним в памяти процесса:
# chat_id -> {"brand":..., "model":..., "model_candidates": [...],
#             "year_from":..., "year_to":..., "max_price":...,
#             "awaiting": "brand_text"|"model_text"|"yf_text"|"yt_text"|"price_text"|None}
DRAFTS = {}

# --------------------------------------------------------------------------
# Вспомогательные функции показа/логики фильтров
# --------------------------------------------------------------------------


def format_rub(v):
    return "{:,.0f}".format(v).replace(",", " ")


def describe_criteria(brand, model, year_from, year_to, max_price):
    model_s = model or "любая модель"
    years = "{}–{}".format(year_from or "…", year_to or "…") if (year_from or year_to) else "любые годы"
    price_s = "до {} ₽".format(format_rub(max_price)) if max_price else "без ограничения по цене"
    return "{} / {} [{}] {}".format(brand, model_s, years, price_s)


def describe_watch(w):
    return "#{}: {}".format(
        w["id"],
        describe_criteria(w["brand"], w.get("model"), w.get("year_from"), w.get("year_to"), w.get("max_price")),
    )


def matches(watch, brand, model_text, year, price):
    if watch["brand"].lower() != brand.lower():
        return False
    if watch.get("model"):
        if not model_text or watch["model"].lower() not in model_text.lower():
            return False
    if watch.get("year_from") and (year is None or year < watch["year_from"]):
        return False
    if watch.get("year_to") and (year is None or year > watch["year_to"]):
        return False
    if watch.get("max_price") and (price is None or price > watch["max_price"]):
        return False
    return True


def parse_price(text):
    if not text:
        return None
    digits = re.sub(r"[^\d.]", "", str(text))
    try:
        return float(digits) if digits else None
    except ValueError:
        return None


def parse_manual_year(text):
    """Год, введённый вручную: понимает и "2026", и короткое "26"."""
    text = (text or "").strip()
    if re.fullmatch(r"\d{2}", text):
        return 2000 + int(text)
    if re.fullmatch(r"(19|20)\d{2}", text):
        return int(text)
    return None


# --------------------------------------------------------------------------
# Аукционы: фид aleado (см. aleado_client.py — там FTP/BZip2/MySQL)
# --------------------------------------------------------------------------


def lot_year(lot):
    raw = lot.get("year")
    if not raw:
        return None
    m = re.search(r"(19|20)\d{2}", str(raw))
    return int(m.group(0)) if m else None


def lot_price_rub(lot):
    """Цена лота в рублях по текущему курсу ЦБ — для сравнения с порогом
    фильтра (порог тоже в рублях). Берём итоговую цену торгов, если она
    уже есть, иначе стартовую."""
    raw = lot.get("end_price") or lot.get("start_price")
    jpy = parse_price(raw)
    if not jpy or jpy <= 0:
        return None
    return jpy * get_jpy_to_rub()


def lot_price_label(lot):
    """Цена для показа пользователю: исходная йена + пересчёт в рубли."""
    raw = lot.get("end_price") or lot.get("start_price")
    jpy = parse_price(raw)
    if not jpy or jpy <= 0:
        return "цена неизвестна"
    rub = jpy * get_jpy_to_rub()
    return "¥{} (≈{} ₽)".format(format_rub(jpy), format_rub(rub))


def lot_grade_label(lot):
    parts = []
    if lot.get("grade_overall"):
        parts.append("общая: {}".format(lot["grade_overall"]))
    for col, val in (lot.get("_extra_grades") or {}).items():
        parts.append("{}: {}".format(col, val))
    return ", ".join(parts) if parts else None


def lot_photo(lot):
    raw = lot.get("photo_url")
    if not raw:
        return None
    raw = str(raw)
    for sep in (",", ";", "|", "\n"):
        if sep in raw:
            raw = raw.split(sep)[0]
            break
    raw = raw.strip()
    return raw or None


def lot_display_name(lot):
    brand = (lot.get("brand") or "?").strip()
    model = (lot.get("model") or "").strip()
    return "{} {}".format(brand, model).strip()


def fetch_model_candidates(brand, limit=MODEL_BUTTON_LIMIT):
    """Реальные названия моделей марки brand из текущих лотов фида aleado,
    отсортированные по частоте встречаемости (самые ходовые — первыми)."""
    counts = {}
    order = []

    def add(name):
        name = (name or "").strip()
        if not name:
            return
        if name not in counts:
            counts[name] = 0
            order.append(name)
        counts[name] += 1

    brand_l = brand.strip().lower()
    for lot in aleado.get_lots():
        if (lot.get("brand") or "").strip().lower() != brand_l:
            continue
        add(lot.get("model"))

    order.sort(key=lambda k: (-counts[k], k))
    return order[:limit]


def format_lot_text(lot, brand, model, year):
    name = lot_display_name(lot)
    lines = ["🏍 <b>{}</b>".format(html.escape(name or "Мотоцикл"))]
    lines.append("Год: {} | Цена: {}".format(year or "?", html.escape(lot_price_label(lot))))

    grade = lot_grade_label(lot)
    if grade:
        lines.append("Оценка состояния: {}".format(html.escape(grade)))

    meta = []
    if lot.get("mileage"):
        meta.append("пробег {}".format(lot["mileage"]))
    if lot.get("engine_volume"):
        meta.append("объём {}".format(lot["engine_volume"]))
    if lot.get("color"):
        meta.append("цвет {}".format(lot["color"]))
    if lot.get("body_type"):
        meta.append("тип {}".format(lot["body_type"]))
    if meta:
        lines.append(html.escape(", ".join(meta)))

    aux = []
    if lot.get("auction_name"):
        aux.append("аукцион {}".format(lot["auction_name"]))
    if lot.get("auction_date"):
        aux.append("дата {}".format(lot["auction_date"]))
    if lot.get("lot_number"):
        aux.append("лот №{}".format(lot["lot_number"]))
    if aux:
        lines.append(html.escape(", ".join(aux)))

    if lot.get("vin"):
        lines.append("VIN: {}".format(html.escape(str(lot["vin"]))))
    if lot.get("result"):
        lines.append("Результат торгов: {}".format(html.escape(str(lot["result"]))))

    lines.append("Источник: aleado (Япония)")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Фоновая проверка всех фильтров по таймеру
# --------------------------------------------------------------------------


def send_lot(bot, chat_id, text, photo_url=None):
    if photo_url:
        try:
            bot.send_photo(chat_id=chat_id, photo=photo_url, caption=text, parse_mode="HTML")
            return
        except Exception as e:
            log.warning("send_photo failed, falling back to text: %s", e)
    try:
        bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=False)
    except Exception as e:
        log.warning("send_message failed for chat %s: %s", chat_id, e)


def check_all(bot):
    watches = db.list_watches()
    if not watches:
        return

    lots = aleado.get_lots()
    if not lots:
        if aleado.last_error():
            log.warning("aleado: фид сейчас недоступен (%s), пропускаю проверку", aleado.last_error())
        return

    # Разом читаем все уже отправленные ключи и разом же дописываем новые
    # в конце — вместо отдельного похода в базу на каждую пару
    # лот×фильтр (при сотнях лотов и нескольких подписках это быстро
    # превращается в тысячи соединений за один проход).
    seen = db.list_seen_keys()
    newly_seen = []

    for lot in lots:
        lot_id = lot.get("lot_id") or lot.get("lot_number")
        brand = (lot.get("brand") or "").strip()
        if not lot_id or not brand:
            continue
        model = lot.get("model") or ""
        year = lot_year(lot)
        price = lot_price_rub(lot)

        for w in watches:
            if not matches(w, brand, model, year, price):
                continue
            key = "aleado:{}:{}".format(lot_id, w["chat_id"])
            if key in seen:
                continue
            seen.add(key)
            newly_seen.append(key)
            text = format_lot_text(lot, brand, model, year)
            send_lot(bot, w["chat_id"], text, lot_photo(lot))

    db.mark_seen_bulk(newly_seen)


def check_job(context: CallbackContext):
    try:
        check_all(context.bot)
    except Exception:
        log.exception("check_job failed")


# --------------------------------------------------------------------------
# Мгновенный просмотр текущих лотов ("Показать сейчас" / "🔍 Проверить")
# --------------------------------------------------------------------------


def format_preview(brand, model, year_from, year_to, max_price, limit=PREVIEW_LIMIT):
    watch = {
        "brand": brand, "model": model,
        "year_from": year_from, "year_to": year_to, "max_price": max_price,
    }
    lines = []
    total = 0

    for lot in aleado.get_lots():
        lbrand = (lot.get("brand") or "").strip()
        if not lbrand:
            continue
        lmodel = lot.get("model") or ""
        year = lot_year(lot)
        price = lot_price_rub(lot)
        if not matches(watch, lbrand, lmodel, year, price):
            continue
        total += 1
        if len(lines) < limit:
            lines.append(format_lot_text(lot, lbrand, lmodel, year))

    if not lines:
        extra = ""
        if aleado.last_error():
            extra = "\n\n(Похоже, фид aleado сейчас недоступен: {})".format(aleado.last_error())
        return "Прямо сейчас по этим критериям на аукционах ничего не найдено." + extra

    header = "Сейчас подходит лотов: {}\n\n".format(total)
    body = "\n\n".join(lines)
    footer = "" if total <= len(lines) else "\n\n…и ещё {}, показаны первые {}.".format(total - len(lines), len(lines))
    return header + body + footer


def send_preview(context, chat_id, brand, model, year_from, year_to, max_price):
    try:
        context.bot.send_chat_action(chat_id=chat_id, action="typing")
    except Exception:
        pass
    text = format_preview(brand, model, year_from, year_to, max_price)
    context.bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=False)


# --------------------------------------------------------------------------
# Клавиатуры
# --------------------------------------------------------------------------


def main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить фильтр", callback_data="menu:new")],
        [InlineKeyboardButton("📋 Мои фильтры", callback_data="menu:list")],
    ])


def brand_kb():
    rows = []
    row = []
    for b in BRANDS:
        row.append(InlineKeyboardButton(b, callback_data="brand:{}".format(b)))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Другая марка (ввести)", callback_data="brand:{}".format(OTHER_BRAND))])
    rows.append([InlineKeyboardButton("✖ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def model_kb(candidates):
    rows = []
    row = []
    for i, m in enumerate(candidates):
        label = m if len(m) <= 26 else m[:23] + "…"
        row.append(InlineKeyboardButton(label, callback_data="modelidx:{}".format(i)))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Любая модель", callback_data="model:any")])
    rows.append([InlineKeyboardButton("Указать модель (ввести текст)", callback_data="model:text")])
    rows.append([InlineKeyboardButton("✖ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def year_choices():
    y = datetime.date.today().year
    ys = sorted({y + 1, y, y - 1, y - 2, y - 3, y - 5, y - 8, y - 12, y - 18, y - 25}, reverse=True)
    return ys


def year_kb(prefix):
    rows = []
    row = []
    for y in year_choices():
        row.append(InlineKeyboardButton(str(y), callback_data="{}:{}".format(prefix, y)))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Без ограничения", callback_data="{}:none".format(prefix))])
    rows.append([InlineKeyboardButton("Ввести вручную", callback_data="{}:text".format(prefix))])
    rows.append([InlineKeyboardButton("✖ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


# Пороги цены — в рублях (раньше были в $, привязанные к bid.cars; сейчас
# единственный источник — японские аукционы в йенах, поэтому удобнее
# сразу рубли). Ориентир по типичным ценам на японские мотоаукционы.
PRICE_VALUES = [100000, 200000, 300000, 500000, 800000, 1200000, 2000000, 3000000]


def price_kb():
    rows = []
    row = []
    for v in PRICE_VALUES:
        row.append(InlineKeyboardButton("{} ₽".format(format_rub(v)), callback_data="price:{}".format(v)))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Без ограничения", callback_data="price:none")])
    rows.append([InlineKeyboardButton("Ввести вручную", callback_data="price:text")])
    rows.append([InlineKeyboardButton("✖ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def confirm_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Сохранить подписку", callback_data="confirm:save")],
        [InlineKeyboardButton("👀 Показать, что есть сейчас", callback_data="confirm:preview")],
        [InlineKeyboardButton("✖ Отмена", callback_data="cancel")],
    ])


def mywatches_view(chat_id):
    """Возвращает (текст, клавиатура) со списком фильтров пользователя,
    либо (None, None), если фильтров нет."""
    mine = db.list_watches(chat_id=chat_id)
    if not mine:
        return None, None
    lines = ["Ваши фильтры:"] + [describe_watch(w) for w in mine]
    rows = []
    for w in mine:
        rows.append([
            InlineKeyboardButton("🔍 Проверить #{}".format(w["id"]), callback_data="checknow:{}".format(w["id"])),
            InlineKeyboardButton("❌ Удалить #{}".format(w["id"]), callback_data="del:{}".format(w["id"])),
        ])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


# --------------------------------------------------------------------------
# Хендлеры команд
# --------------------------------------------------------------------------


def start_cmd(update: Update, context: CallbackContext):
    update.effective_message.reply_text(HELP_TEXT, reply_markup=main_menu_kb())


def watch_cmd(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    DRAFTS[chat_id] = {}
    update.effective_message.reply_text("Выберите марку:", reply_markup=brand_kb())


def mywatches_cmd(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    text, kb = mywatches_view(chat_id)
    if not kb:
        update.effective_message.reply_text("У вас пока нет фильтров. Нажмите /watch, чтобы добавить.")
        return
    update.effective_message.reply_text(text, reply_markup=kb)


def cancel_cmd(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    DRAFTS.pop(chat_id, None)
    update.effective_message.reply_text("Отменено.", reply_markup=main_menu_kb())


# --------------------------------------------------------------------------
# Хендлер нажатий на кнопки
# --------------------------------------------------------------------------


def on_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    chat_id = query.message.chat_id
    data = query.data

    if data == "cancel":
        DRAFTS.pop(chat_id, None)
        query.edit_message_text("Отменено.")
        context.bot.send_message(chat_id, "Главное меню:", reply_markup=main_menu_kb())
        return

    if data == "menu:new":
        DRAFTS[chat_id] = {}
        query.edit_message_text("Выберите марку:", reply_markup=brand_kb())
        return

    if data == "menu:list":
        text, kb = mywatches_view(chat_id)
        if not kb:
            query.edit_message_text("У вас пока нет фильтров. Нажмите /watch, чтобы добавить.")
        else:
            query.edit_message_text(text, reply_markup=kb)
        return

    if data.startswith("del:"):
        wid = int(data.split(":", 1)[1])
        changed = db.delete_watch(wid, chat_id)
        if changed:
            query.edit_message_text("Удалено.")
        else:
            query.edit_message_text("Не найдено (возможно, уже удалено).")
        return

    if data.startswith("checknow:"):
        wid = int(data.split(":", 1)[1])
        w = db.get_watch(wid, chat_id)
        if not w:
            context.bot.send_message(chat_id, "Фильтр не найден (возможно, уже удалён).")
            return
        send_preview(context, chat_id, w["brand"], w.get("model"), w.get("year_from"), w.get("year_to"), w.get("max_price"))
        return

    if data.startswith("brand:"):
        brand = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if brand == OTHER_BRAND:
            draft["awaiting"] = "brand_text"
            query.edit_message_text("Напишите марку текстом (одно сообщение):")
            return
        draft["brand"] = brand
        draft.pop("awaiting", None)
        query.edit_message_text("Марка: {}\nИщу модели в данных аукционов…".format(brand))
        candidates = fetch_model_candidates(brand)
        draft["model_candidates"] = candidates
        if candidates:
            note = "Марка: {}\nВыберите модель (это реальные названия из текущих лотов):".format(brand)
        else:
            note = (
                "Марка: {}\nСейчас не нашёл активных лотов этой марки, чтобы "
                "подсказать модели — выберите «Любая модель» или введите вручную:"
            ).format(brand)
        context.bot.send_message(chat_id, note, reply_markup=model_kb(candidates))
        return

    if data.startswith("modelidx:"):
        idx = int(data.split(":", 1)[1])
        draft = DRAFTS.setdefault(chat_id, {})
        candidates = draft.get("model_candidates", [])
        model = candidates[idx] if 0 <= idx < len(candidates) else None
        draft["model"] = model
        draft.pop("awaiting", None)
        query.edit_message_text("Модель: {}\nГод от:".format(model or "любая"), reply_markup=year_kb("yf"))
        return

    if data.startswith("model:"):
        choice = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if choice == "any":
            draft["model"] = None
            draft.pop("awaiting", None)
            query.edit_message_text("Модель: любая\nГод от:", reply_markup=year_kb("yf"))
        else:
            draft["awaiting"] = "model_text"
            query.edit_message_text("Напишите модель текстом (одно сообщение):")
        return

    if data.startswith("yf:"):
        val = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if val == "text":
            draft["awaiting"] = "yf_text"
            query.edit_message_text("Введите год «от» текстом (например, 2015 или просто 15):")
            return
        draft["year_from"] = None if val == "none" else int(val)
        query.edit_message_text("Год до:", reply_markup=year_kb("yt"))
        return

    if data.startswith("yt:"):
        val = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if val == "text":
            draft["awaiting"] = "yt_text"
            query.edit_message_text("Введите год «до» текстом (например, 2026 или просто 26):")
            return
        draft["year_to"] = None if val == "none" else int(val)
        query.edit_message_text("Максимальная цена, ₽:", reply_markup=price_kb())
        return

    if data.startswith("price:"):
        val = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if val == "text":
            draft["awaiting"] = "price_text"
            query.edit_message_text("Введите максимальную цену в рублях текстом (например, 500000):")
            return
        draft["max_price"] = None if val == "none" else float(val)
        show_confirm(query, draft)
        return

    if data == "confirm:save":
        draft = DRAFTS.get(chat_id, {})
        finalize_watch(query, context, chat_id, draft)
        return

    if data == "confirm:preview":
        draft = DRAFTS.get(chat_id, {})
        send_preview(
            context, chat_id,
            draft.get("brand", "?"), draft.get("model"),
            draft.get("year_from"), draft.get("year_to"), draft.get("max_price"),
        )
        return


def show_confirm(query, draft):
    summary = describe_criteria(
        draft.get("brand", "?"), draft.get("model"),
        draft.get("year_from"), draft.get("year_to"), draft.get("max_price"),
    )
    query.edit_message_text("Фильтр готов:\n{}\n\nЧто дальше?".format(summary), reply_markup=confirm_kb())


def finalize_watch(query, context, chat_id, draft):
    brand = draft.get("brand", "?")
    model = draft.get("model")
    year_from = draft.get("year_from")
    year_to = draft.get("year_to")
    max_price = draft.get("max_price")
    watch_id = db.add_watch(chat_id, brand, model, year_from, year_to, max_price)
    w = {
        "id": watch_id, "chat_id": chat_id, "brand": brand, "model": model,
        "year_from": year_from, "year_to": year_to, "max_price": max_price,
    }
    DRAFTS.pop(chat_id, None)
    query.edit_message_text("✅ Фильтр сохранён:\n{}".format(describe_watch(w)))
    context.bot.send_message(chat_id, "Готово. Ждите уведомлений о новых лотах.", reply_markup=main_menu_kb())


# --------------------------------------------------------------------------
# Текстовые сообщения (для шагов "введите ... текстом")
# --------------------------------------------------------------------------


def on_text(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    draft = DRAFTS.get(chat_id)
    text = (update.effective_message.text or "").strip()

    if not draft or "awaiting" not in draft:
        update.effective_message.reply_text(
            "Не понял. Нажмите /start, чтобы открыть меню, или /watch, чтобы добавить фильтр."
        )
        return

    awaiting = draft["awaiting"]

    if awaiting == "brand_text":
        if not text:
            update.effective_message.reply_text("Пустая марка не подходит, напишите ещё раз:")
            return
        draft["brand"] = text
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Марка: {}\nИщу модели в данных аукционов…".format(text))
        candidates = fetch_model_candidates(text)
        draft["model_candidates"] = candidates
        update.effective_message.reply_text("Выберите модель:", reply_markup=model_kb(candidates))
        return

    if awaiting == "model_text":
        draft["model"] = text or None
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Год от:", reply_markup=year_kb("yf"))
        return

    if awaiting == "yf_text":
        year = parse_manual_year(text)
        if year is None:
            update.effective_message.reply_text("Не понял год. Напишите числом, например 2015 или 15:")
            return
        draft["year_from"] = year
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Год до:", reply_markup=year_kb("yt"))
        return

    if awaiting == "yt_text":
        year = parse_manual_year(text)
        if year is None:
            update.effective_message.reply_text("Не понял год. Напишите числом, например 2026 или 26:")
            return
        draft["year_to"] = year
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Максимальная цена, ₽:", reply_markup=price_kb())
        return

    if awaiting == "price_text":
        price = parse_price(text)
        if price is None:
            update.effective_message.reply_text("Не понял цену. Напишите числом, например 500000:")
            return
        draft["max_price"] = price
        draft.pop("awaiting", None)
        summary = describe_criteria(
            draft.get("brand", "?"), draft.get("model"),
            draft.get("year_from"), draft.get("year_to"), draft.get("max_price"),
        )
        update.effective_message.reply_text(
            "Фильтр готов:\n{}\n\nЧто дальше?".format(summary), reply_markup=confirm_kb()
        )
        return


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------


def main():
    # Создаёт bot_watches/bot_seen_lots, если их ещё нет — падает сразу
    # и понятно на старте, если база недоступна, а не при первом нажатии
    # кнопки пользователем.
    db.init_schema()
    log.info("Схема БД готова (MYSQL_HOST=%s MYSQL_DB=%s).", db.MYSQL_HOST, db.MYSQL_DB)

    updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler(["start", "help"], start_cmd))
    dp.add_handler(CommandHandler("watch", watch_cmd))
    dp.add_handler(CommandHandler("mywatches", mywatches_cmd))
    dp.add_handler(CommandHandler("cancel", cancel_cmd))
    dp.add_handler(CallbackQueryHandler(on_callback))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, on_text))

    updater.job_queue.run_repeating(check_job, interval=CHECK_INTERVAL_SECONDS, first=10)

    log.info("Бот запущен, проверка каждые %s сек.", CHECK_INTERVAL_SECONDS)
    updater.start_polling(drop_pending_updates=True)
    updater.idle()


if __name__ == "__main__":
    main()
