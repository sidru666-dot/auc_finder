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

# Сколько лотов отправлять за один раз (и в мгновенном просмотре, и в
# фоновых уведомлениях) — дальше показываем по кнопке "Показать ещё",
# а не высыпаем все найденные лоты одним потоком сообщений подряд.
BATCH_SIZE = 10

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
    "Всё управление — кнопками, ничего печатать не нужно (кроме случаев, "
    "когда сами захотите ввести марку/модель/цифру вручную):\n"
    "🏍 «Аукционы онлайн» — задать марку/модель/годы/цену и другие "
    "фильтры кнопками, как на сайте, и сразу посмотреть, что подходит\n"
    "📊 «Статистика» — история проданных лотов (пока недоступна, см. "
    "пояснение в самой кнопке)\n"
    "🔔 «Мои оповещения» — список сохранённых фильтров: можно мгновенно "
    "проверить или удалить\n\n"
    "Команды остаются для тех, кому так привычнее: /watch (то же самое, "
    "что «Аукционы онлайн»), /mywatches, /cancel.\n\n"
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


def describe_criteria(brand, model, year_from, year_to, max_price, extra=None):
    model_s = model or "любая модель"
    years = "{}–{}".format(year_from or "…", year_to or "…") if (year_from or year_to) else "любые годы"
    price_s = "до {} ₽".format(format_rub(max_price)) if max_price else "без ограничения по цене"
    base = "{} / {} [{}] {}".format(brand, model_s, years, price_s)
    return base + "; " + extra if extra else base


def describe_extra(w):
    """Доп. критерии (пробег/объём/оценка/лот/VIN/текст) — те же поля,
    что в форме поиска на jmmoto.ru, отдельной строкой, если заданы."""
    parts = []
    if w.get("mileage_from") or w.get("mileage_to"):
        parts.append("пробег {}–{} км".format(w.get("mileage_from") or "…", w.get("mileage_to") or "…"))
    if w.get("engine_from") or w.get("engine_to"):
        parts.append("объём {}–{} см³".format(w.get("engine_from") or "…", w.get("engine_to") or "…"))
    if w.get("grade_from") is not None or w.get("grade_to") is not None:
        gf = w.get("grade_from")
        gt = w.get("grade_to")
        if gf is not None or gt is not None:
            parts.append("оценка {}–{}".format(gf if gf is not None else "…", gt if gt is not None else "…"))
    if w.get("lot_number"):
        parts.append("лот №{}".format(w["lot_number"]))
    if w.get("vin"):
        parts.append("VIN {}".format(w["vin"]))
    if w.get("free_text"):
        parts.append('текст "{}"'.format(w["free_text"]))
    return ", ".join(parts)


def describe_watch(w):
    return "#{}: {}".format(
        w["id"],
        describe_criteria(
            w["brand"], w.get("model"), w.get("year_from"), w.get("year_to"), w.get("max_price"),
            extra=describe_extra(w),
        ),
    )


def matches(watch, brand, model_text, year, price, lot=None):
    if (watch.get("brand") or "").lower() != brand.lower():
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

    if lot is None:
        return True

    if watch.get("mileage_from") or watch.get("mileage_to"):
        mileage = parse_price(lot.get("mileage"))
        if mileage is None:
            return False
        if watch.get("mileage_from") and mileage < watch["mileage_from"]:
            return False
        if watch.get("mileage_to") and mileage > watch["mileage_to"]:
            return False

    if watch.get("engine_from") or watch.get("engine_to"):
        engine = parse_price(lot.get("engine_volume"))
        if engine is None:
            return False
        if watch.get("engine_from") and engine < watch["engine_from"]:
            return False
        if watch.get("engine_to") and engine > watch["engine_to"]:
            return False

    if watch.get("grade_from") is not None or watch.get("grade_to") is not None:
        grade = parse_price(lot.get("grade_overall"))
        if grade is None:
            return False
        if watch.get("grade_from") is not None and grade < watch["grade_from"]:
            return False
        if watch.get("grade_to") is not None and grade > watch["grade_to"]:
            return False

    if watch.get("lot_number"):
        if not lot.get("lot_number") or watch["lot_number"].lower() not in str(lot["lot_number"]).lower():
            return False

    if watch.get("vin"):
        if not lot.get("vin") or watch["vin"].lower() not in str(lot["vin"]).lower():
            return False

    if watch.get("free_text"):
        haystack = " ".join(
            str(lot.get(k) or "") for k in
            ("description_en", "description_ru", "model", "brand", "auction_name")
        ).lower()
        if watch["free_text"].lower() not in haystack:
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


# Реальные фото лота (поле pictures у aleado) склеены знаком "#", а НЕ
# запятой — важно, иначе вся склейка из нескольких ссылок улетает в
# Telegram одной "битой" строкой и фото не показывается вовсе, либо
# показывается не то, что нужно. На всякий случай поддерживаем и другие
# возможные разделители, если формат когда-нибудь поменяется.
_PHOTO_SEPARATORS = ("#", ",", ";", "|", "\n")

# Схемы повреждений/техосмотра (например, от аукционного дома BDS) — это
# ДРУГОЙ набор картинок, встроенный в текст description_en/ru (поле
# parsed_data_en/ru), не в pictures. Он никогда не должен использоваться
# как фото лота, но на случай, если он всё же просочится через
# resolve_columns (например, поставщик переименует колонки) —
# подстраховываемся и просто отбрасываем ссылки с этих доменов/путей.
_DIAGRAM_URL_HINTS = ("jupiter.ac", "bdsc", "disp/bds", "image_item")


def lot_photos(lot):
    """Все ссылки на реальные фото лота (может быть несколько ракурсов),
    без диаграмм повреждений.

    ВАЖНО (проверено вживую на сайте jmmoto.ru, который берёт фото из
    того же самого поля aleado): в списке pictures/photo_url первая
    ссылка (npic=0) — это ВСЕГДА общая схема/чек-лист техосмотра (общий
    силуэт мотоцикла с отметками повреждений или лист с оценками), а не
    фото конкретного лота — одинаково что у аукциона JBA, что у BDS.
    Настоящие фото самого мотоцикла идут со второй ссылки (npic=1) и
    дальше. Поэтому первую ссылку в списке всегда пропускаем."""
    raw = lot.get("photo_url")
    if not raw:
        return []
    raw = str(raw)
    for sep in _PHOTO_SEPARATORS[1:]:
        raw = raw.replace(sep, "#")
    urls = []
    for part in raw.split("#"):
        part = part.strip()
        if not part or not part.lower().startswith(("http://", "https://")):
            continue
        if any(hint in part.lower() for hint in _DIAGRAM_URL_HINTS):
            continue
        urls.append(part)
    # Первая ссылка (npic=0) — общая схема техосмотра, не фото лота;
    # пропускаем её, если есть из чего выбирать дальше.
    return urls[1:] if len(urls) > 1 else urls


def lot_photo(lot):
    """Первое настоящее фото лота (npic=1) для превью, либо None."""
    urls = lot_photos(lot)
    return urls[0] if urls else None


def lot_display_name(lot):
    brand = (lot.get("brand") or "?").strip()
    model = (lot.get("model") or "").strip()
    return "{} {}".format(brand, model).strip()


# Сайт-первоисточник этих же данных (тот же поставщик, что и у фида
# aleado). Формат ссылки на карточку лота подтверждён вживую на сайте:
# https://jmmoto.ru/<lotId>, где <lotId> — числовой ID лота (у Yamaha
# MT-09, например, https://jmmoto.ru/2038905137) — то же самое число,
# что лежит в колонке aleado "id" (наше каноническое поле lot_id). Это
# ДРУГОЕ число, чем "номер лота"/bid (которое видно в карточке как
# "Номер лота" — например 3047), их нельзя путать местами.
JMMOTO_BASE_URL = "https://jmmoto.ru/"


def lot_jmmoto_url(lot):
    lot_id = lot.get("lot_id")
    if lot_id:
        return "{}{}".format(JMMOTO_BASE_URL, lot_id)
    return JMMOTO_BASE_URL


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

    lines.append(
        '<a href="{}">Смотреть на jmmoto.ru</a> | Источник: aleado (Япония)'.format(
            html.escape(lot_jmmoto_url(lot), quote=True)
        )
    )
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


# --------------------------------------------------------------------------
# Показ лотов порциями по BATCH_SIZE (и в фоновых уведомлениях, и в
# мгновенном просмотре) — вместо того, чтобы высыпать все найденные лоты
# одним потоком сообщений подряд, показываем по BATCH_SIZE штук и ждём
# нажатия "Показать ещё"/"Прекратить".
# --------------------------------------------------------------------------

PENDING_LOTS = {}  # chat_id -> список {"text":..., "photo":...}, ещё не показанных


def batch_kb(remaining):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "▶️ Показать ещё {} (осталось {})".format(min(BATCH_SIZE, remaining), remaining),
            callback_data="morelots",
        )],
        [InlineKeyboardButton("⏹ Прекратить показ", callback_data="stoplots")],
    ])


def send_lots_batch(bot, chat_id):
    """Отправляет очередную порцию (до BATCH_SIZE) лотов из очереди
    PENDING_LOTS[chat_id]; если после неё в очереди ещё что-то осталось —
    добавляет кнопки "Показать ещё"/"Прекратить показ"."""
    queue = PENDING_LOTS.get(chat_id) or []
    if not queue:
        PENDING_LOTS.pop(chat_id, None)
        return
    batch, rest = queue[:BATCH_SIZE], queue[BATCH_SIZE:]
    if rest:
        PENDING_LOTS[chat_id] = rest
    else:
        PENDING_LOTS.pop(chat_id, None)

    for item in batch:
        send_lot(bot, chat_id, item["text"], item.get("photo"))

    if rest:
        try:
            bot.send_message(
                chat_id=chat_id,
                text="Показано {} лотов, осталось ещё {}.".format(len(batch), len(rest)),
                reply_markup=batch_kb(len(rest)),
            )
        except Exception as e:
            log.warning("batch control message failed for chat %s: %s", chat_id, e)


def enqueue_lots(bot, chat_id, items):
    """Кладёт лоты в очередь показа для chat_id. Если очередь была
    пустой — сразу отправляет первую порцию; если у пользователя уже
    есть непросмотренная порция — просто дописывает в конец, не шлём
    вторую порцию поверх ещё не просмотренной."""
    if not items:
        return
    was_empty = not PENDING_LOTS.get(chat_id)
    PENDING_LOTS.setdefault(chat_id, []).extend(items)
    if was_empty:
        send_lots_batch(bot, chat_id)


def check_all(bot):
    # Сначала обновляем/греем кэш фида в любом случае (даже если сейчас
    # ни у кого нет фильтров) — иначе кэш никогда не обновлялся бы
    # фоново без подписок, и первый же пользователь, нажавший кнопку,
    # сам того не зная, попадал бы на холодное обновление (скачивание +
    # импорт дампа) прямо во время нажатия кнопки, и ждал бы результата.
    lots = aleado.get_lots()

    watches = db.list_watches()
    if not watches:
        return

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
    by_chat = {}

    for lot in lots:
        lot_id = lot.get("lot_id") or lot.get("lot_number")
        brand = (lot.get("brand") or "").strip()
        if not lot_id or not brand:
            continue
        model = lot.get("model") or ""
        year = lot_year(lot)
        price = lot_price_rub(lot)

        for w in watches:
            if not matches(w, brand, model, year, price, lot=lot):
                continue
            key = "aleado:{}:{}".format(lot_id, w["chat_id"])
            if key in seen:
                continue
            seen.add(key)
            newly_seen.append(key)
            text = format_lot_text(lot, brand, model, year)
            by_chat.setdefault(w["chat_id"], []).append({"text": text, "photo": lot_photo(lot)})

    db.mark_seen_bulk(newly_seen)

    for chat_id, items in by_chat.items():
        enqueue_lots(bot, chat_id, items)


def check_job(context: CallbackContext):
    try:
        check_all(context.bot)
    except Exception:
        log.exception("check_job failed")


# --------------------------------------------------------------------------
# Мгновенный просмотр текущих лотов ("Показать сейчас" / "🔍 Проверить")
# --------------------------------------------------------------------------


def send_preview(context, chat_id, watch):
    """Мгновенный просмотр лотов по критериям (watch — словарь с ключами
    brand/model/year_from/year_to/max_price и, опционально, доп. полями
    mileage_from/mileage_to/engine_from/engine_to/grade_from/grade_to/
    lot_number/vin/free_text) — отдельным сообщением с фото на каждый
    лот, порциями по BATCH_SIZE (см. enqueue_lots), а не одним слитным
    текстовым сообщением на все найденные лоты сразу."""
    try:
        context.bot.send_chat_action(chat_id=chat_id, action="typing")
    except Exception:
        pass

    items = []
    for lot in aleado.get_lots():
        lbrand = (lot.get("brand") or "").strip()
        if not lbrand:
            continue
        lmodel = lot.get("model") or ""
        year = lot_year(lot)
        price = lot_price_rub(lot)
        if not matches(watch, lbrand, lmodel, year, price, lot=lot):
            continue
        items.append({"text": format_lot_text(lot, lbrand, lmodel, year), "photo": lot_photo(lot)})

    if not items:
        extra = ""
        if aleado.last_error():
            extra = "\n\n(Похоже, фид aleado сейчас недоступен: {})".format(aleado.last_error())
        context.bot.send_message(
            chat_id=chat_id,
            text="Прямо сейчас по этим критериям на аукционах ничего не найдено." + extra,
        )
        return

    context.bot.send_message(chat_id=chat_id, text="Сейчас подходит лотов: {}".format(len(items)))
    enqueue_lots(context.bot, chat_id, items)


# --------------------------------------------------------------------------
# Клавиатуры
# --------------------------------------------------------------------------


def main_menu_kb():
    """Главное меню — как на сайте: сразу выбор режима (Аукционы
    онлайн / Статистика), а не команда текстом."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏍 Аукционы онлайн", callback_data="menu:new")],
        [InlineKeyboardButton("📊 Статистика", callback_data="menu:stats")],
        [InlineKeyboardButton("🔔 Мои оповещения", callback_data="menu:list")],
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


# Доп. фильтры — как на форме поиска jmmoto.ru (пробег/объём
# двигателя/оценка состояния/номер лота/VIN/свободный текст). Шаг
# необязательный: после выбора цены можно сразу нажать "Готово, к
# сохранению" в adv_menu_kb() и пропустить всё это.
MILEAGE_VALUES = [5000, 10000, 20000, 30000, 50000, 80000, 120000, 200000]
ENGINE_VALUES = [125, 250, 400, 600, 750, 900, 1000, 1300]
GRADE_VALUES = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]


def range_kb(prefix, values, suffix=""):
    rows = []
    row = []
    for v in values:
        row.append(InlineKeyboardButton("{}{}".format(v, suffix), callback_data="{}:{}".format(prefix, v)))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Без ограничения", callback_data="{}:none".format(prefix))])
    rows.append([InlineKeyboardButton("Ввести вручную", callback_data="{}:text".format(prefix))])
    rows.append([InlineKeyboardButton("‹ Назад к доп. фильтрам", callback_data="adv:menu")])
    return InlineKeyboardMarkup(rows)


def adv_menu_kb(draft):
    def mark(is_set):
        return "✅ " if is_set else ""

    rows = [
        [InlineKeyboardButton(
            "{}Пробег, км".format(mark(draft.get("mileage_from") or draft.get("mileage_to"))),
            callback_data="adv:mileage",
        )],
        [InlineKeyboardButton(
            "{}Объём двигателя, см³".format(mark(draft.get("engine_from") or draft.get("engine_to"))),
            callback_data="adv:engine",
        )],
        [InlineKeyboardButton(
            "{}Оценка состояния".format(mark(draft.get("grade_from") is not None or draft.get("grade_to") is not None)),
            callback_data="adv:grade",
        )],
        [InlineKeyboardButton("{}Номер лота".format(mark(draft.get("lot_number"))), callback_data="adv:lot")],
        [InlineKeyboardButton("{}VIN / номер рамы".format(mark(draft.get("vin"))), callback_data="adv:vin")],
        [InlineKeyboardButton("{}Свободный поиск (текст)".format(mark(draft.get("free_text"))), callback_data="adv:text")],
        [InlineKeyboardButton("✅ Готово, к сохранению", callback_data="adv:done")],
        [InlineKeyboardButton("✖ Отмена", callback_data="cancel")],
    ]
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
        update.effective_message.reply_text(
            "У вас пока нет сохранённых оповещений.", reply_markup=main_menu_kb()
        )
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

    if data == "menu:stats":
        query.edit_message_text(
            "«Статистика» (история проданных лотов, как на jmmoto.ru) пока "
            "недоступна: для неё нужен отдельный раздел фида aleado с уже "
            "прошедшими торгами, а сейчас подключены только текущие/будущие "
            "лоты («Аукционы онлайн»). Как только появится доступ к данным "
            "по статистике — добавлю этот раздел и сюда.",
            reply_markup=main_menu_kb(),
        )
        return

    if data == "menu:list":
        text, kb = mywatches_view(chat_id)
        if not kb:
            query.edit_message_text("У вас пока нет сохранённых оповещений.", reply_markup=main_menu_kb())
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

    if data == "morelots":
        try:
            query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        send_lots_batch(context.bot, chat_id)
        return

    if data == "stoplots":
        PENDING_LOTS.pop(chat_id, None)
        try:
            query.edit_message_text(
                "Показ остановлен. Оставшиеся лоты уже отмечены как показанные и "
                "повторно не придут — при следующей проверке пришлём только новые."
            )
        except Exception:
            pass
        return

    if data.startswith("checknow:"):
        wid = int(data.split(":", 1)[1])
        w = db.get_watch(wid, chat_id)
        if not w:
            context.bot.send_message(chat_id, "Фильтр не найден (возможно, уже удалён).")
            return
        send_preview(context, chat_id, w)
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
        query.edit_message_text(
            "Марка: {}\nИщу модели в данных аукционов… (обычно доля секунды; "
            "если бот только что перезапускался — может занять до пары минут "
            "на первый раз, дальше будет быстро)".format(brand)
        )
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
        query.edit_message_text(
            "Готово. Можно ещё уточнить (необязательно) — как на jmmoto.ru: пробег, "
            "объём двигателя, оценку состояния, номер лота, VIN, свободный текст.\n"
            "Или сразу «Готово, к сохранению».",
            reply_markup=adv_menu_kb(draft),
        )
        return

    if data == "adv:menu":
        draft = DRAFTS.setdefault(chat_id, {})
        query.edit_message_text("Доп. фильтры (необязательно):", reply_markup=adv_menu_kb(draft))
        return

    if data == "adv:mileage":
        query.edit_message_text("Пробег от, км:", reply_markup=range_kb("mf", MILEAGE_VALUES, " км"))
        return

    if data == "adv:engine":
        query.edit_message_text("Объём двигателя от, см³:", reply_markup=range_kb("ef", ENGINE_VALUES, " см³"))
        return

    if data == "adv:grade":
        query.edit_message_text("Оценка состояния от:", reply_markup=range_kb("gf", GRADE_VALUES))
        return

    if data == "adv:lot":
        draft = DRAFTS.setdefault(chat_id, {})
        draft["awaiting"] = "lot_text"
        query.edit_message_text("Введите номер лота текстом:")
        return

    if data == "adv:vin":
        draft = DRAFTS.setdefault(chat_id, {})
        draft["awaiting"] = "vin_text"
        query.edit_message_text("Введите VIN / номер рамы текстом (можно частично):")
        return

    if data == "adv:text":
        draft = DRAFTS.setdefault(chat_id, {})
        draft["awaiting"] = "freetext_text"
        query.edit_message_text("Введите слово/фразу для свободного поиска по описанию лота:")
        return

    if data == "adv:done":
        draft = DRAFTS.get(chat_id, {})
        show_confirm(query, draft)
        return

    if data.startswith("mf:"):
        val = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if val == "text":
            draft["awaiting"] = "mf_text"
            query.edit_message_text("Введите пробег «от» числом, км:")
            return
        draft["mileage_from"] = None if val == "none" else int(val)
        query.edit_message_text("Пробег до, км:", reply_markup=range_kb("mt", MILEAGE_VALUES, " км"))
        return

    if data.startswith("mt:"):
        val = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if val == "text":
            draft["awaiting"] = "mt_text"
            query.edit_message_text("Введите пробег «до» числом, км:")
            return
        draft["mileage_to"] = None if val == "none" else int(val)
        query.edit_message_text("Доп. фильтры:", reply_markup=adv_menu_kb(draft))
        return

    if data.startswith("ef:"):
        val = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if val == "text":
            draft["awaiting"] = "ef_text"
            query.edit_message_text("Введите объём двигателя «от» числом, см³:")
            return
        draft["engine_from"] = None if val == "none" else int(val)
        query.edit_message_text("Объём двигателя до, см³:", reply_markup=range_kb("et", ENGINE_VALUES, " см³"))
        return

    if data.startswith("et:"):
        val = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if val == "text":
            draft["awaiting"] = "et_text"
            query.edit_message_text("Введите объём двигателя «до» числом, см³:")
            return
        draft["engine_to"] = None if val == "none" else int(val)
        query.edit_message_text("Доп. фильтры:", reply_markup=adv_menu_kb(draft))
        return

    if data.startswith("gf:"):
        val = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if val == "text":
            draft["awaiting"] = "gf_text"
            query.edit_message_text("Введите оценку «от» числом (0–9):")
            return
        draft["grade_from"] = None if val == "none" else float(val)
        query.edit_message_text("Оценка состояния до:", reply_markup=range_kb("gt", GRADE_VALUES))
        return

    if data.startswith("gt:"):
        val = data.split(":", 1)[1]
        draft = DRAFTS.setdefault(chat_id, {})
        if val == "text":
            draft["awaiting"] = "gt_text"
            query.edit_message_text("Введите оценку «до» числом (0–9):")
            return
        draft["grade_to"] = None if val == "none" else float(val)
        query.edit_message_text("Доп. фильтры:", reply_markup=adv_menu_kb(draft))
        return

    if data == "confirm:save":
        draft = DRAFTS.get(chat_id, {})
        finalize_watch(query, context, chat_id, draft)
        return

    if data == "confirm:preview":
        draft = DRAFTS.get(chat_id, {})
        send_preview(context, chat_id, draft)
        return


def show_confirm(query, draft):
    summary = describe_criteria(
        draft.get("brand", "?"), draft.get("model"),
        draft.get("year_from"), draft.get("year_to"), draft.get("max_price"),
        extra=describe_extra(draft),
    )
    query.edit_message_text("Фильтр готов:\n{}\n\nЧто дальше?".format(summary), reply_markup=confirm_kb())


def finalize_watch(query, context, chat_id, draft):
    brand = draft.get("brand", "?")
    model = draft.get("model")
    year_from = draft.get("year_from")
    year_to = draft.get("year_to")
    max_price = draft.get("max_price")
    watch_id = db.add_watch(
        chat_id, brand, model, year_from, year_to, max_price,
        mileage_from=draft.get("mileage_from"), mileage_to=draft.get("mileage_to"),
        engine_from=draft.get("engine_from"), engine_to=draft.get("engine_to"),
        grade_from=draft.get("grade_from"), grade_to=draft.get("grade_to"),
        lot_number=draft.get("lot_number"), vin=draft.get("vin"), free_text=draft.get("free_text"),
    )
    w = dict(draft)
    w["id"] = watch_id
    w["chat_id"] = chat_id
    w["brand"] = brand
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
        update.effective_message.reply_text(
            "Готово. Можно ещё уточнить (необязательно) — как на jmmoto.ru: пробег, "
            "объём двигателя, оценку состояния, номер лота, VIN, свободный текст.\n"
            "Или сразу «Готово, к сохранению».",
            reply_markup=adv_menu_kb(draft),
        )
        return

    if awaiting == "lot_text":
        draft["lot_number"] = text or None
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Доп. фильтры:", reply_markup=adv_menu_kb(draft))
        return

    if awaiting == "vin_text":
        draft["vin"] = text or None
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Доп. фильтры:", reply_markup=adv_menu_kb(draft))
        return

    if awaiting == "freetext_text":
        draft["free_text"] = text or None
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Доп. фильтры:", reply_markup=adv_menu_kb(draft))
        return

    if awaiting == "mf_text":
        val = parse_price(text)
        if val is None:
            update.effective_message.reply_text("Не понял пробег. Напишите числом, км:")
            return
        draft["mileage_from"] = int(val)
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Пробег до, км:", reply_markup=range_kb("mt", MILEAGE_VALUES, " км"))
        return

    if awaiting == "mt_text":
        val = parse_price(text)
        if val is None:
            update.effective_message.reply_text("Не понял пробег. Напишите числом, км:")
            return
        draft["mileage_to"] = int(val)
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Доп. фильтры:", reply_markup=adv_menu_kb(draft))
        return

    if awaiting == "ef_text":
        val = parse_price(text)
        if val is None:
            update.effective_message.reply_text("Не понял объём. Напишите числом, см³:")
            return
        draft["engine_from"] = int(val)
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Объём двигателя до, см³:", reply_markup=range_kb("et", ENGINE_VALUES, " см³"))
        return

    if awaiting == "et_text":
        val = parse_price(text)
        if val is None:
            update.effective_message.reply_text("Не понял объём. Напишите числом, см³:")
            return
        draft["engine_to"] = int(val)
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Доп. фильтры:", reply_markup=adv_menu_kb(draft))
        return

    if awaiting == "gf_text":
        val = parse_price(text)
        if val is None:
            update.effective_message.reply_text("Не понял оценку. Напишите числом, 0–9:")
            return
        draft["grade_from"] = val
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Оценка состояния до:", reply_markup=range_kb("gt", GRADE_VALUES))
        return

    if awaiting == "gt_text":
        val = parse_price(text)
        if val is None:
            update.effective_message.reply_text("Не понял оценку. Напишите числом, 0–9:")
            return
        draft["grade_to"] = val
        draft.pop("awaiting", None)
        update.effective_message.reply_text("Доп. фильтры:", reply_markup=adv_menu_kb(draft))
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

    # Греем кэш фида aleado сразу при старте, в фоновом потоке, не
    # дожидаясь ни первого срабатывания check_job (через 10 сек), ни уж
    # тем более первого нажатия кнопки пользователем — если бот только
    # что перезапущен и кэш пуст, самое первое скачивание+импорт дампа
    # может занять до пары минут, и лучше, чтобы это ожидание не
    # выпадало на живого пользователя, который в этот момент выбирает
    # марку в /watch.
    threading.Thread(target=aleado.ensure_fresh, name="aleado-warmup", daemon=True).start()

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
