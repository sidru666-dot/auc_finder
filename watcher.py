#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram-бот для отслеживания мотоциклетных лотов на аукционах.

Источники:
  - bid.cars          — агрегатор Copart + IAAI (США), проверенный JSON-эндпоинт поиска.
  - japanlife-moto.ru — брокер японских мотоаукционов, целые мотоциклы (только японские
    марки: Honda, Kawasaki, Suzuki, Yamaha, BMW, Harley-Davidson). Разбор HTML эвристический —
    если сайт поменяет вёрстку, парсинг может сломаться.

Любой человек может подписаться на бота и настроить свои фильтры прямо командами в Telegram —
никакого общего конфига редактировать не нужно:

  /start                         — приветствие и список команд
  /watch Марка | Модель | Год_от | Год_до | Макс_цена$
        любое поле кроме марки можно заменить на "-", чтобы не фильтровать по нему
        пример: /watch Aprilia | Tuono V4 | 2015 | 2024 | 8000
        пример: /watch Honda | - | - | - | -        (вообще все Honda)
  /mywatches                     — список своих правил с номерами
  /unwatch <номер>                — удалить правило

Бот НЕ работает как постоянно висящий процесс (это невозможно бесплатно на GitHub Actions) —
вместо этого при каждом запуске он забирает накопившиеся сообщения через getUpdates,
обрабатывает команды, затем проверяет аукционы и рассылает подходящие лоты подписчикам.
Между запусками может пройти до интервала cron из .github/workflows/watch.yml.

Токен бота берётся из переменной окружения TELEGRAM_BOT_TOKEN (GitHub Secret) —
в коде и в файлах репозитория он никогда не хранится.
"""

import os
import re
import json
import time
import requests
from bs4 import BeautifulSoup

STATE_FILE = "seen_lots.json"
SUBSCRIBERS_FILE = "subscribers.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

JLM_BRANDS = {"honda", "kawasaki", "suzuki", "yamaha", "bmw", "harley-davidson"}

HELP_TEXT = (
    "Привет! Я слежу за мотолотами на аукционах (bid.cars — Copart/IAAI, США; "
    "japanlife-moto.ru — японские аукционы целых мотоциклов) и присылаю новые лоты "
    "по вашим фильтрам.\n\n"
    "<b>Команды:</b>\n"
    "/watch Марка | Модель | Год_от | Год_до | Макс_цена$\n"
    "Любое поле кроме марки можно заменить на «-», чтобы не фильтровать по нему.\n"
    "Пример: <code>/watch Aprilia | Tuono V4 | 2015 | 2024 | 8000</code>\n"
    "Пример: <code>/watch Honda | - | - | - | -</code>\n\n"
    "/mywatches — список ваших правил\n"
    "/unwatch НОМЕР — удалить правило\n\n"
    "Проверка идёт по расписанию (не мгновенно), интервал — в README репозитория.\n"
    "Учтите: japanlife-moto.ru продаёт только Honda/Kawasaki/Suzuki/Yamaha/BMW/"
    "Harley-Davidson — прочие марки проверяются только на bid.cars."
)


# ---------------------------------------------------------------- служебное

def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_state():
    return load_json(STATE_FILE, {})


def save_state(state):
    save_json(STATE_FILE, state)


def load_subscribers():
    return load_json(SUBSCRIBERS_FILE, {"next_id": 1, "update_offset": 0, "watches": []})


def save_subscribers(data):
    save_json(SUBSCRIBERS_FILE, data)


def send_telegram(chat_id, text, photo_url=None):
    if not API:
        print("Нет TELEGRAM_BOT_TOKEN, пропускаю отправку:\n", text)
        return
    try:
        if photo_url:
            resp = requests.post(
                f"{API}/sendPhoto",
                data={"chat_id": chat_id, "caption": text[:1024], "parse_mode": "HTML"},
                params={"photo": photo_url},
                timeout=20,
            )
            if resp.status_code == 200:
                return
        requests.post(
            f"{API}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            },
            timeout=20,
        )
    except Exception as e:
        print(f"Ошибка отправки в Telegram (chat {chat_id}):", e)


# ---------------------------------------------------------- команды бота

def parse_watch_args(raw):
    """'Aprilia | Tuono V4 | 2015 | 2024 | 8000' -> dict с полями, '-' -> None."""
    parts = [p.strip() for p in raw.split("|")]
    parts += [""] * (5 - len(parts))  # дополняем до 5 полей
    brand, model, year_from, year_to, max_price = parts[:5]

    def clean(v):
        v = (v or "").strip()
        return None if v in ("", "-") else v

    brand = clean(brand)
    model = clean(model)
    year_from = clean(year_from)
    year_to = clean(year_to)
    max_price = clean(max_price)

    if not brand:
        return None, "Марка обязательна. Пример: /watch Aprilia | Tuono V4 | 2015 | 2024 | 8000"

    try:
        year_from = int(year_from) if year_from else None
        year_to = int(year_to) if year_to else None
    except ValueError:
        return None, "Год должен быть числом (например 2015), либо «-»."

    try:
        max_price = float(re.sub(r"[^\d.]", "", max_price)) if max_price else None
    except ValueError:
        return None, "Максимальная цена должна быть числом (например 8000), либо «-»."

    return {
        "brand": brand,
        "model": model,
        "year_from": year_from,
        "year_to": year_to,
        "max_price": max_price,
    }, None


def describe_watch(w):
    bits = [w["brand"]]
    if w.get("model"):
        bits.append(w["model"])
    year = ""
    if w.get("year_from") or w.get("year_to"):
        year = f" [{w.get('year_from') or '..'}–{w.get('year_to') or '..'}]"
    price = f" до ${w['max_price']:.0f}" if w.get("max_price") else ""
    return f"#{w['id']}: {' / '.join(bits)}{year}{price}"


def handle_updates(sub_data):
    if not API:
        return
    offset = sub_data.get("update_offset", 0)
    try:
        resp = requests.get(f"{API}/getUpdates", params={"offset": offset, "timeout": 0}, timeout=20)
        resp.raise_for_status()
        result = resp.json().get("result", [])
    except Exception as e:
        print("Ошибка getUpdates:", e)
        return

    for upd in result:
        sub_data["update_offset"] = upd["update_id"] + 1
        msg = upd.get("message") or {}
        chat_id = (msg.get("chat") or {}).get("id")
        text = (msg.get("text") or "").strip()
        if not chat_id or not text:
            continue

        if text.startswith("/start") or text.startswith("/help"):
            send_telegram(chat_id, HELP_TEXT)

        elif text.startswith("/watch"):
            raw = text[len("/watch"):].strip()
            if not raw:
                send_telegram(chat_id, "Использование: /watch Марка | Модель | Год_от | Год_до | Макс_цена")
                continue
            watch, err = parse_watch_args(raw)
            if err:
                send_telegram(chat_id, err)
                continue
            watch["id"] = sub_data["next_id"]
            watch["chat_id"] = chat_id
            sub_data["next_id"] += 1
            sub_data["watches"].append(watch)
            send_telegram(chat_id, f"Готово, слежу: {describe_watch(watch)}")

        elif text.startswith("/mywatches"):
            mine = [w for w in sub_data["watches"] if w["chat_id"] == chat_id]
            if not mine:
                send_telegram(chat_id, "У вас пока нет правил. Добавьте через /watch — см. /help")
            else:
                send_telegram(chat_id, "Ваши правила:\n" + "\n".join(describe_watch(w) for w in mine))

        elif text.startswith("/unwatch"):
            arg = text[len("/unwatch"):].strip()
            try:
                wid = int(arg)
            except ValueError:
                send_telegram(chat_id, "Использование: /unwatch НОМЕР (номер из /mywatches)")
                continue
            before = len(sub_data["watches"])
            sub_data["watches"] = [
                w for w in sub_data["watches"] if not (w["id"] == wid and w["chat_id"] == chat_id)
            ]
            if len(sub_data["watches"]) < before:
                send_telegram(chat_id, f"Правило #{wid} удалено.")
            else:
                send_telegram(chat_id, f"Правило #{wid} не найдено среди ваших.")

        else:
            send_telegram(chat_id, "Не понял команду. Наберите /help")


# ------------------------------------------------------------- источники

def parse_price(text):
    if not text:
        return None
    digits = re.sub(r"[^\d.]", "", str(text))
    try:
        return float(digits) if digits else None
    except ValueError:
        return None


def parse_year(text):
    m = re.search(r"\b(19[5-9]\d|20[0-4]\d)\b", text or "")
    return int(m.group(1)) if m else None


def matches(watch, brand, model_text, year, price):
    if watch["brand"].strip().lower() != brand.strip().lower():
        return False
    if watch.get("model") and watch["model"].lower() not in (model_text or "").lower():
        return False
    if watch.get("year_from") and year and year < watch["year_from"]:
        return False
    if watch.get("year_to") and year and year > watch["year_to"]:
        return False
    if watch.get("max_price") and price is not None and price > watch["max_price"]:
        return False
    return True


def fetch_bidcars(brand):
    url = (
        "https://bid.cars/app/search/request"
        f"?search-type=filters&status=All&type=Motorcycle&make={requests.utils.quote(brand)}"
        "&model=All&year-from=1900&year-to=2027&auction-type=All"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=25)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        print(f"[bidcars] Ошибка запроса для марки {brand}: {e}")
        return []


def fetch_japanlifemoto(brand):
    url = f"https://japanlife-moto.ru/auc/{brand.lower()}/"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        print(f"[japanlifemoto] Ошибка запроса для бренда {brand}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    link_re = re.compile(rf"^/auc/{re.escape(brand.lower())}/[^/]+/\d+/?$")
    items = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not link_re.match(href):
            continue
        lot_id_match = re.search(r"/(\d+)/?$", href)
        if not lot_id_match:
            continue

        title = a.get_text(strip=True)
        if not title:
            parent = a.find_parent()
            hops = 0
            while parent is not None and not title and hops < 4:
                title = parent.get_text(" ", strip=True)
                parent = parent.find_parent()
                hops += 1
        title = re.sub(r"\s+", " ", title or "").strip()

        items.append(
            {
                "lot_id": lot_id_match.group(1),
                "title": title,
                "url": "https://japanlife-moto.ru" + href,
            }
        )
    return items


def check_all(sub_data, state):
    watches = sub_data["watches"]
    if not watches:
        return

    brands = sorted({w["brand"].strip() for w in watches}, key=str.lower)

    for brand in brands:
        brand_watches = [w for w in watches if w["brand"].strip().lower() == brand.lower()]

        # --- bid.cars ---
        for item in fetch_bidcars(brand):
            lot = item.get("lot")
            tag = item.get("tag")
            name = item.get("name") or ""
            if not lot:
                continue
            year = parse_year(name)
            price = parse_price(item.get("final_bid_formatted") or item.get("prebid_price"))
            lot_url = f"https://bid.cars/en/lot/{lot}/{tag}"

            for w in brand_watches:
                if not matches(w, brand, name, year, price):
                    continue
                key = f"bidcars:{lot}:{w['chat_id']}"
                if key in state:
                    continue
                state[key] = True
                location = item.get("location", "")
                close_time = (item.get("prebid_close_time_lang") or {}).get("ru") or item.get(
                    "time_left_formatted", ""
                )
                price_line = item.get("final_bid_formatted") or item.get("prebid_price") or "цена не указана"
                buy_now = item.get("buy_now_price")
                img = (item.get("img") or {}).get("img_1")
                text = (
                    f"🏍 <b>Новый лот на bid.cars</b> (правило {describe_watch(w)})\n"
                    f"{name}\n"
                    f"Локация: {location}\n"
                    f"Ставка: {price_line}" + (f" | Buy Now: {buy_now}" if buy_now else "") + "\n"
                    f"Окончание: {close_time}\n"
                    f"{lot_url}"
                )
                send_telegram(w["chat_id"], text, img)
                time.sleep(1)

        # --- japanlife-moto.ru (только поддерживаемые марки) ---
        if brand.lower() in JLM_BRANDS:
            for item in fetch_japanlifemoto(brand):
                year = parse_year(item["title"])
                price = parse_price(item["title"])  # эвристика, может не найти цену
                for w in brand_watches:
                    if not matches(w, brand, item["title"], year, price):
                        continue
                    key = f"jlm:{brand.lower()}:{item['lot_id']}:{w['chat_id']}"
                    if key in state:
                        continue
                    state[key] = True
                    text = (
                        f"🇯🇵 <b>Новый лот на japanlife-moto.ru</b> (правило {describe_watch(w)})\n"
                        f"{item['title'] or 'Без названия — откройте ссылку'}\n"
                        f"{item['url']}"
                    )
                    send_telegram(w["chat_id"], text)
                    time.sleep(1)

        time.sleep(1)


def main():
    sub_data = load_subscribers()
    state = load_state()

    handle_updates(sub_data)
    save_subscribers(sub_data)  # сохраняем сразу, чтобы новые правила не потерялись при сбое ниже

    check_all(sub_data, state)
    save_state(state)


if __name__ == "__main__":
    main()
