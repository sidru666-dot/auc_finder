# Свежий Debian-базовый образ — сознательно НЕ CentOS 7 (который уже
# снят с поддержки и у которого протухли зеркала пакетов, из-за чего и
# затевался переезд с самостоятельного сервера сюда). apt на актуальном
# Debian просто работает, без плясок с репозиториями.
FROM python:3.11-slim

# default-mysql-client даёт команду `mysql`, которой aleado_client.py
# импортирует дамп фида (см. комментарий там же про то, почему это
# делает именно родной клиент, а не Python-библиотека).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Не буферизуем stdout/stderr — иначе логи в Railway появляются с
# задержкой/пачками, а не по мере вывода.
ENV PYTHONUNBUFFERED=1

CMD ["python3", "bot_server.py"]
