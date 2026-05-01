# Zertix Pinger Bot (Telegram + Minecraft)

Бот каждые 5 секунд пингует Minecraft-серверы (Java protocol), суммирует онлайн и пишет общую метрику в SQLite.

## Что умеет
- `/now` — текстовая статистика за последние 24 часа.
- `/stats` — график онлайна за 24 часа + подпись с ключевыми метриками.
- Источники серверов в `targets.txt`: поддержка `host[:port]`, `ip[:port]`, `CIDR`.
- Автоперезагрузка списка целей на каждом цикле (можно менять `targets.txt` без рестарта).

## Безопасность
- Ограничение по `ALLOWED_USER_IDS` и (опционально) `ALLOWED_CHAT_IDS`.
- Простой rate-limit команд на пользователя.
- Ограничение `MAX_CIDR_HOSTS` для защиты от слишком больших подсетей.
- Таймаут и ограничение параллелизма пингов (`PING_TIMEOUT_SECONDS`, `MAX_CONCURRENCY`).

## Быстрый запуск
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# заполните TELEGRAM_BOT_TOKEN и ALLOWED_USER_IDS
python bot.py
```

## Настройка targets.txt
Пример:
```text
play.example.net
192.168.10.20:25566
144.31.225.0/24
```

> Для CIDR используется порт `25565`.

## База данных
Файл: `stats.sqlite3` (или путь из `DB_PATH`).
Таблица `samples`:
- `ts` — Unix timestamp (UTC)
- `total_online` — суммарный онлайн
- `success_count`, `failure_count` — служебная диагностика
