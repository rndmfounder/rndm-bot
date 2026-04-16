"""Локальная проверка токена: BOT_TOKEN в окружении или .env (не коммить токены в файл)."""
import os
import urllib.request

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise SystemExit("Укажи BOT_TOKEN в переменных окружения.")

url = f"https://api.telegram.org/bot{TOKEN}/getMe"

try:
    with urllib.request.urlopen(url, timeout=20) as response:
        print(response.read().decode())
except Exception as e:
    print("ОШИБКА:", e)
