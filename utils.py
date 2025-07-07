import pandas as pd
import random
import string
import time
import os
import pytz
import asyncio
from datetime import datetime
from colorama import Fore, Style, init
from eth_account import Account
import json

# Инициализация Colorama для цветного вывода в консоль
init(autoreset=True)

def log_message(message, level="info"):
    """Выводит цветное лог-сообщение в консоль с временной меткой."""
    now = datetime.now().strftime('%H:%M:%S')
    if level == "success":
        print(f"{Style.BRIGHT}{Fore.GREEN}[{now}] {message}{Style.RESET_ALL}")
    elif level == "error":
        print(f"{Style.BRIGHT}{Fore.RED}[{now}] {message}{Style.RESET_ALL}")
    elif level == "warning":
        print(f"{Style.BRIGHT}{Fore.YELLOW}[{now}] {message}{Style.RESET_ALL}")
    else:
        print(f"{Style.BRIGHT}{Fore.CYAN}[{now}] {message}{Style.RESET_ALL}")

async def random_delay(min_sec, max_sec):
    """Асинхронно ждет случайное количество секунд в заданном диапазоне."""
    delay = random.uniform(min_sec, max_sec) if min_sec < max_sec else min_sec
    log_message(f"Пауза: {int(delay)} сек.", "info")
    await asyncio.sleep(delay)

def print_account_summary(account_data):
    """Выводит красивый и информативный отчет на русском языке по итогам работы аккаунта."""
    log_message("--- [ Итоги по аккаунту ] ---", "warning")
    email = account_data.get('email', 'N/A')
    
    summary = f"\n    ╔═════════════════════════════════════════════════════\n    │ 👤 Почта:     {email}\n"
    
    password = account_data.get('password')
    # Показываем пароль только для новосозданных аккаунтов
    if pd.notna(password) and pd.isna(account_data.get('last_run')):
        summary += f"    │ 🔑 Пароль:     {password}\n"
        
    summary += "    ├──────────────────────────────────────────────────\n"
    
    if account_data.get('bearer_token'):
        summary += f"    │ ✅ Статус:         Обработан успешно\n"
        try:
            guardian_stats_raw = account_data.get('guardian_stats')
            if pd.notna(guardian_stats_raw) and isinstance(guardian_stats_raw, str):
                guardian_stats = json.loads(guardian_stats_raw)
                if guardian_stats:
                    guardian = guardian_stats[0]
                    loot_str = f"[{guardian.get('rarity', 'N/A').capitalize()}] {guardian.get('class', 'N/A').capitalize()}"
                    summary += f"    │ 📦 Дроп из бокса: {loot_str}\n"
                    summary += f"    │ ⚔️  Урон воина:    {guardian.get('damage', 0)}\n"
        except (json.JSONDecodeError, IndexError, TypeError):
            summary += "    │ 📦 Дроп из бокса:   Ошибка чтения\n"
        
        total_damage = account_data.get('total_damage', 0)
        summary += f"    │ 💥 Общий урон:     {total_damage if pd.notna(total_damage) else 0}\n"
        
        final_balance_raw = account_data.get('final_balance', 'N/A')
        final_balance = 'N/A'
        if pd.notna(final_balance_raw) and isinstance(final_balance_raw, (int, float)):
            final_balance = int(final_balance_raw)
        
        summary += f"    │ 💰 Финальный баланс: {final_balance} GC\n"
    else:
        summary += f"    │ ❌ Статус:         Обработка провалена\n"
    
    summary += "    ╚═════════════════════════════════════════════════════\n"
    print(summary)

def generate_random_name():
    """Генерирует более правдоподобное имя с годом рождения."""
    first_names = ["John", "Peter", "Mike", "James", "Robert", "David", "Chris", "Steve", "Brian", "Kevin"]
    last_names = ["Smith", "Jones", "Williams", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Martin"]
    year = random.randint(1985, 2005)
    return f"{random.choice(first_names)}{random.choice(last_names)}{year}"

def generate_random_password(length=12):
    """Генерирует случайный пароль, состоящий из букв и цифр."""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for i in range(length))

def get_wallet_from_pk(private_key):
    """Получает адрес кошелька из приватного ключа."""
    try:
        pk = "0x" + private_key if not private_key.startswith("0x") else private_key
        return Account.from_key(pk).address
    except Exception as e:
        log_message(f"Invalid private key: {e}", "error"); return None

def get_iso_timestamp():
    """Возвращает текущее время в формате ISO 8601 UTC."""
    return datetime.now(pytz.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def load_file(filename):
    """Загружает строки из файла, убирая пустые строки и пробелы по краям."""
    if not os.path.exists(filename):
        log_message(f"{filename} not found. Please create it.", "error"); return []
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def load_or_create_df(filename="accounts.xlsx"):
    """Загружает DataFrame из Excel или создает новый, если файл не найден или поврежден."""
    if os.path.exists(filename):
        try:
            return pd.read_excel(filename)
        except Exception as e:
            log_message(f"Could not read {filename}, maybe it's corrupted. Creating a new one. Error: {e}", "error")
    else:
        log_message(f"{filename} not found. Creating a new one.", "warning")
    
    # Определяем колонки для нового DataFrame
    columns = ['email', 'password', 'proxy', 'user_agent', 'bearer_token', 'user_id', 'private_key', 'wallet_address', 'full_name', 'welcome_bonus_claimed', 'box_opened', 'guardian_stats', 'claimed_quests_log', 'last_run', 'referral_confirmed', 'wallet_address_added', 'total_damage', 'final_balance']
    return pd.DataFrame(columns=columns)

def save_df(df, filename="accounts.xlsx"):
    """Сохраняет DataFrame в Excel файл с обработкой ошибки доступа."""
    try:
        df.to_excel(filename, index=False)
        log_message("Progress saved successfully.", "success")
    except PermissionError:
        log_message(f"Could not save to {filename}. PLEASE CLOSE THE FILE IF IT'S OPEN IN EXCEL.", "error")
    except Exception as e:
        log_message(f"Failed to save to {filename}: {e}", "error")
