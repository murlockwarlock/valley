import json
import pandas as pd
from itertools import cycle
from datetime import datetime
import asyncio
import os
import sys
from utils import (
    log_message, load_file, load_or_create_df, save_df, 
    generate_random_name, generate_random_password, 
    get_wallet_from_pk, print_account_summary, random_delay
)
from playwright_handler import ValleyGuardianBrowser
from email_handler import generate_temp_email, get_verification_link
from fake_useragent import UserAgent

async def process_single_account(account_data, config):
    """Асинхронно обрабатывает один аккаунт."""
    browser_handler = None
    
    proxy_info = account_data.get('proxy', 'No Proxy (Direct Connection)')
    email_or_wallet = account_data.get('email') or account_data.get('wallet_address', 'N/A')
    log_message(f"--- [ Начинаю работу ] Аккаунт: {email_or_wallet} | Прокси: {proxy_info} ---", "yellow")
    
    try:
        browser_handler = ValleyGuardianBrowser(account_data, config)
        await browser_handler.launch()

        if pd.notna(account_data.get('bearer_token')):
            if not await browser_handler.login():
                raise Exception("Login failed after UI and API attempts.")
        else:
            if not await browser_handler.submit_registration_form():
                raise Exception("Failed to submit registration form.")
            
            verification_link = await get_verification_link(account_data['email_mailbox'], account_data['proxy'], account_data['user_agent'])
            if not verification_link:
                raise Exception("Failed to get verification link from email.")
            
            if not await browser_handler.complete_registration(verification_link):
                raise Exception("Failed to complete registration after verification.")
            log_message("Registration successful.", "success")

        await browser_handler.run_gameplay()
        return account_data, True
    
    except Exception as e:
        log_message(f"Critical error for {account_data.get('email', 'N/A')}: {e}", "error")
        return account_data, False
    finally:
        if browser_handler:
            try:
                await browser_handler.close()
            except Exception as e:
                if "Connection closed" not in str(e):
                    log_message(f"Non-critical error during browser close: {e}", "warning")


def update_or_add_account(df, account_data):
    """Безопасно обновляет или добавляет аккаунт в DataFrame."""
    account_data.pop('email_mailbox', None)
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame()

    if 'wallet_address' in account_data and pd.notna(account_data['wallet_address']):
        # Гарантируем, что колонка существует перед поиском
        if 'wallet_address' not in df.columns:
            df['wallet_address'] = None
        
        existing_index = df.index[df['wallet_address'] == account_data['wallet_address']].tolist()
        if existing_index:
            for key, value in account_data.items():
                if key not in df.columns: # Добавляем колонку, если ее нет
                    df[key] = pd.NA
                df.loc[existing_index[0], key] = value
            return df

    return pd.concat([df, pd.DataFrame([account_data])], ignore_index=True)


async def main():
    log_message("Valley of Guardians Bot Started", "warning")
    config_path = 'config.json'
    if not os.path.exists(config_path):
        log_message(f"{config_path} not found. Exiting.", "error"); return
    with open(config_path) as f: config = json.load(f)

    proxies = load_file("proxies.txt"); private_keys = load_file("private_keys.txt")
    ua = UserAgent()
    proxy_cycle = cycle(proxies) if proxies else cycle([None])
    if not proxies:
        log_message("No proxies found. The script will run using your direct connection.", "warning")

    accounts_df = load_or_create_df()
    
    try:
        # ШАГ 1: ОБРАБАТЫВАЕМ СУЩЕСТВУЮЩИЕ АККАУНТЫ
        if config.get("run_gameplay_for_existing", True) and not accounts_df.empty:
             log_message("Starting daily tasks for existing accounts...", "warning")
             indices_to_process = accounts_df.index.tolist()
             for index in indices_to_process:
                account_data = accounts_df.loc[index].to_dict()
                
                last_run_str = account_data.get('last_run')
                if pd.notna(last_run_str):
                    try:
                        last_run_date = datetime.strptime(str(last_run_str), '%Y-%m-%d %H:%M:%S').date()
                        if last_run_date < datetime.now().date():
                            log_message(f"Resetting daily quests for {account_data['email']}", "info")
                            claimed_quests_raw = account_data.get('claimed_quests_log', '[]')
                            claimed_log = json.loads(claimed_quests_raw) if isinstance(claimed_quests_raw, str) else []
                            account_data['claimed_quests_log'] = json.dumps([q for q in claimed_log if "daily" not in q])
                    except (ValueError, TypeError, json.JSONDecodeError): pass
                
                processed_data, success = await process_single_account(account_data, config)
                accounts_df = update_or_add_account(accounts_df, processed_data)
                print_account_summary(processed_data)
                save_df(accounts_df)
                await random_delay(config.get("delay_between_accounts_min_seconds", 60), config.get("delay_between_accounts_max_seconds", 300))

        # ШАГ 2: РЕГИСТРИРУЕМ НОВЫЕ
        if config.get("register_new_accounts", True):
            # Перечитываем файл, чтобы видеть самые свежие данные после обработки старых акков
            accounts_df = load_or_create_df()
            registered_wallets = []
            if 'wallet_address' in accounts_df.columns:
                registered_wallets = accounts_df['wallet_address'].astype(str).tolist()
                
            unregistered_keys = [pk for pk in private_keys if get_wallet_from_pk(pk) not in registered_wallets]
            if unregistered_keys:
                log_message(f"Found {len(unregistered_keys)} new private keys to register.", "info")
                for pk in unregistered_keys:
                    proxy = next(proxy_cycle)
                    user_agent = ua.random
                    mailbox = await generate_temp_email(proxy, user_agent)
                    if not mailbox:
                        log_message(f"Skipping registration for PK {pk[:10]}... due to email generation failure.", "error")
                        continue
                    
                    password = generate_random_password()
                    log_message(f"Generated password for {mailbox['mailbox']}: {password}", "success")
                    
                    account_info = {"password": password, "proxy": proxy, "private_key": pk, "wallet_address": get_wallet_from_pk(pk), "full_name": generate_random_name(), "user_agent": user_agent, "email": mailbox['mailbox'], "email_mailbox": mailbox}
                    
                    processed_data, success = await process_single_account(account_info, config)
                    
                    if success:
                        accounts_df = update_or_add_account(accounts_df, processed_data)
                    
                    print_account_summary(processed_data)
                    save_df(accounts_df)
                    await random_delay(config.get("delay_between_accounts_min_seconds", 60), config.get("delay_between_accounts_max_seconds", 300))
    
    except KeyboardInterrupt:
        log_message("KeyboardInterrupt detected. Shutting down gracefully...", "warning")
    
    finally:
        log_message("Script finished.", "info")
        save_df(accounts_df)

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
