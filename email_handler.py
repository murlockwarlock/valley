import cloudscraper
import asyncio
import re
from utils import log_message
import time

def _get_new_mailbox_sync():
    """Синхронная функция для создания ящика через Cloudscraper для обхода защиты."""
    try:
        scraper = cloudscraper.create_scraper()
        response = scraper.post("https://web2.temp-mail.org/mailbox")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log_message(f"Cloudscraper mailbox generation failed: {e}", "error")
        return None

def _get_messages_sync(mailbox_data, proxy, user_agent):
    """Синхронная функция для получения списка писем с использованием прокси."""
    try:
        scraper = cloudscraper.create_scraper()
        if proxy:
            scraper.proxies.update({'http': proxy, 'https': proxy})
        scraper.headers.update({'User-Agent': user_agent})
        
        token = mailbox_data.get('token')
        if not token: return None
        headers = {'Authorization': f"Bearer {token}"}
        response = scraper.get("https://web2.temp-mail.org/messages", headers=headers)
        if response.status_code == 404: return None # Нормальная ситуация, если писем еще нет
        response.raise_for_status()
        return response.json().get('messages')
    except Exception as e:
        log_message(f"Cloudscraper message list fetch failed: {e}", "error")
        return None

def _get_full_message_sync(mailbox_data, message_id, proxy, user_agent):
    """Синхронная функция для получения полного тела одного письма."""
    try:
        scraper = cloudscraper.create_scraper()
        if proxy:
            scraper.proxies.update({'http': proxy, 'https': proxy})
        scraper.headers.update({'User-Agent': user_agent})

        token = mailbox_data.get('token')
        if not token: return None
        headers = {'Authorization': f"Bearer {token}"}
        response = scraper.get(f"https://web2.temp-mail.org/messages/{message_id}", headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log_message(f"Cloudscraper full message fetch failed: {e}", "error")
        return None

async def generate_temp_email():
    """Асинхронная обертка для создания ящика."""
    log_message("Generating temporary email via Cloudscraper...", "info")
    mailbox_data = await asyncio.to_thread(_get_new_mailbox_sync)
    if mailbox_data and mailbox_data.get("mailbox"):
        log_message(f"Generated temporary email: {mailbox_data['mailbox']}", "success")
    return mailbox_data

async def get_verification_link(mailbox_data, proxy, user_agent, timeout=120):
    """Асинхронная обертка для получения письма и ссылки."""
    log_message(f"Waiting for verification email at [{mailbox_data['mailbox']}]...", "warning")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        # Получаем список писем
        messages = await asyncio.to_thread(_get_messages_sync, mailbox_data, proxy, user_agent)
        
        if messages and isinstance(messages, list) and len(messages) > 0:
            latest_message = messages[0]
            log_message(f"Found email with subject: '{latest_message.get('subject')}'", "info")

            # Получаем полное тело письма по его ID
            full_message = await asyncio.to_thread(_get_full_message_sync, mailbox_data, latest_message.get('_id'), proxy, user_agent)
            
            if full_message and full_message.get('bodyHtml'):
                body = full_message.get('bodyHtml')
                match = re.search(r'href="([^"]*verify\?token=[^"]*)"', body)
                if match:
                    link = match.group(1).replace("&amp;", "&")
                    log_message("Verification link found!", "success")
                    return link
        
        await asyncio.sleep(10)
            
    log_message("Verification link not found within timeout.", "error")
    return None
