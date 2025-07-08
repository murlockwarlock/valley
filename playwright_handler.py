from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from urllib.parse import urlparse
from utils import log_message, random_delay, get_iso_timestamp
import json
from datetime import datetime
import asyncio
import pandas as pd


class ValleyGuardianBrowser:
    def __init__(self, account_data, config):
        self.account = account_data;
        self.config = config;
        self.page = None
        self.token = account_data.get('bearer_token');
        self.user_id = account_data.get('user_id')
        self.playwright = None;
        self.browser = None;
        self.max_retries = config.get("max_retries", 3)

    async def launch(self):
        self.playwright = await async_playwright().start();
        proxy_settings = None
        if self.account.get('proxy'):
            proxy_string = self.account['proxy']
            if not proxy_string.startswith('http://'): proxy_string = 'http://' + proxy_string
            try:
                parsed = urlparse(proxy_string);
                proxy_settings = {"server": f"http://{parsed.hostname}:{parsed.port}"}
                if parsed.username: proxy_settings["username"] = parsed.username
                if parsed.password: proxy_settings["password"] = parsed.password
            except Exception as e:
                log_message(f"Could not parse proxy string: {e}", "error"); return False

        self.browser = await self.playwright.chromium.launch(headless=False, proxy=proxy_settings)
        context = await self.browser.new_context(user_agent=self.account['user_agent'])
        self.page = await context.new_page()
        return True

    async def close(self):
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()
        log_message("Browser closed.", "info")

    async def _execute_fetch(self, url, options, verbose=True):
        clean_url_for_log = url.split('?')[0].split('/')[-1]
        for attempt in range(self.max_retries):
            try:
                if verbose:
                    await random_delay(self.config['min_delay_seconds'], self.config['max_delay_seconds'])
                js_code = f"fetch('{url}', {json.dumps(options)}).then(res => res.text().then(text => ({{status: res.status, text: text}})))"
                result = await self.page.evaluate(js_code)
                if result['status'] >= 400:
                    raise Exception(f"API returned status {result['status']}: {result['text']}")
                parsed_json = json.loads(result['text']) if result['text'] and result['text'].strip() else {}
                if verbose:
                    log_message(f"SUCCESS: API Call to {clean_url_for_log}", "success")
                return parsed_json
            except Exception as e:
                error_message = str(e).splitlines()[0]
                if verbose:
                    log_message(
                        f"Attempt {attempt + 1}/{self.max_retries} failed for {clean_url_for_log}: {error_message}",
                        "warning")
                if attempt + 1 == self.max_retries:
                    if verbose: log_message(f"All retries failed for {clean_url_for_log}.", "error")
                    return None
        return None

    async def _goto_with_retries(self, url):
        for attempt in range(self.max_retries):
            try:
                await self.page.goto(url, wait_until="domcontentloaded", timeout=90000)
                log_message(f"Successfully navigated to {url}", "success");
                return True
            except Exception as e:
                log_message(f"Attempt {attempt + 1}/{self.max_retries} to navigate to {url} failed: {e}", "warning")
                if attempt + 1 == self.max_retries: log_message(f"All navigation attempts failed for {url}.",
                                                                "error"); return False
        return False

    async def _simulate_post_quest_activity(self):
        log_message("Simulating background activity...", "info")
        base_url = self.config['base_url']
        headers = {'apikey': self.config['api_key'], 'Authorization': f'Bearer {self.token}'}
        await self._execute_fetch(f"{base_url}/auth/v1/user", {'method': 'GET', 'headers': headers}, verbose=False)
        await self._execute_fetch(f"{base_url}/rest/v1/users?select=*&id=eq.{self.user_id}",
                                  {'method': 'GET', 'headers': {**headers, 'Accept-Profile': 'public'}}, verbose=False)
        headers_post = {**headers, 'Content-Type': 'application/json'}
        await self._execute_fetch(f"{base_url}/rest/v1/rpc/get_my_gc",
                                  {'method': 'POST', 'headers': headers_post, 'body': json.dumps({})}, verbose=False)

    async def submit_registration_form(self):
        ref_url = f"https://valleyofguardians.xyz/?modal=register&ref={self.config['referral_code']}"
        if not await self._goto_with_retries(ref_url): return False
        full_name_input = self.page.locator("#name");
        await full_name_input.wait_for(state="visible", timeout=60000)
        log_message(f"Registering with temp email: {self.account['email']}", "warning")
        await full_name_input.fill(self.account['full_name']);
        await self.page.locator("#email").fill(self.account['email']);
        await self.page.locator("#password").fill(self.account['password']);
        await self.page.locator("#confirmPassword").fill(self.account['password']);
        await self.page.locator("#inviteCode").fill(self.config['referral_code'])
        await self.page.locator('form button:has-text("Register")').click()
        return True

    async def complete_registration(self, verification_link):
        if not await self._goto_with_retries(verification_link): return False
        log_message("Email successfully verified!", "success")
        await self.page.wait_for_url("**/social-quest", timeout=60000)
        log_message("Redirected to social quests page.", "success")
        project_ref = self.config['base_url'].split('.')[0].split('//')[1]
        token_data_str = await self.page.evaluate(f"localStorage.getItem('sb-{project_ref}-auth-token')")
        if token_data_str:
            token_data = json.loads(token_data_str)
            self.token = token_data['access_token'];
            self.user_id = token_data['user']['id']
            self.account.update({'bearer_token': self.token, 'user_id': self.user_id})
            return True
        return False

    async def login(self):
        """Выполняет вход через API для получения свежего токена."""
        log_message("Attempting to log in via API to get fresh token...", "info")

        # Сначала просто заходим на сайт, чтобы браузер был в правильном контексте
        if not await self._goto_with_retries("https://valleyofguardians.xyz/"):
            return False

        # Выполняем API-запрос на логин
        url = f"{self.config['base_url']}/auth/v1/token?grant_type=password"
        payload = {"email": self.account['email'], "password": self.account['password']}
        headers = {'apikey': self.config['api_key'], 'Content-Type': 'application/json'}

        response = await self._execute_fetch(url, {'method': 'POST', 'headers': headers, 'body': json.dumps(payload)})

        if response and response.get('access_token'):
            # Успешно получили новый токен
            self.token = response['access_token']
            self.user_id = response['user']['id']
            self.account.update({'bearer_token': self.token, 'user_id': self.user_id})

            # Вручную "вживляем" токен в localStorage браузера, чтобы он тоже "узнал" о входе
            project_ref = self.config['base_url'].split('.')[0].split('//')[1]
            await self.page.evaluate(f"localStorage.setItem('sb-{project_ref}-auth-token', '{json.dumps(response)}')")

            # Переходим на страницу с квестами
            await self._goto_with_retries("https://valleyofguardians.xyz/social-quest")
            log_message("API Login successful! Token refreshed.", "success")
            return True

        log_message("API Login failed.", "error")
        return False

    async def run_gameplay(self):
        await random_delay(10, 15)
        base_url = self.config['base_url'];
        headers = {'apikey': self.config['api_key'], 'Authorization': f'Bearer {self.token}',
                   'Content-Type': 'application/json'}

        log_message("Claiming available social & weekly quests...", "info")
        social_quests = {
            "social_tweet_4": 500,
            "social_tweet_5": 100,
            "social_tweet_6": 100,
            "weekly_twitter": 300,
            "weekly_telegram": 300,
            "weekly_telegram_1": 300
        }

        claimed_quests_raw = self.account.get('claimed_quests_log', '[]');
        claimed_log = json.loads(claimed_quests_raw) if isinstance(claimed_quests_raw, str) else []
        await self._simulate_post_quest_activity()

        user_info_resp = await self._execute_fetch(
            f"{base_url}/rest/v1/users?select=guardians_coin&id=eq.{self.user_id}",
            {'method': 'GET', 'headers': headers}, verbose=False)
        current_balance = 0
        if user_info_resp and isinstance(user_info_resp, list) and user_info_resp:
            current_balance = user_info_resp[0].get('guardians_coin', 0)

        if not isinstance(current_balance, (int, float)) or pd.isna(current_balance): current_balance = 0
        current_balance = int(current_balance)

        for quest_id, reward in social_quests.items():
            if quest_id not in claimed_log:
                log_message(f"Claiming quest via API: {quest_id}", "info")
                res = await self._execute_fetch(f'{base_url}/rest/v1/rpc/secure_claim_quest',
                                                {'method': 'POST', 'headers': headers, 'body': json.dumps(
                                                    {'user_id': self.user_id, 'quest_id': quest_id,
                                                     'reward_amount': reward})})
                if res and isinstance(res, list) and res[0].get('quest_claimed'):
                    claimed_log.append(quest_id)
                    current_balance = res[0].get('new_balance', current_balance)
                    log_message(f"Quest '{quest_id}' claimed! New balance: {current_balance}", "success")
                    await self._simulate_post_quest_activity()

        self.account['claimed_quests_log'] = json.dumps(claimed_log)
        self.account['final_balance'] = current_balance
        self.account['last_run'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
