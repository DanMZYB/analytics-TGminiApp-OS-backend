import os
import hmac
import hashlib
import json
from urllib.parse import parse_qs, unquote
import base64
import re

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Header, Depends, Body
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import httpx
#test
# 1. Загрузка переменных окружения
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
BASE_URL = os.getenv("BASE_URL")
ADMIN_ID = os.getenv("ADMIN_TELEGRAM_ID")

ACTORS = {
    "instagram": "apify/instagram-post-scraper",
    "tiktok": "clockworks/free-tiktok-scraper",
    "youtube": "streamers/youtube-scraper",
    "vk": "jupri/vkontakte",
}

system_logs = []

# 2. Инициализация Supabase и FastAPI
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
app = FastAPI()

# --- Настройка CORS ---
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, # Разрешаем все домены
    allow_credentials=True, # Разрешаем cookies (хотя и не нужны)
    allow_methods=["*"], # Разрешаем все методы (POST, GET, OPTIONS и т.д.)
    allow_headers=["*"], # Разрешаем все заголовки, включая наш 'Authorization'
)

# --- Модели данных (Pydantic) ---

# Модель для данных, которые будут добавляться в таблицу 'analytics'
class AnalyticsData(BaseModel):
    account_name: str
    post_url: str
    # likes и views могут быть опциональными, если их не всегда отправляют
    likes: int = 0
    views: int = 0

# Модель для приема нескольких записей аналитики за один раз
class AnalyticsBatch(BaseModel):
    data: list[AnalyticsData]

# --- Модели для регистрации ---

class AccountCreate(BaseModel):
    account_name: str
    social_network: str

class UserRegistration(BaseModel):
    telegram_id: int
    username: str      # Главный ник пользователя
    name_soname: str   # Имя и Фамилия одним полем
    accounts: list[AccountCreate]

class UserUpdatePayload(BaseModel):
    target_telegram_id: int
    new_name_soname: str = None
    new_accounts: list[AccountCreate] = [] # Модель AccountCreate мы создали ранее

class ApifyTaskInfo(BaseModel):
    urls: list[str]
    platform: str

# --- Валидация Telegram ---

def validate_telegram_data(authorization: str = Header(None)):
   
    if not authorization:
        raise HTTPException(status_code=401, detail="No Authorization header")
    
    # Формат заголовка ожидаем: "twa-init-data <initDataString>"
    try:
        auth_type, init_data = authorization.split(" ", 1)
        if auth_type != "twa-init-data":
            raise HTTPException(status_code=401, detail="Invalid auth type")
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid header format")

    # Парсим строку данных
    parsed_data = parse_qs(init_data)
    
    # Достаем хэш, который прислал телеграм
    received_hash = parsed_data.get('hash', [None])[0]
    if not received_hash:
        raise HTTPException(status_code=401, detail="No hash found")

    # Формируем строку для проверки (сортируем ключи, убираем hash)
    data_check_list = []
    for key, value in parsed_data.items():
        if key != 'hash':
            data_check_list.append(f"{key}={value[0]}")
    
    data_check_list.sort()
    data_check_string = "\n".join(data_check_list)

    # Вычисляем секретный ключ
    secret_key = hmac.new("WebAppData".encode(), BOT_TOKEN.encode(), hashlib.sha256).digest()
    # Вычисляем наш хэш
    calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    # Сравниваем
    if calculated_hash != received_hash:
        raise HTTPException(status_code=403, detail="Data integrity check failed")

    # Если всё ок, возвращаем данные пользователя
    user_data_json = parsed_data.get('user', [None])[0]
    if user_data_json:
        return json.loads(user_data_json)
    return {} 

# ВРЕМЕННАЯ ЗАГЛУШКА ДЛЯ ТЕСТОВ
"""def validate_telegram_data(authorization: str = Header(None)):
    return {"id": 1027611560, "username": "@zybastuk"}"""


def get_all_recent_urls():
    seven_days_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    # Берем ВСЕ записи из таблицы analytics
    response = supabase.table('analytics') \
        .select("post_url") \
        .gte('added_at', seven_days_ago) \
        .execute()
    
    if not response.data: return {}

    grouped = {"instagram": set(), "tiktok": set(), "youtube": set(), "vk": set()}
    for record in response.data:
        url = record['post_url']
        low_url = url.lower()
        if "instagram" in low_url: grouped["instagram"].add(url)
        elif "tiktok" in low_url: grouped["tiktok"].add(url)
        elif "youtube" in low_url or "youtu.be" in low_url: grouped["youtube"].add(url)
        elif "vk.com" in low_url: grouped["vk"].add(url)
    
    return {k: list(v) for k, v in grouped.items() if v}

async def call_apify_actor(platform: str, urls: list[str], team_name: str):
    actor_id = ACTORS.get(platform)
    if not actor_id:
        return None

    # Используем тильду для именных акторов
    api_url = f"https://api.apify.com/v2/acts/{actor_id.replace('/', '~')}/runs"
    
    # Настройка вебхука
    webhook_config = [{
        "eventTypes": ["ACTOR_RUN.SUCCEEDED"],
        "requestUrl": f"{BASE_URL}/webhooks/apify",
        "payloadTemplate": json.dumps({
            "platform": platform,
            "team": team_name,
            "resource_id": "{{resource.defaultDatasetId}}"
        })
    }]

    # Формируем INPUT (оставляем как было)
    if platform == "tiktok":
        actor_input = {
            "postURLs": urls,
            "resultsPerPage": len(urls),
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False
        }
    elif platform == "instagram":
        actor_input = {
            "resultsLimit": len(urls),
            "skipPinnedPosts": False,
            "username": urls 
        }
    elif platform == "vk":
        # ДЛЯ VK: Извлекаем ID из ссылок для поля query
        vk_ids = []
        for u in urls:
            vid_id = extract_video_id(u)
            if vid_id:
                vk_ids.append(vid_id)

        actor_input = {
            "query": vk_ids,            # Отправляем массив ID типа ["1086143610_456239017"]
            "search_mode": "video",
            "limit": len(vk_ids),
            "hd": False,
            "is_online": False,
            "with_photo": False,
            "dev_dataset_clear": False,
            "dev_no_strip": False
        }
    else:
        actor_input = {
            "startUrls": [{"url": u} for u in urls],
            "maxResults": len(urls)
        }

    async with httpx.AsyncClient() as client:
        try:
            # Заголовки
            headers = {
                "Content-Type": "application/json",
                # Передаем вебхуки через заголовок, предварительно превратив в Base64, 
                # чтобы избежать проблем с кодировкой символов
                "X-Apify-Webhooks": base64.b64encode(json.dumps(webhook_config).encode()).decode()
            }
            
            # В параметрах оставляем ТОЛЬКО токен
            params = {"token": APIFY_TOKEN}
            
            response = await client.post(
                api_url, 
                json=actor_input, 
                params=params,
                headers=headers,
                timeout=30.0
            )

            if response.status_code not in [200, 201]:
                print(f"--- APIFY ERROR --- {platform}")
                print(f"Status: {response.status_code}, Body: {response.text}")
                return {"error": response.status_code}

            print(f"--- SUCCESS --- {platform} started!")
            return response.json()
            
        except Exception as e:
            print(f"--- CONNECTION ERROR --- {platform}: {str(e)}")
            return {"error": "connection_failed"}
        
def extract_video_id(url: str):
    if not url: return None
    
    # YouTube (v=ID или youtu.be/ID)
    yt_match = re.search(r"(?:v=|\/|be\/)([0-9A-Za-z_-]{11})", url)
    if "youtu" in url and yt_match:
        return yt_match.group(1)
    
    # Instagram (p/ID или reel/ID)
    ig_match = re.search(r"/(?:p|reel|tv)/([A-Za-z0-9_-]+)", url)
    if "instagram" in url and ig_match:
        return ig_match.group(1)
    
    # TikTok (video/ID или v/ID)
    tt_match = re.search(r"video/(\d+)", url)
    if "tiktok" in url and tt_match:
        return tt_match.group(1)
    
    vk_match = re.search(r"(clip|video)(-?\d+_\d+)", url)
    if "vk.com" in url and vk_match:
        return vk_match.group(2)
        
    return url # Если не узнали формат, возвращаем как есть
        
def add_log(message: str):
    now = datetime.datetime.now().strftime("%H:%M:%S")
    log_entry = f"[{now}] {message}"
    system_logs.append(log_entry)
    # Храним только последние 30 записей, чтобы не забивать память
    if len(system_logs) > 30:
        system_logs.pop(0)

# --- Эндпоинты ---

@app.post("/auth")
def authenticate_user(user_data: dict = Depends(validate_telegram_data)):
    """
    Проверяет существование пользователя и возвращает его данные, включая команду.
    """
    telegram_id = str(user_data.get('id'))
    
    response = supabase.table('users').select("*").eq('telegram_id', telegram_id).execute()
    
    if not response.data:
        raise HTTPException(
            status_code=403, 
            detail="User not registered. Please contact the administrator."
        )
    
    return {"status": "success", "user": response.data[0]}

@app.get("/accounts_list")
def get_accounts_list(user_data: dict = Depends(validate_telegram_data)):
    """
    Возвращает список аккаунтов, привязанных к telegram_id пользователя.
    """
    telegram_id = user_data.get('id')
    
    # Запрашиваем аккаунты конкретного пользователя
    response = supabase.table('accounts').select("account_name").eq('user_id', telegram_id).execute()
    
    if response.data is None:
        return []

    return [item['account_name'] for item in response.data]

@app.post("/analytics_add")
def add_analytics_batch(
    batch_data: AnalyticsBatch, 
    user_data: dict = Depends(validate_telegram_data)
):
    """
    Добавляет аналитику, автоматически привязывая её к команде пользователя.
    """
    telegram_id = user_data.get('id')
    
    # 1. Сначала узнаем команду пользователя из таблицы 'users'
    user_info = supabase.table('users').select("team").eq('telegram_id', str(telegram_id)).single().execute()
    
    if not user_info.data:
        raise HTTPException(status_code=404, detail="User profile not found.")
    
    user_team = user_info.data.get('team')

    # 2. Формируем записи для вставки
    records_to_insert = []
    for item in batch_data.data:
        record = item.model_dump()
        record["user_id"] = telegram_id
        record["team"] = user_team  # Присваиваем команду из профиля юзера
        records_to_insert.append(record)

    if not records_to_insert:
        raise HTTPException(status_code=400, detail="No data provided.")

    # 3. Пакетная вставка
    response = supabase.table('analytics').insert(records_to_insert).execute()
    
    return {"status": "success", "inserted_count": len(response.data), "team": user_team}

# --- Эндпоинт регистрации ---

"""
@app.post("/register_user")
def register_new_user(
    payload: UserRegistration, 
    admin_data: dict = Depends(validate_telegram_data)
):
    admin_tg_id = str(admin_data.get('id'))

    # 1. Проверяем, что вызывающий — админ
    admin_info = supabase.table('users').select("whois, team").eq('telegram_id', admin_tg_id).single().execute()
    
    if not admin_info.data or admin_info.data.get('whois') != 'admin':
        raise HTTPException(status_code=403, detail="Only admins can register new users")

    admin_team = admin_info.data.get('team')

    # 2. Создаем запись в таблице 'users'
    new_user_data = {
        "telegram_id": payload.telegram_id,
        "username": payload.username,
        "name_soname": payload.name_soname,
        "whois": "creator",
        "team": admin_team
    }
    
    user_resp = supabase.table('users').insert(new_user_data).execute()
    
    if not user_resp.data:
        raise HTTPException(status_code=500, detail="Failed to create user")

    # 3. Создаем записи в таблице 'accounts'
    if payload.accounts:
        accounts_to_insert = []
        for acc in payload.accounts:
            accounts_to_insert.append({
                "user_id": payload.telegram_id,
                "username_at": acc.username_at,
                "account_name": acc.account_name,
                "social_network": acc.social_network,
                "team": admin_team
            })
        
        supabase.table('accounts').insert(accounts_to_insert).execute()

    return {
        "status": "success", 
        "message": f"User {payload.username} registered in team {admin_team}"
    }

"""

@app.get("/admin/get_full_team_data")
def get_full_team_data(admin_data: dict = Depends(validate_telegram_data)):
    admin_tg_id = str(admin_data.get('id'))
    
    # 1. Проверка прав админа и получение его команды
    admin_info = supabase.table('users').select("team, whois").eq('telegram_id', admin_tg_id).single().execute()
    if not admin_info.data or admin_info.data.get('whois') != 'admin':
        raise HTTPException(status_code=403, detail="Доступ запрещен")
    
    team_name = admin_info.data.get('team')

    # 2. Получаем всех пользователей команды
    users = supabase.table('users').select("*").eq('team', team_name).execute().data
    
    # 3. Получаем все аккаунты команды
    accounts = supabase.table('accounts').select("*").eq('team', team_name).execute().data

    # 4. Получаем самую свежую запись для каждого юзера из аналитики
    activities = supabase.table('analytics').select("user_id, added_at").eq('team', team_name).order("added_at", desc=True).execute().data

    # 5. Собираем всё в один массив
    full_data = []
    for user in users:
        u_id = user['telegram_id']
        
        # Фильтруем аккаунты этого юзера
        user_accounts = [a for a in accounts if str(a['user_id']) == str(u_id)]
        
        # Находим дату последней активности
        last_act = next((act['added_at'] for act in activities if str(act['user_id']) == str(u_id)), None)
        
        full_data.append({
            **user,
            "accounts": user_accounts,
            "last_activity": last_act
        })

    return {"team": team_name, "members": full_data}

@app.post("/sync/start")
async def start_sync(user_data: dict = Depends(validate_telegram_data)):
    # 1. Проверка прав (только твой ID из .env)
    if str(user_data.get('id')) != str(ADMIN_ID):
        raise HTTPException(
            status_code=403, 
            detail="У вас нет прав для запуска глобальной синхронизации"
        )

    add_log("Запуск синхронизации...")

    # 2. Собираем ВСЕ ссылки за 7 дней (без привязки к команде)
    data_to_sync = get_all_recent_urls()

    if not data_to_sync:
        add_log("⚠️ Ссылок для обновления не найдено")
        return {"status": "empty", "message": "Нет новых ссылок в базе за последние 7 дней"}

    # 3. Запуск акторов
    launch_details = {}
    
    for platform, urls in data_to_sync.items():
        add_log(f"Запуск актора {platform} ({len(urls)} ссылок)")
        # Передаем "global", так как команды нам теперь не важны при запуске
        run_data = await call_apify_actor(platform, urls, "global")
        
        # Проверяем структуру ответа Apify (они возвращают данные в корне или в ключе 'data')
        if run_data and ("id" in run_data or "data" in run_data):
            run_id = run_data.get("id") or run_data.get("data", {}).get("id")
            launch_details[platform] = f"Started (RunID: {run_id})"
        else:
            launch_details[platform] = "Failed to start (Check logs)"
    
    add_log("✅ Все запросы в Apify отправлены")
    return {
        "status": "processing",
        "scope": "all_teams",
        "launched": launch_details,
        "counts": {p: len(u) for p, u in data_to_sync.items()}
    }

@app.post("/webhooks/apify")
async def apify_webhook_handler(payload: dict = Body(...)):
    dataset_id = payload.get("resource_id")
    platform = payload.get("platform")
    add_log(f"📥 Вебхук получен: {platform}")

    # 1. Получаем все недавние ссылки из нашей базы, чтобы знать, что мы вообще ищем
    # (Это даже лучше кеша в памяти, так как база всегда под рукой)
    # Выбираем записи за последние 7 дней
    db_records = supabase.table('analytics').select("id, post_url").execute()
    
    # Создаем карту { 'ID_ВИДЕО': 'ID_СТРОКИ_В_БАЗЕ' }
    video_to_row_map = {
        extract_video_id(r['post_url']): r['id'] 
        for r in db_records.data if r['post_url']
    }

    items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}"
    
    async with httpx.AsyncClient() as client:
        res = await client.get(items_url)
        if res.status_code != 200: 
            return {"status": "error"}
        
        data = res.json()
        for item in data:
            # Сбор ссылки
            raw_url = item.get("url") or item.get("direct_url") or item.get("webVideoUrl") or item.get("inputUrl") or item.get("player")
            vid_id = extract_video_id(raw_url)
            
            row_id = video_to_row_map.get(vid_id)

            if row_id:
                # ЛОГИКА LIKES
                raw_likes = item.get("likes")
                if isinstance(raw_likes, dict):
                    likes = raw_likes.get("count", 0)
                else:
                    likes = item.get("likes")
                    if likes is None: likes = item.get("likesCount")
                    if likes is None: likes = item.get("diggCount")
                    if likes is None: likes = 0

                # ЛОГИКА VIEWS
                views = item.get("views")
                if views is None: views = item.get("videoPlayCount")
                if views is None: views = item.get("viewCount")
                if views is None: views = item.get("playCount")
                if views is None: views = 0
                
                # Обновление в Supabase
                supabase.table('analytics').update({
                    "likes": likes,
                    "views": views
                }).eq('id', row_id).execute()
                
                print(f"✅ Updated {platform}: Row {row_id} (V: {views}, L: {likes})")
            else:
                print(f"⚠️ No match for: {vid_id}")

    add_log(f"✨ Обновлено {platform}: {len(data)} объектов")
    return {"status": "success"}

@app.get("/sync/logs")
async def get_logs():
    return {"logs": system_logs}