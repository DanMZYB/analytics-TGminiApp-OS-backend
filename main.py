import os
import hmac
import hashlib
import json
from urllib.parse import parse_qs, unquote

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Header, Depends, Body
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv

# 1. Загрузка переменных окружения
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")

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

@app.post("/register_user")
def register_new_user(
    payload: UserRegistration, 
    admin_data: dict = Depends(validate_telegram_data)
):
    """
    Регистрация пользователя админом. 
    Автоматически присваивает команду админа и роль 'creator'.
    """
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

@app.get("/admin/get_team_users")
def get_team_users(admin_data: dict = Depends(validate_telegram_data)):
    admin_tg_id = str(admin_data.get('id'))
    
    # 1. Получаем команду админа
    admin_info = supabase.table('users').select("team, whois").eq('telegram_id', admin_tg_id).single().execute()
    
    if not admin_info.data or admin_info.data.get('whois') != 'admin':
        raise HTTPException(status_code=403, detail="Only admins can register new users")
    
    team_name = admin_info.data.get('team')

    # 2. Получаем всех юзеров этой команды
    users_resp = supabase.table('users').select("*").eq('team', team_name).execute()
    
    # 3. Получаем все аккаунты этой команды для наглядности
    accounts_resp = supabase.table('accounts').select("*").eq('team', team_name).execute()

    # Группируем аккаунты по пользователям для удобства фронтенда
    team_data = []
    for user in users_resp.data:
        user_id = user['telegram_id']
        user_accounts = [acc for acc in accounts_resp.data if acc['user_id'] == user_id]
        user['accounts'] = user_accounts
        team_data.append(user)

    return {"team": team_name, "members": team_data}

@app.post("/admin/update_user")
def update_user_data(
    payload: UserUpdatePayload, 
    admin_data: dict = Depends(validate_telegram_data)
):
    admin_tg_id = str(admin_data.get('id'))
    
    # 1. Проверка прав админа
    admin_info = supabase.table('users').select("team, whois").eq('telegram_id', admin_tg_id).single().execute()
    if not admin_info.data or admin_info.data.get('whois') != 'admin':
        raise HTTPException(status_code=403, detail="Denied")
    
    admin_team = admin_info.data.get('team')

    # 2. Получаем текущие данные целевого пользователя (имя и ник для username_at)
    target_res = supabase.table('users').select("name_soname, username, team").eq('telegram_id', str(payload.target_telegram_id)).single().execute()
    
    if not target_res.data:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Проверка команды
    if target_res.data.get('team') != admin_team:
        raise HTTPException(status_code=403, detail="Target user is from another team")

    current_username = target_res.data.get('username')

    # 3. Обновляем имя, ТОЛЬКО если оно передано в запросе
    if payload.new_name_soname is not None:
        supabase.table('users').update({"name_soname": payload.new_name_soname})\
            .eq('telegram_id', payload.target_telegram_id).execute()

    # 4. Добавляем аккаунты, используя username из профиля пользователя
    if payload.new_accounts:
        added_accounts = []
        for acc in payload.new_accounts:
            added_accounts.append({
                "user_id": payload.target_telegram_id,
                "username_at": current_username, # Авто-подстановка ника из профиля
                "account_name": acc.account_name,
                "social_network": acc.social_network,
                "team": admin_team
            })
        supabase.table('accounts').insert(added_accounts).execute()

    return {"status": "success", "message": "Updated"}

@app.get("/admin/team_activity")
def get_team_activity(admin_data: dict = Depends(validate_telegram_data)):
    admin_tg_id = str(admin_data.get('id'))
    
    # 1. Получаем команду админа
    admin_info = supabase.table('users').select("team, whois").eq('telegram_id', admin_tg_id).single().execute()
    
    if not admin_info.data or admin_info.data.get('whois') != 'admin':
        raise HTTPException(status_code=403, detail="Доступ только для админов")
    
    team_name = admin_info.data.get('team')

    # 2. Получаем всех пользователей команды
    users_resp = supabase.table('users').select("telegram_id, username, name_soname").eq('team', team_name).execute()
    
    activity_report = []

    for user in users_resp.data:
        user_id = user['telegram_id']
        
        # 3. Ищем последнюю запись в аналитике для этого пользователя
        # Сортируем по 'added_at' (или как называется колонка времени в твоей таблице analytics)
        # Если колонка называется иначе, замени 'added_at' на правильное имя (например, 'created_at')
        last_entry = supabase.table('analytics')\
            .select("added_at")\
            .eq('user_id', user_id)\
            .order("added_at", desc=True)\
            .limit(1)\
            .execute()

        last_active = None
        if last_entry.data:
            last_active = last_entry.data[0]['added_at']

        activity_report.append({
            "telegram_id": user_id,
            "username": user['username'],
            "name_soname": user['name_soname'],
            "last_activity": last_active  # Вернет дату или None, если записей нет
        })

    return {
        "team": team_name,
        "report": activity_report
    }

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
    # Используем PostgREST для получения последних записей (сортировка внутри группы сложна в простом SDK, 
    # поэтому сделаем запрос последних действий для всей команды)
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