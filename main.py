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
origins = [
    "*", # ВАЖНО: На проде можно ограничить, но для Mini Apps часто оставляют '*'
]

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

# --- Валидация Telegram ---

def validate_telegram_data(authorization: str = Header(None)):
    """
    Проверяет, что запрос пришел действительно из Телеграма.
    Telegram передает строку initData, которую мы должны проверить по хэшу.
    """
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
    1. Валидирует initData.
    2. Проверяет, существует ли пользователь в БД.
    3. ЕСЛИ НЕТ - возвращает ошибку (регистрация запрещена).
    """
    telegram_id = user_data.get('id')
    telegram_id_str = str(telegram_id)
    
    # 2. Проверяем существование
    response = supabase.table('users').select("*").eq('telegram_id', telegram_id_str).execute()
    
    if not response.data:
        # Если пользователя нет, возвращаем 403 Forbidden
        raise HTTPException(
            status_code=403, 
            detail="User not registered. Please contact the administrator."
        )
    
    # Если пользователь найден, возвращаем его данные
    return {"status": "success", "user": response.data[0]}

@app.get("/accounts_list")
def get_accounts_list(user_data: dict = Depends(validate_telegram_data)):
    """
    Возвращает список названий аккаунтов, привязанных к telegram_id пользователя.
    """
    telegram_id = user_data.get('id')
    
    # 1. Запрос только столбца 'account_name' для текущего user_id
    response = supabase.table('accounts').select("account_name").eq('user_id', telegram_id).execute()
    
    if response.data is None:
        # Если данные пусты или возникла ошибка, вернем пустой список, чтобы фронтенд не упал
        return []

    # 2. Форматируем результат в список строк (JSON array of strings)
    account_names = [item['account_name'] for item in response.data]
    
    return account_names

@app.post("/analytics_add")
def add_analytics_batch(
    # Теперь мы ожидаем список данных, используя нашу модель AnalyticsBatch
    batch_data: AnalyticsBatch, 
    user_data: dict = Depends(validate_telegram_data)
):
    """
    Добавляет одну или несколько строк аналитики в таблицу 'analytics'.
    Автоматически добавляет user_id и время добавления.
    """
    telegram_id = user_data.get('id')
    
    # Формируем список словарей для вставки
    records_to_insert = []
    for item in batch_data.data:
        record = item.model_dump() # Преобразуем Pydantic объект в словарь
        record["user_id"] = telegram_id
        # added_at (время) заполнится автоматически базой данных (DEFAULT timezone('utc'::text, now()))
        records_to_insert.append(record)

    if not records_to_insert:
        raise HTTPException(status_code=400, detail="No data provided for insertion.")

    # Выполняем пакетную вставку в Supabase
    response = supabase.table('analytics').insert(records_to_insert).execute()
    
    return {"status": "success", "inserted_count": len(response.data), "data": response.data}