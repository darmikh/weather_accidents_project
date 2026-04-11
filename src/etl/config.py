import os
from dotenv import load_dotenv

load_dotenv()  

class Config:
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')
    YANDEX_APIKEY = os.getenv('YANDEX_APIKEY')
    DADATA_API_KEY = os.getenv('DADATA_API_KEY')
    
    USER_AGENT_EMAIL = os.getenv('USER_AGENT_EMAIL')
    
    SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
    SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')
    SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
    SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')
    SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT')
    
    START_YEAR = 2015
    END_YEAR = 2026  # последний полный год
    
    MONTHS_TO_REFRESH = 6  # количество месяцев для перепроверки данных о ДТП
    
    MAX_RETRIES = 3
    REQUEST_TIMEOUT = 30
    REQUEST_DELAY = 2.0


config = Config()