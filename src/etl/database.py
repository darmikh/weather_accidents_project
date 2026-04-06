import requests
import time 
from dotenv import load_dotenv
from logger_config import get_logger
from config import config

url = config.SUPABASE_URL
key = config.SUPABASE_KEY

db_user = config.SUPABASE_DB_USER
db_password = config.SUPABASE_DB_PASSWORD
db_host = config.SUPABASE_DB_HOST
db_name = config.SUPABASE_DB_NAME

logger = get_logger('database')

# Проверяем наличие хотя бы одного способа подключения
has_rest = url and key
has_direct = db_user and db_password and db_host and db_name

if not has_rest and not has_direct:
    logger.error("Нет данных для подключения к Supabase. Нужны либо SUPABASE_URL/KEY, либо SUPABASE_DB_* переменные")
    raise ValueError("Отсутствуют параметры подключения к Supabase")


class SupabaseClient:
    
    def __init__(self):
        self.url = url.rstrip('/') if url else None
        self.key = key
        self.headers = None
        if self.url and self.key:
            self.headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json',
                'Prefer': 'return=representation'
            }
            logger.info("Supabase REST клиент инициализирован")
        else:
            logger.info("REST клиент не инициализирован")
    
    def test_connection(self):
        if not self.headers:
            logger.warning("REST API не доступен, пропускаем проверку")
            return True
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"Попытка подключения {attempt + 1}/{max_retries}")
                response = requests.get(
                    f"{self.url}/rest/v1/raw_cities_data",
                    headers=self.headers,
                    params={"limit": "1"},
                    timeout=60
                )
                if response.status_code in [200, 206]:
                    logger.info("Подключение к Supabase успешно")
                    return True
                else:
                    logger.warning(f"Подключение вернуло статус {response.status_code}")
            except requests.exceptions.Timeout:
                logger.warning(f"Таймаут подключения (попытка {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2)
            except Exception as e:
                logger.warning(f"Ошибка подключения: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
        
        logger.error("Не удалось подключиться к Supabase после всех попыток")
        return False
    
    def insert_raw_cities_data(self, data):
        response = requests.post(
            f"{self.url}/rest/v1/raw_cities_data",
            headers=self.headers,
            json=data,
            timeout=10
        )
        if response.status_code == 201:
            return response.json()[0]['id']
        logger.error(f"Ошибка вставки raw_cities_data: {response.status_code}")
        return None

    def get_raw_cities_data(self, staging_id):
        response = requests.get(
            f"{self.url}/rest/v1/raw_cities_data",
            headers=self.headers,
            params={"id": f"eq.{staging_id}"},
            timeout=60
        )
        if response.status_code == 200 and response.json():
            return response.json()[0]
        return None

    def update_raw_cities_data_status(self, staging_id, processed, error_message=None):
        update_data = {"processed": processed}
        if error_message:
            update_data["error_message"] = error_message
        
        response = requests.patch(
            f"{self.url}/rest/v1/raw_cities_data?id=eq.{staging_id}",
            headers=self.headers,
            json=update_data,
            timeout=60
        )
        return response.status_code == 204
    
    def insert_city(self, city_data):
        if not self.headers:
            logger.error("REST API не доступен для вставки city")
            return None
        try:
            response = requests.post(
                f"{self.url}/rest/v1/cities",
                headers=self.headers,
                json=city_data,
                timeout=60
            )
            
            if response.status_code == 201:
                data = response.json()
                if data:
                    city_id = data[0].get('id')
                    logger.info(f"Город {city_data.get('city_name')} добавлен в cities, ID: {city_id}")
                    return city_id
            elif response.status_code == 409:
                logger.debug(f"Город {city_data.get('city_name')} уже существует в cities")
                return None
            else:
                logger.error(f"Ошибка вставки city. Статус: {response.status_code}, Город: {city_data.get('city_name')}")
                
        except Exception as e:
            logger.error(f"Ошибка при вставке city {city_data.get('city_name')}: {e}")
        
        return None
    
    def city_exists(self, city_name, region, fuzzy=False):
        if not self.headers:
            logger.error("REST API не доступен для проверки city")
            return False
        
        try:
            if fuzzy:
                params = {
                    "region": f"eq.{region}",
                    "city_name": f"like.{city_name}%",
                    "select": "id"
                }
            else:
                params = {
                    "city_name": f"eq.{city_name}",
                    "region": f"eq.{region}",
                    "select": "id"
                }
            
            response = requests.get(
                f"{self.url}/rest/v1/cities",
                headers=self.headers,
                params=params,
                timeout=60
            )
            
            if response.status_code == 200:
                return len(response.json()) > 0
            else:
                logger.warning(f"Ошибка при проверке города. Статус: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка проверки города {city_name}: {e}")
            return False


db = SupabaseClient()
logger.info("Модуль database.py загружен, клиент Supabase готов")