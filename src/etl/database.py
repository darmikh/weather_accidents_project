import os
import requests
from dotenv import load_dotenv
from etl.logger_config import get_logger

load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
yandex_apikey = os.getenv('YANDEX_APIKEY')

logger = get_logger('database')

if not url or not key:
    logger.error("Проверьте .env файл - отсутствуют SUPABASE_URL или SUPABASE_KEY")
    raise ValueError("Проверьте .env файл")

class SupabaseClient:
    
    def __init__(self):
        self.url = url.rstrip('/')
        self.key = key
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        logger.info("Supabase клиент инициализирован")
    
    def test_connection(self):
        try:
            response = requests.get(
                f"{self.url}/rest/v1/raw_cities",
                headers=self.headers,
                params={"limit": "1"},
                timeout=10
            )
            if response.status_code in [200, 206]:
                logger.info("Подключение к Supabase успешно")
                return True
            else:
                logger.warning(f"Подключение к Supabase вернуло статус {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Ошибка подключения к Supabase: {e}")
            return False
    
    def insert_raw_city(self, city_data):
        try:
            response = requests.post(
                f"{self.url}/rest/v1/raw_cities",
                headers=self.headers,
                json=city_data,
                timeout=10
            )
            
            if response.status_code == 201:
                data = response.json()
                if data and len(data) > 0:
                    city_id = data[0].get('id')
                    logger.debug(f"Сырой город {city_data.get('original_city_name')} сохранен, ID: {city_id}")
                    return city_id
            elif response.status_code == 409:
                logger.debug(f"Город {city_data.get('original_city_name')} уже существует")
                return None
            else:
                logger.error(f"Ошибка вставки raw_city: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Ошибка при вставке raw_city: {e}")
        
        return None
    
    def raw_city_exists(self, city_name, region):
        """Проверяет, есть ли уже сырой город в БД"""
        try:
            response = requests.get(
                f"{self.url}/rest/v1/raw_cities",
                headers=self.headers,
                params={
                    "original_city_name": f"eq.{city_name}",
                    "original_region": f"eq.{region}",
                    "select": "id"
                },
                timeout=10
            )
            
            if response.status_code == 200:
                return len(response.json()) > 0
            return False
        except Exception as e:
            logger.error(f"Ошибка проверки raw_city {city_name}: {e}")
            return False   
    
    def insert_city(self, city_data):
        try:
            response = requests.post(
                f"{self.url}/rest/v1/cities",
                headers=self.headers,
                json=city_data,
                timeout=10
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
    
    def get_raw_cities_pending(self):
        try:
            logger.debug("Запрос списка raw_cities со статусом 'pending'")
            response = requests.get(
                f"{self.url}/rest/v1/raw_cities",
                headers=self.headers,
                params={"status": "eq.pending", "select": "*"},
                timeout=10
            )
            
            if response.status_code == 200:
                cities = response.json()
                logger.info(f"Получено {len(cities)} сырых городов для обработки")
                return cities
            else:
                logger.warning(f"Не удалось получить raw_cities. Статус: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Ошибка получения raw_cities: {e}")
            return []
    
    def update_raw_city_status(self, raw_city_id, status, error_message=None):
        try:
            update_data = {"status": status}
            if error_message:
                update_data["error_message"] = error_message
                logger.warning(f"Обновление статуса raw_city ID {raw_city_id} на '{status}' с ошибкой: {error_message[:50]}...")
            else:
                logger.info(f"Обновление статуса raw_city ID {raw_city_id} на '{status}'")
                
            response = requests.patch(
                f"{self.url}/rest/v1/raw_cities?id=eq.{raw_city_id}",
                headers=self.headers,
                json=update_data,
                timeout=10
            )
                
            if response.status_code == 204:
                    logger.debug(f"Статус raw_city {raw_city_id} успешно обновлен")
                    return True
            else:
                logger.error(f"Ошибка обновления статуса. Статус ответа: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка обновления статуса для raw_city {raw_city_id}: {e}")
            return False
    
    def city_exists(self, city_name, region):
        try:
            logger.debug(f"Проверка существования города {city_name}, {region}")
            response = requests.get(
                f"{self.url}/rest/v1/cities",
                headers=self.headers,
                params={
                    "city_name": f"eq.{city_name}",
                    "region": f"eq.{region}",
                    "select": "id"
                },
                timeout=10
            )
            
            if response.status_code == 200:
                exists = len(response.json()) > 0
                if exists:
                    logger.debug(f"Город {city_name} уже существует в БД")
                else:
                    logger.debug(f"Город {city_name} не найден в БД")
                return exists
            else:
                logger.warning(f"Ошибка при проверке существования города. Статус: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка проверки города {city_name}: {e}")
            return False
        

db = SupabaseClient()
logger.info("Модуль database.py загружен, клиент Supabase готов")