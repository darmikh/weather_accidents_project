import requests
from dotenv import load_dotenv
from logger_config import get_logger
from config import config

url = config.SUPABASE_URL
key = config.SUPABASE_KEY
YANDEX_APIKEY = config.YANDEX_APIKEY

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
            logger.info("REST клиент не инициализирован (используется только прямой доступ к БД)")
    
    def test_connection(self):
        """Проверка подключения (если есть REST)"""
        if not self.headers:
            logger.warning("REST API не доступен, пропускаем проверку")
            return True
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
        """Вставка сырого города (только через REST)"""
        if not self.headers:
            logger.error("REST API не доступен для вставки raw_city")
            return None
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
        """Проверка существования сырого города (только через REST)"""
        if not self.headers:
            logger.error("REST API не доступен для проверки raw_city")
            return False
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
        """Вставка города (только через REST)"""
        if not self.headers:
            logger.error("REST API не доступен для вставки city")
            return None
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
        """Получение сырых городов (только через REST)"""
        if not self.headers:
            logger.error("REST API не доступен для получения raw_cities")
            return []
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
        """Обновление статуса (только через REST)"""
        if not self.headers:
            logger.error("REST API не доступен для обновления статуса")
            return False
        try:
            update_data = {"status": status}
            if error_message:
                update_data["error_message"] = error_message
                logger.warning(f"Обновление статуса raw_city ID {raw_city_id} на '{status}' с ошибкой: {error_message[:50]}...")
            else:
                logger.debug(f"Обновление статуса raw_city ID {raw_city_id} на '{status}'")
                
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
        """Проверка существования города (только через REST)"""
        if not self.headers:
            logger.error("REST API не доступен для проверки city")
            return False
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