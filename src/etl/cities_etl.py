import requests
import pandas as pd
import time
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from typing import Optional, Tuple
from config import config

from database import db
from etl.logger_config import get_logger

logger = get_logger('cities_etl')

YANDEX_APIKEY = config.YANDEX_APIKEY
DADATA_API_KEY = config.DADATA_API_KEY

# Пробуем импортировать dadata
try:
    from dadata import Dadata as DadataClient
    DADATA_AVAILABLE = True
except ImportError:
    DADATA_AVAILABLE = False
    logger.warning("Библиотека dadata-py не установлена. ОКАТО не будет получен")


class CitiesParser:
    
    def run(self):
        logger.info("Запуск парсера городов")
        if not db.test_connection():
            logger.error("Ошибка подключения к Supabase - прерывание работы")
            return
        
        logger.info("Начинаем парсинг Wikipedia...")
        cities_df = self.parse_wikipedia()
        
        if cities_df.empty:
            logger.error("Не удалось получить данные с Википедии")
            return
        
        logger.info(f"Найдено городов: {len(cities_df)}")
        
        success_count = 0
        error_count = 0
        
        for idx, row in cities_df.iterrows():
            city = row['Город']
            region = row['Регион']
            population = row['Население']
            federal_district = row['Федеральный округ']
            
            try:
                logger.debug(f"Обработка города {idx+1}/{len(cities_df)}: {city}")
                raw_data = {
                    'row_number': idx + 1,
                    'original_city_name': str(city),
                    'original_region': str(region),
                    'original_federal_district': str(federal_district),
                    'original_population': str(population) if pd.notna(population) else None,
                    'status': 'pending',
                    'error_message': None
                }
                
                if not db.raw_city_exists(city, region):
                    raw_id = db.insert_raw_city(raw_data)
                    if raw_id:
                        success_count += 1
                    else:
                        error_count += 1
                else:
                    logger.debug(f"Город {city} уже есть в raw_cities, пропускаем")
                    success_count += 1  # считаем как успех, но не вставляем
                
                time.sleep(0.05)
                
            except Exception as e:
                error_count += 1
                logger.error(f"Исключение при обработке {city}: {e}")
                continue
        
        logger.info(f"Загрузка завершена. Успешно: {success_count}, Ошибок: {error_count}")
        
    
    def parse_wikipedia(self):
        params = {
            'action': 'parse',
            'page': 'Список_городов_России',
            'format': 'json',
            'prop': 'text',
            'contentmodel': 'wikitext'
        }
        
        headers = {
            'User-Agent': 'dasha.da.123.25@gmail.com'
        }
        
        try:
            response = requests.get(
                'https://ru.wikipedia.org/w/api.php',
                params=params, 
                headers=headers,
                timeout=30
            )
            
            if response.status_code != 200:
                return pd.DataFrame()
            
            data = response.json()
            html_content = data['parse']['text']['*']
            
            try:
                tables = pd.read_html(html_content)
                if tables:
                    cities_df = tables[0]
                    return self.clean_dataframe(cities_df)
            except:
                pass
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            target_table = None
            wikitable = soup.find('table', {'class': 'wikitable'})
            if wikitable:
                target_table = wikitable
            
            if not target_table:
                standard_table = soup.find('table', {'class': 'standard'})
                if standard_table:
                    target_table = standard_table
            
            if not target_table:
                all_tables = soup.find_all('table')
                for table in all_tables:
                    rows = table.find_all('tr')
                    if len(rows) > 1000:
                        target_table = table
                        break
            
            if not target_table:
                return pd.DataFrame()
            
            headers = []
            header_row = target_table.find('tr')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    headers.append(th.get_text(strip=True))
            
            data = []
            rows = target_table.find_all('tr')[1:]
            
            for row in rows:
                row_data = []
                cells = row.find_all(['td', 'th'])
                
                for cell in cells:
                    text = cell.get_text(' ', strip=True)
                    row_data.append(text)
                
                if row_data:
                    data.append(row_data)
            
            if not data:
                return pd.DataFrame()
            
            num_cols = len(data[0]) if data else 0
            
            if headers and len(headers) == num_cols:
                column_names = headers
            elif num_cols == 9:
                column_names = ['№', 'Герб', 'Город', 'Регион', 'Федеральный округ', 
                            'Население', 'Основание', 'Статус города', 'Прежние названия']
            else:
                column_names = [f'col_{i}' for i in range(num_cols)]
            
            cities_df = pd.DataFrame(data, columns=column_names)
            return self.clean_dataframe(cities_df)
                    
        except Exception:
            return pd.DataFrame()
    
    def clean_dataframe(self, df):
        if 'Город' not in df.columns:
            for col in df.columns:
                if 'город' in str(col).lower():
                    df = df.rename(columns={col: 'Город'})
                    break
        
        if 'Город' in df.columns:
            df['Город'] = df['Город'].apply(self.clean_city_name)
        
        if 'Население' in df.columns:
            df['Население'] = pd.to_numeric(
                df['Население'].astype(str).str.replace(r'[^\d]', '', regex=True), 
                errors='coerce'
            )
        
        return df
    
    def clean_city_name(self, city_name):
        if not isinstance(city_name, str):
            city_name = str(city_name)
        
        city_name = re.sub(r'\[.*?\]', '', city_name)
        city_name = re.sub(r'не призн\.', '', city_name)
        city_name = city_name.strip()
        
        return city_name


class CitiesProcessor:
    
    def __init__(self):
        self.geolocator = Nominatim(user_agent="weather_accidents_analysis")
        self.geocode = RateLimiter(self.geolocator.geocode, min_delay_seconds=2)
        self.coordinates_cache = {}
        self.yandex_apikey = YANDEX_APIKEY
        
        # Инициализируем DaData клиент
        self.dadata_client = None
        self.dadata_available = False
        
        if DADATA_AVAILABLE and DADATA_API_KEY:
            try:
                self.dadata_client = DadataClient(DADATA_API_KEY)
                self.dadata_available = True
                logger.info("DaData клиент инициализирован")
            except Exception as e:
                logger.info(f"Ошибка инициализации DaData: {e}")
        else:
            logger.info("DaData недоступен, ОКАТО не будет получен")
    
    def get_coordinates_yandex(self, address: str) -> Optional[Tuple[float, float]]:
        """Получение координат через Яндекс API"""
        if not self.yandex_apikey:
            return None
        
        try:
            base_url = "https://geocode-maps.yandex.ru/1.x"
            response = requests.get(
                base_url,
                params={
                    "geocode": address,
                    "apikey": self.yandex_apikey,
                    "format": "json",
                },
                timeout=10
            )
            response.raise_for_status()

            data = response.json()
            found_places = data['response']['GeoObjectCollection']['featureMember']

            if not found_places:
                return None

            most_relevant = found_places[0]
            lon, lat = most_relevant['GeoObject']['Point']['pos'].split(" ")

            return float(lat), float(lon)

        except Exception as e:
            logger.debug(f"Яндекс API ошибка: {type(e).__name__}")
            return None
    
    def get_coordinates(self, city_name: str, region: str) -> Optional[Tuple[float, float]]:
        """Основной метод получения координат"""
        cache_key = f"{city_name}_{region}"
        
        if cache_key in self.coordinates_cache:
            return self.coordinates_cache[cache_key]
        
        # Сначала пробуем Яндекс API
        queries = [
            f"{city_name}, {region}, Россия",
            f"{city_name}, Россия",
            city_name
        ]
        
        for query in queries:
            coords = self.get_coordinates_yandex(query)
            if coords:
                self.coordinates_cache[cache_key] = coords
                return coords
        
        # Если Яндекс не сработал, пробуем Nominatim
        for query in queries:
            try:
                location = self.geocode(query, timeout=15)
                if location:
                    coords = (location.latitude, location.longitude)
                    self.coordinates_cache[cache_key] = coords
                    return coords
            except Exception:
                continue
        
        return None
    
    def get_okato_code(self, city_name: str, region: str) -> Optional[str]:
        """Получить код ОКАТО для города через DaData"""
        if not self.dadata_available or not self.dadata_client:
            return None
        
        query = f"{city_name}, {region}, Россия"
        
        try:
            result = self.dadata_client.suggest("address", query, count=1)
            
            if result and len(result) > 0:
                data = result[0].get('data', {})
                okato = data.get('okato')
                return okato
            
            return None
            
        except Exception as e:
            logger.info(f"DaData API ошибка для {city_name}: {e}")
            return None
    
    def process_raw_cities(self):
        logger.info("Обработка сырых данных городов")
        
        raw_cities = db.get_raw_cities_pending()
        
        if not raw_cities:
            logger.info("Нет городов для обработки")
            return
        
        logger.info(f"Городов для обработки: {len(raw_cities)}")
        
        success_count = 0
        error_count = 0
        dadata_requests = 0
        
        for i, raw_city in enumerate(raw_cities):
            logger.info(f"Обработка {i+1}/{len(raw_cities)}: {raw_city['original_city_name']}")
            
            raw_city_id = raw_city['id']
            city_name = raw_city['original_city_name']
            region = raw_city['original_region']
            federal_district = raw_city['original_federal_district']
            population = raw_city['original_population']
            
            try:
                if db.city_exists(city_name, region):
                    db.update_raw_city_status(raw_city_id, 'processed')
                    continue
                
                population_int = self.parse_population(population)
                
                coordinates = self.get_coordinates(city_name, region)
                if not coordinates:
                    db.update_raw_city_status(raw_city_id, 'error', 'Координаты не найдены')
                    error_count += 1
                    continue
                
                okato_code = None
                if self.dadata_available:
                    okato_code = self.get_okato_code(city_name, region)
                    if okato_code:
                        dadata_requests += 1
                
                city_data = {
                    'city_name': city_name,
                    'region': region,
                    'federal_district': federal_district,
                    'population': population_int,
                    'okato_code': okato_code,
                    'latitude': coordinates[0],
                    'longitude': coordinates[1],
                    'is_active': False,
                    'raw_city_id': raw_city_id
                }
                
                city_id = db.insert_city(city_data)
                
                if city_id:
                    db.update_raw_city_status(raw_city_id, 'processed')
                    success_count += 1
                else:
                    db.update_raw_city_status(raw_city_id, 'error', 'Не удалось добавить в cities')
                    error_count += 1
                
                # Задержка для DaData API (10k запросов в день бесплатно)
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Исключение при обработке {city_name}: {e}")
                db.update_raw_city_status(raw_city_id, 'error', str(e)[:200])
                error_count += 1
        
        logger.info(f"Обработано успешно: {success_count}")
        logger.info(f"Ошибок обработки: {error_count}")
        
        if self.dadata_available:
            logger.info(f"Выполнено запросов к DaData: {dadata_requests}")
    
    def parse_population(self, population_str):
        if not population_str:
            return None
        
        try:
            cleaned = re.sub(r'[^\d]', '', str(population_str))
            return int(cleaned) if cleaned else None
        except:
            return None