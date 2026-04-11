import requests
import pandas as pd
import time
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from typing import Optional, Tuple
from bs4 import BeautifulSoup

from database import db
from logger_config import get_logger
from config import config 
from utils.text_utils import normalize_city_name

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


class CitiesLoader:
    
    def run(self):
        logger.info("Загрузка данных из Википедии")
        
        if not db.test_connection():
            logger.error("Ошибка подключения к Supabase")
            return None
        
        try:
            raw_html = self._fetch_wikipedia_html()
            if not raw_html:
                return None
            
            staging_id = db.insert_raw_cities_data({
                'raw_html': raw_html,
                'processed': False
            })
            
            if staging_id:
                logger.info(f"Сырые данные сохранены, ID: {staging_id}")
            else:
                logger.error("Не удалось сохранить данные")
            
            return staging_id
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке: {e}")
            return None
        
    
    def _fetch_wikipedia_html(self):
        try:
            response = requests.get(
                'https://ru.wikipedia.org/w/api.php',
                params={
                    'action': 'parse',
                    'page': 'Список_городов_России',
                    'format': 'json',
                    'prop': 'text',
                    'contentmodel': 'wikitext'
                },
                headers={'User-Agent': config.USER_AGENT_EMAIL},
                timeout=30
            )
            response.raise_for_status()
            return response.json()['parse']['text']['*']
        except Exception as e:
            logger.error(f"Ошибка загрузки Википедии: {e}")
            return None


  
class CitiesProcessor:
    
    def __init__(self):
        self.geolocator = Nominatim(user_agent="weather_accidents_analysis")
        self.geocode = RateLimiter(self.geolocator.geocode, min_delay_seconds=2)
        self.coordinates_cache = {}
        self.yandex_apikey = YANDEX_APIKEY  # <-- вот эта строка
        
        # DaData клиент (опционально)
        self.dadata_client = None
        if DADATA_AVAILABLE and DADATA_API_KEY:
            try:
                self.dadata_client = DadataClient(DADATA_API_KEY)
                logger.info("DaData клиент инициализирован")
            except Exception as e:
                logger.warning(f"Ошибка инициализации DaData: {e}")
        
    def process(self, staging_id):
        logger.info(f"Обработка данных ID: {staging_id}")
        
        # Получаем сырые данные
        raw_data = db.get_raw_cities_data(staging_id)
        if not raw_data:
            logger.error(f"Данные не найдены: {staging_id}")
            return
        
        # Парсим таблицу
        cities_df = self._parse_cities_table(raw_data['raw_html'])
        if cities_df.empty:
            logger.error("Не удалось распарсить города")
            db.update_raw_cities_data_status(staging_id, True, "Parsing failed")
            return
        
        logger.info(f"Найдено городов: {len(cities_df)}")
        
        # Обрабатываем каждый город
        success_count = 0
        for _, row in cities_df.iterrows():
            city_name = row.get('Город')
            region = row.get('Регион', '')
            population = self._parse_population(row.get('Население'))
            
            if not city_name:
                continue
            
            # Проверяем дубликаты (явные и неявные)
            if self._is_duplicate_city(city_name, region):
                logger.debug(f"Город {city_name} уже существует (или похож), пропускаем")
                continue
            
            # Получаем координаты
            coords = self._get_coordinates(city_name, region)
            if not coords:
                logger.warning(f"Координаты не найдены для {city_name}, пропускаем")
                continue
            
            # Получаем OKATO (опционально)
            okato = self._get_okato(city_name, region) if self.dadata_client else None
            
            # Сохраняем город
            city_data = {
                'city_name': city_name,
                'region': region,
                'federal_district': row.get('Федеральный округ'),
                'population': self._parse_population(row.get('Население')),
                'okato_code': okato,
                'latitude': coords[0],
                'longitude': coords[1],
                'is_active': False
            }
            
            if db.insert_city(city_data):
                success_count += 1
                logger.info(f"Добавлен город ({success_count}): {city_name}")
            
            time.sleep(0.2)  # Пауза между запросами
        
        # Обновляем статус
        db.update_raw_cities_data_status(staging_id, True)
        logger.info(f"Обработка завершена. Добавлено городов: {success_count}")
        
    def _is_duplicate_city(self, city_name, region):
        if db.city_exists(city_name, region, fuzzy=False):
            logger.debug(f"Точное совпадение: {city_name}")
            return True
        
        return False
    
    def clean_dataframe(self, df):
        if 'Город' not in df.columns:
            raise ValueError("В таблице нет колонки 'Город'")
        
        # Нормализуем названия городов с помощью функции из text_utils.py и удаляем пустые названия
        if 'Город' in df.columns:
            df['Город'] = df['Город'].apply(normalize_city_name)
            df = df[df['Город'].notna() & (df['Город'] != '')]
            
        # Преобразуем население в число (для анализа в дальнейшем)
        if 'Население' in df.columns:
            df['Население'] = pd.to_numeric(
                df['Население'].astype(str).str.replace(r'[^\d]', '', regex=True), 
                errors='coerce'
            )
        
        return df
    
    def _parse_cities_table(self, raw_html):
        try:
            # Пробуем pandas read_html
            tables = pd.read_html(raw_html)
            if tables:
                cities_df = tables[0]
                logger.info(f"Таблица найдена через pandas, колонки: {list(cities_df.columns[:5])}")
                return self.clean_dataframe(cities_df)
        except Exception as e:
            logger.debug(f"pandas read_html не сработал: {e}")
        
        # Если pandas не помог, парсим вручную через BeautifulSoup
        try:
            soup = BeautifulSoup(raw_html, 'html.parser')
            
            target_table = None
            
            wikitable = soup.find('table', {'class': 'wikitable'})
            if wikitable:
                target_table = wikitable
                logger.info("Найдена таблица с классом 'wikitable'")
            
            if not target_table:
                standard_table = soup.find('table', {'class': 'standard'})
                if standard_table:
                    target_table = standard_table
                    logger.info("Найдена таблица с классом 'standard'")
            
            if not target_table:
                all_tables = soup.find_all('table')
                for table in all_tables:
                    rows = table.find_all('tr')
                    if len(rows) > 1000:
                        target_table = table
                        logger.info("Найдена большая таблица")
                        break
            
            if not target_table:
                logger.error("Таблица с городами не найдена")
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
            logger.info(f"Таблица найдена через BeautifulSoup, колонки: {column_names[:5]}")
            return self.clean_dataframe(cities_df)
            
        except Exception as e:
            logger.error(f"Ошибка парсинга таблицы: {e}")
            return pd.DataFrame()
    
    def _get_coordinates(self, city_name, region):
        cache_key = f"{city_name}_{region}"
        if cache_key in self.coordinates_cache:
            return self.coordinates_cache[cache_key]
        
        queries = [f"{city_name}, {region}, Россия", f"{city_name}, Россия", city_name]
        
        # Сначала Яндекс
        if self.yandex_apikey:
            for query in queries:
                try:
                    response = requests.get(
                        "https://geocode-maps.yandex.ru/1.x",
                        params={"geocode": query, "apikey": self.yandex_apikey, "format": "json"},
                        timeout=10
                    )
                    data = response.json()
                    places = data['response']['GeoObjectCollection']['featureMember']
                    if places:
                        lon, lat = places[0]['GeoObject']['Point']['pos'].split()
                        coords = (float(lat), float(lon))
                        self.coordinates_cache[cache_key] = coords
                        return coords
                except:
                    pass
        
        # Потом Nominatim
        for query in queries:
            try:
                location = self.geocode(query, timeout=15)
                if location:
                    coords = (location.latitude, location.longitude)
                    self.coordinates_cache[cache_key] = coords
                    return coords
            except:
                continue
        
        return None
    
    def _get_okato(self, city_name, region):
        try:
            result = self.dadata_client.suggest("address", f"{city_name}, {region}, Россия", count=1)
            if result:
                return result[0].get('data', {}).get('okato')
        except Exception as e:
            logger.debug(f"DaData ошибка для {city_name}: {e}")
        return None
    
    def _parse_population(self, population):
        if not population or pd.isna(population):
            return None
        try:
            cleaned = re.sub(r'[^\d]', '', str(population))
            return int(cleaned) if cleaned else None
        except:
            return None


def main():
    logger.info("Запуск ETL процесса для городов России")
    
    # 1. Загружаем сырые данные
    loader = CitiesLoader()
    staging_id = loader.run()
    
    if not staging_id:
        logger.error("Не удалось загрузить сырые данные, обработка остановлена")
        return
    
    # 2. Обрабатываем данные
    processor = CitiesProcessor()
    processor.process(staging_id)
    
    logger.info("ETL процесс завершен")


if __name__ == "__main__":
    main()