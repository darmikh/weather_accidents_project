"""
Загрузка почасовых погодных данных с Open-Meteo (2014-2025)
"""
import sys
from pathlib import Path

src_path = Path(__file__).parent.parent
sys.path.append(str(src_path))

import requests
import time
from sqlalchemy import create_engine, text
import os
from database import db
from etl.logger_config import get_logger
from config import config

logger = get_logger('weather_etl')

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Полный список всех 23 метеопараметров
HOURLY_PARAMS = [
    "temperature_2m",           # температура на 2м
    "relative_humidity_2m",     # относительная влажность
    "dew_point_2m",             # точка росы
    "apparent_temperature",     # ощущаемая температура
    "precipitation",            # осадки
    "rain",                     # дождь
    "snowfall",                 # снег
    "snow_depth",               # высота снежного покрова
    "cloud_cover",              # общая облачность
    "cloud_cover_low",          # нижняя облачность
    "cloud_cover_mid",          # средняя облачность
    "cloud_cover_high",         # верхняя облачность
    "pressure_msl",             # давление на уровне моря
    "surface_pressure",         # давление на поверхности
    "wind_speed_10m",           # скорость ветра на 10м
    "wind_speed_100m",          # скорость ветра на 100м
    "wind_direction_100m",      # направление ветра на 100м
    "wind_gusts_10m",           # порывы ветра на 10м
    "shortwave_radiation",      # коротковолновая радиация
    "direct_radiation",         # прямая радиация
    "diffuse_radiation",        # рассеянная радиация
    "direct_normal_irradiance", # прямая нормальная irradiance
    "terrestrial_radiation"     # земная радиация
]

def get_db_connection():
    """Создает подключение к БД через SQLAlchemy"""
    user = os.getenv('SUPABASE_DB_USER')
    password = os.getenv('SUPABASE_DB_PASSWORD')
    host = os.getenv('SUPABASE_DB_HOST')
    db_name = os.getenv('SUPABASE_DB_NAME')
    
    if not all([user, password, host, db_name]):
        logger.error("Не найдены переменные окружения для подключения к БД")
        return None
    
    database_url = f'postgresql+psycopg2://{user}:{password}@{host}:5432/{db_name}'
    return create_engine(database_url)

def get_active_cities():
    logger.info("Получение списка активных городов")
    
    engine = get_db_connection()
    if not engine:
        return []
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT id, city_name, latitude, longitude FROM cities WHERE is_active = true")
            )
            cities = []
            for row in result:
                cities.append({
                    'id': str(row[0]),  # UUID в строку
                    'city_name': row[1],
                    'latitude': row[2],
                    'longitude': row[3]
                })
            
            logger.info(f"Найдено активных городов: {len(cities)}")
            for city in cities:
                logger.debug(f"  - {city['city_name']} (ID: {city['id']})")
            
            return cities
    except Exception as e:
        logger.error(f"Ошибка при получении городов: {e}")
        return []

def check_month_loaded(city_id, start_date, end_date, engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT id FROM raw_weather_data 
                    WHERE city_id = :city_id 
                    AND start_date = :start_date 
                    AND end_date = :end_date 
                    AND source = 'open-meteo-full'
                """),
                {"city_id": city_id, "start_date": start_date, "end_date": end_date}
            )
            return result.first() is not None
    except Exception as e:
        logger.error(f"Ошибка при проверке загруженного месяца: {e}")
        return False

def save_raw_weather_data(city_id, start_date, end_date, raw_data, engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO raw_weather_data 
                    (city_id, latitude, longitude, start_date, end_date, 
                     request_url, response_status, hourly_data, source)
                    VALUES 
                    (:city_id, :latitude, :longitude, :start_date, :end_date,
                     :request_url, :response_status, :hourly_data, :source)
                    RETURNING id
                """),
                {
                    "city_id": city_id,
                    "latitude": raw_data['latitude'],
                    "longitude": raw_data['longitude'],
                    "start_date": start_date,
                    "end_date": end_date,
                    "request_url": raw_data['request_url'],
                    "response_status": raw_data['response_status'],
                    "hourly_data": raw_data['hourly_data'],
                    "source": 'open-meteo-full'
                }
            )
            conn.commit()
            row = result.first()
            return row[0] if row else None
    except Exception as e:
        logger.error(f"Ошибка при сохранении raw данных: {e}")
        return None

def save_hourly_weather(records, engine):
    if not records:
        return 0
    
    saved = 0
    batch_size = 300
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            with engine.connect() as conn:
                for record in batch:
                    conn.execute(
                        text("""
                            INSERT INTO weather_hourly 
                            (city_id, datetime, temperature_2m, relative_humidity_2m,
                             dew_point_2m, apparent_temperature, precipitation, rain,
                             snowfall, snow_depth, pressure_msl, surface_pressure,
                             cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high,
                             wind_speed_10m, wind_speed_100m, wind_direction_100m,
                             wind_gusts_10m, shortwave_radiation, direct_radiation,
                             diffuse_radiation, direct_normal_irradiance, terrestrial_radiation,
                             raw_weather_id)
                            VALUES 
                            (:city_id, :datetime, :temperature_2m, :relative_humidity_2m,
                             :dew_point_2m, :apparent_temperature, :precipitation, :rain,
                             :snowfall, :snow_depth, :pressure_msl, :surface_pressure,
                             :cloud_cover, :cloud_cover_low, :cloud_cover_mid, :cloud_cover_high,
                             :wind_speed_10m, :wind_speed_100m, :wind_direction_100m,
                             :wind_gusts_10m, :shortwave_radiation, :direct_radiation,
                             :diffuse_radiation, :direct_normal_irradiance, :terrestrial_radiation,
                             :raw_weather_id)
                            ON CONFLICT (city_id, datetime) DO NOTHING
                        """),
                        record
                    )
                conn.commit()
                saved += len(batch)
                logger.debug(f"Сохранено {len(batch)} записей")
        except Exception as e:
            logger.error(f"Ошибка при сохранении почасовых данных: {e}")
    
    return saved

def load_full_weather():
    logger.info("Запуск загрузки погодных данных")
    
    engine = get_db_connection()
    if not engine:
        logger.error("Не удалось подключиться к базе данных")
        return
    
    cities = get_active_cities()
    if not cities:
        logger.error("Нет активных городов для загрузки погоды")
        return
    
    logger.info(f"Начинаем загрузку для {len(cities)} городов за период 2014-2025")
    
    for city_idx, city in enumerate(cities):
        logger.info(f"[{city_idx+1}/{len(cities)}] Обработка города: {city['city_name']}")
        
        for year in range(config.START_YEAR, config.END_YEAR + 1):
            for month in range(1, 13):  # 12 месяцев
                # Формируем даты месяца
                start = f"{year}-{month:02d}-01"
                
                if month in [1, 3, 5, 7, 8, 10, 12]:
                    end = f"{year}-{month:02d}-31"
                elif month in [4, 6, 9, 11]:
                    end = f"{year}-{month:02d}-30"
                elif year % 4 == 0:  # високосный
                    end = f"{year}-02-29"
                else:
                    end = f"{year}-02-28"
                
                logger.info(f"Загрузка за {year}-{month:02d}...")
                
                if check_month_loaded(city['id'], start, end, engine):
                    logger.debug(f"Данные за {year}-{month:02d} уже есть в БД, пропускаем")
                    continue
                
                params = {
                    "latitude": city['latitude'],
                    "longitude": city['longitude'],
                    "start_date": start,
                    "end_date": end,
                    "hourly": ",".join(HOURLY_PARAMS),
                    "timezone": "auto"
                }
                
                try:
                    api_resp = requests.get(BASE_URL, params=params, timeout=45)
                    
                    if api_resp.status_code != 200:
                        logger.error(f"ошибка API: {api_resp.status_code}")
                        time.sleep(2)
                        continue
                
                    raw_data = {
                        'latitude': city['latitude'],
                        'longitude': city['longitude'],
                        'request_url': api_resp.url,
                        'response_status': api_resp.status_code,
                        'hourly_data': api_resp.json(),
                    }
                    
                    raw_id = save_raw_weather_data(city['id'], start, end, raw_data, engine)
                    
                    if not raw_id:
                        logger.error(f"Ошибка сохранения RAW данных для {city['city_name']} {year}-{month:02d}")
                        time.sleep(2)
                        continue
                    
                    logger.debug(f"RAW данные сохранены для {city['city_name']} {year}-{month:02d}, ID: {raw_id}")
                    
                    hourly = api_resp.json().get('hourly', {})
                    times = hourly.get('time', [])
                    
                    if not times:
                        logger.warning(f"Нет данных за {year}-{month:02d} для города {city['city_name']}")
                        continue
                    
                    records = []
                    for i in range(len(times)):
                        record = {
                            'city_id': city['id'],
                            'datetime': times[i],
                            'temperature_2m': hourly.get('temperature_2m', [None])[i],
                            'relative_humidity_2m': hourly.get('relative_humidity_2m', [None])[i],
                            'dew_point_2m': hourly.get('dew_point_2m', [None])[i],
                            'apparent_temperature': hourly.get('apparent_temperature', [None])[i],
                            'precipitation': hourly.get('precipitation', [None])[i],
                            'rain': hourly.get('rain', [None])[i],
                            'snowfall': hourly.get('snowfall', [None])[i],
                            'snow_depth': hourly.get('snow_depth', [None])[i],
                            'cloud_cover': hourly.get('cloud_cover', [None])[i],
                            'cloud_cover_low': hourly.get('cloud_cover_low', [None])[i],
                            'cloud_cover_mid': hourly.get('cloud_cover_mid', [None])[i],
                            'cloud_cover_high': hourly.get('cloud_cover_high', [None])[i],
                            'pressure_msl': hourly.get('pressure_msl', [None])[i],
                            'surface_pressure': hourly.get('surface_pressure', [None])[i],
                            'wind_speed_10m': hourly.get('wind_speed_10m', [None])[i],
                            'wind_speed_100m': hourly.get('wind_speed_100m', [None])[i],
                            'wind_direction_100m': hourly.get('wind_direction_100m', [None])[i],
                            'wind_gusts_10m': hourly.get('wind_gusts_10m', [None])[i],
                            'shortwave_radiation': hourly.get('shortwave_radiation', [None])[i],
                            'direct_radiation': hourly.get('direct_radiation', [None])[i],
                            'diffuse_radiation': hourly.get('diffuse_radiation', [None])[i],
                            'direct_normal_irradiance': hourly.get('direct_normal_irradiance', [None])[i],
                            'terrestrial_radiation': hourly.get('terrestrial_radiation', [None])[i],
                            'raw_weather_id': raw_id
                        }
                        records.append(record)
                    
                    # Вставляем пачками
                    saved = save_hourly_weather(records, engine)
                    
                    logger.info(f"  {year}-{month:02d}... загружено {saved} записей")
                    
                    time.sleep(1.5)
                    
                except Exception as e:
                    logger.error(f"Ошибка при загрузке {year}-{month:02d} для города {city['city_name']}: {type(e).__name__} - {e}")
                    time.sleep(3)
                    continue
    
    logger.info("Загрузка погодных данных завершена")

if __name__ == "__main__":
    load_full_weather()