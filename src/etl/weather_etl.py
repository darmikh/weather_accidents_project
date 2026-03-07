"""
Загрузка ПОЛНЫХ почасовых погодных данных с Open-Meteo (2014-2025)
"""
import sys
from pathlib import Path

# Добавляем папку src в путь
src_path = Path(__file__).parent.parent
sys.path.append(str(src_path))

import requests
import time
from database import db
from etl.logger_config import get_logger

logger = get_logger('weather_etl')

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# ПОЛНЫЙ список всех 23 метеопараметров
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

def get_active_cities():
    """Получить активные города (те, для которых is_active = true)"""
    logger.info("Получение списка активных городов")
    resp = requests.get(
        f"{db.url}/rest/v1/cities",
        headers=db.headers,
        params={"is_active": "eq.true", "select": "id,city_name,latitude,longitude"}
    )
    
    if resp.status_code != 200 or not resp.json():
        logger.warning("Нет активных городов или ошибка БД")
        return []
    
    cities = resp.json()
    logger.info(f"Найдено активных городов: {len(cities)}")
    for city in cities:
        logger.debug(f"  - {city['city_name']} (ID: {city['id']})")
        
    return cities

def load_full_weather():
    logger.info("Запуск загрузки погодных данных")
    cities = get_active_cities()
    if not cities:
        logger.error("Нет активных городов для загрузки погоды")
        return
    
    logger.info(f"Начинаем загрузку для {len(cities)} городов за период 2014-2025")
    
    for city_idx, city in enumerate(cities):
        logger.info(f"[{city_idx+1}/{len(cities)}] Обработка города: {city['city_name']}")
        
        for year in range(2014, 2026):  # 2014-2025
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
                
                # Проверяем, не загружены ли уже полные данные
                resp_check = requests.get(
                    f"{db.url}/rest/v1/raw_weather_data",
                    headers=db.headers,
                    params={
                        "city_id": f"eq.{city['id']}",
                        "start_date": f"eq.{start}",
                        "end_date": f"eq.{end}",
                        "source": f"eq.open-meteo-full"
                    }
                )
                
                if resp_check.status_code == 200 and resp_check.json():
                    logger.debug(f"Данные за {year}-{month:02d} уже есть в БД, пропускаем")
                    continue
                
                # Запрос к API с полным набором параметров
                params = {
                    "latitude": city['latitude'],
                    "longitude": city['longitude'],
                    "start_date": start,
                    "end_date": end,
                    "hourly": ",".join(HOURLY_PARAMS),
                    "timezone": "auto"
                }
                
                try:
                    # Запрос к API
                    api_resp = requests.get(BASE_URL, params=params, timeout=45)
                    
                    if api_resp.status_code != 200:
                        logger.error(f"ошибка API: {api_resp.status_code}")
                        time.sleep(2)
                        continue
                    
                    # Сохраняем ПОЛНЫЕ raw данные
                    raw_data = {
                        'city_id': city['id'],
                        'latitude': city['latitude'],
                        'longitude': city['longitude'],
                        'start_date': start,
                        'end_date': end,
                        'request_url': api_resp.url,
                        'response_status': api_resp.status_code,
                        'hourly_data': api_resp.json(),
                        'source': 'open-meteo-full'
                    }
                    
                    raw_resp = requests.post(
                        f"{db.url}/rest/v1/raw_weather_data",
                        headers=db.headers,
                        json=raw_data
                    )
                    
                    if raw_resp.status_code != 201:
                        logger.error(f"Ошибка сохранения RAW данных для {city['city_name']} {year}-{month:02d}. Статус: {raw_resp.status_code}")
                        time.sleep(2)
                        continue

                    raw_id = raw_resp.json()[0]['id']
                    logger.debug(f"RAW данные сохранены для {city['city_name']} {year}-{month:02d}, ID: {raw_id}")
                    
                    # Сохраняем почасовые данные
                    hourly = api_resp.json().get('hourly', {})
                    times = hourly.get('time', [])
                    
                    if not times:
                        logger.warning(f"Нет данных за {year}-{month:02d} для города {city['city_name']}")
                        continue
                    
                    # Создаем записи для каждого часа
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
                    saved = 0
                    batch_size = 300  
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i+batch_size]
                        resp = requests.post(
                            f"{db.url}/rest/v1/weather_hourly",
                            headers={**db.headers, 'Prefer': 'resolution=merge-duplicates'},
                            json=batch
                        )
                        if resp.status_code == 201:
                            saved += len(batch)
                    
                    logger.info(f"  {year}-{month:02d}... загружено {saved} записей")
                    
                    # Пауза между запросами
                    time.sleep(1.5)
                    
                except Exception as e:
                    logger.error(f"Ошибка при загрузке {year}-{month:02d} для города {city['city_name']}: {type(e).__name__} - {e}")
                    time.sleep(3)
                    continue
    
    logger.info("Загрузка погодных данных завершена")

if __name__ == "__main__":
    load_full_weather()
