"""
Полное обновление ID ГИБДД для всех городов
Скачивает свежие данные с ГИБДД, сопоставляет с городами и обновляет БД
"""
import sys
from pathlib import Path
import requests
import json
import time
from datetime import datetime
import re
import csv

src_path = Path(__file__).parent.parent
sys.path.append(str(src_path))

from database import db
from etl.logger_config import get_logger
from utils.text_utils import normalize_city_name

logger = get_logger('gibdd_full_update')

DATA_RAW = Path(__file__).parent.parent.parent / "data" / "raw"
DATA_PROCESSED = Path(__file__).parent.parent.parent / "data" / "processed"

DATA_RAW.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

# Точные названия регионов в ГИБДД (выгрузили ранее, когда изучали данные, которые отдает ГИБДД)
GIBDD_REGIONS = {
    "Алтайский край": "1",
    "Амурская область": "10",
    "Архангельская область": "11",
    "Астраханская область": "12",
    "Белгородская область": "14",
    "Брянская область": "15",
    "Владимирская область": "17",
    "Волгоградская область": "18",
    "Вологодская область": "19",
    "Воронежская область": "20",
    "Донецкая Народная Республика": "960",
    "Еврейская автономная область": "99",
    "Забайкальский край": "76",
    "Запорожская область": "956",
    "Ивановская область": "24",
    "Иркутская область": "25",
    "Кабардино-Балкарская Республика": "83",
    "Калининградская область": "27",
    "Калужская область": "29",
    "Камчатский край": "30",
    "Карачаево-Черкесская Республика": "91",
    "Кемеровская область - Кузбасс": "32",
    "Кировская область": "33",
    "Костромская область": "34",
    "Краснодарский край": "3",
    "Красноярский край": "4",
    "Курганская область": "37",
    "Курская область": "38",
    "Ленинградская область": "41",
    "Липецкая область": "42",
    "Луганская Народная Республика": "958",
    "Магаданская область": "44",
    "Московская область": "46",
    "Мурманская область": "47",
    "Ненецкий АО": "10011",
    "Нижегородская область": "22",
    "Новгородская область": "49",
    "Новосибирская область": "50",
    "Омская область": "52",
    "Оренбургская область": "53",
    "Орловская область": "54",
    "Пензенская область": "56",
    "Пермский край": "57",
    "Приморский край": "5",
    "Псковская область": "58",
    "Республика Адыгея": "79",
    "Республика Алтай": "84",
    "Республика Башкортостан": "80",
    "Республика Бурятия": "81",
    "Республика Дагестан": "82",
    "Республика Ингушетия": "26",
    "Республика Калмыкия": "85",
    "Республика Карелия": "86",
    "Республика Коми": "87",
    "Республика Крым": "35",
    "Республика Марий Эл": "88",
    "Республика Мордовия": "89",
    "Республика Саха (Якутия)": "98",
    "Республика Северная Осетия - Алания": "90",
    "Республика Татарстан": "92",
    "Республика Тыва": "93",
    "Республика Хакасия": "95",
    "Ростовская область": "60",
    "Рязанская область": "61",
    "Самарская область": "36",
    "Саратовская область": "63",
    "Сахалинская область": "64",
    "Свердловская область": "65",
    "Сириус": "101",
    "Смоленская область": "66",
    "Ставропольский край": "7",
    "Тамбовская область": "68",
    "Тверская область": "28",
    "Томская область": "69",
    "Тульская область": "70",
    "Тюменская область": "71",
    "Удмуртская Республика": "94",
    "Ульяновская область": "73",
    "Хабаровский край": "8",
    "Ханты-Мансийский АО": "71100",
    "Херсонская область": "957",
    "Челябинская область": "75",
    "Чеченская Республика": "96",
    "Чувашская Республика": "97",
    "Чукотский автономный округ": "77",
    "Ямало-Ненецкий АО": "71140",
    "Ярославская область": "78",
    "г. Москва": "45",
    "г. Санкт-Петербург": "40",
    "г. Севастополь": "67"
}

# Маппинг наших названий регионов на названия в ГИБДД
REGION_NAME_MAPPING = {
    # Республики
    "Адыгея": "Республика Адыгея",
    "Алтай": "Республика Алтай",
    "Башкортостан": "Республика Башкортостан",
    "Бурятия": "Республика Бурятия",
    "Дагестан": "Республика Дагестан",
    "Ингушетия": "Республика Ингушетия",
    "Кабардино-Балкария": "Кабардино-Балкарская Республика",
    "Калмыкия": "Республика Калмыкия",
    "Карачаево-Черкесия": "Карачаево-Черкесская Республика",
    "Карелия": "Республика Карелия",
    "Коми": "Республика Коми",
    "Крым": "Республика Крым",
    "Марий Эл": "Республика Марий Эл",
    "Мордовия": "Республика Мордовия",
    "Саха": "Республика Саха (Якутия)",
    "Якутия": "Республика Саха (Якутия)",
    "Северная Осетия": "Республика Северная Осетия - Алания",
    "Татарстан": "Республика Татарстан",
    "Тыва": "Республика Тыва",
    "Удмуртия": "Удмуртская Республика",
    "Хакасия": "Республика Хакасия",
    "Чечня": "Чеченская Республика",
    "Чувашия": "Чувашская Республика",
    
    # Автономные округа
    "Ненецкий АО": "Ненецкий АО",
    "Ханты-Мансийский АО": "Ханты-Мансийский АО",
    "Чукотский АО": "Чукотский автономный округ",
    "Ямало-Ненецкий АО": "Ямало-Ненецкий АО",
    "Еврейская АО": "Еврейская автономная область",
    
    # Края
    "Алтайский край": "Алтайский край",
    "Забайкальский край": "Забайкальский край",
    "Камчатский край": "Камчатский край",
    "Краснодарский край": "Краснодарский край",
    "Красноярский край": "Красноярский край",
    "Пермский край": "Пермский край",
    "Приморский край": "Приморский край",
    "Ставропольский край": "Ставропольский край",
    "Хабаровский край": "Хабаровский край",
    
    # Области
    "Амурская область": "Амурская область",
    "Архангельская область": "Архангельская область",
    "Астраханская область": "Астраханская область",
    "Белгородская область": "Белгородская область",
    "Брянская область": "Брянская область",
    "Владимирская область": "Владимирская область",
    "Волгоградская область": "Волгоградская область",
    "Вологодская область": "Вологодская область",
    "Воронежская область": "Воронежская область",
    "Донецкая Народная Республика": "Донецкая Народная Республика",
    "Запорожская область": "Запорожская область",
    "Ивановская область": "Ивановская область",
    "Иркутская область": "Иркутская область",
    "Калининградская область": "Калининградская область",
    "Калужская область": "Калужская область",
    "Кемеровская область": "Кемеровская область - Кузбасс",
    "Кировская область": "Кировская область",
    "Костромская область": "Костромская область",
    "Курганская область": "Курганская область",
    "Курская область": "Курская область",
    "Ленинградская область": "Ленинградская область",
    "Липецкая область": "Липецкая область",
    "Луганская Народная Республика": "Луганская Народная Республика",
    "Магаданская область": "Магаданская область",
    "Московская область": "Московская область",
    "Мурманская область": "Мурманская область",
    "Нижегородская область": "Нижегородская область",
    "Новгородская область": "Новгородская область",
    "Новосибирская область": "Новосибирская область",
    "Омская область": "Омская область",
    "Оренбургская область": "Оренбургская область",
    "Орловская область": "Орловская область",
    "Пензенская область": "Пензенская область",
    "Псковская область": "Псковская область",
    "Ростовская область": "Ростовская область",
    "Рязанская область": "Рязанская область",
    "Самарская область": "Самарская область",
    "Саратовская область": "Саратовская область",
    "Сахалинская область": "Сахалинская область",
    "Свердловская область": "Свердловская область",
    "Смоленская область": "Смоленская область",
    "Тамбовская область": "Тамбовская область",
    "Тверская область": "Тверская область",
    "Томская область": "Томская область",
    "Тульская область": "Тульская область",
    "Тюменская область": "Тюменская область",
    "Ульяновская область": "Ульяновская область",
    "Херсонская область": "Херсонская область",
    "Челябинская область": "Челябинская область",
    "Ярославская область": "Ярославская область",
    
    # Города федерального значения
    "Москва": "г. Москва",
    "Санкт-Петербург": "г. Санкт-Петербург",
    "Севастополь": "г. Севастополь",
    
    # Сириус
    "Сириус": "Сириус"
}

def download_fresh_gibdd_data():
    """
    Скачивает свежие данные с ГИБДД и сохраняет в JSON
    Возвращает структуру {регион: {название: ID, районы: [...]}}
    """
    now = datetime.now()
    year = now.year
    month = now.month - 1 if now.month > 1 else 12
    if month == 12:
        year -= 1
    
    logger.info(f"Скачиваем свежие данные ГИБДД за {month}.{year}...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Content-Type': 'application/json'
    }
    
    # Получаем список регионов
    rf_payload = {
        "maptype": 1,
        "region": "877",
        "date": f'["MONTHS:{month}.{year}"]',
        "pok": "1"
    }
    
    try:
        response = requests.post(
            "http://stat.gibdd.ru/map/getMainMapData",
            json=rf_payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code != 200:
            logger.error(f"Ошибка получения регионов: {response.status_code}")
            return None
        
        result = response.json()
        metabase = json.loads(result["metabase"])
        maps_data = json.loads(metabase[0]["maps"])
        
        logger.info(f"Найдено {len(maps_data)} регионов")
        
        gibdd_data = {
            "regions": {},
            "districts": {}
        }
        
        for i, region in enumerate(maps_data, 1):
            region_id = region["id"]
            region_name = region["name"]
            
            logger.info(f"[{i}/{len(maps_data)}] {region_name} (ID: {region_id})")
            
            region_payload = {
                "maptype": 1,
                "region": region_id,
                "date": f'["MONTHS:{month}.{year}"]',
                "pok": "1"
            }
            
            try:
                reg_response = requests.post(
                    "http://stat.gibdd.ru/map/getMainMapData",
                    json=region_payload,
                    headers=headers,
                    timeout=30
                )
                
                if reg_response.status_code == 200:
                    reg_result = reg_response.json()
                    reg_metabase = json.loads(reg_result["metabase"])
                    reg_maps_data = json.loads(reg_metabase[0]["maps"])
                    
                    districts = []
                    for district in reg_maps_data:
                        district_info = {
                            "id": district["id"],
                            "name": district["name"]
                        }
                        districts.append(district_info)
                        
                        gibdd_data["districts"][district["id"]] = {
                            "name": district["name"],
                            "region_id": region_id,
                            "region_name": region_name
                        }
                    
                    gibdd_data["regions"][region_id] = {
                        "name": region_name,
                        "districts": districts
                    }
                    
                    logger.info(f"{len(districts)} муниципалитетов")
                    
            except Exception as e:
                logger.error(f"Ошибка: {e}")
            
            time.sleep(0.3)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = DATA_RAW / f"gibdd_full_{timestamp}.json"
        with open(raw_file, "w", encoding="utf-8") as f:
            json.dump(gibdd_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Данные сохранены в {raw_file}")
        
        # Также сохраняем как текущую версию
        current_file = DATA_PROCESSED / "gibdd_current.json"
        with open(current_file, "w", encoding="utf-8") as f:
            json.dump(gibdd_data, f, ensure_ascii=False, indent=2)
        
        return gibdd_data
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        return None

def find_region_id(region_name):
    """
    Находит ID региона по названию с учетом маппинга
    """
    if not region_name:
        return None
    
    # Прямое совпадение с маппингом
    if region_name in REGION_NAME_MAPPING:
        gibdd_name = REGION_NAME_MAPPING[region_name]
        if gibdd_name in GIBDD_REGIONS:
            logger.debug(f"Маппинг: {region_name} -> {gibdd_name} (ID: {GIBDD_REGIONS[gibdd_name]})")
            return GIBDD_REGIONS[gibdd_name], gibdd_name
    
    # Поиск по частичному совпадению
    region_lower = region_name.lower()
    for gibdd_name, region_id in GIBDD_REGIONS.items():
        if region_lower in gibdd_name.lower() or gibdd_name.lower() in region_lower:
            logger.debug(f"Частичное совпадение: {region_name} -> {gibdd_name} (ID: {region_id})")
            return region_id, gibdd_name
    
    return None, None

def find_district_id(city_name, region_id, gibdd_data):
    """
    Ищет ID района по названию города в конкретном регионе
    """
    if not city_name or not region_id or region_id not in gibdd_data["regions"]:
        return None
    
    city_norm = normalize_city_name(city_name).lower()
    
    region = gibdd_data["regions"][region_id]
    best_match = None
    best_score = 0
    
    for district in region["districts"]:
        district_name = district["name"]
        district_norm = normalize_city_name(district_name).lower()
        
        # Точное совпадение
        if city_norm == district_norm:
            logger.debug(f"Точное совпадение: {city_name} == {district_name}")
            return district["id"]
        
        # Город входит в название района (Алагир -> Алагирский район)
        if city_norm in district_norm:
            score = len(city_norm) / len(district_norm)
            if score > best_score:
                best_score = score
                best_match = district["id"]
                logger.debug(f"Частичное совпадение: {city_name} in {district_name} (score: {score:.2f})")
        
        # Район входит в название города (редко)
        elif district_norm in city_norm:
            score = len(district_norm) / len(city_norm)
            if score > best_score:
                best_score = score
                best_match = district["id"]
                logger.debug(f"Частичное совпадение: {district_name} in {city_name} (score: {score:.2f})")
    
    return best_match

def get_all_cities(max_retries=3):
    """Получает все города из БД с повторными попытками при таймауте"""
    all_cities = []
    offset = 0
    limit = 500  
    
    for attempt in range(max_retries):
        try:
            while True:
                response = requests.get(
                    f"{db.url}/rest/v1/cities",
                    headers=db.headers,
                    params={
                        "select": "id,city_name,region",
                        "limit": limit,
                        "offset": offset,
                        "order": "city_name.asc"
                    },
                    timeout=60  
                )
                
                if response.status_code == 200:
                    cities = response.json()
                    if not cities:
                        break
                    
                    all_cities.extend(cities)
                    offset += limit
                    logger.info(f"Загружено {len(all_cities)} городов...")
                    
                    if len(cities) < limit:
                        break
                else:
                    logger.error(f"Ошибка HTTP: {response.status_code}")
                    break
                    
            if all_cities:
                logger.info(f"Всего загружено {len(all_cities)} городов")
                return all_cities
                
        except requests.exceptions.Timeout:
            logger.warning(f"Попытка {attempt + 1}: таймаут при подключении к Supabase")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Попытка {attempt + 1}: ошибка подключения к Supabase")
        except Exception as e:
            logger.warning(f"Попытка {attempt + 1}: ошибка {e}")
        
        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 10
            logger.info(f"Ждем {wait_time} секунд перед следующей попыткой...")
            time.sleep(wait_time)
            offset = 0  # Сбрасываем offset для новой попытки
            all_cities = []  # Очищаем список
    
    logger.error(f"Не удалось получить города после {max_retries} попыток")
    return None

def update_city_in_db(city_id, region_id, district_id):
    """Обновляет ID ГИБДД для города"""
    update_data = {}
    if region_id:
        update_data["gibdd_region_id"] = region_id
    if district_id:
        update_data["gibdd_district_id"] = district_id
    
    if not update_data:
        return False
    
    try:
        response = requests.patch(
            f"{db.url}/rest/v1/cities?id=eq.{city_id}",
            headers=db.headers,
            json=update_data,
            timeout=30
        )
        return response.status_code in [200, 204]
    except Exception as e:
        logger.error(f"Ошибка обновления: {e}")
        return False

def save_results_to_csv(results, filename):
    """Сохраняет результаты в CSV"""
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Город", "Регион", "ID региона", "ID района", "Статус"])
        for row in results:
            writer.writerow(row)

def main():
    logger.info("Обновление ID ГИБДД для всех городов")
    
    # 1. Скачиваем свежие данные с ГИБДД
    logger.info("\nШаг 1: Скачивание данных с ГИБДД")
    gibdd_data = download_fresh_gibdd_data()
    if not gibdd_data:
        logger.error("Не удалось получить данные с ГИБДД")
        return
    
    # 2. Получаем все города из БД
    logger.info("\nШаг 2: Загрузка городов из БД")
    cities = get_all_cities()
    if not cities:
        logger.error("Не удалось получить города из БД после нескольких попыток")
        return
    
    # 3. Обрабатываем каждый город
    logger.info(f"\nШаг 3: Обработка {len(cities)} городов")
    
    results = []
    stats = {
        "region_found": 0,
        "district_found": 0,
        "region_not_found": 0,
        "updated": 0,
        "errors": 0
    }
    
    for i, city in enumerate(cities, 1):
        if i % 100 == 0:
            logger.info(f"Обработано {i}/{len(cities)} городов")
        
        city_id = city['id']
        city_name = city['city_name']
        region_name = city['region']
        
        # Находим регион
        region_id, gibdd_region_name = find_region_id(region_name)
        
        if region_id:
            stats["region_found"] += 1
            
            # Ищем район
            district_id = find_district_id(city_name, region_id, gibdd_data)
            
            if district_id:
                stats["district_found"] += 1
                status = f"Район: {district_id}"
            else:
                status = f"Только регион: {region_id}"
            
            # Обновляем в БД
            if update_city_in_db(city_id, region_id, district_id):
                stats["updated"] += 1
            else:
                stats["errors"] += 1
                status += " (Ошибка обновления)"
            
        else:
            stats["region_not_found"] += 1
            status = "Регион не найден"
        
        # Сохраняем результат
        results.append([
            city_name,
            region_name,
            region_id or "",
            district_id or "",
            status
        ])
        
        # Небольшая задержка
        if i % 50 == 0:
            time.sleep(1)
    
    # 4. Сохраняем результаты
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = DATA_PROCESSED / f"gibdd_update_results_{timestamp}.csv"
    save_results_to_csv(results, results_file)
    
    # 5. Итоговая статистика
    logger.info(f"Всего городов: {len(cities)}")
    logger.info(f"Найдено регионов: {stats['region_found']}")
    logger.info(f"Найдено районов: {stats['district_found']}")
    logger.info(f"Регионов не найдено: {stats['region_not_found']}")
    logger.info(f"Обновлено в БД: {stats['updated']}")
    logger.info(f"Ошибок обновления: {stats['errors']}")
    logger.info(f"\nРезультаты сохранены в: {results_file}")

if __name__ == "__main__":
    main()