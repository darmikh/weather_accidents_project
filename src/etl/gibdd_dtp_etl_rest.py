"""
Скрипт для автоматической локальной загрузки данных о ДТП (REST API версия)
Загружает все поля во все таблицы: dtp_main, dtp_road_conditions, dtp_vehicles, dtp_participants
Использует REST API Supabase для стабильного подключения
"""

import sys
from pathlib import Path
import requests
import time
from datetime import datetime
import json

src_path = Path(__file__).parent.parent
sys.path.append(str(src_path))

from database import db
from etl.logger_config import get_logger
from config import config

logger = get_logger('gibdd_dtp_rest')

API_URL = "http://stat.gibdd.ru/map/getDTPCardData"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
}

MAX_RETRIES = 3
PAGE_SIZE = 1000


def get_active_cities():
    """Получить активные города через REST API"""
    try:
        response = requests.get(
            f"{db.url}/rest/v1/cities",
            headers=db.headers,
            params={
                "is_active": "eq.true",
                "select": "id,city_name,gibdd_region_id,gibdd_district_id"
            },
            timeout=60
        )
        if response.status_code == 200:
            cities = response.json()
            logger.info(f"Найдено активных городов: {len(cities)}")
            return cities
        else:
            logger.error(f"Ошибка получения городов: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Ошибка при получении активных городов: {e}")
        return []


def delete_old_load_log(city_id, year, month):
    """Удалить старые записи из лога загрузки (чтобы перезагрузить месяц)"""
    try:
        response = requests.delete(
            f"{db.url}/rest/v1/dtp_load_log",
            headers=db.headers,
            params={
                "city_id": f"eq.{city_id}",
                "year": f"eq.{year}",
                "month": f"eq.{month}"
            },
            timeout=60
        )
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Ошибка удаления лога: {e}")
        return False


def save_load_log(city_id, region_id, district_id, year, month, records_loaded, status, error_message=None):
    """Сохранить запись в лог загрузки через REST API"""
    try:
        # Сначала удаляем старую запись, если есть
        delete_old_load_log(city_id, year, month)
        
        # Затем вставляем новую
        data = {
            "city_id": city_id,
            "gibdd_region_id": region_id,
            "gibdd_district_id": district_id,
            "year": year,
            "month": month,
            "records_loaded": records_loaded,
            "load_status": status,
            "error_message": error_message,
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat()
        }
        response = requests.post(
            f"{db.url}/rest/v1/dtp_load_log",
            headers=db.headers,
            json=data,
            timeout=60
        )
        if response.status_code != 201:
            logger.error(f"Ошибка сохранения лога: {response.status_code}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении лога: {e}")


def fetch_page(region_id, district_id, year, month, start):
    """Загрузить одну страницу из API ГИБДД"""
    date_str = f"MONTHS:{month}.{year}"
    payload = {
        "data": {
            "date": [date_str],
            "ParReg": region_id,
            "order": {"type": "1", "fieldName": "dat"},
            "reg": district_id,
            "ind": "1",
            "st": str(start),
            "en": str(start + PAGE_SIZE - 1),
            "fil": {"isSummary": False},
            "fieldNames": [
                "dat", "time", "coordinates", "infoDtp", "k_ul", "dor", "ndu",
                "k_ts", "ts_info", "pdop", "pog", "osv", "s_pch", "s_pog",
                "n_p", "n_pg", "obst", "sdor", "t_osv", "t_p", "t_s", "v_p", "v_v"
            ]
        }
    }
    request_data = {"data": json.dumps(payload["data"], separators=(',', ':'))}
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(API_URL, json=request_data, headers=HEADERS, timeout=60)
            if response.status_code != 200:
                if attempt < MAX_RETRIES - 1:
                    time.sleep((attempt + 1) * 5)
                    continue
                return []
            
            data = response.json()
            if "data" not in data:
                return []
            
            return json.loads(data["data"]).get("tab", [])
        except Exception as e:
            logger.warning(f"Ошибка при запросе страницы (попытка {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep((attempt + 1) * 5)
            else:
                return []
    return []


def fetch_all_pages(region_id, district_id, year, month):
    """Загрузить все страницы за месяц"""
    all_cards = []
    start = 1
    
    while True:
        cards = fetch_page(region_id, district_id, year, month, start)
        if not cards:
            break
        all_cards.extend(cards)
        if len(cards) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(0.5)
    
    return all_cards


def parse_card(card, city_id):
    """Парсинг одной карточки ДТП"""
    if not card:
        return None, None, [], []
    
    info = card.get('infoDtp', {})
    kart_id = card.get('KartId')
    
    date_str = card.get('date')
    if date_str:
        try:
            date_obj = datetime.strptime(date_str, '%d.%m.%Y')
            formatted_date = date_obj.date().isoformat()
        except:
            formatted_date = None
    else:
        formatted_date = None
    
    # Основная информация (raw_data сразу сериализуем в JSON)
    main = {
        'kart_id': kart_id,
        'city_id': str(city_id),
        'row_num': card.get('rowNum'),
        'date': formatted_date,
        'time': card.get('Time'),
        'district': card.get('District'),
        'dtp_type': card.get('DTP_V'),
        'fatalities': card.get('POG', 0),
        'injured': card.get('RAN', 0),
        'vehicles_count': card.get('K_TS', 0),
        'participants_count': card.get('K_UCH', 0),
        'emtp_number': card.get('emtp_number'),
        'raw_data': json.dumps(card, ensure_ascii=False)  # ← JSON строка
    }
    
    # Дорожные условия
    road = {
        'kart_id': kart_id,
        'settlement': info.get('n_p'),
        'street': info.get('street'),
        'house': info.get('house'),
        'road': info.get('dor'),
        'kilometer': int(info['km']) if info.get('km') and str(info['km']).isdigit() else None,
        'meter': int(info['m']) if info.get('m') and str(info['m']).isdigit() else None,
        'road_category': info.get('k_ul'),
        'road_code': info.get('dor_k'),
        'road_value': info.get('dor_z'),
        'road_surface': info.get('s_pch'),
        'light_conditions': info.get('osv'),
        'traffic_change': info.get('change_org_motion'),
        'accident_code': info.get('s_dtp'),
        'road_deficiencies': info.get('ndu', []),
        'traffic_scheme': info.get('sdor', []),
        'factors': info.get('factor', []),
        'weather_conditions': info.get('s_pog', []),
        'traffic_objects': info.get('OBJ_DTP', [])
    }
    
    if info.get('COORD_W'):
        try:
            road['latitude'] = float(info['COORD_W'])
        except:
            pass
    if info.get('COORD_L'):
        try:
            road['longitude'] = float(info['COORD_L'])
        except:
            pass
    
    vehicles = []
    participants = []
    
    # Транспортные средства и участники внутри них
    for ts in info.get('ts_info', []):
        vehicle = {
            'kart_id': kart_id,
            'vehicle_number_in_accident': ts.get('n_ts'),
            'vehicle_status': ts.get('ts_s'),
            'vehicle_type': ts.get('t_ts'),
            'make': ts.get('marka_ts'),
            'model': ts.get('m_ts'),
            'color': ts.get('color'),
            'steering': ts.get('r_rul'),
            'year': int(ts['g_v']) if ts.get('g_v') and str(ts['g_v']).isdigit() else None,
            'engine_capacity': ts.get('m_pov'),
            'technical_condition': ts.get('t_n'),
            'ownership_form': ts.get('f_sob'),
            'owner_type': ts.get('o_pf')
        }
        vehicles.append(vehicle)
        
        for uch in ts.get('ts_uch', []):
            participant = {
                'kart_id': kart_id,
                'participant_number': uch.get('N_UCH'),
                'role': uch.get('K_UCH'),
                'injury_severity': uch.get('S_T'),
                'gender': uch.get('POL'),
                'driving_experience': int(uch['V_ST']) if uch.get('V_ST') and str(uch['V_ST']).isdigit() else None,
                'alcohol': uch.get('ALCO'),
                'seatbelt_used': uch.get('SAFETY_BELT'),
                'hid_from_scene': uch.get('S_SM'),
                'violations': uch.get('NPDD', []),
                'additional_violations': uch.get('SOP_NPDD', []),
                'seat_group': uch.get('S_SEAT_GROUP'),
                'injured_card_id': uch.get('INJURED_CARD_ID'),
                'is_from_uch_info': False
            }
            participants.append(participant)
    
    # Дополнительные участники
    for uch in info.get('uchInfo', []):
        participant = {
            'kart_id': kart_id,
            'participant_number': uch.get('N_UCH'),
            'role': uch.get('K_UCH'),
            'injury_severity': uch.get('S_T'),
            'gender': uch.get('POL'),
            'driving_experience': int(uch['V_ST']) if uch.get('V_ST') and str(uch['V_ST']).isdigit() else None,
            'alcohol': uch.get('ALCO'),
            'seatbelt_used': None,
            'hid_from_scene': uch.get('S_SM'),
            'violations': uch.get('NPDD', []),
            'additional_violations': uch.get('SOP_NPDD', []),
            'seat_group': None,
            'injured_card_id': None,
            'is_from_uch_info': True
        }
        participants.append(participant)
    
    return main, road, vehicles, participants


def save_batch(table, data):
    if not data:
        return 0
    
    saved = 0
    for record in data:
        try:
            # Для dtp_main и dtp_road_conditions используем UPSERT через PATCH
            if table in ['dtp_main', 'dtp_road_conditions']:
                # Сначала пытаемся обновить существующую запись
                kart_id = record.get('kart_id')
                if kart_id:
                    # Проверяем, есть ли запись
                    check = requests.get(
                        f"{db.url}/rest/v1/{table}",
                        headers=db.headers,
                        params={"kart_id": f"eq.{kart_id}", "select": "kart_id"},
                        timeout=30
                    )
                    if check.status_code == 200 and check.json():
                        # Запись есть → обновляем
                        response = requests.patch(
                            f"{db.url}/rest/v1/{table}",
                            headers=db.headers,
                            params={"kart_id": f"eq.{kart_id}"},
                            json=record,
                            timeout=30
                        )
                    else:
                        # Записи нет → вставляем
                        response = requests.post(
                            f"{db.url}/rest/v1/{table}",
                            headers=db.headers,
                            json=record,
                            timeout=30
                        )
                else:
                    response = requests.post(
                        f"{db.url}/rest/v1/{table}",
                        headers=db.headers,
                        json=record,
                        timeout=30
                    )
            else:
                # Для dtp_vehicles и dtp_participants просто вставляем
                response = requests.post(
                    f"{db.url}/rest/v1/{table}",
                    headers=db.headers,
                    json=record,
                    timeout=30
                )
            
            if response.status_code in [200, 201, 204]:
                saved += 1
            else:
                logger.debug(f"Статус {response.status_code} для {table}: {response.text[:100]}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении в {table}: {e}")
    
    return saved


def save_month(cards, city_id, region_id, district_id, year, month):
    """Сохранить данные за месяц через REST API"""
    today = datetime.now()
    
    # Определяем, закончился ли месяц
    month_ended = False
    if year < today.year:
        month_ended = True
    elif year == today.year and month < today.month:
        month_ended = True
    
    if not cards:
        if month_ended:
            logger.info(f"Месяц {year}-{month:02d} закончился, ДТП нет")
            save_load_log(city_id, region_id, district_id, year, month, 0, 'success')
        else:
            logger.info(f"Месяц {year}-{month:02d} ещё не закончился, данных пока нет")
        return 0
    
    main_data, road_data, vehicles_data, participants_data = [], [], [], []
    for card in cards:
        main, road, vehicles, participants = parse_card(card, city_id)
        if main:
            main_data.append(main)
            road_data.append(road)
            vehicles_data.extend(vehicles)
            participants_data.extend(participants)
    
    saved_main = save_batch('dtp_main', main_data)
    saved_road = save_batch('dtp_road_conditions', road_data)
    saved_vehicles = save_batch('dtp_vehicles', vehicles_data)
    saved_participants = save_batch('dtp_participants', participants_data)
    
    save_load_log(city_id, region_id, district_id, year, month, len(main_data), 'success')
    
    logger.info(f"Загружено {len(main_data)} ДТП, {len(vehicles_data)} ТС, {len(participants_data)} участников за {year}-{month:02d}")
    return len(main_data)


def update_all():
    """Главная функция обновления данных"""
    logger.info("Запуск обновления ДТП (REST API версия)")
    
    # Проверяем подключение
    if not db.test_connection():
        logger.error("Не удалось подключиться к Supabase")
        return
    
    cities = get_active_cities()
    if not cities:
        logger.error("Нет активных городов")
        return
    
    today = datetime.now()
    current_year = today.year
    current_month = today.month
    
    # Вычисляем начало периода: последние 6 месяцев от текущей даты
    start_year = current_year
    start_month = current_month - config.MONTHS_TO_REFRESH + 1
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    
    logger.info(f"Текущая дата: {today.strftime('%Y-%m')}")
    logger.info(f"Загрузка за период: {start_year}-{start_month:02d} .. {current_year}-{current_month:02d}")
    
    for city in cities:
        city_id = city['id']
        city_name = city['city_name']
        region_id = city['gibdd_region_id']
        district_id = city['gibdd_district_id']
        
        logger.info(f"Обработка {city_name}")
        
        year, month = start_year, start_month
        while year < current_year or (year == current_year and month <= current_month):
            logger.info(f"Загрузка {year}-{month:02d}")
            cards = fetch_all_pages(region_id, district_id, year, month)
            if cards:
                save_month(cards, city_id, region_id, district_id, year, month)
            else:
                logger.info(f"  данных нет")
            
            # Переход к следующему месяцу
            month += 1
            if month > 12:
                month = 1
                year += 1
            time.sleep(1)
    
    logger.info("Загрузка завершена")


if __name__ == "__main__":
    update_all()