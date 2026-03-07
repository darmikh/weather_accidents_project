"""
Скрипт для автоматической загрузки данных о ДТП (полная версия)
Загружает все поля во все таблицы
"""
import sys
from pathlib import Path
import requests
import time
from datetime import datetime
import json
import os

src_path = Path(__file__).parent.parent
sys.path.append(str(src_path))

from database import db
from etl.logger_config import get_logger

logger = get_logger('gibdd_dtp_full')

API_URL = "http://stat.gibdd.ru/map/getDTPCardData"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
}

MAX_RETRIES = 3
PAGE_SIZE = 1000
BATCH_SIZE = 50

def get_active_cities():
    response = requests.get(
        f"{db.url}/rest/v1/cities",
        headers=db.headers,
        params={"is_active": "eq.true", "select": "id,city_name,gibdd_region_id,gibdd_district_id"}
    )
    return response.json() if response.status_code == 200 else []

def get_last_loaded_month(city_id):
    response = requests.get(
        f"{db.url}/rest/v1/dtp_load_log",
        headers=db.headers,
        params={
            "city_id": f"eq.{city_id}",
            "load_status": "eq.success",
            "order": "year.desc,month.desc",
            "limit": 1,
            "select": "year,month"
        }
    )
    if response.status_code == 200 and response.json():
        last = response.json()[0]
        return last['year'], last['month']
    return None, None

def month_loaded(city_id, year, month):
    response = requests.get(
        f"{db.url}/rest/v1/dtp_load_log",
        headers=db.headers,
        params={
            "city_id": f"eq.{city_id}",
            "year": f"eq.{year}",
            "month": f"eq.{month}",
            "load_status": "eq.success",
            "select": "id"
        }
    )
    return response.status_code == 200 and bool(response.json())

def add_to_retry_queue(city_id, year, month, error):
    try:
        check = requests.get(
            f"{db.url}/rest/v1/dtp_retry_queue",
            headers=db.headers,
            params={
                "city_id": f"eq.{city_id}",
                "year": f"eq.{year}",
                "month": f"eq.{month}",
                "status": "eq.pending"
            }
        )
        
        if check.status_code == 200 and not check.json():
            queue_entry = {
                'city_id': str(city_id),
                'year': year,
                'month': month,
                'last_error': error[:200] if error else 'Connection error',
                'next_retry_time': datetime.now().isoformat()
            }
            requests.post(f"{db.url}/rest/v1/dtp_retry_queue", headers=db.headers, json=queue_entry)
    except:
        with open('failed_months.csv', 'a', encoding='utf-8') as f:
            f.write(f"{city_id},{year},{month},{error}\n")

def fetch_page(region_id, district_id, year, month, start):
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
    response = requests.post(API_URL, json=request_data, headers=HEADERS, timeout=30)
    
    if response.status_code != 200:
        return []
    
    data = response.json()
    if "data" not in data:
        return []
    
    return json.loads(data["data"]).get("tab", [])

def fetch_with_retry(region_id, district_id, year, month):
    for attempt in range(MAX_RETRIES):
        try:
            cards = fetch_page(region_id, district_id, year, month, 1)
            return cards  
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep((attempt + 1) * 5)
            else:
                return None
    return None

def fetch_all_pages(region_id, district_id, year, month):
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
    """ПОЛНЫЙ парсинг карточки со всеми полями"""
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

    # 1. Основная информация
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
        'raw_data': card
    }
    
    # 2. Дорожные условия 
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
        except: pass
    if info.get('COORD_L'):
        try:
            road['longitude'] = float(info['COORD_L'])
        except: pass
    
    vehicles = []
    participants = []
    
    # 3. Транспортные средства 
    for ts in info.get('ts_info', []):
        vehicles.append({
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
        })
        
        # Участники внутри ТС 
        for uch in ts.get('ts_uch', []):
            participants.append({
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
            })
    
    # 4. Дополнительные участники 
    for uch in info.get('uchInfo', []):
        participants.append({
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
        })
    
    return main, road, vehicles, participants

def save_batch(table, data):
    if not data:
        return
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        for attempt in range(3):
            try:
                requests.post(
                    f"{db.url}/rest/v1/{table}",
                    headers={**db.headers, 'Prefer': 'resolution=merge-duplicates'},
                    json=batch,
                    timeout=30
                )
                break
            except:
                if attempt < 2:
                    time.sleep(3)

def save_month(cards, city_id, region_id, district_id, year, month):
    if not cards:
        log_entry = {
            'city_id': str(city_id), 'gibdd_region_id': region_id, 'gibdd_district_id': district_id,
            'year': year, 'month': month, 'records_loaded': 0, 'load_status': 'success'
        }
        requests.post(f"{db.url}/rest/v1/dtp_load_log", headers=db.headers, json=log_entry)
        return 0
    
    main_data, road_data, vehicles_data, participants_data = [], [], [], []
    for card in cards:
        main, road, vehicles, participants = parse_card(card, city_id)
        if main:
            main_data.append(main)
            road_data.append(road)
            vehicles_data.extend(vehicles)
            participants_data.extend(participants)
    
    save_batch('dtp_main', main_data)
    save_batch('dtp_road_conditions', road_data)
    save_batch('dtp_vehicles', vehicles_data)
    save_batch('dtp_participants', participants_data)
    
    log_entry = {
        'city_id': str(city_id), 'gibdd_region_id': region_id, 'gibdd_district_id': district_id,
        'year': year, 'month': month, 'records_loaded': len(main_data), 'load_status': 'success'
    }
    requests.post(f"{db.url}/rest/v1/dtp_load_log", headers=db.headers, json=log_entry)
    
    logger.info(f"Загружено {len(main_data)} ДТП за {year}-{month:02d}")
    return len(main_data)

def process_retry_queue():
    response = requests.get(
        f"{db.url}/rest/v1/dtp_retry_queue",
        headers=db.headers,
        params={"status": "eq.pending", "limit": 20}
    )
    if response.status_code != 200:
        return
    
    for item in response.json():
        city_resp = requests.get(
            f"{db.url}/rest/v1/cities",
            headers=db.headers,
            params={"id": f"eq.{item['city_id']}", "select": "city_name,gibdd_region_id,gibdd_district_id"}
        )
        if city_resp.status_code != 200 or not city_resp.json():
            continue
        
        city = city_resp.json()[0]
        cards = fetch_with_retry(city['gibdd_region_id'], city['gibdd_district_id'], item['year'], item['month'])
        
        if cards is not None:
            if cards:
                cards = fetch_all_pages(city['gibdd_region_id'], city['gibdd_district_id'], item['year'], item['month'])
                save_month(cards, item['city_id'], city['gibdd_region_id'], city['gibdd_district_id'], 
                          item['year'], item['month'])
            requests.delete(f"{db.url}/rest/v1/dtp_retry_queue?id=eq.{item['id']}", headers=db.headers)
        time.sleep(2)

def update_all():
    logger.info("Запуск обновления ДТП")
    
    process_retry_queue()
    
    cities = get_active_cities()
    if not cities:
        logger.error("Нет городов")
        return
    
    current_year, current_month = datetime.now().year, datetime.now().month
    
    for city in cities:
        city_id, city_name = city['id'], city['city_name']
        region_id, district_id = city['gibdd_region_id'], city['gibdd_district_id']
        
        logger.info(f"Обработка {city_name}")
        
        last_year, last_month = get_last_loaded_month(city_id)
        
        if not last_year:
            start_year, start_month = 2015, 1
        else:
            start_year, start_month = last_year, last_month
            start_month += 1
            if start_month > 12:
                start_month, start_year = 1, start_year + 1
        
        year, month = start_year, start_month
        while year < current_year or (year == current_year and month <= current_month):
            if month_loaded(city_id, year, month):
                logger.info(f"Месяц {year}-{month:02d} уже загружен")
            else:
                logger.info(f"Загрузка {year}-{month:02d}")
                cards = fetch_with_retry(region_id, district_id, year, month)
                if cards is None:
                    add_to_retry_queue(city_id, year, month, "Connection error")
                elif cards:
                    all_cards = fetch_all_pages(region_id, district_id, year, month)
                    save_month(all_cards, city_id, region_id, district_id, year, month)
                else:
                    save_month([], city_id, region_id, district_id, year, month)
            
            month += 1
            if month > 12:
                month, year = 1, year + 1
            time.sleep(1)
    
    logger.info("Финальная обработка очереди...")
    for _ in range(3):
        process_retry_queue()
        time.sleep(5)
    
    logger.info("Загрузка завершена")

if __name__ == "__main__":
    update_all()