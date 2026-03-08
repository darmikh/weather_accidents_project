"""
Скрипт для автоматической загрузки данных о ДТП (полная версия)
Загружает все поля во все таблицы: dtp_main, dtp_road_conditions, dtp_vehicles, dtp_participants
"""
import sys
from pathlib import Path
import requests
import time
from datetime import datetime
import json
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

src_path = Path(__file__).parent.parent
sys.path.append(str(src_path))

from database import db
from etl.logger_config import get_logger

logger = get_logger('gibdd_dtp_full')

load_dotenv()

API_URL = "http://stat.gibdd.ru/map/getDTPCardData"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
}

MAX_RETRIES = 3
PAGE_SIZE = 1000
BATCH_SIZE = 50

# Количество последних месяцев для полного обновления 
MONTHS_TO_REFRESH = 6

def get_db_connection():
    """Создает подключение к базе данных через SQLAlchemy"""
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
    """Получить активные города через прямой SQL-запрос"""
    engine = get_db_connection()
    if not engine:
        return []
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT id, city_name, gibdd_region_id, gibdd_district_id 
                    FROM cities 
                    WHERE is_active = true
                """)
            )
            cities = []
            for row in result:
                cities.append({
                    'id': str(row[0]),
                    'city_name': row[1],
                    'gibdd_region_id': row[2],
                    'gibdd_district_id': row[3]
                })
            return cities
    except Exception as e:
        logger.error(f"Ошибка при получении активных городов: {e}")
        return []

def get_last_loaded_month(city_id, engine):
    """Получить последний загруженный месяц для города"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT year, month FROM dtp_load_log 
                    WHERE city_id = :city_id 
                    AND load_status = 'success'
                    ORDER BY year DESC, month DESC
                    LIMIT 1
                """),
                {"city_id": city_id}
            )
            row = result.first()
            if row:
                return row[0], row[1]
            return None, None
    except Exception as e:
        logger.error(f"Ошибка при получении последнего месяца: {e}")
        return None, None

def month_loaded(city_id, year, month, engine):
    """Проверить, загружен ли уже месяц"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT id FROM dtp_load_log 
                    WHERE city_id = :city_id 
                    AND year = :year 
                    AND month = :month 
                    AND load_status = 'success'
                """),
                {"city_id": city_id, "year": year, "month": month}
            )
            return result.first() is not None
    except Exception as e:
        logger.error(f"Ошибка при проверке загруженного месяца: {e}")
        return False

def add_to_retry_queue(city_id, year, month, error, engine):
    """Добавить месяц в очередь повторной обработки"""
    try:
        with engine.connect() as conn:
            check = conn.execute(
                text("""
                    SELECT id FROM dtp_retry_queue 
                    WHERE city_id = :city_id 
                    AND year = :year 
                    AND month = :month 
                    AND status = 'pending'
                """),
                {"city_id": city_id, "year": year, "month": month}
            )
            
            if not check.first():
                conn.execute(
                    text("""
                        INSERT INTO dtp_retry_queue 
                        (city_id, year, month, last_error, next_retry_time, status)
                        VALUES 
                        (:city_id, :year, :month, :last_error, NOW(), 'pending')
                    """),
                    {
                        "city_id": city_id,
                        "year": year,
                        "month": month,
                        "last_error": error[:200] if error else 'Connection error'
                    }
                )
                conn.commit()
    except Exception as e:
        logger.error(f"Ошибка при добавлении в очередь: {e}")
        with open('failed_months.csv', 'a', encoding='utf-8') as f:
            f.write(f"{city_id},{year},{month},{error}\n")

def save_load_log(city_id, region_id, district_id, year, month, records_loaded, status, engine, error_message=None):
    """Сохранить запись в лог загрузки"""
    try:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO dtp_load_log 
                    (city_id, gibdd_region_id, gibdd_district_id, year, month, records_loaded, load_status, error_message, started_at, completed_at)
                    VALUES 
                    (:city_id, :region_id, :district_id, :year, :month, :records_loaded, :status, :error_message, NOW(), NOW())
                """),
                {
                    "city_id": city_id,
                    "region_id": region_id,
                    "district_id": district_id,
                    "year": year,
                    "month": month,
                    "records_loaded": records_loaded,
                    "status": status,
                    "error_message": error_message
                }
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Ошибка при сохранении лога: {e}")

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
        
        # Участники внутри ТС 
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
    
    # 4. Дополнительные участники 
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

def save_batch(table, data, engine):
    """Сохраняет пачку данных через прямой SQL"""
    if not data:
        return
    
    table_map = {
        'dtp_main': """
            INSERT INTO dtp_main 
            (kart_id, city_id, row_num, date, time, district, dtp_type, fatalities, injured, 
             vehicles_count, participants_count, emtp_number, raw_data)
            VALUES 
            (:kart_id, :city_id, :row_num, :date, :time, :district, :dtp_type, :fatalities, :injured,
             :vehicles_count, :participants_count, :emtp_number, :raw_data)
            ON CONFLICT (kart_id) DO UPDATE SET
                fatalities = EXCLUDED.fatalities,
                injured = EXCLUDED.injured,
                raw_data = EXCLUDED.raw_data,
                updated_at = NOW()
        """,
        'dtp_road_conditions': """
            INSERT INTO dtp_road_conditions 
            (kart_id, settlement, street, house, road, kilometer, meter, road_category,
             road_code, road_value, road_surface, light_conditions, traffic_change, accident_code,
             latitude, longitude, road_deficiencies, traffic_scheme, factors, weather_conditions, traffic_objects)
            VALUES 
            (:kart_id, :settlement, :street, :house, :road, :kilometer, :meter, :road_category,
             :road_code, :road_value, :road_surface, :light_conditions, :traffic_change, :accident_code,
             :latitude, :longitude, :road_deficiencies, :traffic_scheme, :factors, :weather_conditions, :traffic_objects)
            ON CONFLICT (kart_id) DO UPDATE SET
                road_surface = EXCLUDED.road_surface,
                light_conditions = EXCLUDED.light_conditions,
                road_deficiencies = EXCLUDED.road_deficiencies,
                weather_conditions = EXCLUDED.weather_conditions,
                updated_at = NOW()
        """,
        'dtp_vehicles': """
            INSERT INTO dtp_vehicles 
            (kart_id, vehicle_number_in_accident, vehicle_status, vehicle_type, make, model, color,
             steering, year, engine_capacity, technical_condition, ownership_form, owner_type)
            VALUES 
            (:kart_id, :vehicle_number_in_accident, :vehicle_status, :vehicle_type, :make, :model, :color,
             :steering, :year, :engine_capacity, :technical_condition, :ownership_form, :owner_type)
            ON CONFLICT (id) DO NOTHING
        """,
        'dtp_participants': """
            INSERT INTO dtp_participants 
            (kart_id, participant_number, role, injury_severity, gender, driving_experience, alcohol,
             seatbelt_used, hid_from_scene, violations, additional_violations, seat_group, 
             injured_card_id, is_from_uch_info)
            VALUES 
            (:kart_id, :participant_number, :role, :injury_severity, :gender, :driving_experience, :alcohol,
             :seatbelt_used, :hid_from_scene, :violations, :additional_violations, :seat_group,
             :injured_card_id, :is_from_uch_info)
            ON CONFLICT (id) DO NOTHING
        """
    }
    
    if table not in table_map:
        logger.error(f"Неизвестная таблица: {table}")
        return
    
    try:
        with engine.connect() as conn:
            for record in data:
                conn.execute(text(table_map[table]), record)
            conn.commit()
            logger.debug(f"Сохранено {len(data)} записей в {table}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении в {table}: {e}")

def save_month(cards, city_id, region_id, district_id, year, month, engine):
    """Сохранить данные за месяц"""
    
    today = datetime.now()
    
    # Определяем, закончился ли этот месяц
    month_ended = False
    if year < today.year:
        month_ended = True
    elif year == today.year and month < today.month:
        month_ended = True
    
    if not cards:
        # Если месяц еще не закончился и нет данных - не пишем success
        if month_ended:
            logger.info(f"Месяц {year}-{month:02d} закончился, данных нет")
            save_load_log(city_id, region_id, district_id, year, month, 0, 'success', engine)
        else:
            logger.info(f"Месяц {year}-{month:02d} еще не закончился, данных пока нет - пропускаем логирование")
            # Не пишем в лог, чтобы позже попробовать снова
        return 0
    
    # Если данные есть - загружаем как обычно
    main_data, road_data, vehicles_data, participants_data = [], [], [], []
    for card in cards:
        main, road, vehicles, participants = parse_card(card, city_id)
        if main:
            main_data.append(main)
            road_data.append(road)
            vehicles_data.extend(vehicles)
            participants_data.extend(participants)
    
    save_batch('dtp_main', main_data, engine)
    save_batch('dtp_road_conditions', road_data, engine)
    
    if vehicles_data:
        save_batch('dtp_vehicles', vehicles_data, engine)
    
    if participants_data:
        save_batch('dtp_participants', participants_data, engine)
    
    save_load_log(city_id, region_id, district_id, year, month, len(main_data), 'success', engine)
    
    logger.info(f"Загружено {len(main_data)} ДТП, {len(vehicles_data)} ТС, {len(participants_data)} участников за {year}-{month:02d}")
    return len(main_data)

def process_retry_queue(engine):
    """Обработать очередь повторных попыток"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT id, city_id, year, month FROM dtp_retry_queue 
                    WHERE status = 'pending' 
                    LIMIT 20
                """)
            )
            queue_items = result.fetchall()
            
            for item in queue_items:
                city_result = conn.execute(
                    text("""
                        SELECT gibdd_region_id, gibdd_district_id FROM cities 
                        WHERE id = :city_id
                    """),
                    {"city_id": item[1]}
                )
                city = city_result.first()
                
                if not city:
                    continue
                
                cards = fetch_with_retry(city[0], city[1], item[2], item[3])
                
                if cards is not None:
                    if cards:
                        all_cards = fetch_all_pages(city[0], city[1], item[2], item[3])
                        save_month(all_cards, item[1], city[0], city[1], item[2], item[3], engine)
                    
                    # Удаляем из очереди
                    conn.execute(
                        text("DELETE FROM dtp_retry_queue WHERE id = :id"),
                        {"id": item[0]}
                    )
                    conn.commit()
                
                time.sleep(2)
    except Exception as e:
        logger.error(f"Ошибка при обработке очереди: {e}")

def update_all():
    logger.info("Запуск обновления ДТП")
    
    engine = get_db_connection()
    if not engine:
        logger.error("Не удалось подключиться к базе данных")
        return
    
    # Обрабатываем очередь
    process_retry_queue(engine)
    
    cities = get_active_cities()
    if not cities:
        logger.error("Нет активных городов")
        return
    
    current_year, current_month = datetime.now().year, datetime.now().month
    
    # Вычисляем границу для полного обновления (последние 6 месяцев)
    refresh_year = current_year
    refresh_month = current_month - MONTHS_TO_REFRESH + 1
    while refresh_month <= 0:
        refresh_month += 12
        refresh_year -= 1
    
    logger.info(f"Будут полностью перепроверены (обновлены) месяцы начиная с {refresh_year}-{refresh_month:02d}")
    
    for city in cities:
        city_id, city_name = city['id'], city['city_name']
        region_id, district_id = city['gibdd_region_id'], city['gibdd_district_id']
        
        logger.info(f"Обработка {city_name}")
        
        # Определяем стартовую точку (первый месяц, который еще не загружен)
        last_year, last_month = get_last_loaded_month(city_id, engine)
        
        if not last_year:
            start_year, start_month = 2015, 1
        else:
            start_year, start_month = last_year, last_month
            start_month += 1
            if start_month > 12:
                start_month, start_year = 1, start_year + 1
        
        # Проходим по всем месяцам от стартового до текущего
        year, month = start_year, start_month
        while year < current_year or (year == current_year and month <= current_month):
            
            # Проверяем, нужно ли полностью перепроверить этот месяц (последние 6 месяцев)
            is_recent = (year > refresh_year) or (year == refresh_year and month >= refresh_month)
            
            # Определяем, закончился ли этот месяц
            month_ended = False
            if year < current_year:
                month_ended = True
            elif year == current_year and month < current_month:
                month_ended = True
            
            if is_recent:
                # Для последних 6 месяцев - всегда загружаем (перепроверяем)
                logger.info(f"Перепроверка {year}-{month:02d} (последние {MONTHS_TO_REFRESH} месяцев)")
                cards = fetch_with_retry(region_id, district_id, year, month)
                if cards is None:
                    add_to_retry_queue(city_id, year, month, "Connection error", engine)
                elif cards:
                    all_cards = fetch_all_pages(region_id, district_id, year, month)
                    save_month(all_cards, city_id, region_id, district_id, year, month, engine)
                else:
                    save_month([], city_id, region_id, district_id, year, month, engine)
            else:
                # Для старых месяцев - только если еще не загружены
                if month_loaded(city_id, year, month, engine):
                    logger.info(f"Месяц {year}-{month:02d} уже загружен (пропускаем)")
                else:
                    logger.info(f"Загрузка {year}-{month:02d}")
                    cards = fetch_with_retry(region_id, district_id, year, month)
                    if cards is None:
                        add_to_retry_queue(city_id, year, month, "Connection error", engine)
                    elif cards:
                        all_cards = fetch_all_pages(region_id, district_id, year, month)
                        save_month(all_cards, city_id, region_id, district_id, year, month, engine)
                    else:
                        save_month([], city_id, region_id, district_id, year, month, engine)
            
            month += 1
            if month > 12:
                month, year = 1, year + 1
            time.sleep(1)
    
    logger.info("Финальная обработка очереди...")
    for _ in range(3):
        process_retry_queue(engine)
        time.sleep(5)
    
    logger.info("Загрузка завершена")

if __name__ == "__main__":
    update_all()