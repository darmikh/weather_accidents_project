import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo
from logger_config import get_logger

logger = get_logger('refresh_datamart')

def refresh_materialized_view(view_name: str, concurrently: bool = True):
    """
    Обновляет материализованное представление в БД.
    
    Args:
        view_name: Имя материализованного представления
        concurrently: Использовать CONCURRENTLY (позволяет читать данные во время обновления)
    """
    load_dotenv()
    
    user = os.getenv('SUPABASE_DB_USER')
    password = os.getenv('SUPABASE_DB_PASSWORD')
    host = os.getenv('SUPABASE_DB_HOST')
    db_name = os.getenv('SUPABASE_DB_NAME')
    
    if not all([user, password, host, db_name]):
        logger.error("Не найдены переменные окружения для подключения к БД")
        raise ValueError("Missing database environment variables")
    
    database_url = f'postgresql+psycopg2://{user}:{password}@{host}:5432/{db_name}'
    
    try:
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Подключение к базе данных успешно установлено")
        
        concurrently_clause = "CONCURRENTLY " if concurrently else ""
        refresh_query = text(f"REFRESH MATERIALIZED VIEW {concurrently_clause}{view_name};")
        
        with engine.connect() as conn:
            logger.info(f"Начато обновление материализованного представления {view_name}...")
            conn.execute(refresh_query)
            conn.commit()
            logger.info(f"Материализованное представление {view_name} успешно обновлено")
            
    except Exception as e:
        logger.error(f"Ошибка при обновлении представления {view_name}: {e}")
        raise
    
def update_last_refresh_date():
    """
    Записывает текущую дату и время в таблицу refresh_log.
    Создает таблицу, если она не существует.
    """
    load_dotenv()
        
    user = os.getenv('SUPABASE_DB_USER')
    password = os.getenv('SUPABASE_DB_PASSWORD')
    host = os.getenv('SUPABASE_DB_HOST')
    db_name = os.getenv('SUPABASE_DB_NAME')
        
    database_url = f'postgresql+psycopg2://{user}:{password}@{host}:5432/{db_name}'
    
    msk_time = datetime.now(ZoneInfo("Europe/Moscow"))
        
    try:
        engine = create_engine(database_url)
            
        with engine.connect() as conn:
            # Создаем таблицу для лога обновлений, если её нет
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS refresh_log (
                    id SERIAL PRIMARY KEY,
                    refresh_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    status TEXT,
                    details TEXT
                )
            """))
                
            # Записываем дату успешного обновления
            conn.execute(
                text("""
                    INSERT INTO refresh_log (refresh_date, status, details) 
                    VALUES (:refresh_date, 'success', 'ETL process completed successfully')
                """),
                {"refresh_date": msk_time}
            )
                
            conn.commit()
                
        logger.info(f"Дата обновления записана: {msk_time}")
            
    except Exception as e:
        logger.error(f"Ошибка при записи даты обновления: {e}")
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO refresh_log (status, details) 
                    VALUES ('error', :error)
                """), {"error": str(e)})
                conn.commit()
        except:
            pass