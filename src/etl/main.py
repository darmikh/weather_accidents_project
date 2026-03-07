import sys
from pathlib import Path
import time

src_path = Path(__file__).parent.parent
sys.path.append(str(src_path))

from logger_config import get_logger 
from cities_etl import CitiesParser, CitiesProcessor
from gibdd_okato_etl import main as gibdd_main
from weather_etl import load_full_weather
from gibdd_dtp_etl import update_all as load_dtp_data
from refresh_datamart import refresh_materialized_view, update_last_refresh_date

logger = get_logger('main')

def run_etl_step(step_name, func, *args, **kwargs):
    """Запускает шаг ETL с обработкой ошибок"""
    logger.info(f"Начало: {step_name}")
    
    start_time = time.time()
    try:
        if args or kwargs:
            result = func(*args, **kwargs)
        else:
            result = func()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Успешно: {step_name} завершен за {elapsed_time:.2f} сек")
        return result
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Ошибка в {step_name} после {elapsed_time:.2f} сек: {e}")
        logger.exception("Детали ошибки:")
        raise

def main():
    logger.info("\nЗапуск полного ETL процесса")
    
    total_start = time.time()
    
    try:
        # 1. Парсинг городов с Википедии
        run_etl_step("Парсинг городов с Википедии", lambda: CitiesParser().run())
        
        # 2. Обработка сырых данных городов (координаты)
        run_etl_step("Обработка координат городов", lambda: CitiesProcessor().process_raw_cities())
        
        # 3. Получение кодов ОКАТО/ГИБДД для городов - будем запускать, если будут появлятсья новые города 
        #run_etl_step("Получение кодов ГИБДД", gibdd_main)
        
        # 4. Загрузка погодных данных
        run_etl_step("Загрузка погодных данных", load_full_weather)
        
        # 5. Загрузка данных о ДТП
        run_etl_step("Загрузка данных о ДТП", load_dtp_data)
        
        # 6. Обновление витрины данных для дашборда
        run_etl_step("Обновление витрины mv_dtp_analytics", refresh_materialized_view, 'mv_dtp_analytics')
        
        # 7. Запись даты последнего обновления (для дашборда)
        run_etl_step("Запись даты обновления", update_last_refresh_date)
        
        total_time = time.time() - total_start
        logger.info(f"\nETL процесс завершен за {total_time/60:.2f} минут.")
        
    except Exception as e:
        logger.error("\nETL процесс прерван из-за ошибки.")
        sys.exit(1)

if __name__ == "__main__":
    main()