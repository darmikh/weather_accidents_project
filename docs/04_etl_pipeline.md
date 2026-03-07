# ETL-пайплайн

## Структура скриптов

src/etl/
- `main.py` - главный скрипт, запускает все шаги
- `cities_etl.py` - парсинг городов и координат
- `gibdd_okato_etl.py` - получение кодов ГИБДД
- `weather_etl.py` - загрузка погоды
- `gibdd_dtp_etl.py` - загрузка ДТП
- `refresh_datamart.py` - обновление витрины и лога
- `database.py` - работа с Supabase API
- `logger_config.py` - настройка логирования

## Порядок выполнения
1. Парсинг городов с Wikipedia - `raw_cities`
2. Обработка координат - `cities`
3. Получение кодов ГИБДД - обновление `cities`
4. Загрузка погоды - `raw_weather_data` - `weather_hourly`
5. Загрузка ДТП - `dtp_main`, `dtp_road_conditions`, `dtp_vehicles`, `dtp_participants`
6. Обновление витрины `mv_dtp_analytics`
7. Запись в `refresh_log`

## Логирование
- Консоль: INFO и выше
- Файл: `logs/etl_YYYYMM.log` (DEBUG и выше)