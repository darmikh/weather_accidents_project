# Как запустить проект

## Локальный запуск

### Предварительные требования

- **Python 3.10** или выше
- **Git** (для клонирования репозитория)
- **Аккаунт Supabase** (бесплатный)
- **API ключи:**
  - Open-Meteo (бесплатный, ключ не нужен)
  - DaData (для кодов ОКАТО) - опционально
  - Yandex Геокодер (для координат) - рекомендуемо

### 1. Клонирование репозитория

```bash
git clone https://github.com/ваш-логин/weather_accidents_project.git
cd weather_accidents_project
```
### 2. Настройка виртуального окружения

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Mac/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 4. Настройка переменных окружения

Скопируйте файл `.env.example` в `.env`:

```bash
cp .env.example .env
```

Отредактируйте `.env`, добавив свои данные:

```env
# Supabase
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=your-password
SUPABASE_DB_HOST=db.your-project.supabase.co
SUPABASE_DB_NAME=postgres

# API ключи (рекомендуется для обработки сложных случаев)
YANDEX_APIKEY=your-yandex-api-key
DADATA_API_KEY=your-dadata-api-key

# User-Agent для парсинга Wikipedia (обязательно)
USER_AGENT_EMAIL=your-email@example.com
```

Дополнительные настройки (годы загрузки, количество месяцев для перепроверки) находятся в `src/etl/config.py`:

```python
START_YEAR =         # начало периода загрузки
END_YEAR =           # последний полный год
MONTHS_TO_REFRESH =  # последние N месяцев для полной перепроверки
```

### 5. Создание таблиц в базе данных

Подключитесь к Supabase через SQL Editor и выполните SQL-скрипты в следующем порядке:

1. `sql/01_create_main_tables.sql` - таблицы cities, raw_cities_data, raw_weather_data, weather_hourly
2. `sql/02_create_dtp_tables.sql` - таблицы ДТП
3. `sql/03_create_mv_dtp_analytics.sql` - витрина данных

### 6. Активация городов для анализа

Выполните в Supabase SQL Editor:

```sql
UPDATE cities 
SET is_active = true 
WHERE city_name IN ('Великий Новгород', 'Тамбов', 'Петрозаводск');
```
В дальнейшем можно будет активировать другие города для исследования. 


### 7. Запуск ETL-процесса

```bash
cd src/etl
python main.py
```

### 8. Проверка результатов

- Данные появятся в таблицах Supabase
- Витрина `mv_dtp_analytics` обновится автоматически
- В папке `logs/` появятся лог-файлы



## Запуск отдельных этапов

В файле `src/etl/main.py` можно закомментировать ненужные шаги:

```python
# 1. Парсинг городов с Википедии
# run_etl_step("Парсинг городов", lambda: CitiesParser().run())

# 2. Обработка координат
# run_etl_step("Обработка координат", lambda: CitiesProcessor().process_raw_cities())

# 3. Загрузка погоды
run_etl_step("Загрузка погоды", load_full_weather)

# 4. Загрузка ДТП
run_etl_step("Загрузка ДТП", load_dtp_data)

# 5. Обновление витрины
run_etl_step("Обновление витрины", refresh_materialized_view, 'mv_dtp_analytics')
```


## Обновление данных

### Автоматическое (рекомендуется)

После настройки GitHub Actions данные будут обновляться ежедневно в 6:00 МСК.

### Ручное обновление

```bash
cd src/etl
python main.py
```

### Ручное обновление витрины

В Supabase SQL Editor выполните:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dtp_analytics;
```



## Устранение неполадок

### 1. Ошибки подключения к Supabase

- Проверьте, что в `.env` правильные URL и ключи
- Убедитесь, что IP-адрес не заблокирован (в настройках Supabase)

### 2. Ошибки при загрузке ДТП

- Проверьте, что для городов указаны `gibdd_region_id` и `gibdd_district_id`
- Попробуйте запустить скрипт с включенным VPN

### 3. Альтернативная загрузка ДТП (REST API версия)

Если при запуске `main.py` возникает ошибка `server closed the connection unexpectedly` или проблемы с SQLAlchemy, используйте альтернативный скрипт:

```bash
cd src/etl
python gibdd_dtp_etl_rest.py
```

### 4. Логирование

Все логи сохраняются в папку `logs/`. Проверьте файл `etl_YYYYMM.log` для деталей.

### 5. Ошибки GitHub Actions

- Проверьте, что все секреты добавлены в настройках репозитория
- Посмотрите логи выполнения в разделе Actions


## Ссылки

- [Дашборд в DataLens](https://datalens.yandex/nmn0urv6qczq7)