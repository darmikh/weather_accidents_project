-- 1. Удалить старые таблицы (если они есть)
DROP TABLE IF EXISTS cities CASCADE;
DROP TABLE IF EXISTS raw_cities CASCADE;

-- 2. Создать упрощенные таблицы:

-- Сырые данные (только необходимое)
CREATE TABLE IF NOT EXISTS raw_cities (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    row_number INTEGER,
    original_city_name TEXT NOT NULL,
    original_region TEXT NOT NULL,
    original_federal_district TEXT,
    original_population TEXT,  -- сохраняем как текст для аудита
    parsed_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc', NOW()),
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'processed', 'error')),
    error_message TEXT,  -- для отладки ошибок обработки
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc', NOW())
);

-- Обработанные города (только нужные для анализа)
CREATE TABLE IF NOT EXISTS cities (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    city_name TEXT NOT NULL,
    region TEXT NOT NULL,
    federal_district TEXT,
    population INTEGER,  -- числовое значение для фильтрации/сортировки
    okato_code TEXT,    -- ОКАТО (не подходит для запросов к API ГИБДД)
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    is_active BOOLEAN DEFAULT FALSE,
    raw_city_id UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc', NOW()),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc', NOW()),
    UNIQUE(city_name, region)  -- предотвращаем дубли
);

-- 3. Индексы 
CREATE INDEX IF NOT EXISTS idx_cities_active ON cities (is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_cities_region ON cities (region);
CREATE INDEX IF NOT EXISTS idx_cities_federal_district ON cities (federal_district);
CREATE INDEX IF NOT EXISTS idx_cities_population ON cities (population DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_raw_cities_status ON raw_cities (status);

-- 4. Удалить старые таблицы (если они есть)
DROP TABLE IF EXISTS weather_hourly CASCADE;
DROP TABLE IF EXISTS raw_weather_data CASCADE;

-- 5. Таблица для сырых погодных данных (raw)
-- Таблица для хранения сырых данных из Open-Meteo API
CREATE TABLE IF NOT EXISTS raw_weather_data (
    -- Идентификатор
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    
    -- Связь с городом
    city_id UUID,
    
    -- Координаты 
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    
    -- Период запроса
    start_date DATE NOT NULL,         
    end_date DATE NOT NULL,            
    
    -- Информация о запросе
    request_url TEXT,                  -- полный URL запроса для отладки
    response_status INT,               -- HTTP статус (200, 404, 500 и т.д.)
    error_message TEXT,                -- сообщение об ошибке, если была
    
    -- Сырые данные
    hourly_data JSONB NOT NULL,        -- полный JSONB ответ от API 
    
    -- Метаданные
    source TEXT DEFAULT 'open-meteo',  -- источник данных
    fetched_at TIMESTAMPTZ DEFAULT TIMEZONE('utc', NOW()), -- когда получили данные
    created_at TIMESTAMPTZ DEFAULT TIMEZONE('utc', NOW()), -- когда создали запись
    
    -- Ограничения
    UNIQUE(city_id, start_date, end_date) -- один запрос на город за период
);

-- 6. Таблица для очищенных почасовых данных
-- Таблица для хранения очищенных почасовых погодных данных
CREATE TABLE IF NOT EXISTS weather_hourly (
    -- Идентификатор
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    
    -- Связь с городом
    city_id UUID NOT NULL,
    
    -- Время наблюдения (берется из hourly.time в ответе API)
    datetime TIMESTAMPTZ NOT NULL,     -- час наблюдения (например, '2024-01-01 14:00:00+03')
    
    -- Основные метеопараметры
    temperature_2m FLOAT,              -- температура на высоте 2м (°C)
    relative_humidity_2m INT,          -- относительная влажность (%)
    dew_point_2m FLOAT,                -- точка росы (°C)
    apparent_temperature FLOAT,        -- ощущаемая температура (°C)
    
    -- Осадки
    precipitation FLOAT,               -- общее количество осадков (mm)
    rain FLOAT,                        -- дождь (mm)
    snowfall FLOAT,                    -- снег (mm)
    snow_depth FLOAT,                  -- высота снежного покрова (m)
    
    -- Давление
    pressure_msl FLOAT,                -- давление на уровне моря (hPa)
    surface_pressure FLOAT,            -- давление на поверхности (hPa)
    
    -- Облачность
    cloud_cover INT,                   -- общая облачность (%)
    cloud_cover_low INT,               -- нижняя облачность (%)
    cloud_cover_mid INT,               -- средняя облачность (%)
    cloud_cover_high INT,              -- верхняя облачность (%)
    
    -- Ветер
    wind_speed_10m FLOAT,              -- скорость ветра на 10м (км/ч)
    wind_speed_100m FLOAT,             -- скорость ветра на 100м (км/ч)
    wind_direction_10m INT,            -- направление ветра на 10м (°)
    wind_direction_100m INT,           -- направление ветра на 100м (°)
    wind_gusts_10m FLOAT,              -- порывы ветра на 10м (км/ч)
    
    -- Солнечная радиация
    shortwave_radiation FLOAT,         -- коротковолновая радиация 
    direct_radiation FLOAT,            -- прямая радиация 
    diffuse_radiation FLOAT,           -- рассеянная радиация 
    direct_normal_irradiance FLOAT,    -- прямая нормальная радиация 
    terrestrial_radiation FLOAT,       -- земная радиация 
    
    -- Связь с сырыми данными
    raw_weather_id UUID,
    
    -- Метаданные
    created_at TIMESTAMPTZ DEFAULT TIMEZONE('utc', NOW()),
    
    -- Ограничения
    UNIQUE(city_id, datetime)  -- одно значение погоды на город в час
);

-- Индексы 
-- Основной индекс для JOIN с данными ДТП (обязательный)
CREATE INDEX IF NOT EXISTS idx_weather_city_datetime 
ON weather_hourly (city_id, datetime DESC);

-- Для анализа по температуре
CREATE INDEX IF NOT EXISTS idx_weather_temperature 
ON weather_hourly (temperature_2m);

-- Для анализа по осадкам (частичный индекс - только записи с осадками)
CREATE INDEX IF NOT EXISTS idx_weather_precipitation 
ON weather_hourly (precipitation) 
WHERE precipitation > 0;

-- Для проверки дубликатов (перед загрузкой)
CREATE INDEX IF NOT EXISTS idx_raw_city_start_end 
ON raw_weather_data (city_id, start_date, end_date);


-- Включить только нужные города
UPDATE cities 
SET is_active = true 
WHERE city_name IN ('Великий Новгород', 'Тамбов', 'Петрозаводск');

SELECT city_name, region, is_active 
FROM cities 
WHERE city_name IN ('Великий Новгород', 'Тамбов', 'Петрозаводск')
ORDER BY city_name;

-- Добавление столбцов для хранения ID ГИБДД
ALTER TABLE cities 
ADD COLUMN IF NOT EXISTS gibdd_region_id TEXT,
ADD COLUMN IF NOT EXISTS gibdd_district_id TEXT;

-- Комментарии
COMMENT ON COLUMN cities.gibdd_region_id IS 'ID региона в системе ГИБДД (первые 1-2 цифры)';
COMMENT ON COLUMN cities.gibdd_district_id IS 'ID муниципалитета/города в системе ГИБДД (полный код)';