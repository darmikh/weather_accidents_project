-- 1. Удаляем старую витрину
DROP MATERIALIZED VIEW IF EXISTS public.mv_dtp_analytics;

-- 2. Создаем новую витрину с полями из cities
CREATE MATERIALIZED VIEW public.mv_dtp_analytics AS
WITH
dtp_with_datetime AS (
    SELECT 
        *,
        (date + time)::timestamp AS datetime_full,
        date_trunc('hour', (date + time)::timestamp) AS datetime_hour,
        EXTRACT(HOUR FROM time)::integer AS hour,
        EXTRACT(DOW FROM date)::integer AS day_of_week,
        EXTRACT(MONTH FROM date)::integer AS month,
        EXTRACT(YEAR FROM date)::integer AS year
    FROM dtp_main
    WHERE date IS NOT NULL AND time IS NOT NULL
),
weather_cleaned AS (
    SELECT 
        city_id,
        datetime::timestamp without time zone AS datetime_clean,
        temperature_2m,
        relative_humidity_2m,
        dew_point_2m,
        apparent_temperature,
        precipitation,
        rain,
        snowfall,
        snow_depth,
        pressure_msl,
        surface_pressure,
        cloud_cover,
        cloud_cover_low,
        cloud_cover_mid,
        cloud_cover_high,
        wind_speed_10m,
        wind_speed_100m,
        wind_direction_100m,
        wind_gusts_10m
    FROM weather_hourly
),
weather_flags AS (
    SELECT
        wk.kart_id,
        wk.weather_conditions,
        CASE 
            WHEN 'Дождь' = ANY(wk.weather_conditions) 
              OR 'Снегопад' = ANY(wk.weather_conditions) 
              OR 'Метель' = ANY(wk.weather_conditions) 
            THEN 1 
            ELSE 0 
        END AS has_bad_weather
    FROM dtp_road_conditions wk
),
light_flags AS (
    SELECT
        lc.kart_id,
        lc.light_conditions,
        CASE 
            WHEN lc.light_conditions IN ('В темное время суток, освещение отсутствует', 'В темное время суток, освещение не включено') 
            THEN 1 
            ELSE 0 
        END AS has_light_problems
    FROM dtp_road_conditions lc
)
SELECT
    -- Идентификаторы и даты
    dm.kart_id,
    dm.city_id,
    c.city_name AS district,
    c.latitude,
    c.longitude,
    c.population,
    c.is_active,
    dm.date,
    dm.time,
    dm.datetime_full AS datetime,
    dm.datetime_hour,
    dm.year,
    dm.month,
    dm.hour,
    dm.day_of_week,
    -- Показатели ДТП
    dm.fatalities,
    dm.injured,
    dm.dtp_type,
    dm.vehicles_count,
    dm.participants_count,
    CASE WHEN dm.fatalities > 0 THEN 1 ELSE 0 END AS has_fatalities_flag,
    -- Погодные данные
    w.temperature_2m,
    w.relative_humidity_2m,
    w.dew_point_2m,
    w.apparent_temperature,
    w.precipitation,
    w.rain,
    w.snowfall,
    w.snow_depth,
    w.pressure_msl,
    w.surface_pressure,
    w.cloud_cover,
    w.cloud_cover_low,
    w.cloud_cover_mid,
    w.cloud_cover_high,
    w.wind_speed_10m,
    w.wind_speed_100m,
    w.wind_direction_100m,
    w.wind_gusts_10m,
    -- Флаги
    wf.has_bad_weather,
    -- Дорожные условия
    rc.road_surface,
    rc.light_conditions,
    lf.has_light_problems,
    rc.weather_conditions,
    rc.road_deficiencies,
    rc.traffic_scheme,
    rc.traffic_objects,
    -- Категории
    CASE
        WHEN w.temperature_2m < -20 THEN 'Мороз < -20°C'
        WHEN w.temperature_2m < -10 THEN 'Мороз -20...-10°C'
        WHEN w.temperature_2m < 0 THEN 'Мороз -10...0°C'
        WHEN w.temperature_2m < 10 THEN 'Прохладно 0...10°C'
        WHEN w.temperature_2m < 20 THEN 'Тепло 10...20°C'
        WHEN w.temperature_2m < 30 THEN 'Жарко 20...30°C'
        WHEN w.temperature_2m >= 30 THEN 'Жара > 30°C'
        ELSE 'Нет данных'
    END AS temp_category,
    CASE
        WHEN w.snowfall > 0 THEN 'Снег'
        WHEN w.rain > 0 THEN 'Дождь'
        ELSE 'Без осадков'
    END AS precip_type,
    CASE
        WHEN rc.light_conditions LIKE '%темное время суток%' 
             AND rc.light_conditions NOT LIKE '%освещение включено%' 
        THEN 'Темно, проблемы с освещением'
        WHEN rc.light_conditions LIKE '%темное время суток%' THEN 'Темно, освещение есть'
        WHEN rc.light_conditions = 'Сумерки' THEN 'Сумерки'
        ELSE 'Светло'
    END AS light_category,
    CASE 
        WHEN rc.road_deficiencies IS NOT NULL AND array_length(rc.road_deficiencies, 1) > 0 
        THEN rc.road_deficiencies[1]
        ELSE 'Нет недостатков'
    END AS main_deficiency,
    CASE 
        WHEN rc.weather_conditions IS NOT NULL AND array_length(rc.weather_conditions, 1) > 0 
        THEN rc.weather_conditions[1]
        ELSE 'Не указано'
    END AS main_weather
FROM
    dtp_with_datetime dm
LEFT JOIN cities c ON dm.city_id = c.id
LEFT JOIN weather_cleaned w ON dm.city_id = w.city_id 
    AND dm.datetime_hour = w.datetime_clean
LEFT JOIN dtp_road_conditions rc ON dm.kart_id = rc.kart_id
LEFT JOIN weather_flags wf ON dm.kart_id = wf.kart_id
LEFT JOIN light_flags lf ON dm.kart_id = lf.kart_id;

-- 3. Индексы (с проверкой существования)
DO $$
BEGIN
    -- Уникальный индекс для kart_id
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_mv_dtp_analytics_unique_kart_id'
    ) THEN
        CREATE UNIQUE INDEX idx_mv_dtp_analytics_unique_kart_id ON public.mv_dtp_analytics (kart_id);
    END IF;

    -- Индекс по дате
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_mv_dtp_analytics_date'
    ) THEN
        CREATE INDEX idx_mv_dtp_analytics_date ON public.mv_dtp_analytics (date);
    END IF;

    -- Индекс по городу
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_mv_dtp_analytics_city'
    ) THEN
        CREATE INDEX idx_mv_dtp_analytics_city ON public.mv_dtp_analytics (district);
    END IF;

    -- Индекс по типу ДТП
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_mv_dtp_analytics_dtp_type'
    ) THEN
        CREATE INDEX idx_mv_dtp_analytics_dtp_type ON public.mv_dtp_analytics (dtp_type);
    END IF;

    -- Индекс по температурной категории
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_mv_dtp_analytics_temp_cat'
    ) THEN
        CREATE INDEX idx_mv_dtp_analytics_temp_cat ON public.mv_dtp_analytics (temp_category);
    END IF;

    -- Индекс по типу осадков
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_mv_dtp_analytics_precip'
    ) THEN
        CREATE INDEX idx_mv_dtp_analytics_precip ON public.mv_dtp_analytics (precip_type);
    END IF;

    -- Индекс по категории освещения
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_mv_dtp_analytics_light_cat'
    ) THEN
        CREATE INDEX idx_mv_dtp_analytics_light_cat ON public.mv_dtp_analytics (light_category);
    END IF;

    -- Индекс по datetime
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_mv_dtp_analytics_datetime'
    ) THEN
        CREATE INDEX idx_mv_dtp_analytics_datetime ON public.mv_dtp_analytics (datetime);
    END IF;

    -- Индекс по datetime_hour
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_mv_dtp_analytics_datetime_hour'
    ) THEN
        CREATE INDEX idx_mv_dtp_analytics_datetime_hour ON public.mv_dtp_analytics (datetime_hour);
    END IF;
END $$;

-- 4. Права
GRANT SELECT ON public.mv_dtp_analytics TO authenticated;
GRANT SELECT ON public.mv_dtp_analytics TO anon;
GRANT SELECT ON public.mv_dtp_analytics TO service_role;

-- Для ручного обновления:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dtp_analytics;