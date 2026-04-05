-- Таблица: dtp_main (основная информация о ДТП)
CREATE TABLE IF NOT EXISTS public.dtp_main (
    id BIGSERIAL PRIMARY KEY,
    kart_id BIGINT UNIQUE NOT NULL,
    city_id UUID,  
    row_num INTEGER,
    date DATE,
    time TIME,
    district TEXT,
    dtp_type TEXT,
    fatalities INTEGER DEFAULT 0,
    injured INTEGER DEFAULT 0,
    vehicles_count INTEGER,
    participants_count INTEGER,
    emtp_number TEXT,
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Индексы 
CREATE INDEX idx_dtp_main_kart_id ON public.dtp_main(kart_id);
CREATE INDEX idx_dtp_main_city_id ON public.dtp_main(city_id);
CREATE INDEX idx_dtp_main_date ON public.dtp_main(date);


-- Таблица: dtp_road_conditions (дорожные условия и обстановка)
CREATE TABLE IF NOT EXISTS public.dtp_road_conditions (
    id BIGSERIAL PRIMARY KEY,
    kart_id BIGINT UNIQUE NOT NULL,
    settlement TEXT,
    street TEXT,
    house TEXT,
    road TEXT,
    kilometer INTEGER,
    meter INTEGER,
    road_category TEXT,
    road_code TEXT,
    road_value TEXT,
    road_surface TEXT,
    light_conditions TEXT,
    traffic_change TEXT,
    accident_code TEXT,
    latitude NUMERIC(10, 7),
    longitude NUMERIC(10, 7),
    road_deficiencies TEXT[],
    traffic_scheme TEXT[],
    factors TEXT[],
    weather_conditions TEXT[],
    traffic_objects TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Индекс для связи с dtp_main
CREATE INDEX idx_dtp_road_conditions_kart_id ON public.dtp_road_conditions(kart_id);


-- Таблица: dtp_vehicles (транспортные средства)
CREATE TABLE IF NOT EXISTS public.dtp_vehicles (
    id BIGSERIAL PRIMARY KEY,
    kart_id BIGINT NOT NULL,
    vehicle_number_in_accident TEXT,
    vehicle_status TEXT,
    vehicle_type TEXT,
    make TEXT,
    model TEXT,
    color TEXT,
    steering TEXT,
    year INTEGER,
    engine_capacity TEXT,
    technical_condition TEXT,
    ownership_form TEXT,
    owner_type TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Индексы
CREATE INDEX idx_dtp_vehicles_kart_id ON public.dtp_vehicles(kart_id);
CREATE INDEX idx_dtp_vehicles_vehicle_number ON public.dtp_vehicles(vehicle_number_in_accident);


-- Таблица: dtp_participants (участники ДТП)
CREATE TABLE IF NOT EXISTS public.dtp_participants (
    id BIGSERIAL PRIMARY KEY,
    kart_id BIGINT NOT NULL,
    vehicle_id BIGINT,
    participant_number TEXT,
    role TEXT,
    injury_severity TEXT,
    gender TEXT,
    driving_experience INTEGER,
    alcohol TEXT,
    seatbelt_used TEXT,
    hid_from_scene TEXT,
    seat_group TEXT,
    injured_card_id TEXT,
    violations TEXT[],
    additional_violations TEXT[],
    is_from_uch_info BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Индексы
CREATE INDEX idx_dtp_participants_kart_id ON public.dtp_participants(kart_id);
CREATE INDEX idx_dtp_participants_vehicle_id ON public.dtp_participants(vehicle_id);
CREATE INDEX idx_dtp_participants_role ON public.dtp_participants(role);

-- Таблица: dtp_load_log (логирование загрузок данных)
CREATE TABLE IF NOT EXISTS public.dtp_load_log (
    id BIGSERIAL PRIMARY KEY,
    city_id UUID,  
    gibdd_region_id TEXT,
    gibdd_district_id TEXT,
    year INTEGER,
    month INTEGER,
    records_loaded INTEGER DEFAULT 0,
    load_status TEXT,
    error_message TEXT,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Индекс для отслеживания загрузок
CREATE INDEX idx_dtp_load_log_city_date ON public.dtp_load_log(city_id, year, month);


-- Таблица для очереди повторной обработки проблемных месяцев
CREATE TABLE IF NOT EXISTS public.dtp_retry_queue (
    id BIGSERIAL PRIMARY KEY,
    city_id UUID,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    attempt_count INTEGER DEFAULT 0,
    last_error TEXT,
    next_retry_time TIMESTAMPTZ DEFAULT NOW(),
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'done', 'failed')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Индексы
CREATE INDEX idx_retry_queue_city ON public.dtp_retry_queue(city_id);
CREATE INDEX idx_retry_queue_status ON public.dtp_retry_queue(status);
CREATE INDEX idx_retry_queue_next_retry ON public.dtp_retry_queue(next_retry_time);

-- Комментарии
COMMENT ON TABLE public.dtp_retry_queue IS 'Очередь месяцев для повторной загрузки данных ДТП';
COMMENT ON COLUMN public.dtp_retry_queue.attempt_count IS 'Количество попыток загрузки';
COMMENT ON COLUMN public.dtp_retry_queue.last_error IS 'Последняя ошибка при попытке загрузки';
COMMENT ON COLUMN public.dtp_retry_queue.status IS 'pending - ожидает, processing - в обработке, done - успешно, failed - провалено после 5 попыток';

