# Схема базы данных

## Общая информация

- **База данных:** Supabase (PostgreSQL)
- **Проект:** weather_accidents
- **Схема:** public

---

## 1. Таблицы с данными о городах

### raw_cities

Сырые данные, полученные при парсинге Wikipedia. Содержит исходную информацию о городах России до обработки.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | uuid | Уникальный идентификатор записи |
| `row_number` | integer | Номер строки в исходной таблице Wikipedia |
| `original_city_name` | text | Название города (как в Wikipedia) |
| `original_region` | text | Регион (как в Wikipedia) |
| `original_federal_district` | text | Федеральный округ |
| `original_population` | text | Население (в виде текста, может содержать нечисловые символы) |
| `parsed_at` | timestamptz | Дата и время парсинга |
| `status` | text | Статус обработки (`pending`, `processed`, `error`) |
| `error_message` | text | Сообщение об ошибке (если была) |
| `created_at` | timestamptz | Дата создания записи |

### cities

Обработанные данные о городах с координатами и кодами для API ГИБДД. Активные города (используемые в анализе) отмечены флагом `is_active = true`.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | uuid | Уникальный идентификатор города |
| `city_name` | text | Название города (очищенное) |
| `region` | text | Регион |
| `federal_district` | text | Федеральный округ |
| `population` | integer | Население (числовое значение) |
| `okato_code` | text | Код ОКАТО (для API ГИБДД) |
| `latitude` | float8 | Широта |
| `longitude` | float8 | Долгота |
| `is_active` | boolean | Флаг активности для анализа |
| `raw_city_id` | uuid | Ссылка на исходную запись в `raw_cities` |
| `gibdd_region_id` | text | ID региона в системе ГИБДД |
| `gibdd_district_id` | text | ID муниципалитета/города в системе ГИБДД |
| `created_at` | timestamptz | Дата создания записи |
| `updated_at` | timestamptz | Дата последнего обновления |

---

## 2. Таблицы с погодными данными

### raw_weather_data

Сырые JSON-ответы от Open-Meteo API. Хранятся для аудита и возможности переобработки.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | uuid | Уникальный идентификатор записи |
| `city_id` | uuid | Ссылка на город |
| `latitude` | float8 | Широта запроса |
| `longitude` | float8 | Долгота запроса |
| `start_date` | date | Начало периода запроса |
| `end_date` | date | Конец периода запроса |
| `request_url` | text | Полный URL запроса (для отладки) |
| `response_status` | integer | HTTP статус ответа |
| `error_message` | text | Сообщение об ошибке |
| `hourly_data` | jsonb | Полный JSON-ответ от API |
| `source` | text | Источник данных (open-meteo) |
| `fetched_at` | timestamptz | Время получения данных |
| `created_at` | timestamptz | Дата создания записи |

### weather_hourly

Очищенные почасовые метеоданные, готовые для анализа и соединения с ДТП.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | uuid | Уникальный идентификатор записи |
| `city_id` | uuid | Ссылка на город |
| `datetime` | timestamptz | Дата и время наблюдения (час) |
| `temperature_2m` | float8 | Температура на высоте 2 м (°C) |
| `relative_humidity_2m` | integer | Относительная влажность (%) |
| `dew_point_2m` | float8 | Точка росы (°C) |
| `apparent_temperature` | float8 | Ощущаемая температура (°C) |
| `precipitation` | float8 | Общее количество осадков (mm) |
| `rain` | float8 | Дождь (mm) |
| `snowfall` | float8 | Снег (mm) |
| `snow_depth` | float8 | Высота снежного покрова (m) |
| `pressure_msl` | float8 | Давление на уровне моря (hPa) |
| `surface_pressure` | float8 | Давление на поверхности (hPa) |
| `cloud_cover` | integer | Общая облачность (%) |
| `cloud_cover_low` | integer | Нижняя облачность (%) |
| `cloud_cover_mid` | integer | Средняя облачность (%) |
| `cloud_cover_high` | integer | Верхняя облачность (%) |
| `wind_speed_10m` | float8 | Скорость ветра на 10 м (км/ч) |
| `wind_speed_100m` | float8 | Скорость ветра на 100 м (км/ч) |
| `wind_direction_10m` | integer | Направление ветра на 10 м (°) |
| `wind_direction_100m` | integer | Направление ветра на 100 м (°) |
| `wind_gusts_10m` | float8 | Порывы ветра на 10 м (км/ч) |
| `shortwave_radiation` | float8 | Коротковолновая радиация (W/m²) |
| `direct_radiation` | float8 | Прямая радиация (W/m²) |
| `diffuse_radiation` | float8 | Рассеянная радиация (W/m²) |
| `direct_normal_irradiance` | float8 | Прямая нормальная irradiance (W/m²) |
| `terrestrial_radiation` | float8 | Земная радиация (W/m²) |
| `raw_weather_id` | uuid | Ссылка на сырые данные в `raw_weather_data` |
| `created_at` | timestamptz | Дата создания записи |

---

## 3. Таблицы с данными о ДТП

### dtp_main

Основная информация о каждом ДТП.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | bigint | Уникальный идентификатор записи |
| `kart_id` | bigint | Уникальный ID ДТП в системе ГИБДД |
| `city_id` | uuid | Ссылка на город |
| `row_num` | integer | Номер строки в ответе API |
| `date` | date | Дата ДТП |
| `time` | time | Время ДТП |
| `district` | text | Район/округ |
| `dtp_type` | text | Тип ДТП (столкновение, наезд и т.д.) |
| `fatalities` | integer | Количество погибших |
| `injured` | integer | Количество раненых |
| `vehicles_count` | integer | Количество транспортных средств |
| `participants_count` | integer | Количество участников |
| `emtp_number` | text | Номер карточки EMTP |
| `raw_data` | jsonb | Исходный JSON от API ГИБДД |
| `created_at` | timestamptz | Дата создания записи |
| `updated_at` | timestamptz | Дата последнего обновления |

### dtp_road_conditions

Дорожные условия и обстановка в момент ДТП.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | bigint | Уникальный идентификатор записи |
| `kart_id` | bigint | Ссылка на ДТП |
| `settlement` | text | Населенный пункт |
| `street` | text | Улица |
| `house` | text | Дом |
| `road` | text | Дорога (трасса) |
| `kilometer` | integer | Километр |
| `meter` | integer | Метр |
| `road_category` | text | Категория дороги |
| `road_code` | text | Код дороги |
| `road_value` | text | Значение дороги |
| `road_surface` | text | Состояние покрытия |
| `light_conditions` | text | Условия освещения |
| `traffic_change` | text | Изменение организации движения |
| `accident_code` | text | Код ДТП |
| `latitude` | numeric | Широта (из координат) |
| `longitude` | numeric | Долгота (из координат) |
| `road_deficiencies` | ARRAY | Недостатки дороги |
| `traffic_scheme` | ARRAY | Схема организации движения |
| `factors` | ARRAY | Факторы ДТП |
| `weather_conditions` | ARRAY | Погодные условия |
| `traffic_objects` | ARRAY | Объекты на дороге |
| `created_at` | timestamptz | Дата создания записи |
| `updated_at` | timestamptz | Дата последнего обновления |

### dtp_vehicles

Транспортные средства, участвовавшие в ДТП.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | bigint | Уникальный идентификатор записи |
| `kart_id` | bigint | Ссылка на ДТП |
| `vehicle_number_in_accident` | text | Номер ТС в ДТП |
| `vehicle_status` | text | Статус ТС (скрылось, осталось и т.д.) |
| `vehicle_type` | text | Тип ТС |
| `make` | text | Марка |
| `model` | text | Модель |
| `color` | text | Цвет |
| `steering` | text | Рулевое управление |
| `year` | integer | Год выпуска |
| `engine_capacity` | text | Объем двигателя |
| `technical_condition` | text | Техническое состояние |
| `ownership_form` | text | Форма собственности |
| `owner_type` | text | Тип владельца |
| `created_at` | timestamptz | Дата создания записи |
| `updated_at` | timestamptz | Дата последнего обновления |

### dtp_participants

Участники ДТП (водители, пассажиры, пешеходы).

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | bigint | Уникальный идентификатор записи |
| `kart_id` | bigint | Ссылка на ДТП |
| `vehicle_id` | bigint | Ссылка на ТС (если участник связан с ТС) |
| `participant_number` | text | Номер участника |
| `role` | text | Роль (водитель, пассажир, пешеход) |
| `injury_severity` | text | Тяжесть ранения |
| `gender` | text | Пол |
| `driving_experience` | integer | Стаж вождения (лет) |
| `alcohol` | text | Алкогольное опьянение |
| `seatbelt_used` | text | Использование ремня безопасности |
| `hid_from_scene` | text | Скрылся с места ДТП |
| `seat_group` | text | Группа мест |
| `injured_card_id` | text | ID карточки раненого |
| `violations` | ARRAY | Нарушения ПДД |
| `additional_violations` | ARRAY | Дополнительные нарушения |
| `is_from_uch_info` | boolean | Признак источника данных |
| `created_at` | timestamptz | Дата создания записи |
| `updated_at` | timestamptz | Дата последнего обновления |

---

## 4. Служебные таблицы

### dtp_load_log

Логирование загрузок данных из API ГИБДД.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | bigint | Уникальный идентификатор записи |
| `city_id` | uuid | Ссылка на город |
| `gibdd_region_id` | text | ID региона в ГИБДД |
| `gibdd_district_id` | text | ID муниципалитета в ГИБДД |
| `year` | integer | Год загрузки |
| `month` | integer | Месяц загрузки |
| `records_loaded` | integer | Количество загруженных записей |
| `load_status` | text | Статус загрузки |
| `error_message` | text | Сообщение об ошибке |
| `started_at` | timestamptz | Время начала загрузки |
| `completed_at` | timestamptz | Время завершения загрузки |

### dtp_retry_queue

Очередь месяцев для повторной загрузки данных при ошибках.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | bigint | Уникальный идентификатор записи |
| `city_id` | uuid | Ссылка на город |
| `year` | integer | Год |
| `month` | integer | Месяц |
| `attempt_count` | integer | Количество попыток загрузки |
| `last_error` | text | Последняя ошибка |
| `next_retry_time` | timestamptz | Время следующей попытки |
| `status` | text | Статус (`pending`, `processing`, `done`, `failed`) |
| `created_at` | timestamptz | Дата создания записи |
| `updated_at` | timestamptz | Дата последнего обновления |

### refresh_log

Логирование обновлений витрины данных.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | integer | Уникальный идентификатор записи |
| `refresh_date` | timestamptz | Дата и время обновления |
| `status` | text | Статус обновления (`success`, `error`) |
| `details` | text | Детали (сообщение об ошибке) |

---

## 5. Витрина данных

### mv_dtp_analytics (материализованное представление)

Основная витрина для дашборда. Объединяет данные из всех таблиц, добавляет расчетные категории.

| Поле | Тип | Описание |
|------|-----|----------|
| `kart_id` | bigint | ID ДТП |
| `city_id` | uuid | ID города |
| `district` | text | Название города |
| `latitude` | float8 | Широта города |
| `longitude` | float8 | Долгота города |
| `population` | integer | Население города |
| `is_active` | boolean | Флаг активности |
| `date` | date | Дата ДТП |
| `time` | time | Время ДТП |
| `datetime` | timestamp | Полная дата и время |
| `datetime_hour` | timestamp | Дата и время, округленные до часа |
| `year` | integer | Год |
| `month` | integer | Месяц |
| `hour` | integer | Час |
| `day_of_week` | integer | День недели (0-6) |
| `fatalities` | integer | Погибшие |
| `injured` | integer | Раненые |
| `dtp_type` | text | Тип ДТП |
| `vehicles_count` | integer | Количество ТС |
| `participants_count` | integer | Количество участников |
| `has_fatalities_flag` | integer | Флаг смертельного ДТП (1/0) |
| `temperature_2m` | float8 | Температура |
| `relative_humidity_2m` | integer | Влажность |
| `dew_point_2m` | float8 | Точка росы |
| `apparent_temperature` | float8 | Ощущаемая температура |
| `precipitation` | float8 | Осадки |
| `rain` | float8 | Дождь |
| `snowfall` | float8 | Снег |
| `snow_depth` | float8 | Высота снега |
| `pressure_msl` | float8 | Давление |
| `surface_pressure` | float8 | Давление на поверхности |
| `cloud_cover` | integer | Облачность |
| `cloud_cover_low` | integer | Нижняя облачность |
| `cloud_cover_mid` | integer | Средняя облачность |
| `cloud_cover_high` | integer | Верхняя облачность |
| `wind_speed_10m` | float8 | Скорость ветра |
| `wind_speed_100m` | float8 | Скорость ветра на 100 м |
| `wind_direction_100m` | integer | Направление ветра |
| `wind_gusts_10m` | float8 | Порывы ветра |
| `has_bad_weather` | integer | Флаг плохой погоды |
| `road_surface` | text | Состояние покрытия |
| `light_conditions` | text | Условия освещения |
| `has_light_problems` | integer | Флаг проблем с освещением |
| `weather_conditions` | ARRAY | Погодные условия (массив) |
| `road_deficiencies` | ARRAY | Недостатки дороги (массив) |
| `traffic_scheme` | ARRAY | Схема движения |
| `traffic_objects` | ARRAY | Объекты на дороге |
| `temp_category` | text | Категория температуры |
| `precip_type` | text | Тип осадков |
| `light_category` | text | Категория освещения |
| `main_deficiency` | text | Основной недостаток дороги |
| `main_weather` | text | Основное погодное условие |

---

## Индексы

Для всех таблиц созданы индексы по ключевым полям (дата, город, kart_id и т.д.) для ускорения запросов. Полный список индексов см. в SQL-файлах в папке `sql/`.