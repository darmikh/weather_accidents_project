# Автоматизация (GitHub Actions)

## Настройка

Файл: `.github/workflows/update_data.yml`

```yaml
name: Update DTP and Weather Data
on:
  schedule:
    - cron: '0 3 * * *'  # каждый день в 6:00 МСК
  workflow_dispatch:      # ручной запуск

> **Примечание:** Для GitHub Actions используется основная версия `gibdd_dtp_etl.py` (SQLAlchemy).  
> Альтернативная версия `gibdd_dtp_etl_rest.py` (REST API) предназначена для локального запуска при проблемах с подключением.