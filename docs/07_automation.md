# Автоматизация (GitHub Actions)

## Настройка

Файл: `.github/workflows/update_data.yml`

```yaml
name: Update DTP and Weather Data
on:
  schedule:
    - cron: '0 3 * * *'  # каждый день в 6:00 МСК
  workflow_dispatch:      # ручной запуск