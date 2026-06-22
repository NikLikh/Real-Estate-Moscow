# Real Estate in Russia

Сбор и анализ рынка недвижимости Москвы и МО. Скрапер cian.ru пишет наблюдения в PostgreSQL (append-only), dbt строит слои raw -> stg -> dds -> marts. Отдельно грузятся исторические датасеты Kaggle.

## Структура

```
pipeline/        Python-код проекта
  core/          инфра БД: connection, apply, raw_repo, schema/ (raw DDL)
  cian/          парсер cian.ru, proxy_farm/, extensions/
  kaggle/        загрузчики Kaggle
config/          .env, scraper.yaml, settings.py
dbt/             модели stg/dds/marts, тесты
airflow/         DAG transform, Dockerfile
notebooks/       Jupyter (preprocessing читает marts)
data/            гео-справочники; jars/ JDBC-драйвер
```

## Запуск

```bash
source .venv/Scripts/activate
cp .env.example .env
docker compose up -d                 # PostgreSQL + Airflow (:8080)
python -m pipeline.core.apply        # схема raw

python main.py scrape                # один прогон -> raw.cian_observations
python main.py load kaggle           # Kaggle -> raw.kaggle_flats
python main.py load angultiaev       # отдельный скрипт для обработки источника с .png
```

## Расписание

Скрапер запускает Windows Task Scheduler: нужен GUI-браузер и VPN-расширения, в Linux-контейнере их нет. Обёртка - run_scrape.bat
Airflow (:8080, admin/admin) держит только трансформации: DAG transform (@daily) гоняет dbt build. Время ставить после скрейпа.

## Слои данных

raw через python -m pipeline.core.apply (идемпотентно, CREATE IF NOT EXISTS); stg/dds/marts через dbt.

- raw.cian_observations - append-only лог наблюдений, скрапер пишет только сюда
- raw.kaggle_flats - датасеты Kaggle, источник в колонке source
- stg - типизация и чистка (stg_cian_observations)
- dds - звезда: dim_geo, dim_building; fact_listing_lifecycle (days_on_market, event_closed), fact_price_change
- marts - ml_listings_wide (витрина для ML), market_daily

## Источники

Kaggle:
- mrdaniilak/russia-real-estate-20182021
- mrdaniilak/russia-real-estate-2021
- egorkainov/moscow-housing-price-dataset
- romanbaster/sale-and-rental-of-russian-real-estate-in-4-cities
- ivan314sh/prices-of-moscow-apartments
- hishamhaydar/moscow-2018-housing-prices
- angultiaev/flat-sale-m24ml (162GB, через remotezip)

cian.ru - вторичка и новостройки Москвы и МО.

## Прокси

По умолчанию direct IP. Для расширения пула pipeline/cian/proxy_farm/ подтягивает прокси из browsec/cyberghost/1clickvpn/monosans - включаются в experimental_endpoint_types в config/scraper.yaml. auto_discover валидирует кандидатов через cian. В РФ нужен VPN: публичные IP прокси-сервисов заблокированы.

## Стек

Python 3.13, PostgreSQL 16, Docker Compose, Airflow 2.10 (LocalExecutor), dbt-postgres. Скрапер: Patchright + curl_cffi. Kaggle-loader: PySpark + JDBC. Анализ: pandas, numpy.
