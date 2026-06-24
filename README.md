# Real Estate in Russia

Сбор и анализ рынка недвижимости Москвы и МО. Скрапер cian.ru пишет наблюдения в PostgreSQL (append-only), dbt строит слои raw -> stg -> dds -> marts. LightGBM скорит активные объявления на скорость продажи. Витрины отдают FastAPI + Streamlit. Отдельно грузятся исторические датасеты Kaggle.

## Структура

```
pipeline/        Python-код проекта
  core/          инфра БД: connection, apply, raw_repo, schema/ (raw DDL)
  cian/          парсер cian.ru, proxy_farm/, vpn_ext, extensions/
  kaggle/        загрузчики Kaggle
  ml/            обучение и скоринг модели, фичи, экспорт
config/          .env, scraper.yaml, settings.py
dbt/             модели stg/dds/marts, тесты
airflow/         DAG transform/ml_train/ml_score, Dockerfile
app/             backend (FastAPI) + frontend (Streamlit)
notebooks/       Jupyter (preprocessing читает marts)
data/            гео-справочники; jars/ JDBC-драйвер
```

## Пайплайн

```
scrape -> raw.cian_observations -> dbt (stg/dds/marts) -> ml_train -> ml_score -> hot_listings -> app
```

- scrape - один прогон скрапера пишет наблюдения в raw.cian_observations
- transform - dbt build пересобирает stg/dds/marts из raw
- ml_train - переобучение модели на закрытых объявлениях
- ml_score - применение модели к активным, запись marts.hot_listings
- app - backend читает marts и отдаёт данные frontend-у

Скрапер запускает Windows Task Scheduler: нужен GUI-браузер и VPN-расширения, в Linux-контейнере их нет. Обёртка - run_scrape.bat. Остальное оркестрирует Airflow (:8080, admin/admin):

- transform - @daily 20:00, dbt build
- ml_train - еженедельно (вс 21:00), python -m pipeline.ml.train
- ml_score - @daily 21:00, python -m pipeline.ml.score

ml_score читает витрину, которую собирает transform, поэтому ставится после него. ml_train запускается реже, скоринг ежедневно берёт hot_model_latest.

## Запуск

```bash
source .venv/Scripts/activate
cp .env.example .env
docker compose up -d
python -m pipeline.core.apply 
```

Загрузка данных:

```bash
python main.py scrape 
python main.py load kaggle
python main.py load angultiaev
```

Трансформации и модель прогоняются в контейнерах Airflow. Через DAG-и:

```bash
docker compose exec airflow-scheduler airflow dags trigger transform
docker compose exec airflow-scheduler airflow dags trigger ml_train
docker compose exec airflow-scheduler airflow dags trigger ml_score
```

Приложение после ml_score:

- backend - http://localhost:8000
- frontend - http://localhost:8501

## Слои данных

raw через python -m pipeline.core.apply (идемпотентно, CREATE IF NOT EXISTS); stg/dds/marts через dbt.

- raw.cian_observations - append-only лог наблюдений, скрапер пишет только сюда
- raw.kaggle_flats - датасеты Kaggle, источник в колонке source
- stg - типизация и чистка (stg_cian_observations)
- dds - звезда: dim_geo, dim_building; fact_listing_lifecycle (days_on_market, event_closed), fact_price_change
- marts:
  - ml_listings_wide - одна строка на объявление, фичи и таргет для ML
  - current_listings - активные объявления для дашборда
  - price_index_monthly - помесячная медиана цены за метр по сегментам
  - hot_listings - результат скоринга

## ML-модель

Задача - бинарная классификация: будет ли объявление закрыто в первые 14 дней (days_on_market < 14). Выход hot_score - вероятность быстрой продажи.

- данные: marts.ml_listings_wide, обучение на закрытых объявлениях (event_closed = 1), скоринг на активных (event_closed = 0)
- препроцессинг: sklearn Pipeline с FeatureBuilder (восстановление района по координатам, медианный импьют, ratio-фичи цены и площади), категориальные через TargetEncoder
- модель: LGBMClassifier (1500 деревьев, learning_rate 0.03, num_leaves 127), 55 фич
- метрики (test split 20%): pr_auc и roc_auc, пишутся в hot_model_meta.json
- артефакты в checkpoints/: hot_model_latest.joblib, hot_model_<date>.joblib, hot_model_meta.json
- ml_score применяет hot_model_latest к активным и пишет marts.hot_listings (cian_id, цена, price_per_m2, метро, hot_score)

## Источники

Kaggle:
- mrdaniilak/russia-real-estate-20182021
- mrdaniilak/russia-real-estate-2021
- egorkainov/moscow-housing-price-dataset
- romanbaster/sale-and-rental-of-russian-real-estate-in-4-cities
- ivan314sh/prices-of-moscow-apartments
- hishamhaydar/moscow-2018-housing-prices
- angultiaev/flat-sale-m24ml

cian.ru - вторичка и новостройки Москвы и МО.

## Прокси

По умолчанию direct IP. Для расширения пула pipeline/cian/proxy_farm/ подтягивает прокси из browsec/cyberghost/1clickvpn/monosans и валидирует кандидатов через cian. В РФ нужен VPN: публичные IP прокси-сервисов заблокированы.

## Стек

Python 3.13, PostgreSQL 16, Docker Compose, Airflow 2.10 (LocalExecutor), dbt-postgres. Скрапер: Patchright + curl_cffi. Kaggle-loader: PySpark + JDBC. ML: scikit-learn, LightGBM. Приложение: FastAPI + Streamlit. Анализ: pandas, numpy.
