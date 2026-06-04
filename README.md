# Real Estate in Russia

Сбор и анализ рынка недвижимости Москвы и МО. Скрапер cian.ru пишет в PostgreSQL, отдельно подтягиваются исторические датасеты с Kaggle.

## Структура

```
config/          .env, scraper.yaml, settings.py
db/              схема, пул, репозиторий, загрузчики kaggle
scraper/         парсер cian.ru
proxy_farm/      пул прокси и источники (browsec, cyberghost, 1clickvpn, monosans)
extensions/      браузерные VPN-расширения для прокси-пула
notebooks/       Jupyter (preprocessing, анализ)
data/            гео-справочники (округа, районы, метро)
jars/            JDBC-драйвер для Spark
```

## Запуск

```bash
source .venv/Scripts/activate
cp .env.example .env               # вписать credentials
docker compose up -d                # PostgreSQL
python -m db.apply                  # развернуть схему

python main.py scrape                # парсинг cian в цикле
python main.py scrape --once         # один прогон без цикла
python main.py load kaggle           # загрузить датасеты с Kaggle
python main.py load angultiaev       # отдельно — angultiaev 162GB через remotezip
```

Скорость парсинга — около 10K объявлений в час до того, как WAF циана начнёт резать. С прокси можно выше.

## БД

Четыре таблицы:

- `listings` — живой срез, один ряд на объявление, PK по `cian_id`, lifecycle-поля (`is_active`, `last_seen_at`, `consecutive_misses`).
- `price_history` — append-only лог цен, ловит изменения между прогонами + историю из html.
- `listings_archive` — снапшоты и архивированные неактивные объявления, PK `(cian_id, snapshot_date)`.
- `kaggle_flats` — исторические датасеты, разные источники в одной таблице через колонку `source`.

`python -m db.apply` идемпотентный — гоняет все `db/schema/**/*.sql` через `CREATE ... IF NOT EXISTS`.

## Источники

Исторические (Kaggle):
- mrdaniilak/russia-real-estate-20182021
- mrdaniilak/russia-real-estate-2021
- egorkainov/moscow-housing-price-dataset
- romanbaster/sale-and-rental-of-russian-real-estate-in-4-cities
- ivan314sh/prices-of-moscow-apartments
- hishamhaydar/moscow-2018-housing-prices
- angultiaev/flat-sale-m24ml (162GB, грузится отдельной командой через remotezip)

Текущие:
- cian.ru — вторичка и новостройки Москвы и МО.

## Прокси

Из коробки парсер ходит с direct IP. Этого хватает, но медленно, так как циан режет один IP по rate limit, поэтому, чтобы расширить пул, `proxy_farm/` сам подтягивает прокси из browsec/cyberghost/1clickvpn/monosans, если включить нужные в `experimental_endpoint_types` в `config/scraper.yaml`. При старте `auto_discover` проверяет всех кандидатов, валидирует через cian и оставляет только живые уникальные IP. При запуске скрапера необходимо использовать VPN, так как в РФ публичные IP-адреса прокси-сервисов заблокированы

## Стек

Python 3.13, PostgreSQL 16, Docker Compose. Скрапер на Patchright (форк Playwright) + curl_cffi для HTTP. Kaggle-loader на PySpark с JDBC. Анализ — pandas, numpy.
