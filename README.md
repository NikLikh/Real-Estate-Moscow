# Real Estate in Russia

Анализ рынка недвижимости Москвы и МО: сбор данных, обогащение, анализ цен и трендов.

## Структура

```
config/          конфиг проекта, .env, scraper.yaml
db/              PostgreSQL: схема, пул, CRUD, загрузчики kaggle
scraper/         парсеры cian.ru и наш.дом.рф
notebooks/       Jupyter-ноутбуки (предобработка, анализ)
tests/           юнит-тесты
data/            гео-справочники (округа, районы, метро)
jars/            Spark JDBC driver
```

## Запуск

```bash
source .venv/Scripts/activate
cp .env.example .env               # заполнить credentials
docker compose up -d                # MinIO + PostgreSQL
cat db/init.sql | docker exec -i postgres psql -U user -d real_estate

python main.py scrape cian          # парсинг cian.ru (скорость ~10000 объявлений в час до вылета rate_limit)
python main.py scrape domrf         # парсинг наш.дом.рф
python main.py load kaggle          # загрузка 6 kaggle-датасетов (Spark)
python main.py load angultiaev      # загрузка angultiaev 162GB (remotezip)
```

## Источники данных

**Исторические (Kaggle)**
- mrdaniilak/russia-real-estate-20182021
- mrdaniilak/russia-real-estate-2021
- egorkainov/moscow-housing-price-dataset
- romanbaster/sale-and-rental-of-russian-real-estate-in-4-cities
- ivan314sh/prices-of-moscow-apartments
- hishamhaydar/moscow-2018-housing-prices
- angultiaev/flat-sale-m24ml

**Текущие**
- cian.ru (вторичка и новостройки)
- наш.дом.рф (новостройки)

## Прокси (опционально)

Парсер работает через direct IP из коробки. Для обхода rate limit можно подключить дополнительные endpoints, при запуске `auto_discover` проверит доступные и оставит уникальные IP:

- **VLESS/V2Ray** - любой SOCKS5 прокси. Указать порт в `.env` (`VLESS_SOCKS_PORT`) и `config/scraper.yaml` (`vless_socks_port`)
- **VDS SSH tunnel** - SOCKS5 через SSH. Указать `VDS_HOST`, `VDS_USER` в `.env`, туннель поднимется автоматически. Первый раз настроить SSH-ключ: `ssh-keygen -t ed25519 && ssh-copy-id user@host`

Без прокси парсер будет работать медленнее из-за rate limit на один IP, но всё равно соберёт данные.

## Стек

- Python 3.13, pandas, numpy, scikit-learn, PySpark
- Patchright (Playwright fork), BeautifulSoup
- PostgreSQL 16, MinIO, Docker Compose
