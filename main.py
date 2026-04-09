import argparse
import asyncio


def main():
    # всё через субкоманды, например python main.py scrape cian
    parser = argparse.ArgumentParser(description="Real Estate DA pipeline")
    sub = parser.add_subparsers(dest="command")

    scrape = sub.add_parser("scrape", help="запуск скраперов")
    scrape_sub = scrape.add_subparsers(dest="target")
    cian_parser = scrape_sub.add_parser("cian", help="вторичка и новостройки с cian.ru")
    cian_parser.add_argument("--once", action="store_true", help="один прогон без loop")
    scrape_sub.add_parser("domrf", help="новостройки с наш.дом.рф")

    load = sub.add_parser("load", help="загрузка датасетов в БД")
    load_sub = load.add_subparsers(dest="target")
    load_sub.add_parser("kaggle", help="6 датасетов через Spark")
    load_sub.add_parser("angultiaev", help="angultiaev 162GB через remotezip")

    etl = sub.add_parser("etl", help="ETL-пайплайн Silver/Gold")
    etl_sub = etl.add_subparsers(dest="target")
    etl_sub.add_parser("silver", help="Bronze -> silver_listings")
    etl_sub.add_parser("gold", help="silver_listings -> Gold-витрины")
    etl_sub.add_parser("all", help="полный пайплайн Silver + Gold")
    etl_sub.add_parser("check", help="quality checks Silver + Gold")

    args = parser.parse_args()

    # ленивые импорты, чтобы не тянуть тяжелые зависимости пока не нужны
    if args.command == "scrape":
        if args.target == "cian":
            if getattr(args, "once", False):
                from scraper.cian import main as cian_main

                asyncio.run(cian_main())
            else:
                from scraper.cian import main_loop

                asyncio.run(main_loop())
        elif args.target == "domrf":
            from scraper.domrf import main as domrf_main

            asyncio.run(domrf_main())
        else:
            scrape.print_help()

    elif args.command == "load":
        if args.target == "kaggle":
            from db.kaggle.loader import main as kaggle_main

            kaggle_main()
        elif args.target == "angultiaev":
            from db.kaggle.angultiaev import main as angultiaev_main

            angultiaev_main()
        else:
            load.print_help()

    elif args.command == "etl":
        if args.target in ("silver", "gold", "all", "check"):
            import logging
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

            if args.target in ("silver", "all"):
                from etl.silver.pipeline import run_silver_etl

                run_silver_etl()
            if args.target in ("gold", "all"):
                from etl.gold.pipeline import run_gold_etl

                run_gold_etl()
            if args.target == "check":
                import psycopg2
                from config.settings import DB_CONFIG
                from etl.quality.checks import check_gold, check_silver

                conn = psycopg2.connect(**DB_CONFIG)
                cur = conn.cursor()
                check_silver(cur)
                check_gold(cur)
                cur.close()
                conn.close()
        else:
            etl.print_help()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
