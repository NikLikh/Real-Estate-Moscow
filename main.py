import argparse
import asyncio


def main():
    # всё запускается через субкоманды, например python main.py scrape
    parser = argparse.ArgumentParser(description="Real Estate DA pipeline")
    sub = parser.add_subparsers(dest="command")

    scrape = sub.add_parser("scrape", help="парсинг cian.ru")
    scrape.add_argument("--once", action="store_true", help="один прогон без loop")

    load = sub.add_parser("load", help="загрузка датасетов в БД")
    load_sub = load.add_subparsers(dest="target")
    load_sub.add_parser("kaggle", help="датасеты kaggle")
    load_sub.add_parser("angultiaev", help="angultiaev 162GB через remotezip")

    backfill = sub.add_parser("backfill", help="дотянуть новые поля по существующим cian_id")
    backfill.add_argument("--limit", type=int, default=None)

    args = parser.parse_args()

    # ленивые импорты, чтобы не тянуть тяжелые зависимости пока не нужны
    if args.command == "scrape":
        if args.once:
            from scraper.cian import main as cian_main

            asyncio.run(cian_main())
        else:
            from scraper.cian import main_loop

            asyncio.run(main_loop())

    elif args.command == "load":
        if args.target == "kaggle":
            from db.kaggle.loader import main as kaggle_main

            kaggle_main()
        elif args.target == "angultiaev":
            from db.kaggle.angultiaev import main as angultiaev_main

            angultiaev_main()
        else:
            load.print_help()

    elif args.command == "backfill":
        from scraper.backfill import run as backfill_run

        asyncio.run(backfill_run(limit=args.limit))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
