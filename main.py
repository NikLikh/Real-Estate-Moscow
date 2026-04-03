import argparse
import asyncio


def main():
    # всё через субкоманды, например python main.py scrape cian
    parser = argparse.ArgumentParser(description="Real Estate DA pipeline")
    sub = parser.add_subparsers(dest="command")

    scrape = sub.add_parser("scrape", help="запуск скраперов")
    scrape_sub = scrape.add_subparsers(dest="target")
    scrape_sub.add_parser("cian", help="вторичка и новостройки с cian.ru")
    scrape_sub.add_parser("domrf", help="новостройки с наш.дом.рф")

    load = sub.add_parser("load", help="загрузка датасетов в БД")
    load_sub = load.add_subparsers(dest="target")
    load_sub.add_parser("kaggle", help="6 датасетов через Spark")
    load_sub.add_parser("angultiaev", help="angultiaev 162GB через remotezip")

    args = parser.parse_args()

    # ленивые импорты, чтобы не тянуть тяжелые зависимости пока не нужны
    if args.command == "scrape":
        if args.target == "cian":
            from scraper.cian import main as cian_main
            asyncio.run(cian_main())
        elif args.target == "domrf":
            from scraper.domrf import main as domrf_main
            domrf_main()
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

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
