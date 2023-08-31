import argparse
import sys
import time

from crawler import Crawler

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-s', '--seeds', help='Path to file containing seeds')
    arg_parser.add_argument('-n', '--limit', help='Number of pages to be crawled')
    arg_parser.add_argument('-d', '--debug', action='store_true', help='Debug mode')
    args = arg_parser.parse_args()
    
    if not args.seeds or not args.limit:
        print('Provide -s and -n arguments')
        sys.exit(1)

    crawler = Crawler(args.seeds, args.limit, args.debug)
    crawler.run_crawler()