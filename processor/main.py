import argparse
import os
import sys
from processor import QueryProcessor

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')

    parser.add_argument('-i', '--index', required=True, help='Path to the index file')
    parser.add_argument('-q', '--queries', required=True, help='Path to a file with a list of queries')
    parser.add_argument('-r', '--ranker', required=True, help='Ranking function: <TFTIDF> or <BM25>')

    args = parser.parse_args()

    if not os.path.exists(args.index):
        sys.stderr.write('\nERROR: Index file not found\n\n')
        sys.exit(1)
    if not os.path.exists(args.queries):
        sys.stderr.write('\nERROR: Queries file not found\n\n')
        sys.exit(1)
    if args.ranker.lower() != 'tfidf' and args.ranker.lower() != 'bm25':
        sys.stderr.write('\nERROR: Invalid ranking function\n\n')
        sys.exit(1)

    query_processor = QueryProcessor(args.index, args.queries, args.ranker)
    query_processor.run()