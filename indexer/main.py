import argparse
import sys
import resource
from indexer import run

MEGABYTE = 1024 * 1024

def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '-m',
        dest='memory_limit',
        action='store',
        required=True,
        type=int,
        help='memory available'
    )
    parser.add_argument('-c', '--corpus', required=True, help='Path to a directory containing the corpus WARC files')
    parser.add_argument('-i', '--index', required=True, help='Path to the index file to be generated')
    args = parser.parse_args()

    memory_limit(args.memory_limit)
    try:
        run(args.corpus, args.index, args.memory_limit)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)