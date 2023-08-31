from multiprocessing import Pool
import sys
import resource
import argparse
import os
from time import time
from math import floor
import signal

from index_merge import IndexMerge
from tokenizer import Tokenizer

from warcio.archiveiterator import ArchiveIterator
from tqdm import tqdm

MAX_PROCESSES_NUM = 1

MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


def json_output(index_size, elapsed_time, lists_number, average_list_size):
    print('{{ "Index Size": {},'.format(round(index_size,2)))
    print('  "Elapsed Time": {},'.format(round(elapsed_time,2)))
    print('  "Number of Lists": {},'.format(lists_number))
    print('  "Average List Size": {} }}'.format(round(average_list_size,2)))

def get_memory_usage():
    with open('/proc/self/status') as f:
        memusage = f.read().split('VmRSS:')[1].split('\n')[0][:-3]
    return int(memusage.strip())

def get_main_process_memory_limit(limit):
    return limit - (MAX_PROCESSES_NUM * floor(limit / (MAX_PROCESSES_NUM + 1)))

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


# Implementation of indexer
class Indexer:

    def __init__(self, path_to_corpus, memory_limit):
        self.path_to_corpus = path_to_corpus
        self.path_to_partial_indexes = 'partial_indexes'
        self.path_to_urls = 'urls'
        self.memory_limit = floor(memory_limit / (MAX_PROCESSES_NUM + 1))
        self.index = {}

        self.doc_id_counter = 0
        self.partial_index_counter = 0

        self.MAX_INDEX_SIZE = 0.5 * self.memory_limit * 1024
    
    def merge_urls(self):
        url0 = open(self.path_to_urls + '/urls0.txt', 'a')
        with open(self.path_to_urls + '/urls1.txt', 'r') as url1:
            for line in url1:
                url0.write(line)
        url0.close()
        os.replace(self.path_to_urls + '/urls0.txt', self.path_to_urls + '/urls.txt')
        os.remove(self.path_to_urls + '/urls1.txt')

    def write_index_on_disk(self, process_id):
        self.partial_index_counter += 1
        partial_index_file = 'partial_idx_{}.{}.txt'.format(process_id, self.partial_index_counter)

        with open(os.path.join(self.path_to_partial_indexes, partial_index_file), 'w+') as file:
            for token in sorted(self.index.keys()):
                file.write('{} '.format(token))
                for tuple in self.index[token]:
                    file.write('{};'.format(tuple))
                file.write('\n')
        self.index = {}

    def create_index(self, url, body, process_id):
        url_file = 'urls{}.txt'.format(process_id)
        with open(os.path.join(self.path_to_urls, url_file), 'a') as file:
            file.write('{} {}\n'.format(self.doc_id_counter, url))

        token_freq_list, doc_size = Tokenizer.get_token_freq_list(body)
        for (token, freq) in token_freq_list:
            if token not in self.index:
                self.index[token] = []
            self.index[token].append((self.doc_id_counter, freq, doc_size))

        if (self.doc_id_counter + 1) % 4000 == 0:
            self.write_index_on_disk(process_id) 
    
    def read_file(self, file_list, process_id):
        file_list_size = len(file_list)
        cont = 0
        for file in file_list:
            with open(os.path.join(self.path_to_corpus, file), 'rb') as stream:
                try:
                    for record in tqdm(ArchiveIterator(stream), position=process_id, desc=file):
                        if record.rec_type == 'response':
                            url = record.rec_headers.get_header('WARC-Target-URI')
                            body = record.raw_stream.read().decode('utf-8')
                            self.doc_id_counter = cont + (process_id * file_list_size * 10000)
                            self.create_index(url, body, process_id)
                            cont += 1
                except:
                    pass
        if self.index:
            self.write_index_on_disk(process_id)

    def start_process(self, file_list, process_id):
        memory_limit(self.memory_limit)
        self.read_file(file_list, process_id)

    def run_indexer(self):
        if not os.path.exists(self.path_to_partial_indexes):
            os.makedirs(self.path_to_partial_indexes)
        if not os.path.exists(self.path_to_urls):
            os.makedirs(self.path_to_urls)
        if os.path.exists(self.path_to_urls + '/urls.txt'):
            os.remove(self.path_to_urls + '/urls.txt')

        file_list = os.listdir(self.path_to_corpus)
        files_per_process = floor(len(file_list) / MAX_PROCESSES_NUM)
        files = [list(t) for t in zip(*[iter(file_list)] * files_per_process)]

        pool = Pool(MAX_PROCESSES_NUM, init_worker)
        try:
            for i in range(MAX_PROCESSES_NUM):
                pool.apply_async(self.start_process, args=(files[i], i))
            pool.close()
            pool.join()
        except KeyboardInterrupt:
            print ("Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
            pool.join()


# Start indexer, then merge the partial indexes generated and outputs the index information
def main(path_to_corpus, path_to_index, memory_limit):
    indexer = Indexer(path_to_corpus, memory_limit)
    index_merge = IndexMerge(path_to_index, indexer.path_to_partial_indexes)

    start = time()
    indexer.run_indexer()
    index_merge.run_index_merge()
    end = time()
    indexer.merge_urls()

    index_size_bytes, lists_number, list_size = index_merge.get_data()

    json_output(index_size_bytes / MEGABYTE, end - start, lists_number, list_size / lists_number)


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
        main(args.corpus, args.index, args.memory_limit)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)