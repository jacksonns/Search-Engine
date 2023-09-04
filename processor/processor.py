from multiprocessing import Pool
from queue import PriorityQueue
from math import floor
import signal
import time

from tokenizer import Tokenizer
from ranker import Ranker

MAX_PROCESSES_NUM = 100

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def get_process_index(query_list, path_to_index):
    words = []
    INDEX = {}
    for query in query_list:
        for token in Tokenizer.tokenize(query.strip()):
            words.append(token)
    words = sorted(list(set(words)))

    word = words.pop(0)
    file = open(path_to_index, 'r')
    line = file.readline()
    documents = []
    total_doc_length = 0
    while(line):
        token, doc_list = line.strip().split(maxsplit=1)
        if token == word:
            INDEX[word] = {}
            doc_list = list(map(eval, doc_list.strip(';').split(';')))
            for tuple in doc_list:
                INDEX[word][tuple[0]] = (tuple[1], tuple[2])
                documents.append(tuple[0])
                total_doc_length += tuple[2]
            if len(words) == 0:
                break
            else:
                word = words.pop(0)
        elif token > word:
            try:
                word = words.pop(0)
            except:
                break
        line = file.readline()
    file.close()
    return INDEX, list(set(documents)), total_doc_length

def process_queries(query_list, ranker, path_to_index):
    results = {}
    INDEX, doc_list, total_doc_length = get_process_index(query_list, path_to_index)
    num_docs = len(doc_list)
    if num_docs == 0: return results
    avg_doc_length = total_doc_length / num_docs
    for query in query_list:
        q = PriorityQueue()
        words = Tokenizer.tokenize(query)
        for doc in doc_list:
            rank = 0
            for word in words:
                if word in INDEX and doc in INDEX[word]:
                    freq = INDEX[word][doc][0]
                    doc_length = INDEX[word][doc][1]
                    rank += Ranker.get_document_rank(ranker, 
                                                    freq, num_docs,len(INDEX[word]),
                                                    doc_length, avg_doc_length)
            q.put((-round(rank,2), doc))
        results[query] = [q.get() for i in range(10)]
    return results

def json_output(results, doc_urls):
    for result in results:
        for query in result:
            print('{{ "Query": "{}",'.format(query))
            print('  "Results": [')
            cont = 0
            for (rank, doc_id) in result[query]:
                cont += 1
                if cont != 10:
                    print('    {{ "URL": "{}",\n      "Score": {} }},'.format(doc_urls[doc_id], -rank))
                else: 
                    print('    {{ "URL": "{}",\n      "Score": {} }} ] }}'.format(doc_urls[doc_id], -rank))
        print('')


class QueryProcessor:
    
    def __init__(self, path_to_index, path_to_queries, ranker):
        self.path_to_index = path_to_index
        self.path_to_queries = path_to_queries
        self.path_to_urls = 'urls'
        self.ranker = ranker
                
    def get_queries_lists(self):
        queries = []
        with open(self.path_to_queries, 'r') as file:
            for line in file:
                if line.strip() != '':
                    queries.append(line.strip())
        queries_per_process = floor(len(queries) / MAX_PROCESSES_NUM)
        if queries_per_process == 0: queries_per_process += 1
        return [list(t) for t in zip(*[iter(queries)] * queries_per_process)]

    def retrieve_urls(self, results):
        docs = []
        for result in results:
            for query in result:
                for (rank, doc_id) in result[query]:
                    if doc_id not in docs:
                        docs.append(doc_id)
        docs = sorted(docs)
        doc_urls = {}
        doc = docs.pop(0)
        with open(self.path_to_urls + '/urls.txt', 'r') as file:
            for line in file:
                doc_id, url = line.strip().split(maxsplit=1)
                if int(doc_id) == doc:
                    doc_urls[doc] = url
                    if len(docs) > 0: 
                        doc = docs.pop(0)
                    else: 
                        break
        return doc_urls

    def run(self):
        queries = self.get_queries_lists()
        pool = Pool(MAX_PROCESSES_NUM, init_worker)
        results = []
        try:
            for i in range(len(queries)):
                results.append(pool.apply_async(process_queries, args=(queries[i], self.ranker, self.path_to_index)))
            pool.close()
            pool.join()
            results = [r.get() for r in results]
        except KeyboardInterrupt:
            print ("Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
            pool.join()
        
        INDEX = {}
        doc_urls = self.retrieve_urls(results)
        json_output(results, doc_urls)