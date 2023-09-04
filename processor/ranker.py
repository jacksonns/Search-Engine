from math import log

k = 1.5
b = 0.75

class Ranker:

    def BM25(t_freq, num_docs, num_docs_with_t, doc_length, avg_doc_length):
        tf = t_freq / doc_length
        norm = tf * (k + 1) / (tf + k *( (1-b) + b * doc_length / avg_doc_length) )
        idf = log( ((num_docs - num_docs_with_t + 0.5) / (num_docs_with_t + 0.5)) + 1 )
        return idf * norm

    def TFIDF(t_freq, num_docs, num_docs_with_t, doc_length):
        tf = t_freq / doc_length
        idf = log( ((num_docs - num_docs_with_t + 0.5) / (num_docs_with_t + 0.5)) + 1 )
        return tf * idf

    def get_document_rank(ranker, t_freq, num_docs, num_docs_with_t, doc_length, avg_doc_length):
        if ranker.lower() == 'bm25':
            return Ranker.BM25(t_freq, num_docs, num_docs_with_t, doc_length, avg_doc_length)
        elif ranker.lower() == 'tfidf':
            return Ranker.TFIDF(t_freq, num_docs, num_docs_with_t, doc_length)