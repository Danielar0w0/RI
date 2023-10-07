import json
import math
import time
from typing import List, Dict

from collections import Counter
from index import InvertedIndex
from reader import QuestionsReader
from tokenizers import PubMedTokenizer
from utils import dynamically_init_class
from utils import term_frequency_weighting, document_frequency_weighting, normalize_weights


def dynamically_init_searcher(**kwargs):
    """Dynamically initializes a Tokenizer object from this
    module.

    Parameters
    ----------
    kwargs : Dict[str, object]
        python dictionary that holds the variables and their values
        that are used as arguments during the class initialization.
        Note that the variable `class` must be here and that it will
        not be passed as an initialization argument since it is removed
        from this dict.
    
    Returns
        ----------
        object
            python instance
    """
    return dynamically_init_class(__name__, **kwargs)


class BaseSearcher:

    def search(self, index: InvertedIndex, query_tokens: List[str], top_k: int) -> tuple[dict, int]:
        """
        Searches the index for the given query tokens and returns the top k results.
        Should be implemented by the subclasses.
        """
        raise NotImplementedError()

    def batch_search(self, index: InvertedIndex, reader: QuestionsReader, tokenizer: PubMedTokenizer, output_file: str,
                     top_k=1000):

        # Start reading the documents (= questions)
        while reader.has_queries_to_read():

            # Read the documents
            queries = reader.read()
            reader.purge_data()

            for query in queries:
                query_results, query_processing_time, total_results = self.__process_query(queries[query], index,
                                                                                           tokenizer, top_k)
                self.show_results(queries[query], query_results, query_processing_time, total_results)
                self.save_results(queries[query], query_results, output_file)

    def interactive_search(self, index: InvertedIndex, tokenizer: PubMedTokenizer, output_file: str,
                           top_k=1000):

        current_page = 0
        query_results = {}
        query_processing_time = 0
        total_results = 0

        while True:

            query = input("Write a Query: ")

            if query == "[next]":
                current_page += 1
                self.show_results(query, query_results, query_processing_time, total_results, pagination=True,
                                  page=current_page)
                continue

            if query == "[quit]":
                print("Quiting...")
                break

            query_results, query_processing_time, total_results = self.__process_query(query, index, tokenizer, top_k)
            self.show_results(query, query_results, query_processing_time, total_results, pagination=True,
                              page=current_page)
            self.save_results(query, query_results, output_file)

    def __process_query(self, query: str, index: InvertedIndex, tokenizer: PubMedTokenizer, top_k: int) -> (
            dict, float):

        start_time = time.time()

        # Tokenize the document terms
        terms = tokenizer.tokenize(query)

        # Apply search
        results, total_results_count = self.search(index, terms, top_k)
        if not results:
            return

        # Record query processing time
        query_processing_time = time.time() - start_time

        return results, query_processing_time, total_results_count

    @staticmethod
    def save_results(query: str, results: Dict[str, float], output_file: str):

        query_name = "_" + query.replace(" ", "_")

        with open(output_file + f"/query{query_name}.json", "w") as f:
            query_json = {"query": query}
            f.write(json.dumps(query_json) + "\n")

            for document in results:
                result_json = {"doc_id": document, "score": results[document]}
                f.write(json.dumps(result_json) + "\n")

    @staticmethod
    def show_results(query: str, results: Dict[str, float], query_time: float, total_results: int,
                     pagination: bool = False, page: int = 0, results_per_page: int = 10):

        resulting_documents = list(results.keys())

        if pagination:
            min_result_idx = page * results_per_page
            max_result_idx = min([page * results_per_page + results_per_page, len(resulting_documents)])
            results_to_present = resulting_documents[min_result_idx:max_result_idx]
        else:
            min_result_idx = 0
            max_result_idx = len(resulting_documents)
            results_to_present = resulting_documents

        print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
        print()
        print(f"Query: {query}")
        print()
        print(f" {total_results} results found in {query_time} seconds")
        print(f" [!] Limited to first {len(results)} results by Top_K")
        print()

        if len(results_to_present) > 0:
            print(" Documents: ")
            for document_id in results_to_present:
                print(f"  * {document_id} - Score {results[document_id]}")
            print()
            print(f"Presented results {min_result_idx} - {max_result_idx} - Page {page + 1}.")
        else:
            print(" This page doesn't exist - no more documents were found.")

        if pagination:
            print()
            print("Write '[next]' to see next page.")
            print("Write '[quit]' to quit.")

        print()
        print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

    @staticmethod
    def clear_query(original_query_tokens, indexed_terms) -> List[str]:

        # Create a copy of the query tokens to avoid modifying the original list
        query_tokens = original_query_tokens.copy()

        for query_term in original_query_tokens:
            if query_term not in indexed_terms:
                query_tokens.remove(query_term)
                print(f"Query token {query_term} not found in collection.")

        return query_tokens


class TFIDFRanking(BaseSearcher):

    def __init__(self, smart, **kwargs) -> None:
        super().__init__(**kwargs)
        self.smart = smart
        print("init TFIDFRanking|", f"{smart=}")
        if kwargs:
            print(f"{self.__class__.__name__} also caught the following additional arguments {kwargs}")

    def search(self, index: InvertedIndex, original_query_tokens: List[str], top_k: int) -> tuple[dict, int]:

        # Initialize results
        results = {}

        indexed_terms = index.get_terms()
        documents_info = index.get_documents_info()

        # Remove query tokens that aren't found in the collection
        query_tokens = self.clear_query(original_query_tokens, indexed_terms)

        # If no query token is found in collection
        if len(query_tokens) == 0:
            return {}, 0

        # Obtain documents lengths
        documents_norms = {str(doc_id): documents_info.documents[doc_id][0] for doc_id in
                           documents_info.documents.keys()}

        # Calculate the number of documents
        N = documents_info.documents_count

        # Get terms frequency for the query
        terms_frequency = dict(Counter(query_tokens))

        # Get document frequency for each term
        document_frequency = {term_frequency_key: len(indexed_terms[term_frequency_key])
                              for term_frequency_key, term_frequency_value in terms_frequency.items()}

        query_normalized_weights = self.calculate_query_normalized_weights(self.smart.split(".")[1], terms_frequency,
                                                                           document_frequency, N)

        terms = index.get_terms()
        for query_term in query_tokens:

            for document in terms[query_term]:

                # Same document frequency as query
                document_normalized_weights = self.calculate_document_normalized_weights(self.smart.split(".")[0],
                                                                                         query_term, document, terms,
                                                                                         document_frequency,
                                                                                         documents_norms,
                                                                                         N,
                                                                                         documents_info.cache_status[0])

                document_score = sum(document_normalized_weights[i] * query_normalized_weights[i]
                                     for i in document_normalized_weights.keys())

                # Add document score to results
                if document not in results:
                    results[document] = 0
                results[document] += document_score

        # Sort results by score
        results = dict(sorted(results.items(), key=lambda item: item[1], reverse=True))

        # Return top k results
        return {k: results[k] for k in list(results)[:top_k]}, len(results)

    @staticmethod
    def calculate_query_normalized_weights(ranking_variants, terms_frequency, document_frequency,
                                           documents_count) -> dict:

        # Obtain ranking variants
        tf = ranking_variants[0]
        df = ranking_variants[1]
        norm = ranking_variants[2]

        # Returns the term frequency 'weighted', i.e., if we are using 'ltc', the term frequency is 'l'
        # and, therefore, this dict transforms the terms_frequency using 1 + log10(tf)
        weighted_terms_frequency = term_frequency_weighting(tf, terms_frequency)
        # print("Weighted terms frequency: ", weighted_terms_frequency)

        # Returns the document frequency 'weighted', i.e., if we are using 'ltc', the document frequency is 't'
        # and, therefore, this dict transforms the document frequency in IDF
        weighted_documents_frequency = document_frequency_weighting(df, document_frequency, documents_count)
        # print("Weighted documents frequency: ", weighted_documents_frequency)

        # Calculate absolute weights
        terms_weights = {term: (weighted_terms_frequency[term] * weighted_documents_frequency[term])
                         for term in weighted_terms_frequency}

        # Calculate normalized weights with normalization norm
        normalized_weights = normalize_weights(norm, terms_weights)

        return normalized_weights

    @staticmethod
    def calculate_document_normalized_weights(ranking_variants, query_term, document, terms, document_frequency,
                                              documents_norms, document_count, cache_status) -> dict:

        tf = ranking_variants[0]
        df = ranking_variants[1]
        norm = ranking_variants[2]

        # Get terms frequency for the document
        if cache_status:
            weighted_terms_frequency = terms[query_term][document]

        else:
            terms_frequency = {query_term: terms[query_term][document]}
            weighted_terms_frequency = term_frequency_weighting(tf, terms_frequency)

        weighted_documents_frequency = document_frequency_weighting(df, document_frequency, document_count)

        # Calculate absolute weights
        terms_weights = {term: (weighted_terms_frequency[term] * weighted_documents_frequency[term]) for term in
                         weighted_terms_frequency}

        # Calculate normalized weights
        if norm == "c":
            document_norm = documents_norms[document]
            document_normalized_weights = {term: terms_weights[term] / document_norm for term in terms_weights}
        elif norm == "n":
            document_normalized_weights = terms_weights
        else:
            raise NotImplementedError(f"Normalization {norm} not implemented.")

        return document_normalized_weights


class BM25Ranking(BaseSearcher):

    def __init__(self, k1, b, **kwargs) -> None:
        super().__init__(**kwargs)
        self.k1 = k1
        self.b = b
        print("init BM25Ranking|", f"{k1=}", f"{b=}")
        if kwargs:
            print(f"{self.__class__.__name__} also caught the following additional arguments {kwargs}")

    def search(self, index: InvertedIndex, original_query_tokens: List[str], top_k: int) -> tuple[dict, int]:

        indexed_terms = index.get_terms()
        documents_info = index.get_documents_info()

        # Remove query tokens that aren't found in the collection
        query_tokens = self.clear_query(original_query_tokens, indexed_terms)

        # If no query token is found in collection
        if len(query_tokens) == 0:
            return {}, 0

        if documents_info.cache_status[1]:
            results = {}
            for term in query_tokens:

                # Get the postings list for the term
                postings_list = indexed_terms[term]

                for document in postings_list.keys():

                    # Obtain cached rsv
                    rsv = postings_list[document]

                    # Add the rsv to the results
                    if document not in results:
                        results[document] = 0
                    results[document] += rsv

        else:
            # Obtain documents lengths
            documents_lengths = {str(doc_id): documents_info.documents[doc_id][1] for doc_id in
                                 documents_info.documents.keys()}

            # Calculate the number of documents
            N = documents_info.documents_count

            # Calculate average document length
            avgdl = self.average_document_length(documents_info.documents)

            results = {}
            for term in query_tokens:

                # Get the postings list for the term
                postings_list = indexed_terms[term]

                # Calculate idf for current term
                df = len(postings_list)
                idf = math.log10(N / df)

                for document in postings_list.keys():

                    tf = postings_list[document]
                    dl = documents_lengths[document]

                    # print(f"Term: {term}, Document: {document}, N: {N}, dl: {dl}, tf: {tf}, df: {df}")

                    # Calculate retrieval status value
                    rsv = self.calculate_rsv(idf, tf, dl, avgdl)

                    # Add the rsv to the results
                    if document not in results:
                        results[document] = 0
                    results[document] += rsv

        # Sort results by score
        results = dict(sorted(results.items(), key=lambda item: item[1], reverse=True))

        # Return top k results
        return {k: results[k] for k in list(results)[:top_k]}, len(results)

    def calculate_rsv(self, idf: float, tf: int, dl: int, avgdl: float) -> float:

        # Document length normalization
        B = ((1 - self.b) + self.b * (dl / avgdl))

        # Retrieval status value
        return idf * ((self.k1 + 1) * tf) / (self.k1 * B + tf)

    @staticmethod
    def average_document_length(documents: Dict[int, tuple]) -> float:

        # Returns the average document length for given documents
        return sum(documents[doc_id][1] for doc_id in documents.keys()) / len(documents)
