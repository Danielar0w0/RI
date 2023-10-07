"""
Base template created by: Tiago Almeida & Sérgio Matos
Authors: Daniela Dias (nMec 98039), Hugo Gonçalves (nMec 98497)

Indexer module

Holds the code/logic addressing the Indexer class
and the index management.

"""
import math
import os
import json
import time
from collections import Counter

import utils
from utils import *
from entities import Document
from memory_manager import MemoryManager

def dynamically_init_indexer(**kwargs):
    """Dynamically initializes a Indexer object from this
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


class Indexer:
    """
    Top-level Indexer class
    
    This loosely defines a class over the concept of
    an index.

    """

    def __init__(self,
                 index_instance,
                 **kwargs):
        super().__init__()
        self._index = index_instance

    def get_index(self):
        return self._index

    def build_index(self, reader, tokenizer, index_output_folder):
        """
        Holds the logic for the indexing algorithm.
        
        This method should be implemented by more specific sub-classes
        
        Parameters
        ----------
        reader : Reader
            a reader object that knows how to read the collection
        tokenizer: Tokenizer
            a tokenizer object that knows how to convert text into
            tokens
        index_output_folder: str
            the folder where the resulting index or indexes should
            be stored, with some additional information.
            
        """
        raise NotImplementedError()

    def save_partial_index(self, index_output_folder):
        """
        Saves the index to disk.

        Parameters
        ----------
        index_output_folder: str
            the folder where the resulting index or indexes should
            be stored, with some additional information.
        """

        raise NotImplementedError()

    def merge_partial_indexes(self, partial_indexes_output_folder, final_index_output_dir):
        """
        Merges the partial indexes into a single index.
        """

        raise NotImplementedError()


class SPIMIIndexer(Indexer):
    """
    The SPIMIIndexer represents an indexer that
    holds the logic to build an index according to the
    SPIMI algorithm.
    """

    def __init__(self,
                 posting_threshold,
                 memory_threshold,
                 partial_index_subdir,
                 final_index_subdir,
                 **kwargs):

        # Let's suppose that the SPIMI Index uses the inverted index, so
        # it initializes this type of index

        super().__init__(InvertedIndex(), **kwargs)
        print("init SPIMIIndexer|", f"{posting_threshold=}, {memory_threshold=}")

        if kwargs:
            print(f"{self.__class__.__name__} also caught the following additional arguments {kwargs}")

        # Available memory in Bytes
        self.memory_threshold = memory_threshold * 1024 * 1024

        self.partial_index_subdir = partial_index_subdir
        self.final_index_subdir = final_index_subdir

        self.memory_manager = MemoryManager(self.memory_threshold, 80)

        self.rankings = {}

        if kwargs:
            self.rankings = {x: kwargs[x] for x in kwargs.keys() if x in ["tfidf", "bm25"]}

        self.bm25_cache = kwargs["bm25"]["cache_in_disk"]
        self.tfidf_cache = kwargs["tfidf"]["cache_in_disk"]

        if self.bm25_cache and self.tfidf_cache:
            raise RuntimeError

        self.documents_info = DocumentsInfo((self.tfidf_cache, self.bm25_cache,))
        self.rsv_cache = {}

    def build_index(self, reader, tokenizer, index_output_dir):

        # Start recording indexing time
        indexing_time_start = time.time()

        # Create path for Sub Directory that will contain partial index files
        partial_index_output_dir = os.path.join(index_output_dir, self.partial_index_subdir)
        final_index_output_dir = os.path.join(index_output_dir, self.final_index_subdir)

        # Clear partial indexes directory
        utils.remove_non_empty_dir(partial_index_output_dir)

        # Clear final indexes directory
        utils.remove_non_empty_dir(final_index_output_dir)

        # Set the directory where the indexes will be stored
        self.get_index().set_index_directory(final_index_output_dir)

        # Set tokenizer parameters
        self.get_index().set_tokenizer_kwargs(tokenizer.get_tokenizer_kwargs())

        # Start reading the collection
        while reader.has_documents_to_read():

            # Read the documents
            documents = reader.read()
            reader.purge_data()

            document_count = 0

            for document_id in documents:

                document_count += 1

                # Tokenize the document terms
                terms = tokenizer.tokenize(documents[document_id])

                # Get terms frequency for the document
                terms_frequency = dict(Counter(terms))

                if "tfidf" in self.rankings:
                    self.calculate_norm(terms_frequency, document_id)

                if "bm25" in self.rankings:
                    self.calculate_length(terms_frequency, document_id)

                for term in terms:
                    # Add term and posting (docID) to the current index
                    self.get_index().add_term(term, {document_id: terms_frequency[term]})

                # Displays indexing real-time statistics
                docs_reading_percentage = round(document_count * 100 / len(documents), 1)

                # Update used/reserved memory
                mem_used = self.get_index().get_terms().__sizeof__() + documents.__sizeof__()
                self.memory_manager.update_used_memory(mem_used)

                # Obtain used/reserved memory percentage
                mem_used_percentage = round(self.memory_manager.get_used_memory_percentage(), 1)

                print("[-] Analysing Documents: Documents Read - {0} % out of {1} document(s); Reserved Memory - {2} % "
                      .format(docs_reading_percentage, len(documents), mem_used_percentage), end='\r')

                # Occupied memory shouldn't be greater than 80% of the available memory
                if not self.memory_manager.has_available_memory():
                    print()
                    print("[!] Memory Limit Reached. Writing index to disk & dumping memory...")
                    # Reset index - sort, save to disk, clear memory
                    self.reset_index(partial_index_output_dir)

            print()

        # Clear last memory
        if self.get_index().get_terms() != {}:
            # Reset index - sort, save to disk, clear memory
            self.reset_index(partial_index_output_dir)

        # Save documents info
        self.documents_info.save_to_disk("pubmedSPIMIIndex/documents_info.jsonl")

        # Save tokenizer info
        self.save_tokenizer_info("pubmedSPIMIIndex")

        print()
        print("[!] Starting SPIMI Merge...")

        # Start merging indexes
        start = time.time()
        self.merge_partial_indexes(partial_index_output_dir, final_index_output_dir)
        end = time.time()

        # Save merging time
        self.get_index().save_merge_time(end - start)

        indexing_time_end = time.time()

        self.get_index().save_total_index_time(indexing_time_end-indexing_time_start)

    def reset_index(self, index_output_dir):

        """
        Reset the In-memory Index - sort, save to disk and clear memory

        Parameters
        ----------
        index_output_dir the output directory where the index will be stored into

        Returns None
        -------
        """
        # Sort the index
        self.get_index().sort()
        # Save the partial index to disk
        self.save_partial_index(index_output_dir)
        # Clear memory
        self.get_index().purge_data()

    def merge_partial_indexes(self, partial_indexes_output_dir, final_index_output_dir):

        """
        Merge SPIMI partial indexes into final inverted index.
        """

        merge_completed = False

        # Structure to store the smallest term of each partial index
        temp_index = {}

        # Structure to store open files (partial indexes)
        files = {}

        partial_id = 0
        for path in os.scandir(partial_indexes_output_dir):
            if path.is_file():
                partial_index_file = open(path, "r")

                # Obtain first line
                line = partial_index_file.readline()

                # Obtain (first) term and postings list
                term = json.loads(line)["term"]
                postings = json.loads(line)["postings"]

                # Temporarily store term and postings list
                temp_index[partial_id] = {term: postings}

                # Store file
                files.update({partial_id: partial_index_file})

                # Update partial index id
                partial_id += 1

        while not merge_completed:

            # Obtain the smallest term (first in alphabetical order)
            smallest_index = min(temp_index, key=lambda x: list(temp_index[x].keys())[0])

            # Obtain the term and postings list
            smallest_term = list(temp_index[smallest_index].keys())[0]

            # Fetch all IDs of partial indexes that contain the same term in temp_index
            smallest_partial_ids = []
            for partial_id in temp_index:
                if smallest_term in [term for term in temp_index[partial_id]]:
                    smallest_partial_ids.append(partial_id)

            """
            Build a new postings list with the current smallest term (Example): 
            temp_index = {1: {"hello": {1: 3, 2: 2, 3: 3}}, 2: {"hello": {4: 1}}, 3: {"hello": {6: 1}}}
            term = "hello" => temp_index[1][term] = [{1: 3}, {2: 2}, {3: 3}]
            new_postings = {1: 3, 2: 2, 3: 3, 4: 1, 6: 1}
            """

            # Build a new postings list with the current smallest term
            new_postings = {}
            for partial_id in temp_index:
                for term in temp_index[partial_id]:
                    if term == smallest_term:
                        # postings = {'1854001': 1, '1854092': 1, '1854676': 2, '1854746': 2}
                        postings = temp_index[partial_id][term]
                        for doc_id in postings:
                                new_postings[doc_id] = postings[doc_id]

            # Add term and postings list to the final index
            self.get_index().add_term(smallest_term, new_postings)
            self.get_index().register_new_term()

            # Occupied memory shouldn't be greater than 30% of the available memory
            if self.get_index().get_used_memory() >= self.memory_threshold * 30 / 100:
                # Save the index to disk
                self.save_final_index(final_index_output_dir)
                # Clear memory
                self.get_index().purge_data()

            # Continue with the documents that contain the previous smallest term
            # Obtain next entry (next smallest term)
            for partial_id in smallest_partial_ids:

                # Read next entry
                line = files[partial_id].readline()

                # If reached EOF (no more terms), remove the partial index
                # Else obtain the next term and postings list

                if line != "":

                    # Obtain next smallest term and postings list
                    term = json.loads(line)["term"]
                    postings = json.loads(line)["postings"]

                    # Store new smallest term and postings list
                    temp_index[partial_id] = {term: postings}

                else:
                    # Close file
                    files[partial_id].close()

                    # Remove partial index file
                    del files[partial_id]
                    partial_index_file_name = "/index{0}.json".format(partial_id)
                    os.remove(partial_indexes_output_dir + partial_index_file_name)

                    # Remove entry from temp_index
                    del temp_index[partial_id]

                    # print("Finished merging partial index with id:", partial_id)

            # Check if there are no more entries in the temporary index
            if not files:
                merge_completed = True
                print("[!] Finished SPIMI Merge!")

        # Save the index to disk
        self.save_final_index(final_index_output_dir)
        # Clear memory
        self.get_index().purge_data()

    def save_partial_index(self, index_output_folder):

        # Get the index id
        index_id = utils.get_index_id(index_output_folder)
        index_file_name = "/index{0}.json".format(index_id)

        # Write the index to disk
        with open(index_output_folder + index_file_name, "w") as index_file:
            terms = self.get_index().get_terms()
            for term in terms:
                term_json = {"term": term, "postings": terms[term]}
                index_file.write(json.dumps(term_json) + "\n")

        self.get_index().register_partial_index()

    def save_final_index(self, index_output_folder):

        # Get the index id
        index_id = utils.get_index_id(index_output_folder)
        index_file_name = "/merged_index{0}.json".format(index_id)
        cache_file_name = "/cache{0}.jsonl".format(index_id)

        # Write the merged index to disk

        file = open(index_output_folder + index_file_name, "w")
        cache_file = open(index_output_folder + cache_file_name, "w")

        terms = self.get_index().get_terms()

        if "tfidf" in self.rankings:

            # Obtain ranking variants
            ranking_variants = self.rankings["tfidf"]["smart"].split(".")[0]
            tf = ranking_variants[0]

            new_terms = {}
            for term in terms:

                if self.tfidf_cache:

                    new_terms[term] = {}

                    for doc in terms[term]:
                        term_frequency = {term: terms[term][doc]}
                        new_terms[term][doc] = term_frequency_weighting(tf, term_frequency)

                    term_json = {"term": term, "postings": new_terms[term]}
                    cache_file.write(json.dumps(term_json) + "\n")

                term_json = {"term": term, "postings": terms[term]}
                file.write(json.dumps(term_json) + "\n")

        if "bm25" in self.rankings:

            # Obtain b and k1
            b = self.rankings["bm25"]["b"]
            k1 = self.rankings["bm25"]["k1"]

            # Obtain total number of documents
            N = self.documents_info.documents_count

            # Calculate average document length
            avgdl = self.average_document_length()

            # Obtain documents lengths
            documents_lengths = {str(doc_id): self.documents_info.documents[doc_id][1] for doc_id in
                                 self.documents_info.documents.keys()}

            new_terms = {}
            for term in terms:

                if self.bm25_cache:

                    # Get the postings list for the term
                    postings_list = terms[term]

                    # Calculate idf for current term
                    df = len(postings_list)
                    idf = math.log10(N / df)

                    new_terms[term] = {}

                    for document in postings_list.keys():

                        tf = postings_list[document]
                        dl = documents_lengths[document]

                        # Calculate retrieval status value
                        rsv = self.calculate_rsv(idf, tf, dl, avgdl, b, k1)

                        new_terms[term][document] = rsv

                    term_json = {"term": term, "postings": new_terms[term]}
                    cache_file.write(json.dumps(term_json) + "\n")

                term_json = {"term": term, "postings": terms[term]}
                file.write(json.dumps(term_json) + "\n")

        file.close()
        cache_file.close()

        # Write the final index to disk
        with open(index_output_folder + "/index.json", "a") as file:
            terms = list(self.get_index().get_terms().keys())

            index_entry = {"start": terms[0], "end": terms[-1], "index": index_id}
            file.write(json.dumps(index_entry) + "\n")

    def save_tokenizer_info(self, output_folder):
        # Write the tokenizer kwargs to disk
        with open(output_folder + "/tokenizer_info.json", "w") as file:
            file.write(json.dumps(self.get_index().get_tokenizer_kwargs()))

    def calculate_norm(self, terms_frequency, document_id):

        # Obtain ranking variants
        ranking_variants = self.rankings["tfidf"]["smart"].split(".")[0]

        tf = ranking_variants[0]
        df = ranking_variants[1]
        norm = ranking_variants[2]

        new_terms_frequency = term_frequency_weighting(tf, terms_frequency)

        # Expected!
        if df == "n":
            # Obtain document norm
            # For lnc - the weight is equal to tf_log. So, we square all tf_log for all terms in the document
            # and apply a square root to get the document length
            document_norm = math.sqrt(sum(list(map(lambda value: value ** 2, new_terms_frequency.values()))))

            # Store the length of the document in memory
            # document_lengths[document_id] = document_length
            self.documents_info.add_item(Document(document_id, document_norm, None))

            # This will be calculated when the query is built by the user
            # weights_normalized = normalize_weights(norm, new_terms_frequency, document_length)

    def calculate_length(self, terms_frequency, document_id):

        # Useful for BM25
        document_length = len(terms_frequency)

        # Store the length of the document in memory
        self.documents_info.add_item(Document(document_id, None, document_length))

    def calculate_rsv(self, idf: float, tf: int, dl: int, avgdl: float, b: float, k1: float) -> float:

        key = (idf, tf, dl,)

        if key in self.rsv_cache:
            return self.rsv_cache[key]
        else:

            # Document length normalization
            B = ((1 - b) + b * (dl / avgdl))

            final_calc = idf * ((k1 + 1) * tf) / (k1 * B + tf)

            self.rsv_cache[key] = final_calc

            # Retrieval status value
            return final_calc

    def average_document_length(self):

        documents = self.documents_info.documents
        # Returns the average document length for given documents
        return sum(documents[doc_id][1] for doc_id in documents.keys()) / len(documents)


class BaseIndex:
    """
    Top-level Index class
    
    This loosely defines a class over the concept of
    an index.

    """

    def __init__(self):
        self.tokenizer_kwargs = {}
        # Set Default Cache to False??
        self.documents_info = DocumentsInfo((False, False,))

    def get_documents_info(self):
        return self.documents_info

    def set_documents_info(self, documents_info):
        self.documents_info = documents_info

    def get_tokenizer_kwargs(self):
        """
        Index should store the arguments used to initialize the index as aditional metadata
        """
        return self.tokenizer_kwargs

    def set_tokenizer_kwargs(self, tokenizer_kwargs):
        self.tokenizer_kwargs = tokenizer_kwargs

    def add_term(self, *args, **kwargs):
        raise NotImplementedError()
    
    def print_statistics(self):
        raise NotImplementedError()
    
    @classmethod
    def load_from_disk(cls, path_to_folder:str):
        """
        Loads the index from disk, note that this
        the process may be complex, especially if your index
        cannot be fully loaded. Think of ways to coordinate
        this job and have a top-level abstraction that can
        represent the entire index even without being fully load
        in memory.
        
        Tip: The most important thing is to always know where your
        data is on disk and how to easily access it. Recall that the
        disk access are the slowest operation in a computation device, 
        so they should be minimized.
        
        Parameters
        ----------
        path_to_folder: str
            the folder where the index or indexes are stored.
            
        """
        # Initialize the index
        index = InvertedIndex()
                    
        # Load tokenizer info
        with open(path_to_folder + "/tokenizer_info.json", "r") as tokenizer_info_file:
            tokenizer_info = json.loads(tokenizer_info_file.read())
            index.set_tokenizer_kwargs(tokenizer_info)

        # Load documents info
        with open(path_to_folder + "/documents_info.jsonl", "r") as documents_info_file:

            info = json.loads(documents_info_file.readline())

            N = info["n_documents"]
            tfidf_cache = info["tf_idf_cache"]
            bm25_cache = info["bm25_cache"]

            documents_info = DocumentsInfo((tfidf_cache, bm25_cache,))

            for line in documents_info_file.readlines():
                document = json.loads(line)
                documents_info.add_item(Document(document["doc_id"], document["document_norm"], document["document_length"]))

            index.set_documents_info(documents_info)

        # tfidf cache or bm25 cache
        if documents_info.cache_status[0] or documents_info.cache_status[1]:
            # Read cache index files from folder
            files = [file for file in os.listdir(path_to_folder + "/Final_Index/") if file.startswith("cache")]
        else:
            # Read merged index files from folder
            files = [file for file in os.listdir(path_to_folder + "/Final_Index/") if file.startswith("merged_index")]

        # Load the index from disk
        for file in files:

            with open(path_to_folder + "/Final_Index/" + file, "r") as index_file:
                for line in index_file:
                    # Load index_entry (term-postings)
                    index_entry = json.loads(line)

                    # Add term to index
                    index.add_term(index_entry["term"], index_entry["postings"])

        return index


class InvertedIndex(BaseIndex):

    def __init__(self):
        super().__init__()
        self.n_terms = 0
        self.terms = {}
        self.occupied_memory = 0
        self.merge_time = 0
        self.total_index_time = 0
        self.partial_index_count = 0
        self.index_directory = ""

    def get_terms(self):
        return self.terms

    def get_used_memory(self):
        return self.terms.__sizeof__()

    def save_merge_time(self, merge_time):
        self.merge_time = merge_time

    def save_total_index_time(self, total_index_time):
        self.total_index_time = total_index_time

    def set_index_directory(self, index_directory):
        self.index_directory = index_directory

    def get_total_index_size(self):
        return utils.get_directory_size(self.index_directory)

    def register_partial_index(self):
        self.partial_index_count += 1

    def register_new_term(self):
        self.n_terms += 1

    def add_term(self, term, postings):
        """
        Adds a term to the index.
        """

        if term not in self.terms:
            self.terms[term] = postings
        else:
            self.terms[term].update(postings)

    def sort(self):
        """
        Sorts the index by term.
        """
        self.terms = dict(sorted(self.terms.items()))
        # sorted_terms = dict(sorted(self.terms.items()))
        # self.terms = calculate_term_frequency(sorted_terms)

    def purge_data(self):
        self.terms = {}
        self.occupied_memory = 0

    def print_statistics(self):

        print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
        print()
        print(" Indexing Statistics")
        print()
        print(f"   Total Indexing time: {round(self.total_index_time, 2)}s")
        print(f"   Merging time (Last SPIMI step): {round(self.merge_time, 2)}s")
        print(f"   Number of temporary index segments written to disk: {self.partial_index_count}")
        print(f"   Vocabulary Size: {self.n_terms} terms ")
        print(f"   Total index size on disk: {self.get_total_index_size()} bytes")
        print()
        print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")


class DocumentsInfo:

    def __init__(self, cache_status):
        self.documents_count = 0
        # DocId -> (doc_norm, doc_length)
        self.documents = {}
        # Tuple -> (tf_idf_cache, bm25_cache)
        self.cache_status = cache_status

    def add_item(self, document: Document):

        if document.document_id in self.documents:

            if document.document_length:
                self.documents[document.document_id] = (self.documents[document.document_id][0],
                                                        document.document_length,)

            if document.document_norm:
                self.documents[document.document_id] = (document.document_norm,
                                                        self.documents[document.document_id][1],)

        else:
            self.documents[document.document_id] = (document.document_norm, document.document_length,)
            self.documents_count += 1

    def save_to_disk(self, path):

        if not os.path.exists(path) and not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))

        with open(path, "w") as document_file:

            # Unnecessary, we have documents count
            doc_json = {"n_documents": self.documents_count, "tf_idf_cache": self.cache_status[0],
                        "bm25_cache": self.cache_status[1]}
            document_file.write(json.dumps(doc_json) + "\n")

            for document_id in self.documents.keys():
                document = Document(document_id, self.documents[document_id][0], self.documents[document_id][1])
                doc_json = {"doc_id": document.document_id, "document_length": document.document_length, "document_norm": document.document_norm}
                document_file.write(json.dumps(doc_json) + "\n")
