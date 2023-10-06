"""
Base template created by: Tiago Almeida & Sérgio Matos
Authors: Daniela Dias (nMec 98039), Hugo Gonçalves (nMec 98497)

Indexer module

Holds the code/logic addressing the Indexer class
and the index management.

"""
import os
import json
import time
from collections import Counter
import gc

import utils
from memory_manager import MemoryManager
from utils import dynamically_init_class


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

    def build_index(self, reader, tokenizer, index_output_dir):

        indexing_time_start = time.time()

        # Create path for Sub Directory that will contain partial index files
        partial_index_output_dir = os.path.join(index_output_dir, self.partial_index_subdir)
        final_index_output_dir = os.path.join(index_output_dir, self.final_index_subdir)

        # Clear partial indexes directory
        utils.remove_non_empty_dir(partial_index_output_dir)

        # Clear final indexes directory
        utils.remove_non_empty_dir(final_index_output_dir)

        self.get_index().set_index_directory(final_index_output_dir)

        while reader.has_documents_to_read():

            # Read the documents
            documents = reader.read()
            reader.purge_data()

            document_count = 0

            for document_id in documents:

                document_count += 1

                # Tokenize the document terms
                terms = tokenizer.tokenize(documents[document_id])
                for term in terms:
                    # Add term and posting (docID) to the current index
                    self.get_index().add_term(term, document_id)

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
            # if len(os.listdir(partial_indexes_output_folder)) == 0:
            #     merge_completed = True

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

    """
    def save_final_index(self, index_output_folder):

        # Write the index to disk
        with open(index_output_folder + "/final_index.json", "w") as index_file:
            terms = self.get_index().get_terms()
            for term in terms:
                term_json = {"term": term, "postings": terms[term]}
                index_file.write(json.dumps(term_json) + "\n")
    """

    def save_final_index(self, index_output_folder):

        # Get the index id
        index_id = utils.get_index_id(index_output_folder)
        index_file_name = "/merged_index{0}.json".format(index_id)

        # Write the merged index to disk
        with open(index_output_folder + index_file_name, "w") as file:
            terms = self.get_index().get_terms()

            for term in terms:
                term_json = {"term": term, "postings": terms[term][0]}
                file.write(json.dumps(term_json) + "\n")

        # Write the final index to disk
        with open(index_output_folder + "/index.json", "a") as file:
            terms = list(self.get_index().get_terms().keys())

            index_entry = {"start": terms[0], "end": terms[-1], "index": index_id}
            file.write(json.dumps(index_entry) + "\n")


def calculate_term_frequency(terms):
    """
    Calculates the term frequency for each term-doc.
    """

    terms_with_frequency = {}
    for term in terms:
        terms_with_frequency[term] = dict(Counter(terms[term]))
    return terms_with_frequency


class BaseIndex:
    """
    Top-level Index class
    
    This loosely defines a class over the concept of
    an index.

    """

    def __init__(self):
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

    def add_term(self, term, doc_id, *args, **kwargs):
        """
        Adds a term to the index.
        """

        if term not in self.terms:
            self.terms[term] = [doc_id]
        else:
            self.terms[term].append(doc_id)

    def sort(self):
        """
        Sorts the index by term.
        """
        sorted_terms = dict(sorted(self.terms.items()))
        # print(calculate_term_frequency(sorted_terms))
        self.terms = calculate_term_frequency(sorted_terms)

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

    @classmethod
    def load_from_disk(cls, path_to_folder: str):
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
        raise NotImplementedError()


class InvertedIndex(BaseIndex):

    # make an efficient implementation of an inverted index

    @classmethod
    def load_from_disk(cls, path_to_folder: str):
        raise NotImplementedError()

    # def print_statistics(self):
    # print("Print some stats about this index.. This should be implemented by the base classes")
