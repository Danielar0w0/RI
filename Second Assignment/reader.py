"""
Base template created by: Tiago Almeida & Sérgio Matos
Authors: Daniela Dias (nMec 98039), Hugo Gonçalves (nMec 98497)

Reader module

Holds the code/logic addressing the Reader class
and how to read text from a specific data format.

"""
from memory_manager import MemoryManager
from utils import dynamically_init_class
import json
import gzip


def dynamically_init_reader(**kwargs):
    """Dynamically initializes a Reader object from this
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


class Reader:
    """
    Top-level Reader class
    
    This loosly defines a class over the concept of 
    a reader.
    
    Since there are multiple ways for implementing
    this class, we did not defined any specific method 
    in this started code.

    """

    def __init__(self,
                 path_to_collection: str,
                 **kwargs):
        super().__init__()
        self.path_to_collection = path_to_collection


class PubMedReader(Reader):

    def __init__(self,
                 path_to_collection: str,
                 memory_threshold,
                 **kwargs):
        super().__init__(path_to_collection, **kwargs)
        print("init PubMedReader|", f"{self.path_to_collection=}")
        if kwargs:
            print(f"{self.__class__.__name__} also caught the following additional arguments {kwargs}")

        self.memory_threshold = memory_threshold * 1024 * 1024
        self.memory_manager = MemoryManager(self.memory_threshold, 80)
        self.documents_file = gzip.open(self.path_to_collection)
        self.documents = {}

    # Read .jsonl files (list of json objects)
    def read(self):

        while self.memory_manager.has_available_memory():

            current_line = self.documents_file.readline().decode()

            # If reached EOF, close documents_file
            if current_line == "":
                self.documents_file.close()
                self.documents_file = None

                return self.documents

            document = json.loads(current_line)
            self.documents[int(document['pmid'])] = document['title'] + document['abstract']
            self.memory_manager.update_used_memory(self.documents.__sizeof__())

        return self.documents

    def has_documents_to_read(self):
        return self.documents_file is not None

    def purge_data(self):
        self.documents = {}
        self.memory_manager.update_used_memory(self.documents.__sizeof__())


class QuestionsReader(Reader):

    def __init__(self,
                 path_to_questions: str,
                 **kwargs):
        super().__init__(path_to_questions, **kwargs)
        # I do not want to refactor Reader and here path_to_collection does not make any sense.
        # So consider using self.path_to_questions instead (but both variables point to the same thing, it just to not break old code)
        self.path_to_questions = self.path_to_collection
        print("init QuestionsReader|", f"{self.path_to_questions=}")
        if kwargs:
            print(f"{self.__class__.__name__} also caught the following additional arguments {kwargs}")

        self.queries_file = open(self.path_to_questions)
        self.queries = {}

    def read(self):

        current_line = self.queries_file.readline()

        # If reached EOF, close queries_file
        if current_line == "":
            self.queries_file.close()
            self.queries_file = None

            return self.queries

        query = json.loads(current_line)
        self.queries[int(query['id'])] = query['query']

        return self.queries

    def has_queries_to_read(self):
        return self.queries_file is not None

    def purge_data(self):
        self.queries = {}
