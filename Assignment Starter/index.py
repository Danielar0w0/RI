"""
Base template created by: Tiago Almeida & Sérgio Matos
Authors: 

Indexer module

Holds the code/logic addressing the Indexer class
and the index managment.

"""

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
    
    This loosly defines a class over the concept of 
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
    

class SPIMIIndexer(Indexer):
    """
    The SPIMIIndexer represents an indexer that
    holds the logic to build an index according to the
    spimi algorithm.

    """
    def __init__(self, 
                 posting_threshold, 
                 memory_threshold, 
                 **kwargs):
        # lets suppose that the SPIMIIindex uses the inverted index, so
        # it initializes this type of index
        super().__init__(InvertedIndex(), **kwargs)
        print("init SPIMIIndexer|", f"{posting_threshold=}, {memory_threshold=}")
        if kwargs:
            print(f"{self.__class__.__name__} also caught the following additional arguments {kwargs}")
        
    def build_index(self, reader, tokenizer, index_output_folder):
        print("Indexing some documents...")
        
        
class BaseIndex:
    """
    Top-level Index class
    
    This loosly defines a class over the concept of 
    an index.

    """

    def get_tokenizer_kwargs(self):
        """
        Index should store the arguments used to initialize the index as aditional metadata
        """
        return {}

    def add_term(self, term, doc_id, *args, **kwargs):
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
        return cls()

class InvertedIndex(BaseIndex):
    
    # make an efficient implementation of an inverted index
        
    @classmethod
    def load_from_disk(cls, path_to_folder:str):
        raise NotImplementedError()
    
    def print_statistics(self):
        print("Print some stats about this index.. This should be implemented by the base classes")
    
    