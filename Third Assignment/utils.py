"""
Base template created by: Tiago Almeida & Sérgio Matos
Authors: Daniela Dias (nMec 98039), Hugo Gonçalves (nMec 98497)

Utility/Auxiliar code

Holds utility code that can be reused by several modules.

"""
import sys
import os
import math

from typing import List, Dict


def dynamically_init_class(module_name, **kwargs):
    """Dynamically initializes a python object based
    on the given class name that resides inside module
    specified by the `module_name`.
    
    The `class` name must be specified as an additional argument,
    this argument will be caught under kwargs variable.
    
    The reason for not directly specifying the class as argument is 
    because `class` is a reserved keyword in python, which may be
    confusing if it is seen as an argument of a function. 
    Additionally, this way the function integrates nicely with the
    `.get_kwargs()` method from the `Param` object.

    Parameters
    ----------
    module_name : str
        the name of the module where the class resides
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

    class_name = kwargs.pop("class")
    return getattr(sys.modules[module_name], class_name)(**kwargs)


def remove_non_empty_dir(directory_path):
    """
    Removes a non empty directory by removing all files.
    """
    if not os.path.exists(directory_path):
        return

    for path in os.scandir(directory_path):
        if path.is_file():
            os.remove(path)


def get_directory_size(start_path='.'):
    """
    Returns the size of a directory in bytes.
    """
    total_size = 0
    for dir_path, dir_names, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dir_path, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size


def get_index_id(folder):
    """
    Returns the index id.
    """
    indexes_count = 0
    if not os.path.exists(folder) or not os.path.isdir(folder):
        os.makedirs(folder)

    # Count the number of indexes
    for path in os.scandir(folder):
        if path.is_file():
            indexes_count += 1

    return indexes_count


def term_frequency_weighting(tf, terms_frequency):
    """
    Calculates the term frequency with algorithm tf.
    """

    if tf == 'l':
        return dict(map(lambda item: (item[0], round(1 + math.log10(item[1]), 3)), terms_frequency.items()))

    if tf == 'a':
        return dict(map(lambda item: (item[0], round(0.5 + 0.5 * item[1] / max(terms_frequency.values()), 3)),
                        terms_frequency.items()))

    if tf == 'n':
        return terms_frequency

    raise NotImplementedError()


def document_frequency_weighting(df, documents_frequency, N):
    """
    Calculates the document frequency with algorithm df.
    """

    if df == 't':  # idf
        return dict(map(lambda item: (item[0], round(math.log10(N / item[1]), 3)), documents_frequency.items()))

    if df == 'p':
        return dict(map(lambda item: (item[0], max(0, math.log10(N - item[1]) / item[1])), documents_frequency.items()))

    if df == 'n':
        return dict(map(lambda item: (item[0], 1), documents_frequency.items()))

    raise NotImplementedError()


def normalize_weights(norm, weights):
    """
    Normalizes the weights with algorithm norm.
    """

    if norm == 'c':
        length = math.sqrt(sum(list(map(lambda value: value ** 2, weights.values()))))
        return dict(map(lambda item: (item[0], round(item[1] / length, 3)), weights.items()))

    if norm == 'n':
        return weights

    raise NotImplementedError(f"Normalization {norm} not implemented.")


def precision(relevant_retrieved: List, total_retrieved: List) -> float:
    """
    Calculates the precision.
    """

    if len(total_retrieved) == 0:
        return 0

    return len(relevant_retrieved) / len(total_retrieved)


def recall(relevant_retrieved: List, total_relevant: List) -> float:
    """
    Calculates the recall.
    """

    if len(total_relevant) == 0:
        return 0

    return len(relevant_retrieved) / len(total_relevant)


# Measure that trades off precision versus recall
def f_measure(precision: float, recall: float) -> float:
    """
    Calculates the f-measure.
    """

    if precision + recall == 0:
        return 0

    return 2 * precision * recall / (precision + recall)


# Measure that considers the order in which the returned documents are presented
# Mean of the precision scores after each relevant document is retrieved
# https://link.springer.com/referenceworkentry/10.1007/978-0-387-39940-9_482
def average_precision(relevant_retrieved: List, total_retrieved: List) -> float:
    """
    Calculates the average precision.
    """

    # The total number of relevant documents
    R = len(relevant_retrieved)

    if R == 0:
        return 0

    avg_precision = 0
    for r in range(1, R + 1):
        # print(f"Precision at {r}: {precision(relevant_docs[:r], retrieved_docs[:r])}")
        avg_precision += precision(relevant_retrieved[:r], total_retrieved[:r]) / r

    avg_precision = avg_precision / R
    return avg_precision


def median_query_latency(query_times: List) -> float:
    """
    Calculates the median query latency.
    """

    if len(query_times) == 0:
        return 0

    query_times.sort()

    # If the number of queries is even
    if len(query_times) % 2 == 0:
        return (query_times[len(query_times) // 2] + query_times[len(query_times) // 2 - 1]) / 2

    # If the number of queries is odd
    return query_times[len(query_times) // 2]


def query_throughput(query_times: List) -> float:
    """
    Calculates the query throughput (Number of queries processed per second).
    """
    if len(query_times) == 0:
        return 0

    return len(query_times) / sum(query_times)


def flatten_list(list_of_lists) -> List:
    """
    Flattens a list of lists.
    """

    outlist = []
    for i in list_of_lists:
        if type(i) == list:
            outlist.extend(i)
        else:
            outlist.append(i)
    return outlist


def generate_combinations(list_of_lists: List[List[int]]) -> List[List[int]]:
    """
    Generates all possible combinations of the given arguments.
    """

    templist = [[]]

    for sublist in list_of_lists:

        # Store combinations of previous sublists
        outlist = templist

        # Create new combination
        templist = [[]]

        for sitem in sublist:
            for oitem in outlist:

                newitem = [oitem]

                # First time will only contain elements from first sublist
                if newitem == [[]]:
                    newitem = [sitem]

                else:
                    newitem = [newitem[0], sitem]

                templist.append(flatten_list(newitem))

    # Remove some partial lists that also creep in
    outlist = list(filter(lambda x: len(x) == len(list_of_lists), templist))

    return outlist


def obtain_window_size(positions_list: List[List[int]]) -> int:
    """
    Calculates the window size of a list of positions.
    """

    if len(positions_list) <= 1:
        return 0

    all_combinations = generate_combinations(positions_list)
    return min([max(combination) - min(combination) + 1 for combination in all_combinations])
