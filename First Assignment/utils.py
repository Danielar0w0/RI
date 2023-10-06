"""
Base template created by: Tiago Almeida & Sérgio Matos
Authors: Daniela Dias (nMec 98039), Hugo Gonçalves (nMec 98497)

Utility/Auxiliar code

Holds utility code that can be reused by several modules.

"""

import sys
import os


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

    if not os.path.exists(directory_path):
        return

    for path in os.scandir(directory_path):
        if path.is_file():
            os.remove(path)


def get_directory_size(start_path='.'):

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
