""" misc.py

A collection of various functions without a clear classification
"""

import hashlib
import inspect


def show_help(filename='README.md'):
    with open(filename, 'r') as file_content:
        print(file_content.read())


def md5_checksum(filename):
    """This function returns the md5 checksum for the filename specified.
    Input:
        filename: name of the file to get the md5 checksum

    Output:
        String with the md5 checksum of the file
    """

    md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
            md5.update(chunk)
    return md5.hexdigest()


def var_dump(variable):
    if inspect.isclass(variable):
        elements = dir(variable)

        for element in elements:
            attr = getattr(variable, element)
            print element, type(attr), attr


# Decorator for deprecated functions
def deprecated(recommendation=''):
    def fdef(f):
        def wrapper(*args, **kw):
            print 'WARNING: The function {0} is deprecated. {1}'.format(f.func_name, recommendation)
            return f(*args, **kw)
        return wrapper
    return fdef
