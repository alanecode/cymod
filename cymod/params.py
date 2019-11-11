# -*- coding: utf-8 -*-
"""
cymod.params
~~~~~~~~~~~~

This module contains functions used to parse external files specifying global
parameters intended to be applied to every node in a model. Sets of parameters
are returned as python dicts. 
"""
import json
import six


def all_keys_are_strings_p(test_dict):
    """Check all keys in dict are strings. Returns True if so, o/w False.
    
    Args:
        test_dict (dict): Dictionary whose keys are to be checked to ensure
            they're strings.
    """
    for k in test_dict.keys():
        if not isinstance(k, six.string_types):
            return False
    return True


def validate_cypher_params(test_dict):
    """Ensure that supplied dictionary specifies valid Cypher parameters.

    Args:
        test_dict (dict): Dictionary containing parameters to be validated.
    """
    if not all_keys_are_strings_p(test_dict):
        raise TypeError("All parameter names must be strings:\n" + str(test_dict))

    return True


def read_json_params(fname):
    """Read json file, return dict."""
    with open(fname, "r") as f:
        return json.loads(f.read())


def read_params_file(fname, ftype="json"):
    """Parse given file to extract specified parameters as Python dict.
    
    Args:
        fname (str): File name of the file containing parameters.
        ftype (str, optional): Type of the file containing parameters. 
            Defaults to 'json' (the only file type currently supported).

    Returns:
        dict: parameter name, parameter value pairs specified in `fname`.
    """
    FILE_READERS = {"json": read_json_params}

    if ftype not in FILE_READERS.keys():
        raise ValueError("'{0}' is not a supported file type".format(ftype))

    params = FILE_READERS[ftype](fname)

    if validate_cypher_params(params):
        return params
