# -*- coding: utf-8 -*-
"""
cymod.cybase
~~~~~~~~~~~~

This module contains basic classes used to hold and manipulate data about 
cypher queries.
"""
import json

import six
    
class CypherQuerySource(object):
    """Container for information about a Cypher query's original source."""

    def __init__(self, ref, ref_type, index):
        """    
        Args:
            ref (str or :obj:`pandas.DataFrame`): A reference to the original 
                data source which the user might need for debugging purposes.
            form (str): The form/ type of the original data source. Either 
                'cypher' or 'tabular' (i.e. a pandas DataFrame).
            index (int): An indication of where in the original data source the 
                query came from. In the case of a cypher file, this will be the
                query number. For tabular data it will be the row number.            
        """
        self.ref = ref
        self.ref_type = ref_type
        self.index = index

    @property
    def ref_type(self):
        return self._ref_type

    @ref_type.setter
    def ref_type(self, val):
        if val not in ["cypher", "tabular"]:
            raise ValueError("CypherQuerySource.ref_type must be either " \
            "'cypher' or 'tabular'")
        self._ref_type = val

    def __repr__(self):
        return "ref_type: {0}\nindex: {1}\nref: {2}" \
        .format(self.ref_type, self.index, str(self.ref))

class CypherQuery(object):
    """Container for data speficying an individual Cypher query."""
    
    def __init__(self, statement, params=None, source=None):
        """
        Args:
            statement (str): An individual Cypher statement.
            params (dict, optional): Cypher parameters relevant for query.
            source (:obj:`CypherQuerySource`, optional): Data specifying origin
                of query, useful for debugging purposes.            
        """
        self.statement = statement
        self.params = params
        self.source = source    


class CypherParams(object):
    """Container for Cypher parameters.
    
    Initialised with a dict or a file name string referencing a json file.

    Args:
        data (filename str or dict): Either a dict or a string referencing a
            valid json file containing Cypher parameters.
    """

    def __init__(self, data):
        self.data = data

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, val):
        if isinstance(val, dict):
            data = val
        else:
            data = self.read_json_params(val)
        
        if self.all_keys_are_strings_p(data):
            self._data = data
        else:
            raise TypeError("All keys in CypherParams must be strings. " \
                "Review " + str(data))

    def read_json_params(self, fname):
        """Read json file, return dict."""
        with open(fname, "r") as f:
            return json.load(f)

    def all_keys_are_strings_p(self, test_dict):
        """Check all keys in dict are strings. Returns True if so."""
        for k in test_dict.keys():
            if not isinstance(k, six.string_types):
                return False
        return True

    def keys(self):
        return self.data.keys()

    def as_dict(self):
        return self.data

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, val):
        if isinstance(key, str):
            self.data[key] = val
        else:
            raise TypeError("All keys in CypherParams must be strings. " \
                + str(key) + " is not a string.")

    def __iter__(self):
        return iter(self.data)


