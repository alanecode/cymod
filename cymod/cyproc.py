# -*- coding: utf-8 -*-
"""
cymod.cyproc
~~~~~~~~~~~~

This module contains classes used to load Cypher queries from files in the file 
system, and to generate Cypher to represent data contained in 
:class:`pandas.DataFrame` objects.
"""
from __future__ import print_function

class CypherQuerySource(object):
    """Container for information about a Cypher query's original source.

    Args:
        ref (str or :class:`pandas.DataFrame`): A reference to the original 
            data source which the user might need for debugging purposes.
        form (str): The form/ type of the original data source. Either 'cypher'
            or 'tabular' (i.e. a pandas DataFrame).
        index (int): An indication of where in the original data source the 
            query came from. In the case of a cypher file, this will be the
            query number. For tabular data it will be the row number.            
    """
    def __init__(self, ref, form, index):
        self.ref = ref
        self.form = form
        self.index = index

    @property
    def form(self):
        return self._form

    @form.setter
    def form(self, val):
        print("Setting form")
        if val not in ["cypher", "tabular"]:
            raise ValueError("CypherQuerySource.form must be either 'cypher'" \
            " or 'tabular'")
        self._form = val

    def __repr__(self):
        return "form: {0}\nindex: {1}\nref: {2}" \
        .format(self.form, self.index, str(self.ref))

class CypherQuery(object):
    def __init__(self, statement, params=None, source=None):
        self.statement = statement
        self.params = params
        self.source = source    
        