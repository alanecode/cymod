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
            raise ValueError(
                "CypherQuerySource.ref_type must be either " "'cypher' or 'tabular'"
            )
        self._ref_type = val

    def __repr__(self):
        return "ref_type: {0}\nindex: {1}\nref: {2}".format(
            self.ref_type, self.index, str(self.ref)
        )


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

    def __repr__(self):
        return (
            "[statement: "
            + self.statement
            + "\n params: "
            + str(self.params)
            + "\n source: [\n"
            + str(self.source)
            + "]\n]"
        )

    def __eq__(self, other):
        """Override the default Equals behavior"""
        if isinstance(other, self.__class__):
            return (
                self.statement == other.statement
                and self.params == other.params
                and self.source == other.source
            )
        return False

    def __ne__(self, other):
        """Override the default Unequal behavior"""
        return (
            self.statement != other.statement
            or self.params != other.params
            or self.source != other.source
        )
