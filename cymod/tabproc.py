# -*- coding: utf-8 -*-
"""
cymod.tabproc
~~~~~~~~~~~~

This module contains classes involved in loading Cypher queries from a tabular
data source.
"""
import six

import pandas as pd

from cymod.params import validate_cypher_params
from cymod.cybase import CypherQuery, CypherQuerySource

class TransTableProcessor(object):
    """Processes a :obj:`pandas.DataFrame` and produces Cypher queries.
    
    Args:
        df (:obj:`pandas.DataFrame`): Table containing data which will be 
            converted into Cypher.
        start_state_col (str): Name of the column specifying the start state of
            the transition described by each row.
        end_state_col (str): Name of the column specifying the end state of
            the transition described by each row.
        labels (:obj:`CustomLabels`, optional): The transition table specified 
            by `df` will be interpreted such that each row corresponds to a 
            :Transition from one :State to another :State caused by a 
            particular :Condition. a `CustomLabels` object can be used to 
            customise the way that State, Transition and Condition nodes are 
            labelled in the generated graph.
        global_params (dict, optional): property name/ value pairs which will 
            be added as parameters to every query.
        """
    def __init__(self, df, start_state_col, end_state_col, labels=None,
            global_params=None):
        self.df = df
        self.start_state_col = start_state_col
        self.end_state_col = end_state_col
        self.labels = labels
        self.global_params = global_params


    def _row_to_query_statement_string(self, row):
        """Build the string specifying the cypher query for a single row."""
        start_node = 'MERGE (start:{state_lab} {{code:"{start_state}"}})'\
            .format(state_lab="State", start_state=row[self.start_state_col])

        end_node = 'MERGE (end:{state_lab} {{code:"{end_state}"}})'\
            .format(state_lab="State", end_state=row[self.end_state_col])

        transition \
            = 'MERGE (start)<-[:SOURCE]-(trans:{trans_lab})-[:TARGET]->(end)'\
                . format(trans_lab="Transition")

        def _conditions_str(row, start_state_col, end_state_col):
            """Build the string used to express transition conditions.
            
            This is the part in curly braces specifying the Condition node's
            properties.
            """
            row = row.drop([start_state_col, end_state_col])
            count = 0                                
            s = ""                                   
            for i, val in row.iteritems():   
                s += i + ":"                         
                if isinstance(val, six.string_types):
                    s += '"' + val + '"'
                elif isinstance(val, bool):
                    s += str(val).lower()
                else:
                    s += str(val)    
                if count < len(row) - 1:
                    s += ", " 
                count += 1

            return "{" + s + "}"

        condition = 'MERGE (cond:{cond_lab} {cond_str})-[:CAUSES]->(trans)'\
                .format(cond_lab="Condition", 
                    cond_str=_conditions_str(row, self.start_state_col, 
                        self.end_state_col))

        return start_node + " " + end_node + " " + transition + " "\
            + condition + ";"

    def _row_to_cypher_query(self, row_index, row):
        statement = self._row_to_query_statement_string(row)
        source = CypherQuerySource(self.df, "tabular", row_index)
        return CypherQuery(statement, params=None, source=source)

    def iterqueries(self):
        for i, row in self.df.iterrows():
            yield self._row_to_cypher_query(i, row)


    

        

        



    

    

    