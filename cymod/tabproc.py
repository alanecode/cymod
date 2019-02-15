# -*- coding: utf-8 -*-
"""
cymod.tabproc
~~~~~~~~~~~~

This module contains classes involved in loading Cypher queries from a tabular
data source.
"""
import re
import json
import collections

import six
import pandas as pd

from cymod.params import validate_cypher_params
from cymod.cybase import CypherQuery, CypherQuerySource
from cymod.customise import CustomLabels

class TransTableProcessor(object):
    """Processes a :obj:`pandas.DataFrame` and produces Cypher queries.

    Attrs:
        node_re (Pattern): Compiled regular expression which matches the Cypher
            expression for a node beginning with '(start:', '(end:', '(trans:',
            or '(cond:'. Useful for identitying these elements in a larger
            query before making modifications.
        props_re (Pattern): Compiled regular expression which matches the 
            Cypher syntax for the properties part of a node or edge. Useful for
            detecting whether or not a particular node has any properties 
            specified or not.
    """

    def __init__(self, df, start_state_col, end_state_col, labels=None,
            global_params=None):
        """
        Args:
            df (:obj:`pandas.DataFrame`): Table containing data which will be 
                converted into Cypher.
            start_state_col (str): Name of the column specifying the start 
                state of the transition described by each row.
            end_state_col (str): Name of the column specifying the end state of
                the transition described by each row.
            labels (:obj:`CustomLabels`, optional): The transition table 
                specified by `df` will be interpreted such that each row 
                corresponds to a :Transition from one :State to another :State 
                caused by a particular :Condition. a `CustomLabels` object can 
                be used to customise the way that State, Transition and 
                Condition nodes are labelled in the generated graph.
            global_params (dict, optional): property name/ value pairs which 
                will be added as parameters to every query.
        """
        self.df = df
        self.start_state_col = start_state_col
        self.end_state_col = end_state_col
        self.global_params = global_params

        if labels:
            self.labels = labels
        else:
            self.labels = CustomLabels()

        self.node_re = re.compile(r"\((start:|end:|trans:|cond:)[^(]*\)")
        self.props_re = re.compile(r"\{.*\}")

    def _dict_to_cypher_properties(self, dict):
        """Convert a Python dict to predictably ordered Cypher properties.
        
        Args:
            dict (dict): Python dictionary containing data which should be 
                converted to Cypher properties.

        Returns:
            str: Representation of data as Cypher. 

        Examples:
            >>> _dict_to_cypher_properties({"version": 2, "id": "test-id"})
            'id:"test-id", version:2'
        """
        ordered = collections.OrderedDict(sorted(dict.items()))
        string = json.dumps(ordered)
        # Remove quotation marks from property name, leaving them around
        # property value if it's a string
        string = re.sub(r"(\")([a-zA-Z_\d]*)(\"):", r"\2:", string)
        # Remove space after key:value colon
        string = re.sub(r"(:)( )([\d\"\'])", r"\1\3", string)
        return string[1:-1]

    def _add_global_params_to_query_string(self, query_str, global_params):
        """Modify a query string specifying node creation, add parameters.

        Looks for the pattern idenityfying nodes in a query string and modifies
        it so that all the parameters specified in the given `global_params` 
        are included as properties of the node(s).

        Args:
            query_str (str): Original query string.
            global_params (dict): Parameters to add to created nodes as 
                properties.

        Returns:
            str: Modified string including global parameters.        
        """
        out_str = query_str
        param_str = self._dict_to_cypher_properties(global_params)
        for node in self.node_re.finditer(query_str):
            node_str = node.group()
            prop_match = self.props_re.search(node_str)
            if prop_match:
                beginning = node_str[:prop_match.end()-1]
                ending = node_str[prop_match.end()-1:]
                new_node_str = beginning + ", " + param_str + ending
            else:
                beginning = node_str[:-1]
                ending = node_str[-1]
                new_node_str = beginning + " {" + param_str + "}" + ending

            out_str = out_str.replace(node_str, new_node_str)

        return out_str

    def _row_to_query_statement_string(self, row):
        """Build the string specifying the cypher query for a single row."""
        start_node = 'MERGE (start:{state_lab} {{code:"{start_state}"}})'\
            .format(state_lab=self.labels.state, 
                start_state=row[self.start_state_col])

        end_node = 'MERGE (end:{state_lab} {{code:"{end_state}"}})'\
            .format(state_lab=self.labels.state, 
                end_state=row[self.end_state_col])

        transition \
            = 'MERGE (start)<-[:SOURCE]-(trans:{trans_lab})-[:TARGET]->(end)'\
                . format(trans_lab=self.labels.transition)

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
                .format(cond_lab=self.labels.condition, 
                    cond_str=_conditions_str(row, self.start_state_col, 
                        self.end_state_col))

        query_str =  start_node + " " + end_node + " " + transition + " "\
            + condition + ";"

        if self.global_params:
            query_str = self._add_global_params_to_query_string(query_str, 
                self.global_params)

        return query_str

    def _row_to_cypher_query(self, row_index, row):
        statement = self._row_to_query_statement_string(row)
        source = CypherQuerySource(self.df, "tabular", row_index)
        return CypherQuery(statement, params=None, source=source)

    def iterqueries(self):
        for i, row in self.df.iterrows():
            yield self._row_to_cypher_query(i, row)


    

        

        



    

    

    