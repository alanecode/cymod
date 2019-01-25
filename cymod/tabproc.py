# -*- coding: utf-8 -*-
"""
cymod.tabproc
~~~~~~~~~~~~

This module contains classes involved in loading Cypher queries from a tabular
data source.
"""
import pandas as pd

class TransTableProcessor(object):
    """Processes a :obj:`pandas.DataFrame` and produces Cypher queries.
    
    Args:
        df (:obj:`pandas.DataFrame`): Table containing data which will be 
            converted into Cypher.
        start_state_col (str): Name of the column specifying the start state of
            the transition described by each row.
        end_state_col (str): Name of the column specifying the end state of
            the transition described by each row.
        labels (:obj:`CustomLabels`, optional): The transition table specified by `df`
            will be interpreted such that each row corresponds to a :Transition
            from one :State to another :State caused by a particular 
            :Condition. a `CustomLabels` object can be used to customise the
            way that State, Transition and Condition nodes are labelled in 
            the generated graph.
        global_params (dict, optional): property name/ value pairs which will 
            be added as parameters to every query.
        """
    def __init__(self, df, start_state_col, end_state_col, labels=None,
        global_params=None):
        self.df = df
        self.start_state_col = start_state_col
        self.end_state_col = end_state_col
        self.labels = labels
        self.global_params = None

    

    

    