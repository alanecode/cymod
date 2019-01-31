# -*- coding: utf-8 -*-
"""
Tests for cymod.tabproc
"""
from __future__ import print_function

import unittest

import pandas as pd

from cymod.cybase import CypherQuery
from cymod.tabproc import TransTableProcessor

class TransTableProcessorTestCase(unittest.TestCase):
    def setUp(self):
        self.demo_explicit_table = pd.DataFrame({
            "start": ["state1", "state2"], 
            "end": ["state2", "state3"], 
            "cond": ["low", "high"]
        })

    def tearDown(self):
        del self.demo_explicit_table    

    def test_explicit_codes_queries_correct(self):
        """TransTableProcessorTestCase.iterqueries() yields correct queries."""
        ttp = TransTableProcessor(self.demo_explicit_table,
            "start", "end")
        query_iter = ttp.iterqueries()

        query1 = CypherQuery('MERGE (start:State {code:"state1"}) '
            + 'MERGE (end:State {code:"state2"}) '
            + 'MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) '
            + 'MERGE (cond:Condition {cond:"low"})-[:CAUSES]->(trans);'
            )

        query2 = CypherQuery('MERGE (start:State {code:"state2"}) '
            + 'MERGE (end:State {code:"state3"}) '
            + 'MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) '
            + 'MERGE (cond:Condition {cond:"high"})-[:CAUSES]->(trans);'
        )

        self.assertEqual(query_iter.next(), query1)

        self.assertEqual(query_iter.next(), query2)
        
        self.assertRaises(StopIteration, query_iter.next)









