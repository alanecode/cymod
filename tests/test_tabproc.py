# -*- coding: utf-8 -*-
"""
Tests for cymod.tabproc
"""
from __future__ import print_function

import unittest

import pandas as pd

from cymod.cybase import CypherQuery
from cymod.tabproc import TransTableProcessor
from cymod.customise import CustomLabels

class TransTableProcessorTestCase(unittest.TestCase):
    def setUp(self):
        self.demo_explicit_table = pd.DataFrame({
            "start": ["state1", "state2"], 
            "end": ["state2", "state3"], 
            "cond": ["low", "high"]
        })

        self.demo_explicit_table_more_conds = pd.DataFrame({
            "start": ["state1", "state2"], 
            "end": ["state2", "state3"], 
            "cond1": ["low", "high"],
            "cond2": [2, 3],
            "cond3": [True, False]
        })

    def tearDown(self):
        del self.demo_explicit_table    

    def test_explicit_codes_queries_correct(self):
        """TransTableProcessorTestCase.iterqueries() yields correct queries."""
        ttp = TransTableProcessor(self.demo_explicit_table, "start", "end")
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

        #this_query = query_iter.next()
        self.assertEqual(query_iter.next().statement, query1.statement)

        #this_query = query_iter.next()
        self.assertEqual(query_iter.next().statement, query2.statement)
        
        self.assertRaises(StopIteration, query_iter.next)

    def test_explicit_codes_queries_multiple_conds_correct(self):
        """Correct queries with multiple conditions."""
        ttp = TransTableProcessor(self.demo_explicit_table_more_conds,
            "start", "end")
        query_iter = ttp.iterqueries()

        query1 = CypherQuery('MERGE (start:State {code:"state1"}) '
            + 'MERGE (end:State {code:"state2"}) '
            + 'MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) '
            + 'MERGE (cond:Condition {cond1:"low", cond2:2, cond3:true})'
            + '-[:CAUSES]->(trans);'
            )

        query2 = CypherQuery('MERGE (start:State {code:"state2"}) '
            + 'MERGE (end:State {code:"state3"}) '
            + 'MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) '
            + 'MERGE (cond:Condition {cond1:"high", cond2:3, cond3:false})'
            + '-[:CAUSES]->(trans);'
        )

        self.assertEqual(query_iter.next().statement, query1.statement)
        self.assertEqual(query_iter.next().statement, query2.statement)
        self.assertRaises(StopIteration, query_iter.next)

    def test_global_params_included_in_node_properties(self):
        """If global parameters specified, should apply to every node."""
        ttp = TransTableProcessor(self.demo_explicit_table, "start", "end",
            global_params={"id": "test-id", "version": 2})

        query_iter = ttp.iterqueries()

        query1 = CypherQuery(
            'MERGE (start:State {code:"state1", id:"test-id", version:2}) '
            + 'MERGE (end:State {code:"state2", id:"test-id", version:2}) '
            + 'MERGE (start)<-[:SOURCE]-'
            + '(trans:Transition {id:"test-id", version:2})-[:TARGET]->(end) '
            + 'MERGE (cond:Condition {cond:"low", id:"test-id", version:2})'
            + '-[:CAUSES]->(trans);'
            )

        query2 = CypherQuery(
            'MERGE (start:State {code:"state2", id:"test-id", version:2}) '
            + 'MERGE (end:State {code:"state3", id:"test-id", version:2}) '
            + 'MERGE (start)<-[:SOURCE]-'
            + '(trans:Transition {id:"test-id", version:2})-[:TARGET]->(end) '
            + 'MERGE (cond:Condition {cond:"high", id:"test-id", version:2})'
            + '-[:CAUSES]->(trans);'
        )

        self.assertEqual(query_iter.next().statement, query1.statement)
        self.assertEqual(query_iter.next().statement, query2.statement)
        self.assertRaises(StopIteration, query_iter.next)   


    def test_custom_labels_can_be_applied(self):
        """If custom labels specified, should apply to relevant nodes."""
        ttp = TransTableProcessor(self.demo_explicit_table, "start", "end",
            labels=CustomLabels({"State": "MyState"}))

        query_iter = ttp.iterqueries()

        query1 = CypherQuery('MERGE (start:MyState {code:"state1"}) '
            + 'MERGE (end:MyState {code:"state2"}) '
            + 'MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) '
            + 'MERGE (cond:Condition {cond:"low"})-[:CAUSES]->(trans);'
            )

        query2 = CypherQuery('MERGE (start:MyState {code:"state2"}) '
            + 'MERGE (end:MyState {code:"state3"}) '
            + 'MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) '
            + 'MERGE (cond:Condition {cond:"high"})-[:CAUSES]->(trans);'
        )
        
        self.assertEqual(query_iter.next().statement, query1.statement)
        self.assertEqual(query_iter.next().statement, query2.statement)
        self.assertRaises(StopIteration, query_iter.next)   
        









