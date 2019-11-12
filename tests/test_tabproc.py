# -*- coding: utf-8 -*-
"""
Tests for cymod.tabproc
"""
from __future__ import print_function
from functools import partial

import unittest

import six

import pandas as pd

from cymod.cybase import CypherQuery
from cymod.tabproc import TransTableProcessor, EnvrStateAliasTranslator
from cymod.customise import NodeLabels


class TransTableProcessorTestCase(unittest.TestCase):
    def setUp(self):
        self.demo_explicit_table = pd.DataFrame(
            {
                "start": ["state1", "state2"],
                "end": ["state2", "state3"],
                "cond": ["low", "high"],
            }
        )

        self.demo_explicit_table_more_conds = pd.DataFrame(
            {
                "start": ["state1", "state2"],
                "end": ["state2", "state3"],
                "cond1": ["low", "high"],
                "cond2": [2, 3],
                "cond3": [True, False],
            }
        )

        self.demo_coded_table = pd.DataFrame(
            {
                "start": [0, 1],
                "end": [1, 2],
                "cond1": [0, 1],
                "cond2": [2, 3],
                "cond3": [1, 0],
            }
        )

    def tearDown(self):
        del self.demo_explicit_table

    def test_explicit_codes_queries_correct(self):
        """TransTableProcessorTestCase.iterqueries() yields correct queries."""
        ttp = TransTableProcessor(self.demo_explicit_table, "start", "end")
        query_iter = ttp.iterqueries()

        query1 = CypherQuery(
            'MERGE (start:State {code:"state1"}) '
            + 'MERGE (end:State {code:"state2"}) '
            + "MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) "
            + 'MERGE (cond:Condition {cond:"low"})-[:CAUSES]->(trans);'
        )

        query2 = CypherQuery(
            'MERGE (start:State {code:"state2"}) '
            + 'MERGE (end:State {code:"state3"}) '
            + "MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) "
            + 'MERGE (cond:Condition {cond:"high"})-[:CAUSES]->(trans);'
        )

        # this_query = six.next(query_iter)
        self.assertEqual(six.next(query_iter).statement, query1.statement)

        # this_query = query_iter.next()
        self.assertEqual(six.next(query_iter).statement, query2.statement)

        self.assertRaises(StopIteration, partial(six.next, query_iter))

    def test_explicit_codes_queries_multiple_conds_correct(self):
        """Correct queries with multiple conditions."""
        ttp = TransTableProcessor(self.demo_explicit_table_more_conds, "start", "end")
        query_iter = ttp.iterqueries()

        query1 = CypherQuery(
            'MERGE (start:State {code:"state1"}) '
            + 'MERGE (end:State {code:"state2"}) '
            + "MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) "
            + 'MERGE (cond:Condition {cond1:"low", cond2:2, cond3:true})'
            + "-[:CAUSES]->(trans);"
        )

        query2 = CypherQuery(
            'MERGE (start:State {code:"state2"}) '
            + 'MERGE (end:State {code:"state3"}) '
            + "MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) "
            + 'MERGE (cond:Condition {cond1:"high", cond2:3, cond3:false})'
            + "-[:CAUSES]->(trans);"
        )

        self.assertEqual(six.next(query_iter).statement, query1.statement)
        self.assertEqual(six.next(query_iter).statement, query2.statement)
        self.assertRaises(StopIteration, partial(six.next, query_iter))

    def test_global_params_included_in_node_properties(self):
        """If global parameters specified, should apply to every node."""
        ttp = TransTableProcessor(
            self.demo_explicit_table,
            "start",
            "end",
            global_params={"id": "test-id", "version": 2},
        )

        query_iter = ttp.iterqueries()

        query1 = CypherQuery(
            'MERGE (start:State {code:"state1", id:"test-id", version:2}) '
            + 'MERGE (end:State {code:"state2", id:"test-id", version:2}) '
            + "MERGE (start)<-[:SOURCE]-"
            + '(trans:Transition {id:"test-id", version:2})-[:TARGET]->(end) '
            + 'MERGE (cond:Condition {cond:"low", id:"test-id", version:2})'
            + "-[:CAUSES]->(trans);"
        )

        query2 = CypherQuery(
            'MERGE (start:State {code:"state2", id:"test-id", version:2}) '
            + 'MERGE (end:State {code:"state3", id:"test-id", version:2}) '
            + "MERGE (start)<-[:SOURCE]-"
            + '(trans:Transition {id:"test-id", version:2})-[:TARGET]->(end) '
            + 'MERGE (cond:Condition {cond:"high", id:"test-id", version:2})'
            + "-[:CAUSES]->(trans);"
        )

        self.assertEqual(six.next(query_iter).statement, query1.statement)
        self.assertEqual(six.next(query_iter).statement, query2.statement)
        self.assertRaises(StopIteration, partial(six.next, query_iter))

    def test_custom_labels_can_be_applied(self):
        """If custom labels specified, should apply to relevant nodes."""
        ttp = TransTableProcessor(
            self.demo_explicit_table,
            "start",
            "end",
            labels=NodeLabels({"State": "MyState"}),
        )

        query_iter = ttp.iterqueries()

        query1 = CypherQuery(
            'MERGE (start:MyState {code:"state1"}) '
            + 'MERGE (end:MyState {code:"state2"}) '
            + "MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) "
            + 'MERGE (cond:Condition {cond:"low"})-[:CAUSES]->(trans);'
        )

        query2 = CypherQuery(
            'MERGE (start:MyState {code:"state2"}) '
            + 'MERGE (end:MyState {code:"state3"}) '
            + "MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) "
            + 'MERGE (cond:Condition {cond:"high"})-[:CAUSES]->(trans);'
        )

        self.assertEqual(six.next(query_iter).statement, query1.statement)
        self.assertEqual(six.next(query_iter).statement, query2.statement)
        self.assertRaises(StopIteration, partial(six.next, query_iter))

    def test_coded_queries_correct(self):
        """A state alias translator can be used to convert codes to names."""
        trans = EnvrStateAliasTranslator()
        trans.state_aliases = {0: "state1", 1: "state2", 2: "state3"}
        trans.add_cond_aliases("cond1", {0: "low", 1: "high"})
        trans.add_cond_aliases("cond3", {0: False, 1: True})

        ttp = TransTableProcessor(
            self.demo_coded_table, "start", "end", state_alias_translator=trans
        )
        query_iter = ttp.iterqueries()

        query1 = CypherQuery(
            'MERGE (start:State {code:"state1"}) '
            + 'MERGE (end:State {code:"state2"}) '
            + "MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) "
            + 'MERGE (cond:Condition {cond1:"low", cond2:2, cond3:true})'
            + "-[:CAUSES]->(trans);"
        )

        query2 = CypherQuery(
            'MERGE (start:State {code:"state2"}) '
            + 'MERGE (end:State {code:"state3"}) '
            + "MERGE (start)<-[:SOURCE]-(trans:Transition)-[:TARGET]->(end) "
            + 'MERGE (cond:Condition {cond1:"high", cond2:3, cond3:false})'
            + "-[:CAUSES]->(trans);"
        )

        self.assertEqual(six.next(query_iter).statement, query1.statement)
        self.assertEqual(six.next(query_iter).statement, query2.statement)
        self.assertRaises(StopIteration, partial(six.next, query_iter))


class EnvrStateAliasTranslatorTestCase(unittest.TestCase):
    """Tests for the ``EnvrStateAliasTranslator`` class.
    
    This is the class which holds data allowing a transition table with codes
    used to specify state transitions instead of human readable values. The 
    ``EnvrStateAliasTranslator`` can be passed to a ``TransTableProcessor``
    upon initialisation. This will then be used to load appropriate human
    readable values into the graph database.
    """

    def test_can_add_state_aliases(self):
        trans = EnvrStateAliasTranslator()
        trans.state_aliases = {0: "state1", 1: "state2", 2: "state3"}
        self.assertEqual(trans.state_alias(0), "state1")
        self.assertEqual(trans.state_alias(1), "state2")
        self.assertEqual(trans.state_alias(2), "state3")

    def test_exception_thrown_if_requested_state_alias_not_provided(self):
        trans = EnvrStateAliasTranslator()
        trans.state_aliases = {0: "state1", 1: "state2", 2: "state3"}
        with self.assertRaises(ValueError) as cm:
            trans.state_alias(3)
        self.assertEqual(
            str(cm.exception), "No alias specified for state " "with code '3'."
        )

    def test_can_add_condition_aliases(self):
        trans = EnvrStateAliasTranslator()
        trans.add_cond_aliases("cond1", {0: False, 1: True})
        trans.add_cond_aliases("cond2", {0: "low", 1: "high"})

        self.assertEqual(trans.cond_alias("cond1", 0), False)
        self.assertEqual(trans.cond_alias("cond1", 1), True)

        self.assertEqual(trans.cond_alias("cond2", 0), "low")
        self.assertEqual(trans.cond_alias("cond2", 1), "high")

    def test_can_get_list_of_all_aliased_conditions(self):
        trans = EnvrStateAliasTranslator()
        trans.add_cond_aliases("cond1", {0: False, 1: True})
        trans.add_cond_aliases("cond2", {0: "low", 1: "high"})

        self.assertEqual(trans.all_conds, ["cond1", "cond2"])

    def test_exception_thrown_if_condition_not_specified(self):
        trans = EnvrStateAliasTranslator()
        trans.add_cond_aliases("cond2", {0: "low", 1: "high"})
        with self.assertRaises(ValueError) as cm:
            trans.cond_alias("cond1", 0)
        self.assertEqual(
            str(cm.exception), "No aliases specified for" " condition 'cond1'."
        )

    def test_exception_thrown_if_requested_cond_alias_not_provided(self):
        trans = EnvrStateAliasTranslator()
        trans.add_cond_aliases("cond1", {0: False, 1: True})
        trans.add_cond_aliases("cond2", {0: "low", 1: "high"})
        with self.assertRaises(ValueError) as cm:
            trans.cond_alias("cond2", 2)
        self.assertEqual(
            str(cm.exception),
            "No alias specified for" " condition 'cond2' with value '2'.",
        )
