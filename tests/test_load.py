# -*- coding: utf-8 -*-
"""
Tests for cymod.load
"""
from __future__ import print_function

from functools import partial
import shutil, tempfile
import os
from os import path
import unittest
import warnings

import six

import pandas as pd

from cymod.cybase import CypherQuery
from cymod.load import GraphLoader, EmbeddedGraphLoader
from cymod.customise import NodeLabels
from cymod.tabproc import EnvrStateAliasTranslator


def touch(path):
    """Immitate *nix `touch` behaviour, creating directories as required."""
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
    with open(path, "a"):
        os.utime(path, None)


def write_query_set_1_to_file(fname):
    s = '{ "priority": 1 }\n' "MERGE (n:TestNode {test_bool: true})"
    with open(fname, "w") as f:
        f.write(s)


def write_query_set_2_to_file(fname):
    s = (
        '{ "priority": 0 }\n'
        + 'MERGE (n:TestNode {test_str: "test value"});\n'
        + "MERGE (n:TestNode {test_int: 2});"
    )
    with open(fname, "w") as f:
        f.write(s)


def write_query_set_3_to_file(fname):
    s = "MERGE (n:TestNode {test_param: $paramval});"
    with open(fname, "w") as f:
        f.write(s)


class GraphLoaderTestCase(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

        self.demo_explicit_table = pd.DataFrame(
            {
                "start": ["state1", "state2"],
                "end": ["state2", "state3"],
                "cond": ["low", "high"],
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
        # Remove the temp directory after the test
        shutil.rmtree(self.test_dir)

    def test_iterqueries_is_callable(self):
        """GraphLoader should have an iterqueries() method."""
        gl = GraphLoader()
        try:
            gl.iterqueries()
        except AttributeError:
            self.fail("GraphLoader should have an iterqueries() method")

    def test_load_cypher(self):
        """Check load_cypher works."""
        fname1 = path.join(self.test_dir, "file1.cql")
        write_query_set_1_to_file(fname1)

        fname2 = path.join(self.test_dir, "file2.cql")
        write_query_set_2_to_file(fname2)

        gl = GraphLoader()
        gl.load_cypher(self.test_dir)
        queries = gl.iterqueries()

        self.assertEqual(
            six.next(queries).statement, 'MERGE (n:TestNode {test_str: "test value"});'
        )
        self.assertEqual(
            six.next(queries).statement, "MERGE (n:TestNode {test_int: 2});"
        )
        self.assertEqual(
            six.next(queries).statement, "MERGE (n:TestNode {test_bool: true});"
        )

        with self.assertRaises(StopIteration):
            six.next(queries)

    def test_load_cypher_with_file_suffix(self):
        """Check load_cypher works when a file suffix is specified."""
        fname1 = path.join(self.test_dir, "file1_include.cql")
        write_query_set_1_to_file(fname1)

        fname2 = path.join(self.test_dir, "file2.cql")
        write_query_set_2_to_file(fname2)

        gl = GraphLoader()
        gl.load_cypher(self.test_dir, cypher_file_suffix="_include")
        queries = gl.iterqueries()

        self.assertEqual(
            six.next(queries).statement, "MERGE (n:TestNode {test_bool: true});"
        )

        with self.assertRaises(StopIteration):
            six.next(queries)

    def test_load_cypher_with_global_params(self):
        """Check load_cypher can take global params."""
        # assert False, "Implement test Check load_cypher can take global params"
        fname = path.join(self.test_dir, "file.cql")
        write_query_set_3_to_file(fname)

        gl = GraphLoader()
        gl.load_cypher(
            self.test_dir, global_params={"paramval": 5, "dummy_paramval": 3}
        )
        queries = gl.iterqueries()

        this_query = six.next(queries)
        self.assertEqual(
            this_query.statement, "MERGE (n:TestNode {test_param: $paramval});"
        )

        self.assertEqual(this_query.params, {"paramval": 5})

    def test_multiple_calls_to_graph_loader(self):
        """Check load_cypher can be called multiple times.

        All queries identified based on the first call should be dispatched, 
        then all queries based on the second.
        """
        dir1 = path.join(self.test_dir, "dir1")
        if not os.path.exists(dir1):
            os.makedirs(dir1)

        dir2 = path.join(self.test_dir, "dir2")
        if not os.path.exists(dir2):
            os.makedirs(dir2)

        file1 = path.join(dir1, "file1.cql")
        write_query_set_1_to_file(file1)  # in dir1

        file2 = path.join(dir2, "file2.cql")
        write_query_set_2_to_file(file2)  # in dir 2

        gl = GraphLoader()
        gl.load_cypher(dir1)
        gl.load_cypher(dir2)
        queries = gl.iterqueries()

        self.assertEqual(
            six.next(queries).statement, "MERGE (n:TestNode {test_bool: true});"
        )
        self.assertEqual(
            six.next(queries).statement, 'MERGE (n:TestNode {test_str: "test value"});'
        )
        self.assertEqual(
            six.next(queries).statement, "MERGE (n:TestNode {test_int: 2});"
        )

        with self.assertRaises(StopIteration):
            six.next(queries)

    def test_global_params_specified_for_tabular(self):
        """Global params specified in load_tabular should appear in queries."""
        gl = GraphLoader()
        gl.load_tabular(
            self.demo_explicit_table,
            "start",
            "end",
            global_params={"id": "test-id", "version": 2},
        )

        query_iter = gl.iterqueries()

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

    def test_custom_labels_applied_for_tabular(self):
        """Custom labels should be applied via load_tabular."""
        gl = GraphLoader()
        gl.load_tabular(
            self.demo_explicit_table,
            "start",
            "end",
            labels=NodeLabels({"State": "MyState"}),
        )

        query_iter = gl.iterqueries()

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

    def test_coded_transition_table_can_be_used(self):
        trans = EnvrStateAliasTranslator()

        gl = GraphLoader()
        try:
            gl.load_tabular(
                self.demo_coded_table, "start", "end", state_alias_translator=trans
            )
        except Exception:
            self.fail("Could not use state_alias_translator in load_tabular.")


class EmbeddedGraphLoaderTestCase(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Remove the temp directory after the test
        shutil.rmtree(self.test_dir)

    def test_parameters_replaced_with_strings(self):
        """Parameters should be replaced with concrete strings."""
        fname = path.join(self.test_dir, "file1.cql")
        write_query_set_3_to_file(fname)

        egl = EmbeddedGraphLoader()
        egl.load_cypher(self.test_dir, global_params={"paramval": 3})

        query_strings = egl.query_generator()
        self.assertEqual(six.next(query_strings), "MERGE (n:TestNode {test_param: 3});")

        with self.assertRaises(StopIteration):
            six.next(query_strings)
