# -*- coding: utf-8 -*-
"""
Tests for cymod.load
"""
from __future__ import print_function

import shutil, tempfile
import os
from os import path
import unittest
import warnings

from cymod.load import GraphLoader

def touch(path):
    """Immitate *nix `touch` behaviour, creating directories as required."""
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
    with open(path, "a"):
        os.utime(path, None)

def write_query_set_1_to_file(fname):
    s = '{ "priority": 1 }\n'\
        'MERGE (n:TestNode {test_bool: true})'
    with open(fname, "w") as f:
        f.write(s)

def write_query_set_2_to_file(fname):
    s = '{ "priority": 0 }\n'\
        + 'MERGE (n:TestNode {test_str: "test value"});\n'\
        + 'MERGE (n:TestNode {test_int: 2});'
    with open(fname, "w") as f:
        f.write(s)

def write_query_set_3_to_file(fname):
    s = 'MERGE (n:TestNode {test_param: $paramval});'
    with open(fname, "w") as f:
        f.write(s)


class GraphLoaderTestCase(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

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

        self.assertEqual(queries.next().statement,
            'MERGE (n:TestNode {test_str: "test value"});')
        self.assertEqual(queries.next().statement, 
            'MERGE (n:TestNode {test_int: 2});')
        self.assertEqual(queries.next().statement, 
            'MERGE (n:TestNode {test_bool: true});')

        with self.assertRaises(StopIteration):
            queries.next()

    def test_load_cypher_with_file_suffix(self):
        """Check load_cypher works when a file suffix is specified."""
        fname1 = path.join(self.test_dir, "file1_include.cql")
        write_query_set_1_to_file(fname1)

        fname2 = path.join(self.test_dir, "file2.cql")
        write_query_set_2_to_file(fname2)

        gl = GraphLoader()
        gl.load_cypher(self.test_dir, cypher_file_suffix="_include")
        queries = gl.iterqueries()

        self.assertEqual(queries.next().statement, 
            'MERGE (n:TestNode {test_bool: true});')

        with self.assertRaises(StopIteration):
            queries.next()

    def test_load_cypher_with_global_params(self):
        """Check load_cypher can take global params."""
        #assert False, "Implement test Check load_cypher can take global params"
        fname = path.join(self.test_dir, "file.cql")
        write_query_set_3_to_file(fname)

        gl = GraphLoader()
        gl.load_cypher(self.test_dir, 
            global_params={"paramval": 5, "dummy_paramval": 3})
        queries = gl.iterqueries()

        this_query = queries.next()
        self.assertEqual(this_query.statement, 
            'MERGE (n:TestNode {test_param: $paramval});')

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
        write_query_set_1_to_file(file1) # in dir1

        file2 = path.join(dir2, "file2.cql")
        write_query_set_2_to_file(file2) # in dir 2

        gl = GraphLoader()
        gl.load_cypher(dir1)
        gl.load_cypher(dir2)
        queries = gl.iterqueries()

        self.assertEqual(queries.next().statement, 
            'MERGE (n:TestNode {test_bool: true});')        
        self.assertEqual(queries.next().statement,
            'MERGE (n:TestNode {test_str: "test value"});')
        self.assertEqual(queries.next().statement, 
            'MERGE (n:TestNode {test_int: 2});')

        with self.assertRaises(StopIteration):
            queries.next()