# -*- coding: utf-8 -*-
"""
Tests for cymod.cybase
"""
from __future__ import print_function

import shutil, tempfile
from os import path
import json
import unittest

import pandas as pd

from cymod.cybase import (CypherQuery, CypherQuerySource,
    CypherParams)


class CypherParamsTestCase(unittest.TestCase):

    def make_test_param_files(self):
        self.test_param_json_file_name \
            = path.join(self.test_dir, "test_params.json")
       
        with open(self.test_param_json_file_name, "w") as f:
            f.write('{"param1": 10, "param2": "some_string", "param3": true}')

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()
        self.make_test_param_files()

    def tearDown(self):
        # Remove the temp directory after the test
        shutil.rmtree(self.test_dir)
            
    def test_init_by_dict(self):
        """Should be able to initialise CypherParams with a dictionary."""
        cp = CypherParams({"param1": 10, "param2": "some_string"})
        self.assertEqual(cp["param1"], 10)
        self.assertEqual(cp["param2"], "some_string")

    def test_init_by_file(self):
        """Should be able to initialise CypherParams with a file name."""
        cp = CypherParams(self.test_param_json_file_name)
        self.assertEqual(cp["param1"], 10)
        self.assertEqual(cp["param2"], "some_string")
        self.assertEqual(cp["param3"], True)

    def test_can_add_items_to_params(self):
        """Should be able to add value like a dict."""
        cp = CypherParams({})
        cp["param1"] = 2
        self.assertEqual(cp["param1"], 2)

    def test_can_only_use_string_as_key(self):
        """Should throw a type error if key anything other than string."""
        with self.assertRaises(TypeError):
            CypherParams({1: "key_is_bad"})

        with self.assertRaises(TypeError):
            CypherParams({True: "key_is_bad"})

        self.assertEqual(CypherParams({"param1": "key_is_okay"})["param1"],
            "key_is_okay")

    def test_can_get_list_of_keys(self):
        """Should be able to get a list of parameter keys"""
        cp = CypherParams({"param1": 0, "param2": "one"})
        self.assertEqual(set(cp.keys()), set(["param1", "param2"]))

    def test_can_retrieve_dictionary(self):
        """Should be able to retrieve data as a vanilla dictionary."""
        cp = CypherParams({"param1": 0, "param2": "one"})
        self.assertEqual(cp.as_dict(), {"param2": "one", "param1": 0})

class CypherQuerySourceTestCase(unittest.TestCase):
    def setUp(self):
        self.demo_table = pd.DataFrame({
            "start": ["state1", "state2"], 
            "end": ["state2", "state3"], 
            "cond": ["low", "high"]
        })

    def tearDown(self):
        del self.demo_table    

    def test_ref(self):
        """CypherQuerySource.file_name returns source file name."""
        s = CypherQuerySource("queries.cql", "cypher", 10)
        self.assertEqual(s.ref, "queries.cql")

    def test_ref_type(self):
        """CypherQuerySource.ref_type returns source type (cypher or tabular)."""
        s1 = CypherQuerySource("queries.cql", "cypher", 10)
        self.assertEqual(s1.ref_type, "cypher")

        s2 = CypherQuerySource(self.demo_table, "tabular", 2)
        self.assertEqual(s2.ref_type, "tabular")

    def test_invalid_source_ref_type_throws_error(self):
        """CypherQuerySource throws value error if invalid ref_type given"""
        with self.assertRaises(ValueError):
            CypherQuerySource("queries.cql", "not_cypher_or_tabular", 10)

    def test_repr(self):
        """__repr__ should be as expected."""
        s1 = CypherQuerySource("queries.cql", "cypher", 10)
        self.assertEqual(str(s1), 
            "ref_type: cypher\nindex: 10\nref: queries.cql")

        s2 = CypherQuerySource(self.demo_table, "tabular", 2)
        self.assertEqual(str(s2), 
            "ref_type: tabular\nindex: 2\nref: " \
            "   cond     end   start\n0   low  state2  state1\n"\
            "1  high  state3  state2")

class CypherQueryTestCase(unittest.TestCase):
    def test_statement(self):
        """CypherQuery.statement should return query string."""
        q = CypherQuery("MATCH (n) RETURN n LIMIT 10;")
        self.assertEqual(q.statement, "MATCH (n) RETURN n LIMIT 10;")

    def test_params(self):
        """CypherQuery.params should return parameter dict."""
        q = CypherQuery("MATCH (n) WHERE n.prop=$prop RETURN n LIMIT 10;",
                params={"prop": "test-prop"})
        self.assertEqual(q.params, {"prop": "test-prop"})

    def test_source(self):
        """CypherQuery.source should return CypherQuerySource object."""
        q = CypherQuery("MATCH (n) RETURN n LIMIT 10;", 
            source=CypherQuerySource("queries.cql", "cypher", 10))
        self.assertIsInstance(q.source, CypherQuerySource)









