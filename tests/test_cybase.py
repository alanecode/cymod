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

from cymod.cybase import CypherQuery, CypherQuerySource


class CypherQuerySourceTestCase(unittest.TestCase):
    def setUp(self):
        self.demo_table = pd.DataFrame(
            {
                "start": ["state1", "state2"],
                "end": ["state2", "state3"],
                "cond": ["low", "high"],
            }
        )
        # Ensure columns are in a predictable order
        self.demo_table = self.demo_table[["start", "end", "cond"]]

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
        self.assertEqual(str(s1), "ref_type: cypher\nindex: 10\nref: queries.cql")

        s2 = CypherQuerySource(self.demo_table, "tabular", 2)
        self.assertEqual(
            str(s2),
            "ref_type: tabular\nindex: 2\nref:"
            "     start     end  cond\n0  state1  state2   low\n"
            "1  state2  state3  high",
        )


class CypherQueryTestCase(unittest.TestCase):
    def test_statement(self):
        """CypherQuery.statement should return query string."""
        q = CypherQuery("MATCH (n) RETURN n LIMIT 10;")
        self.assertEqual(q.statement, "MATCH (n) RETURN n LIMIT 10;")

    def test_params(self):
        """CypherQuery.params should return parameter dict."""
        q = CypherQuery(
            "MATCH (n) WHERE n.prop=$prop RETURN n LIMIT 10;",
            params={"prop": "test-prop"},
        )
        self.assertEqual(q.params, {"prop": "test-prop"})

    def test_source(self):
        """CypherQuery.source should return CypherQuerySource object."""
        q = CypherQuery(
            "MATCH (n) RETURN n LIMIT 10;",
            source=CypherQuerySource("queries.cql", "cypher", 10),
        )
        self.assertIsInstance(q.source, CypherQuerySource)
