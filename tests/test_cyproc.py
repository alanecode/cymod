from __future__ import print_function

import shutil, tempfile
from os import path
import unittest

import pandas as pd
from cymod.cyproc import CypherQuery, CypherQuerySource


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

    def test_form(self):
        """CypherQuerySource.form returns source form (cypher or tabular)."""
        s1 = CypherQuerySource("queries.cql", "cypher", 10)
        self.assertEqual(s1.form, "cypher")

        s2 = CypherQuerySource(self.demo_table, "tabular", 2)
        self.assertEqual(s2.form, "tabular")

    def test_invalid_source_form_throws_error(self):
        """CypherQuerySource throws value error if invalid form given"""
        with self.assertRaises(ValueError):
            CypherQuerySource("queries.cql", "not_cypher_or_tabular", 10)

    def test_repr(self):
        """__repr__ should be as expected."""
        s1 = CypherQuerySource("queries.cql", "cypher", 10)
        self.assertEqual(str(s1), 
            "form: cypher\nindex: 10\nref: queries.cql")

        s2 = CypherQuerySource(self.demo_table, "tabular", 2)
        self.assertEqual(str(s2), 
            "form: tabular\nindex: 2\nref: " \
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


class CypherFileTestCase(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)
