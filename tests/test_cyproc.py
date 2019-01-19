# -*- coding: utf-8 -*-
"""
Tests for cymod.cyproc
"""
from __future__ import print_function

import shutil, tempfile
from os import path
import json
import unittest

import pandas as pd
from cymod.cyproc import CypherQuery, CypherQuerySource, CypherFile


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


    def dummy_cypher_file_writer_maker(self, statement_list, 
        params=None):
        """Callable which writes Cypher statements and parameters to file.

        Arguments configure the returned function by specifying the contents
        of the file which it will write.
        
        Args:
            statement_list (str): List of strings representing Cypher 
                statements which may or may not require parameters.
            params (dict of str: str, optional): Mappings from parameter names
                to their values for the present file.            
        """
        def file_header_string(file_name):
            pg_break_str = "//" + "="*77 + "\n"
            return pg_break_str + "// file: " + file_name + "\n" + \
                pg_break_str + "\n"

        def dummy_cypher_file(file_handle):
            file_handle.write(file_header_string(file_handle.name))
            if params:
                file_handle.write(json.dumps(params, sort_keys=True,
                    indent=2, separators=(',', ': ')) + "\n"*2)
            for i, s in enumerate(statement_list):
                if i:
                    print("\n")
                file_handle.write(s + "\n"*2)
        
        return dummy_cypher_file  

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

        # Make dummy cypher files
        # three queries with no parameters
        write_three_query_explicit_cypher_file = \
            self.dummy_cypher_file_writer_maker([
                '// Statement 1\n' +
                'MERGE (n1:TestNode {role:"influencer", name:"Sue"})' +
                '-[:INFLUENCES]->\n' +
                '(n2:TestNode {name:"Billy", role:"follower"});',
                
                '// Statement 2\n' +
                'MATCH (n:TestNode {role:"influencer"})\n' +
                'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", ' +
                'name:"Sarah"});',

                '// Statement 3\n' +
                'MERGE (n:TestNode {role:"loner", name:"Tim"});'
                ])
        
        self.three_query_explicit_cypher_file_name = path.join(self.test_dir,
            "three_query_explicit.cql")
        with open(self.three_query_explicit_cypher_file_name, "w") as f:
            write_three_query_explicit_cypher_file(f)

        # two queries including parameters given in file
        write_two_query_param_cypher_file = \
            self.dummy_cypher_file_writer_maker([
                # statement 1
                'MERGE (n1:TestNode {role:"influencer", name:$name1})' +
                '-[:INFLUENCES]->\n' +
                '(n2:TestNode {name:"Billy", role:"follower"});',
                # statement 2
                'MATCH (n:TestNode {role:"influencer"})\n' +
                'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", ' +
                'name:$name2});',
                ], params={"name1": "Sue", "name2": "Sarah"})

        self.two_query_param_cypher_file_name = path.join(self.test_dir,
            "two_query_param.cql")
        with open(self.two_query_param_cypher_file_name, "w") as f:
            write_two_query_param_cypher_file(f)

        # two parameters used in file, but only one specified. Other one must
        # be given in global_parameters
        write_two_query_partial_param_cypher_file = \
            self.dummy_cypher_file_writer_maker([
                # statement 1
                'MERGE (n1:TestNode {role:"influencer", name:$name1})' +
                '-[:INFLUENCES]->\n' +
                '(n2:TestNode {name:"Billy", role:"follower"});',
                # statement 2
                'MATCH (n:TestNode {role:"influencer"})\n' +
                'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", ' +
                'name:$name2});',
                ], params={"name1": "Sue"})
        
        self.two_query_partial_param_cypher_file_name = path.join(
            self.test_dir, "two_query_partial_param.cql")
        with open(self.two_query_partial_param_cypher_file_name, "w") as f:
            write_two_query_partial_param_cypher_file(f)
        

    def tearDown(self):
        # Remove the temp directory after the test
        shutil.rmtree(self.test_dir)

    def test_queries_is_a_tuple(self):
        """CypherFile.queries should be a tuple of CypherQuery objects."""
        cf1 = CypherFile(self.three_query_explicit_cypher_file_name)
        self.assertIsInstance(cf1.queries, tuple)
        
        cf2 = CypherFile(self.two_query_param_cypher_file_name)
        self.assertIsInstance(cf2.queries, tuple)

        cf3 = CypherFile(self.two_query_partial_param_cypher_file_name)
        self.assertIsInstance(cf3.queries, tuple)


    def test_queries_has_correct_number_of_elements(self):
        """Generator returned by queries() should have correct no. elements."""
        cf1 = CypherFile(self.three_query_explicit_cypher_file_name)
        self.assertEqual(len(cf1.queries), 3)
        
        cf2 = CypherFile(self.two_query_param_cypher_file_name)
        self.assertEqual(len(cf2.queries), 2)

        cf3 = CypherFile(self.two_query_partial_param_cypher_file_name)
        self.assertEqual(len(cf3.queries), 2)

    def test_explicit_statement_file_has_expected_queries(self):
        """Cypher file with three explicit statements has expected queries."""
        cf = CypherFile(self.three_query_explicit_cypher_file_name)
        self.assertEqual(cf.queries[0].statement,
            'MERGE (n1:TestNode {role:"influencer", name:"Sue"})' +
            '-[:INFLUENCES]->' + # newlines removed
            '(n2:TestNode {name:"Billy", role:"follower"});')

        self.assertEqual(cf.queries[1].statement,
            'MATCH (n:TestNode {role:"influencer"}) ' +
            'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", ' +
            'name:"Sarah"});')

        self.assertEqual(cf.queries[2].statement,
            'MERGE (n:TestNode {role:"loner", name:"Tim"});')

    def test_explicit_statement_file_has_expected_params(self):
        """Cypher file with three explicit statements has no params."""
        cf = CypherFile(self.three_query_explicit_cypher_file_name)
        self.assertEqual(cf.queries[0].params, None)
        self.assertEqual(cf.queries[1].params, None)
        self.assertEqual(cf.queries[2].params, None)

    def test_parameterised_statement_file_has_expected_queries(self):
        """Cypher file with two parameter statements has expected queries."""
        cf = CypherFile(self.two_query_param_cypher_file_name)

        self.assertEqual(cf.queries[0].statement,
            'MERGE (n1:TestNode {role:"influencer", name:$name1})' +
            '-[:INFLUENCES]->' +
            '(n2:TestNode {name:"Billy", role:"follower"});')

        self.assertEqual(cf.queries[1].statement,
            'MATCH (n:TestNode {role:"influencer"})\n' +
            'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", ' +
            'name:$name2});')

    def test_parameterised_statement_file_has_expected_params(self):
        """Cypher file with two param statements has expected params."""
        cf = CypherFile(self.two_query_param_cypher_file_name)
        self.assertEqual(cf.queries[0].params, {"name1": "Sue"})
        self.assertEqual(cf.queries[1].params, {"name2": "Sarah"})

    def test_partially_param_statement_file_has_expected_queries(self):
        """Partially parameterised statement file has expected queries."""
        cf = CypherFile(self.two_query_partial_param_cypher_file_name)

        self.assertEqual(cf.queries[0].statement,
            'MERGE (n1:TestNode {role:"influencer", name:$name1})' +
            '-[:INFLUENCES]->' +
            '(n2:TestNode {name:"Billy", role:"follower"});')

        self.assertEqual(cf.queries[1].statement,
            'MATCH (n:TestNode {role:"influencer"})\n' +
            'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", ' +
            'name:$name2});')

    def test_partially_param_statement_file_has_expected_params(self):
        """Partially parameterised statement file has expected params."""
        cf = CypherFile(self.two_query_partial_param_cypher_file_name)
        self.assertEqual(cf.queries[0].params, {"name1": "Sue"})

        self.assertEqual(cf.queries[1].params, {"name2": None}, 
            "No relevant parameters specified in file")        
    
    def exclude_test_check_write_test_cypher_file(self):
        with open(self.two_query_partial_param_cypher_file_name, "r") as f:
            print(f.read())