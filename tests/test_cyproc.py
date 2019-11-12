# -*- coding: utf-8 -*-
"""
Tests for cymod.cyproc
"""
from __future__ import print_function

import shutil, tempfile
import os
from os import path
import json
import unittest
import warnings

import six

from cymod.cyproc import CypherFile, CypherFileFinder
from cymod.cybase import CypherQuery, CypherQuerySource


def touch(path):
    """Immitate *nix `touch` behaviour, creating directories as required."""
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
    with open(path, "a"):
        os.utime(path, None)


class CypherFileTestCase(unittest.TestCase):
    def dummy_cypher_file_writer_maker(self, statement_list, params=None):
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
            pg_break_str = "//" + "=" * 77 + "\n"
            return pg_break_str + "// file: " + file_name + "\n" + pg_break_str + "\n"

        def dummy_cypher_file(file_handle):
            file_handle.write(file_header_string(file_handle.name))
            if params:
                file_handle.write(
                    json.dumps(params, sort_keys=True, indent=2, separators=(",", ": "))
                    + "\n" * 2
                )
            for i, s in enumerate(statement_list):
                if i:
                    print("\n")
                file_handle.write(s + "\n" * 2)

        return dummy_cypher_file

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

        # Make dummy cypher files
        # three queries with no parameters
        write_three_query_explicit_cypher_file = self.dummy_cypher_file_writer_maker(
            [
                "// Statement 1\n"
                + 'MERGE (n1:TestNode {role:"influencer", name:"Sue"})'
                + "-[:INFLUENCES]->\n"
                + '(n2:TestNode {name:"Billy", role:"follower"});',
                "// Statement 2\n"
                + 'MATCH (n:TestNode {role:"influencer"})\n'
                + 'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", '
                + 'name:"Sarah"});',
                "// Statement 3\n" + 'MERGE (n:TestNode {role:"loner", name:"Tim"});',
            ]
        )

        self.three_query_explicit_cypher_file_name = path.join(
            self.test_dir, "three_query_explicit.cql"
        )
        with open(self.three_query_explicit_cypher_file_name, "w") as f:
            write_three_query_explicit_cypher_file(f)

        # two queries including parameters given in file
        write_two_query_param_cypher_file = self.dummy_cypher_file_writer_maker(
            [
                # statement 1
                'MERGE (n1:TestNode {role:"influencer", name:$name1})'
                + "-[:INFLUENCES]->\n"
                + '(n2:TestNode {name:"Billy", role:"follower"});',
                # statement 2
                'MATCH (n:TestNode {role:"influencer"})\n'
                + 'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", '
                + "name:$name2});",
            ],
            params={"name1": "Sue", "name2": "Sarah"},
        )

        self.two_query_param_cypher_file_name = path.join(
            self.test_dir, "two_query_param.cql"
        )
        with open(self.two_query_param_cypher_file_name, "w") as f:
            write_two_query_param_cypher_file(f)

        # two parameters used in file, but only one specified. Other one must
        # be given in global_parameters
        write_two_query_partial_param_cypher_file = self.dummy_cypher_file_writer_maker(
            [
                # statement 1
                'MERGE (n1:TestNode {role:"influencer", name:$name1})'
                + "-[:INFLUENCES]->\n"
                + '(n2:TestNode {name:"Billy", role:"follower"});',
                # statement 2
                'MATCH (n:TestNode {role:"influencer"})\n'
                + 'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", '
                + "name:$name2});",
            ],
            params={"name1": "Sue"},
        )

        self.two_query_partial_param_cypher_file_name = path.join(
            self.test_dir, "two_query_partial_param.cql"
        )
        with open(self.two_query_partial_param_cypher_file_name, "w") as f:
            write_two_query_partial_param_cypher_file(f)

        # One parameter used in file, none specified. Single paramter must be
        # given in global_parameters
        write_one_query_req_global_param_cypher_file = self.dummy_cypher_file_writer_maker(
            [
                # statement 1
                "MERGE (n:TestNode {test_param: $paramval});"
            ]
        )

        self.one_query_req_global_param_cypher_file_name = path.join(
            self.test_dir, "one_query_req_global_param.cql"
        )
        with open(self.one_query_req_global_param_cypher_file_name, "w") as f:
            write_one_query_req_global_param_cypher_file(f)

        # One query file with two parameters, one of which is the special
        # 'priority' parameter which specifies the order in which files should
        # be loaded.
        write_one_query_param_cypher_file_w_priority = self.dummy_cypher_file_writer_maker(
            [
                # statement 1
                'MERGE (n1:TestNode {role:"influencer", name:$name1})'
                + "-[:INFLUENCES]->\n"
                + '(n2:TestNode {name:"Billy", role:"follower"});'
            ],
            params={"name1": "Sue", "priority": 2},
        )

        self.one_query_param_cypher_file_w_priority_name = path.join(
            self.test_dir, "one_query_param_w_priority.cql"
        )
        with open(self.one_query_param_cypher_file_w_priority_name, "w") as f:
            write_one_query_param_cypher_file_w_priority(f)

    def tearDown(self):
        # Remove the temp directory after the test
        shutil.rmtree(self.test_dir)

    def test_priority_is_retrievable(self):
        """CypherFile.priority should be retrieved from file parameters.
        
        If not specified, priority assumed to be 0.
        """
        cf1 = CypherFile(self.one_query_param_cypher_file_w_priority_name)
        self.assertEqual(cf1.priority, 2)

        cf2 = CypherFile(self.three_query_explicit_cypher_file_name)
        self.assertEqual(cf2.priority, 0)

        cf3 = CypherFile(self.two_query_partial_param_cypher_file_name)
        self.assertEqual(cf3.priority, 0)

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
        self.assertEqual(
            cf.queries[0].statement,
            'MERGE (n1:TestNode {role:"influencer", name:"Sue"})'
            + "-[:INFLUENCES]-> "
            + '(n2:TestNode {name:"Billy", role:"follower"});',  # newlines replaced with spaces
        )

        self.assertEqual(
            cf.queries[1].statement,
            'MATCH (n:TestNode {role:"influencer"}) '
            + 'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", '
            + 'name:"Sarah"});',
        )

        self.assertEqual(
            cf.queries[2].statement, 'MERGE (n:TestNode {role:"loner", name:"Tim"});'
        )

    def test_explicit_statement_file_has_expected_params(self):
        """Cypher file with three explicit statements has no params."""
        cf = CypherFile(self.three_query_explicit_cypher_file_name)
        self.assertEqual(cf.queries[0].params, {})
        self.assertEqual(cf.queries[1].params, {})
        self.assertEqual(cf.queries[2].params, {})

    def test_parameterised_statement_file_has_expected_queries(self):
        """Cypher file with two parameter statements has expected queries."""
        cf = CypherFile(self.two_query_param_cypher_file_name)

        self.assertEqual(
            cf.queries[0].statement,
            'MERGE (n1:TestNode {role:"influencer", name:$name1})'
            + "-[:INFLUENCES]-> "
            + '(n2:TestNode {name:"Billy", role:"follower"});',
        )

        self.assertEqual(
            cf.queries[1].statement,
            'MATCH (n:TestNode {role:"influencer"}) '
            + 'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", '
            + "name:$name2});",
        )

    def test_parameterised_statement_file_has_expected_params(self):
        """Cypher file with two param statements has expected params."""
        cf = CypherFile(self.two_query_param_cypher_file_name)
        self.assertEqual(cf.queries[0].params, {"name1": "Sue"})
        self.assertEqual(cf.queries[1].params, {"name2": "Sarah"})

    def test_partially_param_statement_file_has_expected_queries(self):
        """Partially parameterised statement file has expected queries."""
        cf = CypherFile(self.two_query_partial_param_cypher_file_name)

        self.assertEqual(
            cf.queries[0].statement,
            'MERGE (n1:TestNode {role:"influencer", name:$name1})'
            + "-[:INFLUENCES]-> "  # newline replaced with space +
            '(n2:TestNode {name:"Billy", role:"follower"});',
        )

        self.assertEqual(
            cf.queries[1].statement,
            'MATCH (n:TestNode {role:"influencer"}) '
            + 'MERGE (n)-[:INFLUENCES]->(:TestNode {role:"follower", '
            + "name:$name2});",
        )

    def test_partially_param_statement_file_has_expected_params(self):
        """Partially parameterised statement file has expected params."""
        cf = CypherFile(self.two_query_partial_param_cypher_file_name)
        self.assertEqual(cf.queries[0].params, {"name1": "Sue"})

        self.assertEqual(
            cf.queries[1].params,
            {"name2": None},
            "No relevant parameters specified in file",
        )

    def test_file_with_no_explicit_parameters_works(self):
        """Okay to have no params specified but require global params."""
        cf = CypherFile(self.one_query_req_global_param_cypher_file_name)
        self.assertEqual(cf.queries[0].params, {"paramval": None})

    def test_params_specifications_allowed(self):
        """Confirm various allowed parameter specifications register."""
        f1_name = path.join(self.test_dir, "queries1.cql")
        with open(f1_name, "w") as f:
            f.write(
                '{ "priority": 1, "other_param": "some value" }\n'
                + "MATCH (n) RETURN n;"
            )
        cf = CypherFile(f1_name)
        self.assertEqual(cf.priority, 1)

    def test_extant_but_empty_file_gives_warning(self):
        """If a Cypher file doesn't contain any queries it should warn user."""
        empty_fname = os.path.join(self.test_dir, "empty.cql")
        touch(empty_fname)
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            # Trigger the warning
            CypherFile(empty_fname)
            assert len(w) == 1
            assert issubclass(w[-1].category, UserWarning)
            assert "No queries found in " + empty_fname == str(w[-1].message)


class CypherFileFinderTestCase(unittest.TestCase):
    def make_empty_dummy_files(self, root_dir, fname_list):
        """Creates empty files with the names given in fname_list."""
        for fname in fname_list:
            touch(os.path.join(root_dir, fname))

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

    def test_files_in_arbitrary_dir_depth_found(self):
        """CypherFileFinder should explore to leaves of directory tree."""
        dummy_files = [
            path.join("dir1", "dir_1_1", "dir_1_1_1", "queries1.cql"),
            path.join("dir1", "dir_1_2", "queries2.cql"),
            path.join("dir2", "queries3.cql"),
        ]

        self.make_empty_dummy_files(self.test_dir, dummy_files)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cff = CypherFileFinder(self.test_dir)

            self.assertEqual(len(list(cff.iterfiles())), 3)

    def test_default_cypher_file_extensions_recognised(self):
        """Exts .cypher, .cql and .cyp should be recognised by default."""
        dummy_files = [
            path.join("dir1", "queries1.cypher"),
            path.join("dir1", "queries2.cql"),
            path.join("dir2", "queries3.cyp"),
            path.join("dir2", "exclude_queries.notcypher"),
        ]

        self.make_empty_dummy_files(self.test_dir, dummy_files)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cff = CypherFileFinder(self.test_dir)

            self.assertEqual(len(list(cff.iterfiles())), 3)

    def test_custom_cypher_file_extension_can_be_used(self):
        """User can specify use of custom file name extension .mycypher."""
        dummy_files = [
            path.join("dir1", "queries1.mycypher"),
            path.join("dir1", "queries2.mycypher"),
            path.join("dir2", "exclude_queries.notcypher"),
        ]

        self.make_empty_dummy_files(self.test_dir, dummy_files)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cff = CypherFileFinder(self.test_dir, cypher_exts=[".mycypher"])

            self.assertEqual(len(list(cff.iterfiles())), 2)

    def test_cypher_file_suffix_recognised(self):
        """User can specify file name suffix to filter files to load."""
        dummy_files = [
            path.join("dir1", "queries1_w.cql"),
            path.join("dir1", "exclude_queries2.cql"),
            path.join("dir2", "queries3_w.cql"),
        ]

        self.make_empty_dummy_files(self.test_dir, dummy_files)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cff = CypherFileFinder(self.test_dir, cypher_file_suffix="_w")

            self.assertEqual(len(list(cff.iterfiles())), 2)

    def test_repr_is_as_expected(self):
        dummy_files = [
            path.join("dir1", "queries1.cql"),
            path.join("dir2", "queries2.cql"),
        ]

        self.make_empty_dummy_files(self.test_dir, dummy_files)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cff = CypherFileFinder(self.test_dir)

        exp_repr = "[\n{0}\n{1}\n]".format(
            path.join(self.test_dir, "dir1", "queries1.cql"),
            path.join(self.test_dir, "dir2", "queries2.cql"),
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.assertEqual(repr(cff), exp_repr)

    def test_can_sort_files_by_priority(self):
        """Should sort files so they emerge in priority order.
        
        Correct order is:
        1. queries1.cql OR queries5.cql
        2. queries1.cql OR queries5.cql
        3. queries3.cql OR queries4.cql
        4. queries3.cql OR queries4.cql
        5. queries2.cql
        
        """
        f1_name = path.join(self.test_dir, "queries1.cql")
        with open(f1_name, "w") as f:
            f.write('{ "priority": 0 }\nMATCH (n) RETURN n;')

        f2_name = path.join(self.test_dir, "queries2.cql")
        with open(f2_name, "w") as f:
            f.write('{ "priority": 3 }\nMATCH (n) RETURN n;')

        f3_name = path.join(self.test_dir, "queries3.cql")
        with open(f3_name, "w") as f:
            f.write('{ "priority": 1 }\nMATCH (n) RETURN n;')

        f4_name = path.join(self.test_dir, "queries4.cql")
        with open(f4_name, "w") as f:
            f.write('{ "priority": 1 }\nMATCH (n) RETURN n;')

        f5_name = path.join(self.test_dir, "queries5.cql")
        touch(f5_name)  # should be assumed priority 0

        cff = CypherFileFinder(self.test_dir)
        file_iter = cff.iterfiles(priority_sorted=True)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            this_fname = six.next(file_iter).filename
            self.assertTrue((this_fname == f1_name) or (this_fname == f5_name))

            this_fname = six.next(file_iter).filename
            self.assertTrue((this_fname == f1_name) or (this_fname == f5_name))

        this_fname = six.next(file_iter).filename
        self.assertTrue((this_fname == f3_name) or (this_fname == f4_name))

        this_fname = six.next(file_iter).filename
        self.assertTrue((this_fname == f3_name) or (this_fname == f4_name))

        this_fname = six.next(file_iter).filename
        self.assertTrue(this_fname == f2_name)

        with self.assertRaises(StopIteration):
            six.next(file_iter)
