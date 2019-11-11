# -*- coding: utf-8 -*-
"""
Tests for cymod.cybase
"""
from __future__ import print_function

import shutil, tempfile
from os import path
import json
import unittest

from cymod.params import validate_cypher_params, read_params_file


class ReadParamsTestCase(unittest.TestCase):
    def make_test_param_files(self):
        # Make valid test file
        self.test_param_json_file_name = path.join(self.test_dir, "test_params.json")
        with open(self.test_param_json_file_name, "w") as f:
            f.write('{"param1": 10, "param2": "some_string", "param3": true}')

    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()
        self.make_test_param_files()

    def tearDown(self):
        # Remove the temp directory after the test
        shutil.rmtree(self.test_dir)

    def test_json_file_can_be_read(self):
        """Should be able to read json file to dictionary."""
        params = read_params_file(self.test_param_json_file_name, ftype="json")
        self.assertEqual(
            params, {"param1": 10, "param2": "some_string", "param3": True}
        )


class ValidateParamsTestCase(unittest.TestCase):
    def test_validate_cypher_params_ensures_param_name_is_string(self):
        """Should detect if parameter name is a string, error if not."""
        with self.assertRaises(TypeError):
            validate_cypher_params({1: "key_is_bad"})

        with self.assertRaises(TypeError):
            validate_cypher_params({True: "key_is_bad"})

        self.assertTrue(validate_cypher_params({"param1": "key_is_okay"}))
