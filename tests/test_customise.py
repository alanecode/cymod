# -*- coding: utf-8 -*-
"""
Tests for cymod.customise
"""
from __future__ import print_function

import unittest

from cymod.customise import CustomLabels

class CustomLabelsTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_error_if_invalid_label_given(self):
        """ValueError raised if nonsense original node label given."""
        with self.assertRaises(ValueError):
            CustomLabels({"State": "MyState","BadTransition": "MyTransition", 
                "BadCondition": "MyCondition"})

    def test_custom_values_returned_when_specified(self):
        cl = CustomLabels({"State": "MyState"})
        self.assertEqual(cl.state, "MyState")

    def test_default_values_returned_if_unspecified(self):
        cl = CustomLabels({"State": "MyState"})
        self.assertEqual(cl.transition, "Transition")
        self.assertEqual(cl.condition, "Condition")

    

