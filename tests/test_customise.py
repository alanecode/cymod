# -*- coding: utf-8 -*-
"""
Tests for cymod.customise
"""
from __future__ import print_function

import unittest

from cymod.customise import NodeLabels


class CustomLabelsTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_error_if_invalid_label_given(self):
        """ValueError raised if nonsense original node label given."""
        with self.assertRaises(ValueError):
            NodeLabels(
                {
                    "State": "MyState",
                    "BadTransition": "MyTransition",
                    "BadCondition": "MyCondition",
                }
            )

    def test_custom_values_returned_when_specified(self):
        cl = NodeLabels({"State": "MyState"})
        self.assertEqual(cl.state, "MyState")

    def test_default_values_returned_if_unspecified(self):
        cl = NodeLabels({"State": "MyState"})
        self.assertEqual(cl.transition, "Transition")
        self.assertEqual(cl.condition, "Condition")
