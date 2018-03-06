#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""graphloader.py

Loads Cypher files and uses them to construct and execute a database
transaction.

Checks the database for the existence of nodes with the same global Parameters
and carries out some user defined behaviour, e.g. append or remove and replace
"""
from filesystem import CypherFile, CypherFileFinder

class GraphLoader(object):

    def __init__(self, username, password, root_dir, global_param_file):
        self.cypher_files
        self.global_param_file # file containing global parameters
