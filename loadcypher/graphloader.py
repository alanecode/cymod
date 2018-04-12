#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""graphloader.py

Loads Cypher files and uses them to construct and execute a database
transaction.

Checks the database for the existence of nodes with the same global Parameters
and carries out some user defined behaviour, e.g. append or remove and replace
"""
import sys
import json

from py2neo import Graph
from httpstream.http import ClientError
from py2neo.database.http import Unauthorized

from filesystem import CypherFile, CypherFileFinder

class GraphLoader(object):

    def __init__(self, hostname, username, password, root_dir, fname_suffix,
            global_param_file=None):
        self.global_param_file = global_param_file
        self.global_params = self._load_global_params()
        self.graph = self._get_graph(hostname, username, password)
        self.cypher_files = self._get_sorted_cypher_files(self._get_cypher_files(root_dir, fname_suffix))

    def _load_global_params(self):
        """Read global parameters from instance's global_param_file.

        Global parameters will be provided to all queries executed while loading
        the graph. These should be provided in a json file.

        Returns:
            dict: Key/value pairs specifying global parameters.
        """
        if self.global_param_file:
            with open(self.global_param_file, 'r') as f:
                return json.load(f)
        else:
            return dict()

    def _get_graph(self, hostname, username, password):
        """Attempt to connect to Neo4j graph.

        exit program if we can't connect to the graph

        returns a connected py2neo Graph object

        TODO finish this docstring

        """
        try:
            graph = Graph(host=hostname, user=username, password=password)
            return graph
        except (KeyError, ClientError, Unauthorized) as e:
            print('Could not load graph. Check password.', file=sys.stderr)
            print('Exception: %s' % str(e), file=sys.stderr)
            sys.exit(1)

    def _get_cypher_files(self, root_dir, fname_suffix):
        """Load a list of Cypher files to be loaded into the Graph.

        Returns list of CypherFile-s"""
        cff = CypherFileFinder(root_dir, fname_suffix=fname_suffix)
        return cff.get_cypher_files()

    def _get_sorted_cypher_files(self, unsorted_cypher_files):
        """Create a stack of CypherFile objects oredered ready to be loaded.

        Priority 0 files should be at the top of the stack.

        TODO: method contents. MOVE FUNCTIONALITY OF SORTING FILES BY PRIORITY
        TO THE CypherFileFinder CLASS
        """
        def get_priority_number(cypher_file, max_priority):
            """Return numerical value to sort cypher file priorities.

            If no priority is specified for a cypher file, its `priority`
            attribute will be None. To account for this when sorting, all
            files which have not been given a priority are given the same
            `max_priority` value. In practice this can always be safely set to
            the total number of files.

            Files are sorted so highest priority files are at the end of the
            resulting list, i.e. are at the top of the stack.
            """
            p = cypher_file.priority
            if isinstance(p, int):
                return p
            else:
                return max_priority

        n = len(unsorted_cypher_files)
        return sorted(unsorted_cypher_files,
                key=lambda f: get_priority_number(f, n), reverse=True)

    def _load_cypher_query(self, cypher_file, global_params):
        """Load an individual cypher query into the graph.

        use py2neo Graph.run method using paramaters
        http://py2neo.org/v3/database.html#py2neo.database.Graph.run

        """

    def load_cypher(self):
        """Load Cypher files into the graph.

        """
        # do I need a copy of the list? Could be regenerated from files
        files = list(self.cypher_files)
        while files:
            f = files.pop()
            self._load_cypher_query(f, self.global_params)
