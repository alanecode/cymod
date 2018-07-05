# -*- coding: utf-8 -*-
"""graphloader.py

Loads Cypher files and uses them to construct and execute a database
transaction.

Checks the database for the existence of nodes with the same global Parameters
and carries out some user defined behaviour, e.g. append or remove and replace
"""
from __future__ import print_function

import sys
import os
import json
import time

from neo4j.exceptions import ServiceUnavailable

from py2neo import Graph
from py2neo.database import ClientError

from filesystem import CypherFile, CypherFileFinder

class GraphLoader(object):
    """Retrieve all Cypher data from the file system.

    Also stores information about parameters if given.
    """
    def __init__(self, root_dir, fname_suffix, global_param_file=None):
        self.global_param_file = global_param_file
        self.global_params = self._load_global_params()
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

class ServerGraphLoader(GraphLoader):
    """Loads Cypher data into a running Neo4j database instance."""

    def __init__(self, hostname, username, password, root_dir, fname_suffix,
                 global_param_file=None, refresh_graph=False):
        super(ServerGraphLoader, self).__init__(root_dir,
                                                fname_suffix,
                                                global_param_file)
        self.graph = self._get_graph(hostname, username, password)
        self.refresh_graph = refresh_graph

    def _get_graph(self, hostname, username, password):
        """Attempt to connect to Neo4j graph.
        
        exit program if we can't connect to the graph

        returns a connected py2neo Graph object

        TODO finish this docstring

        """
        try:
            graph = Graph(host=hostname, user=username, password=password)
            return graph
        except (KeyError, ClientError, ServiceUnavailable) as e:
            print('Could not load graph. Check password.', file=sys.stderr)
            print('Exception: %s' % str(e), file=sys.stderr)
            sys.exit(1)

    def _refresh_graph(self):
        """Delete nodes in the graph with the global parameters of this model.

        For cases where we want to update the model specified by the combination
        of global parameters specified in self.global_param_file with new data
        it is necessary to first delete existing data matching those parameters.

        This method will run an appropriate query to refresh thee graph ahead
        of loading new data.
        """
        q = 'MATCH (n) WHERE n.model_ID=$model_ID DETACH DELETE n'
        print('Removing existing nodes matching global parameters')
        self.graph.run(q, self.global_params)

    def _load_cypher_file_queries(self, cypher_file, global_params):
        """Load all queries in an individual cypher file into the graph."""
        if cypher_file.params:
            params = cypher_file.params.copy()
        else:
            params = {}
        params.update(self.global_params)
        for q in cypher_file.queries:
            self.graph.run(q, params)

    def load_cypher(self):
        """Load all Cypher files into the graph."""
        # Creating a copy of the cypher files list avoids side effect of erasing
        # list of files to be loaded during the course of loading
        files = list(self.cypher_files)
        num_files = len(files)
        if self.refresh_graph:
            self._refresh_graph()

        while files:
            f = files.pop()
            print('loading '+os.path.basename(f.filename))
            self._load_cypher_file_queries(f, self.global_params)

        print('\nFinished loading {0} Cypher files'.format(num_files))

