# -*- coding: utf-8 -*-
"""
cymod.load
~~~~~~~~~~

Loads Cypher files and uses them to construct and execute a database
transaction.

Checks the database for the existence of nodes with the same global Parameters
and carries out some user defined behaviour, e.g. append or remove and replace

Previously all code needed to do the following was in the GraphLoader
class:
1. load cypher and parameters from files,
2. load connect to a Neo4j server instance, and
3. insert cypher data into that database

Now 1 is handled by the GraphLoader class. 2 and 3 are achieved by
ServerGraphLoader.

"""
from __future__ import print_function

import sys
import os
import json

from six import iteritems, iterkeys

from neo4j.v1 import GraphDatabase
from neo4j.exceptions import CypherSyntaxError

from cymod.cyproc import CypherFileFinder


class GraphLoader(object):
    """Process requests to load data from files, generate a stream of queries.

    Attributes:
        _load_job_queue (list of :obj:`CypherFileFinder`): A queue containing
            objects which should be handled in order to generate cypher 
            queries.             
    """
    def __init__(self):
        self._load_job_queue = [] 

    def load_cypher(self, root_dir, cypher_file_suffix=None, 
        global_params=None):
        """Add Cypher files to the list of jobs to be loaded.
        
        Args:
            root_dir (str): File system path to root directory to search for 
                Cypher files.
            cypher_file_suffix (str): Suffix at the end of file names 
                (excluding file extension) which indicates file should be 
                loaded into the database. E.g. if files ending '_w.cql' 
                should be loaded, use cypher_file_suffix='_w'. Defaults to 
                None.        
        """
        cff = CypherFileFinder(root_dir, cypher_file_suffix=cypher_file_suffix)
        if global_params:
            self._load_job_queue.append(
                {"file_finder": cff, "global_params": global_params})
        else:
            self._load_job_queue.append(cff)

    def iterqueries(self):
        """Provide an iterable over Cypher queries from loaded sources.

        Yields:
            :obj:`CypherQuery`: Appropriately ordered Cypher queries.        
        
        """
        for load_job in self._load_job_queue:
            if isinstance(load_job, CypherFileFinder):
                for cypher_file in load_job.iterfiles(priority_sorted=True):
                    for query in cypher_file.queries:
                        yield query

            if isinstance(load_job, dict):
                # Case where load_job is a CypherFileFinder with global params
                cff = load_job["file_finder"]
                global_params = load_job["global_params"]
                for cypher_file in cff.iterfiles(priority_sorted=True):
                    for query in cypher_file.queries:
                        # Get list of parameters in query with None value and 
                        # attempt to replace None with value from global_params
                        unspecified_native_params \
                            = [k for k in query.params if not query.params[k]]
                        for unspecified_param in unspecified_native_params:
                            try:
                                query.params[unspecified_param] \
                                    = global_params[unspecified_param]
                            except KeyError:
                                # Raise an exception if a query has a required 
                                # None parameter at this stage
                                raise KeyError("The following query requires "\
                                    + "a parameter not given in its "\
                                    + "originating Cypher file, nor in the "\
                                    + "provided global parameters:\n"\
                                    + str(query))

            else:
                # assumed load_job is a tabular data source
                pass


    def _load_global_params(self):
        """Read global parameters from instance's global_param_file.

        Global parameters will be provided to all queries executed while
        loading the graph. These should be provided in a json file.

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
        cff = CypherFileFinder(root_dir, cypher_file_suffix=fname_suffix)
        return list(cff.iterfiles())

    def _get_sorted_cypher_files(self, unsorted_cypher_files):
        """Create a stack of CypherFile objects oredered ready to be loaded.

        Priority 0 files should be at the top of the stack.

        TODO: method contents. MOVE FUNCTIONALITY OF SORTING FILES BY PRIORITY
        TO THE CypherFileFinder CLASS

        """
        def get_priority_number(cypher_file, max_priority):
            """Return numerical value to sort cypher file priorities.

            If no priority is specified for a cypher file, its `priority`
            attribute will be None. To account for this when sorting, all files
            which have not been given a priority are given the same
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

    def _get_joint_params(self, cypher_file):
        """Unite file-local and global parameters into a single dict."""
        if cypher_file.params:
            params = cypher_file.params.copy()
        else:
            params = {}
        params.update(self.global_params)
        return params

    def _parse_query_params(self, query, params):
        """Construct a query from parameterised query and parameters.

        Given an individual Cypher query and a dictionary of corresponding
        parameters, return a string in which the parameters have been
        substituted into the query.
        
        Args:
            query (str): The parameterised query string
            params (dict): Key/value pairs where the keys are parameter names
                appearing in the parameterised query, and the values are the
                values to be substituted in.
        Returns:
            query_string (str): Complete query with concrete values replacing
            parameters.
        """
        for k in iterkeys(params):
            query = query.replace('$'+str(k), '"'+str(params[k])+'"')
        return query


class ServerGraphLoader(GraphLoader):
    """Loads Cypher data into a running Neo4j database instance."""

    def __init__(self, uri, username, password, root_dir, fname_suffix,
                 global_param_file=None, refresh_graph=False):
        super(ServerGraphLoader, self).__init__(root_dir,
                                                fname_suffix,
                                                global_param_file)
        self.driver = self._get_graph_driver(uri, username, password)
        self.refresh_graph = refresh_graph

    def _get_graph_driver(self, uri, username, password):
        """Attempt to obtain a driver for Neo4j server.

        exit program if we can't obtain a driver

        returns a connected GraphDatabase.driver

        TODO finish this docstring

        """
        try:
            driver = GraphDatabase.driver(uri,
                                          auth=(username, password))
            return driver
        except Exception as e:
            print('Could not load graph. Check password.', file=sys.stderr)
            print('Exception: %s' % str(e), file=sys.stderr)
            sys.exit(1)

    def _refresh_graph(self):
        """Delete nodes in the graph with the global parameters of this model.

        For cases where we want to update the model specified by the
        combination of global parameters specified in self.global_param_file
        with new data it is necessary to first delete existing data matching
        those parameters.

        This method will run an appropriate query to refresh thee graph ahead
        of loading new data.

        """
        def remove_all_nodes(tx, global_params):
            """Remove nodes with properties specified in global_params.

            A callback function called by driver.session().write_transaction

            Args:
                tx: database transaction object provided by write_transaction.
                global_params (dict): key/value pairs of property names and
                    their values which together specify the model to be
                    refreshed.
            """
            # construct query string from provided global_params dict
            WHERE_CLAUSE = ' AND '.join(['n.'+str(k)+'='+'"'+str(v)+'"'
                                         for (k, v)
                                         in iteritems(global_params)])
            q = 'MATCH (n) WHERE '+WHERE_CLAUSE+' DETACH DELETE n'
            print('Removing existing nodes matching global parameters')
            tx.run(q)

        with self.driver.session() as session:
            try:
                session.write_transaction(remove_all_nodes, self.global_params)
            except CypherSyntaxError as e:
                print('Error in Cypher refreshing database. Check syntax.',
                      file=sys.stderr)
                print('Exception: %s' % str(e), file=sys.stderr)
                sys.exit(1)

    def _load_cypher_file_queries(self, cypher_file):
        """Load all queries in an individual cypher file into the graph."""
        params = self._get_joint_params(cypher_file)

        def run_cypher_file_query(tx, query_string, params):
            """Run concrete query (no params) against session transaction."""
            tx.run(self._parse_query_params(query_string, params))

        with self.driver.session() as session:
            for q in cypher_file.queries:
                session.write_transaction(run_cypher_file_query, q, params)

    def load_cypher(self):
        """Load all Cypher files into the graph."""
        # Creating a copy of the cypher files list avoids side effect of
        # erasing list of files to be loaded during the course of loading
        files = list(self.cypher_files)
        num_files = len(files)
        if self.refresh_graph:
            self._refresh_graph()

        while files:
            f = files.pop()
            print('loading '+os.path.basename(f.filename))
            self._load_cypher_file_queries(f)

        print('\nFinished loading {0} Cypher files'.format(num_files))


class EmbeddedGraphLoader(GraphLoader):
    """Loads Cypher data and provides interface to access it

    Provides an embedded jython-friendly interface to the cypher data. This
    will allow cymod to be used within Java applicatons to provide data to an
    embedded Neo4j instance.

    Using cymod within Java applications is a big help when using Neo4j and
    Cypher in situations in which a Java application will have access to a JRE
    but no Neo4j server will be available.

    """
    def __init__(self, root_dir, fname_suffix, global_param_file=None):
        super(EmbeddedGraphLoader, self).__init__(root_dir,
                                                  fname_suffix,
                                                  global_param_file)

    def _get_cypher_file_queries(self, cypher_file):
        """Return concrete (no params) queries for CypherFile"""
        params = self._get_joint_params(cypher_file)
        queries = [self._parse_query_params(q, params)
                   for q
                   in cypher_file.queries]
        return queries

    def query_generator(self):
        """Go through each query in each file in turn, yielding queries."""
        files = list(self.cypher_files)
        num_files = len(files)
        nf = 0
        nq = 0
        while nf < num_files:
            # lis of concrete (no params) queries for each CypherFile in
            # loop
            queries = self._get_cypher_file_queries(files[nf])
            no_queries = len(queries)
            print('Processing {0} queries in file {1}'
                  .format(no_queries, files[nf].filename))
            while nq < len(queries):
                yield queries[nq]
                nq += 1
            nf += 1
            nq = 0
