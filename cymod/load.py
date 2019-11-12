# -*- coding: utf-8 -*-
"""
cymod.load
~~~~~~~~~~

Loads Cypher files and/or tabular state transition data and uses this data to
construct and execute database transactions.

"""
from __future__ import print_function

import sys
import os
import json
import re

from six import iteritems, iterkeys

from neo4j import GraphDatabase
from neo4j.exceptions import CypherSyntaxError

from cymod.cyproc import CypherFileFinder
from cymod.tabproc import TransTableProcessor


class GraphLoader(object):
    """Process requests to load data from files, generate a stream of queries.

    Attributes:
        _load_job_queue (list of :obj:`CypherFileFinder`): A queue containing
            objects which should be handled in order to generate cypher 
            queries.             
    """

    def __init__(self):
        self._load_job_queue = []

    def load_cypher(self, root_dir, cypher_file_suffix=None, global_params=None):
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
                {"file_finder": cff, "global_params": global_params}
            )
        else:
            self._load_job_queue.append(cff)

    def load_tabular(
        self,
        df,
        start_state_col,
        end_state_col,
        labels=None,
        global_params=None,
        state_alias_translator=None,
    ):
        """Generate Cypher queries based data in a :obj:`pandas.DataFrame`.
        
        Args:
            df (:obj:`pandas.DataFrame`): Table whose rows specify transition
                rules which will be coverted into Cypher.
            start_state_col (str): Name of the column specifying the start 
                state of the transition described by each row.
            end_state_col (str): Name of the column specifying the end state of
                the transition described by each row.
            global_params (dict, optional): property name/ value pairs which 
                will be added as parameters to every query.     
            state_alias_translator (:obj:`EnvrStateAliasTranslator`): Container 
                for translations from codes to human readable values.       
        """
        tabular_src = TransTableProcessor(
            df,
            start_state_col,
            end_state_col,
            labels=labels,
            global_params=global_params,
            state_alias_translator=state_alias_translator,
        )
        self._load_job_queue.append(tabular_src)

    def iterqueries(self):
        """Provide an iterable over Cypher queries from all loaded sources.

        Yields:
            :obj:`CypherQuery`: Cypher queries in an order which respects the
                order in which they were loaded into the :obj:`GraphLoader` 
                instance.        
        """

        def handle_cypher_files_no_global_params(file_finder):
            """Yield queries from :obj:`CypherFileFinder` without extra params.
            
            Args:
                file_finder (:obj:`CypherFileFinder`)
                
            Yields:
                :obj:`CypherQuery`
            """
            for cypher_file in file_finder.iterfiles(priority_sorted=True):
                for query in cypher_file.queries:
                    yield query

        def handle_cypher_files_wi_global_params(file_finder_dict):
            """Yield queries from :obj:`CypherFileFinder` with extra params.
            
            Extra parameters are passed along with the file finder inside a
            dict.
            
            Args:
                file_finder_dict (dict): Dictionary with two key/value pairs;
                    file_finder_dict["file_finder"] is a 
                    :obj:`CypherFileFinder` and 
                    file_finder_dict["global_params"] is a dict whose key/value
                    pairs are extra parameters to be added to the queries 
                    identified by the file finder, as appropriate.
            
            Yields:
                :obj:`CypherQuery`
            """
            cff = file_finder_dict["file_finder"]
            global_params = file_finder_dict["global_params"]
            for cypher_file in cff.iterfiles(priority_sorted=True):
                for query in cypher_file.queries:
                    # Get list of parameters in query with None value and
                    # attempt to replace None with value from global_params
                    unspecified_native_params = [
                        k for k in query.params.keys() if not query.params[k]
                    ]
                    for unspecified_param in unspecified_native_params:
                        try:
                            query.params[unspecified_param] = global_params[
                                unspecified_param
                            ]
                        except KeyError:
                            # Raise an exception if a query has a required
                            # None parameter at this stage
                            raise KeyError(
                                "The following query requires  a "
                                + "parameter not given in its originating "
                                + "Cypher file, nor in the provided global "
                                + "parameters:\n"
                                + str(query)
                            )
                    yield query

        def handle_tabular_data_source(tabular_source):
            """Yield queries from a tabular data source.
            
            Args:
                tabular_source (:obj:`TransTableProcessor`)
                
            Yields:
                :obj:`CypherQuery`
            """
            for query in tabular_source.iterqueries():
                yield query

        handler = {
            CypherFileFinder: handle_cypher_files_no_global_params,
            dict: handle_cypher_files_wi_global_params,
            TransTableProcessor: handle_tabular_data_source,
        }

        for load_job in self._load_job_queue:
            for t in handler.keys():
                if isinstance(load_job, t):
                    for query in handler[t](load_job):
                        yield query
                    break


class ServerGraphLoader(GraphLoader):
    """Loads Cypher data into a running Neo4j database instance."""

    def __init__(self, username, password, uri="bolt://localhost:7687"):
        super(ServerGraphLoader, self).__init__()
        self.driver = self._get_graph_driver(uri, username, password)

    def _get_graph_driver(self, uri, username, password):
        """Attempt to obtain a driver for Neo4j server.

        Exit program if we can't obtain a driver.

        Returns:
            :obj:`GraphDatabase.driver`
        """
        try:
            driver = GraphDatabase.driver(uri, auth=(username, password))
            return driver
        except Exception as e:
            print("Could not load graph. Check password.", file=sys.stderr)
            print("Exception: %s" % str(e), file=sys.stderr)
            sys.exit(1)

    def refresh_graph(self, params):
        """Delete nodes in the graph with properties matching given parameters.

        This is useful in cases where we want to update the model specified by 
        a particular combination of parameters (e.g. with a particular model
        ID).

        This method will run an appropriate query to remove old data from the
        graph ahead of loading new data.

        Args:
            params (dict): Key/value pairs of Neo4j node properties. Nodes with
                properties matching these values will be deleted from the 
                graph.
        """

        def remove_all_nodes(tx, params):
            """Remove nodes with properties specified in global_params.

            A callback function called by driver.session().write_transaction

            Args:
                tx: database transaction object provided by write_transaction.
                global_params (dict): key/value pairs of property names and
                    their values which together specify the model to be
                    refreshed.
            """
            # construct query string from provided global_params dict
            WHERE_CLAUSE = " AND ".join(
                [
                    "n." + str(k) + "=" + '"' + str(v) + '"'
                    for (k, v) in iteritems(params)
                ]
            )
            q = "MATCH (n) WHERE " + WHERE_CLAUSE + " DETACH DELETE n"
            print("Removing existing nodes matching global parameters")
            tx.run(q)

        with self.driver.session() as session:
            try:
                session.write_transaction(remove_all_nodes, params)
            except CypherSyntaxError as e:
                print(
                    "Error in Cypher refreshing database. Check syntax.",
                    file=sys.stderr,
                )
                print("Exception: %s" % str(e), file=sys.stderr)
                sys.exit(1)

    def commit(self):
        """Load all queries loaded into :obj:`GraphLoader` into the graph."""
        for cypher_query in self.iterqueries():
            with self.driver.session() as session:
                try:
                    session.run(cypher_query.statement, cypher_query.params)
                except CypherSyntaxError as e:
                    print("Offending cypher query:\n" + repr(cypher_query))
                    raise


class EmbeddedGraphLoader(GraphLoader):
    """Loads Cypher data and provides interface to access it

    Provides an embedded jython-friendly interface to the cypher data. This
    will allow cymod to be used within Java applicatons to provide data to an
    embedded Neo4j instance.

    Using cymod within Java applications is useful when using Neo4j and Cypher 
    in situations in which a Java application will have access to a JRE but no 
    Neo4j server will be available.
    """

    def __init__(self):
        super(EmbeddedGraphLoader, self).__init__()

        self.param_re = re.compile(r"(\$)([a-zA-Z1-9_]*)")

    def _query_to_concrete_str(self, cypher_query):
        """Convert a parameterised :obj:`CypherQuery` to a concrete string.
        
        Args:
            cypher_query (:obj:`CypherQuery`)
            
        Returns:
            str: Cypher query with parameter placeholders replaced with 
                concrete values.
        """
        param_matches = self.param_re.findall(cypher_query.statement)
        working_string = cypher_query.statement
        for match in param_matches:
            param_string = "".join(match)
            working_string = working_string.replace(
                param_string, str(cypher_query.params[match[1]])
            )
        return working_string

    def query_generator(self):
        """Generate concrete strings representing each loaded query.
        
        Yields:
            str: Queries with parameter placeholders replaced with concrete 
                values.
        """
        for q in self.iterqueries():
            yield self._query_to_concrete_str(q)
