#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""loadcypher

Problem:

    My use case involves using Cypher as a modelling language. Different parts
    of a single model are represented in different files, each corresponding to
    a different 'view' of the model's structure (which is too complex to
    represent coherently in its enterity on a 2-dimensional surface). This
    manner of breaking down the model is useful both to make it conceptually
    manageable and to maintain version control over changes between model
    versions (e.g. it will be easier to identify if only a single view has
    changed).

    At the time of writing the top Google result for 'neo4j load in cypher
    files' is this https://stackoverflow.com/questions/43648512/how-to-load-cypher-file-into-neo4j
    Stack Overflow answer whose solution involves piping Cypher queries from a
    file into the cypher-shell https://neo4j.com/docs/operations-manual/current/tools/cypher-shell/
    command line utility which ships with Neo4j. While useful for interactively
    designing queries, cypher-shell currently appears to be limited in its
    capabilities in dealing with external files containing Cypher.

    A particular limitation `loadcypher` aims to address is the ability to
    set global Cypher parameters which will be applied to all files in the
    model. This is important for my model design use-case because every node in
    the database needs to be given `project` and `model_ID` properties to allow
    multiple models to coexist in a single Neo4j instance. `loadcypher` will
    also search from a root node to collect all available Cyoher files with
    respect to a specified root directory. This could be achieved using
    `cypher-shell` commands in a bash script, but `loadcypher` aims to be a
    starting point for various problems which may arise in the future and act as
    a one-stop-shop for Cypher loading tasks.

    At present I have grand designs involving the development of some tools
    to assist in the debugging of errors in the model specification by running
    automated checks on the Cypher input. However, we'll see how it goes.

"""
import os
import sys
import getpass
import argparse

from py2neo import Graph
from httpstream.http import ClientError
from py2neo.database.http import Unauthorized

class CypherFile(object):
    """An individual file containing Cypher queries.

    Multiple queries can be contained in a single file, separated by a
    semicolon.

    Args:
        filename (str)

    Attributes:
        query_start_clauses (:obj:`list` of :obj:`str`): Clauses which can
            legally start a Cypher query.

    getter: queries (list of strings)
    """

    def __init__(self, filename):
        self.filename = filename


        self.query_start_clauses = ['START', 'MATCH', 'MERGE']

    def _read_cypher(self):
        """Read entire (unprocessed) Cypher file.

        Other methods should be used to process this data into separate queries
        and potentially check their validity.

        Returns:
            str: Unprocessed data from Cypher file.
        """
        try:
            with open(self.filename, 'r') as f:
                dat = f.read()
                return dat

        except FileNotFoundError as e:
            print('Could not open Cypher file. ', e)
            raise

    def _remove_comments_and_newlines(self):
        """Remove comment lines and new line characters from Cypher file data.

        Returns:
            str: Processed data from Cypher file.
        """
        dat = self._read_cypher()
        dat_list = dat.split('\n')
        # filter out lines beginning with comment string //
        dat_list = [l for l in dat_list if l[:2] != '//']

        # filter out comments occuring at the end of a line
        for i, l in enumerate(dat_list):
            l_test = l.split('//')
            if len(l_test) > 1:
                # if a line contains //, take only the part before first //
                dat_list[i] = l_test[0]

        dat = ' '.join(dat_list)
        return dat

    def _extract_parameters(self):
        """Identify Cypher parameters at the beginning of the file string.

        Returns:
            :obj:`tuple` of :obj:`str`: A tuple containing two strings: the
                first containing Cypher parameters, the second containing
                queries. If no parameters are found in the file, the first
                element in the tuple will be None.
        """
        dat = self._remove_comments_and_newlines()
        opening_brace_count = 0
        closing_brace_count = 0
        char_num = 0
        while char_num < len(dat):
            if dat[char_num] == '{':
                opening_brace_count += 1
            elif dat[char_num] == '}':
                closing_brace_count += 1
                if closing_brace_count == opening_brace_count:
                    break

            char_num += 1

        return dat[:char_num+1], dat[char_num+1:]

    def _parse_queries(self):
        pass



class CypherFileFinder(object):
    """Searcher to find Cypher files in the provided root directory.

    Args:
        root_dir (str): File system path to root directory to search for Cypher
            files.
        cypher_extensions (:obj:`list` of :obj:`str`): A list of strings
            specifying file extensions which should be taken to denote a file
            containing Cypher queries. Defaults to ['cql', 'cypher'].
    """

    def __init__(self, root_dir, cypher_extensions=['cql', 'cypher']):
        self.root_dir = root_dir
        self.cypher_extentions = cypher_extensions

    def get_cypher_files():
        """Get all applicable Cypher files in directory hierarchy.

        Returns:
            :obj:`list` of :obj:`CypherFile`: A list of Cypher file objects
                ready for subsequent processing.

        Todo:
            * Descend into all possible branches of directory tree
            * Construct a list of Cypher file objects
        """
        pass

class GraphLoader(object):

    def __init__(self, username, password):
        self.global_parameters # reasd from parameters json file.


if __name__ == '__main__':
    intro_string = ('Process cypher (graph database) query files and load ' +
                    'into specified neo4j database.')
    parser = argparse.ArgumentParser(description=intro_string)
    parser.add_argument('--host', default='localhost', type=str,
                help='database hostname')
    parser.add_argument('-d', '--directory', default=os.getcwd(),
                help='root of directories to search')
    parser.add_argument('-p', '--parameters',
                help='cypher parameters to use with queries')

    args = parser.parse_args()
    print('Running graphloader')
    pwd = getpass.getpass('Enter neo4j password:')

    """
    try:
        graph = Graph(host=args.host, password=pwd)

    except (KeyError, ClientError, Unauthorized) as e:
        print('Could not load graph. Check password.', file=sys.stderr)
        print('Exception: %s' % str(e), file=sys.stderr)

        sys.exit(1)
    """

    fname = '/home/andrew/Dropbox/phd/models/GredosModel/database/cypher/PrivateChestnutAfforestation.cql'
    cfile = CypherFile(fname)
    dat = cfile._extract_parameters()
    print(dat)


    #print(graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq").dump())
