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
    starting point for solving various problems which may arise in the future
    and act as a one-stop-shop for Cypher loading tasks.

    At present I have grand designs involving the development of some tools
    to assist in the debugging of errors in the model specification by running
    automated checks on the Cypher input. However, we'll see how it goes.

"""
import os
import sys
import getpass
import argparse

from filesystem import CypherFileFinder, CypherFile

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


    """
    pwd = getpass.getpass('Enter neo4j password:')
    try:
        graph = Graph(host=args.host, password=pwd)

    except (KeyError, ClientError, Unauthorized) as e:
        print('Could not load graph. Check password.', file=sys.stderr)
        print('Exception: %s' % str(e), file=sys.stderr)

        sys.exit(1)
    """

    #fname = '/home/andrew/Dropbox/phd/models/GredosModel/database/cypher/PrivateChestnutAfforestation.cql'
    #fname = '/home/andrew/Dropbox/phd/models/GredosModel/database/cypher/LandCoverType.cql'
    #fname_2queries = '/home/andrew/Dropbox/phd/models/GredosModel/database/cypher/queries/BenefitPathsEco.cql'
    #cfile = CypherFile(fname)
    #print(cfile.params)
    #print(cfile.queries)
    #print(graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq").dump())
    root = '/home/andrew/Dropbox/phd/models/GredosModel/views'
    cff = CypherFileFinder(root, fname_suffix='_w')
    cypher_files = cff.get_cypher_files()
    test_file = cypher_files[2]
    print(len(cypher_files))
    for f in cypher_files:
        print(f.filename.split('/')[-1])
        print(f.params)
        print('\n')


    #print(test_file.filename)
    #print(test_file.queries[0])
