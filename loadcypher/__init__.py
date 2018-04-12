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
from graphloader import GraphLoader

if __name__ == '__main__':
    intro_string = ('Process cypher (graph database) query files and load ' +
                    'into specified neo4j database.')
    parser = argparse.ArgumentParser(description=intro_string)
    parser.add_argument('--host', default='localhost', type=str,
                help='database hostname')
    parser.add_argument('-u', '--username', default='neo4j',
                help='username for Neo4j database connection')
    parser.add_argument('-d', '--directory', default=os.getcwd(),
                help='root of directories to search')
    parser.add_argument('-p', '--parameters',
                help='JSON file containing cypher parameters to use with all queries')

    args = parser.parse_args()
    print('Running graphloader')
    pwd = getpass.getpass('Enter neo4j password:')

    #print(graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq").dump())
    root = '/home/andrew/Dropbox/phd/models/GredosModel/views'
    param_file='/home/andrew/Dropbox/phd/models/GredosModel/global_parameters.json'

    gl = GraphLoader(hostname=args.host, username='andrew', password=pwd,
            root_dir=root, fname_suffix='_w', global_param_file=param_file)
    print(gl.global_params)
    print(gl.graph)
    for f in gl.cypher_files:
        print(f.filename)
        print(f.priority)
