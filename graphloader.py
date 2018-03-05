#!/usr/bin/env python
import os
import sys
import getpass
import argparse

from py2neo import Graph
from httpstream.http import ClientError
from py2neo.database.http import Unauthorized

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

    try:
        graph = Graph(host=args.host, password=pwd)

    except (KeyError, ClientError, Unauthorized) as e: #, py2neo.packages.httpstream.http.ClientError) as e1:
        print('Could not load graph. Check password.', file=sys.stderr)
        print('Exception: %s' % str(e), file=sys.stderr)

        sys.exit(1)

    print(graph.run("UNWIND range(1, 10) AS n RETURN n, n * n as n_sq").dump())
