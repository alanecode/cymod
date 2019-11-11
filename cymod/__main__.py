# -*- coding: utf-8 -*-
from __future__ import print_function

import os
import sys
import getpass
import argparse

from cymod import ServerGraphLoader

if __name__ == "__main__":
    intro_string = (
        "Process cypher (graph database) query files and load "
        + "into specified neo4j database."
    )
    parser = argparse.ArgumentParser(description=intro_string)
    parser.add_argument(
        "--uri", default="bolt://localhost:7687", type=str, help="database uri"
    )
    parser.add_argument(
        "-u",
        "--username",
        default="neo4j",
        help="username for Neo4j database connection",
    )
    parser.add_argument(
        "-d", "--directory", default=os.getcwd(), help="root of directories to search"
    )
    parser.add_argument(
        "-p",
        "--parameters",
        help="JSON file containing cypher parameters to use with all queries",
    )
    parser.add_argument(
        "-s",
        "--suffix",
        help="Identifier at end of file name (before file exension) "
        "indicating cypher file should be loaded",
    )
    parser.add_argument(
        "-r",
        "--refresh",
        action="store_true",
        help="Delete previously stored data in database with matching "
        "global parameters before loading",
    )

    args = parser.parse_args()
    print("running loadcypher")
    pwd = getpass.getpass("Enter neo4j password:")

    gl = ServerGraphLoader(
        uri=args.uri,
        username=args.username,
        password=pwd,
        root_dir=args.directory,
        fname_suffix=args.suffix,
        global_param_file=args.parameters,
        refresh_graph=args.refresh,
    )

    print(gl.global_params)
    print(gl.driver)
    gl.load_cypher()
