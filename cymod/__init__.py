# -*- coding: utf-8 -*-
"""cymod

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
    files' is this
    https://stackoverflow.com/questions/43648512/how-to-load-cypher-file-into-neo4j
    Stack Overflow answer whose solution involves piping Cypher queries from a
    file into the cypher-shell
    https://neo4j.com/docs/operations-manual/current/tools/cypher-shell/
    command line utility which ships with Neo4j. While useful for interactively
    designing queries, cypher-shell currently appears to be limited in its
    capabilities in dealing with external files containing Cypher.

    A particular limitation `cymod` aims to address is the ability to set
    global Cypher parameters which will be applied to all files in the
    model. This is important for my model design use-case because every node in
    the database needs to be given `project` and `model_ID` properties to allow
    multiple models to coexist in a single Neo4j instance. `cymod` will also
    search from a root node to collect all available Cypher files with respect
    to a specified root directory. This could be achieved using `cypher-shell`
    commands in a bash script, but `cymod` aims to be a starting point for
    solving various problems which may arise in the future and act as a
    one-stop-shop for Cypher loading tasks.

    At present I have grand designs involving the development of some tools to
    assist in the debugging of errors in the model specification by running
    automated checks on the Cypher input. However, we'll see how it goes.

"""
from __future__ import print_function

__version__ = "0.0.5"

from cymod.load import ServerGraphLoader
from cymod.load import EmbeddedGraphLoader
from cymod.params import read_params_file
from cymod.customise import NodeLabels
