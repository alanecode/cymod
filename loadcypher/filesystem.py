#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""filesystem.py

Tools for finding Cypher files in the file system and extracting their contained
data.
"""
import os
import re
import json

from py2neo import Graph
from httpstream.http import ClientError
from py2neo.database.http import Unauthorized

class CypherFile(object):
    """An individual file containing Cypher queries.

    Multiple queries can be contained in a single file, separated by a
    semicolon.

    Args:
        filename (str): Qualified filesystem path to the Cypher file underlying
            a CypherFile object which has been instantiated from this class.

    Attributes:
        query_start_clauses (:obj:`list` of :obj:`str`): Clauses which can
            legally start a Cypher query. These are used to discern the end of
            a parameter specification at the beginning of a file, and the
            beginning of the first Cypher query.
        _cached_data (dict): Data cache used to avoid the need to re-read each
            time we use a CypherFile object to access data. Stores a dict of the
            structure {'params':<dict>, 'queries':<list of strings>}.

    """

    def __init__(self, filename):
        self.filename = filename
        self.query_start_clauses = ['START', 'MATCH', 'MERGE', 'CREATE']
        self._cached_data = None

    @property
    def params(self):
        """dict: Cypher parameters identified in file."""
        if not(self._cached_data):
            self._cached_data = self._parse_queries()

        return self._cached_data['params']

    @property
    def queries(self):
        """list of str: Cypher queries identified in file."""
        if not(self._cached_data):
            self._cached_data = self._parse_queries()

        return self._cached_data['queries']

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
            :obj:`tuple` of dict and str: A tuple containing a dict --
                containing Cypher parameters -- and a string containing queries.
                If no parameters are found in the file, the first element in the
                tuple will be None.
        """
        dat = self._remove_comments_and_newlines()
        re_prefix = r'\s*\{[\s*\S*]*\}\s*'
        patterns = [re.compile(re_prefix+clause) for clause
                        in self.query_start_clauses]

        for i, p in enumerate(patterns):
            match = p.match(dat)
            if match:
                first_clause = self.query_start_clauses[i]
                clause_len = len(first_clause)
                match_len = match.end(0) - match.start(0)
                params_end = match_len - clause_len
                params = dat[:params_end]
                queries = dat[params_end:]

        try:
            return json.loads(params), queries
        except UnboundLocalError:
            return None, dat

    def _parse_queries(self):
        """Identify individual Cypher queries.

        Uses semicolons to identify the boundaries of queries within file text.
        If no semicolon is found it will be assumed that the file contains only
        one query and that the terminating semicolon was omitted.

        Returns:
            dict: Parsed file contents in the form of a dictionary with a
                structure of {params:<dict>, queries:<list of str>}
        """
        dat = self._extract_parameters()
        queries = dat[1]
        # only include non-empty strings in results (prevents whitespace at
        # end of file getting an element on its own).
        queries_list = [q + ';' for q in queries.split(';')
                            if q.replace(' ', '')]
        return {'params':dat[0], 'queries':queries_list}

class CypherFileFinder(object):
    """Searcher to find Cypher files in the provided root directory.

    Reaches outwards from a specified root directory an collects all Cypher
    files within reach.

    Args:
        root_dir (str): File system path to root directory to search for Cypher
            files.
        cypher_extensions (:obj:`tuple` of :obj:`str`): A list of strings
            specifying file extensions which should be taken to denote a file
            containing Cypher queries. Defaults to ('.cql', '.cypher').
        fname_suffix (str): Suffix at the end of file names (excluding file
            extension) which indicates file should be loaded into the database.
            e.g. if files ending '_w.cql' should be loaded, use
            fname_suffix='_w'. Defaults to None.
    """

    def __init__(self, root_dir, cypher_extensions=['.cql', '.cypher'],
        fname_suffix=None):
        self.root_dir = root_dir
        self.cypher_extensions = cypher_extensions
        self.fname_suffix = fname_suffix

    def get_cypher_files(self):
        """Get all applicable Cypher files in directory hierarchy.

        Returns:
            :obj:`list` of :obj:`CypherFile`: A list of Cypher file objects
                ready for subsequent processing.

        """
        fnames = []
        for dirpath, subdirs, files in os.walk(self.root_dir):
            for f in files:
                if f.endswith(tuple(self.cypher_extensions)):
                    if self.fname_suffix:
                        test_suffix = f.split('.')[0][-len(self.fname_suffix):]
                        if test_suffix == self.fname_suffix:
                            fnames.append(os.path.join(dirpath, f))
                    else:
                        # if no fname_suffix specified, all all cypher files
                        fnames.append(os.path.join(dirpath, f))

        return [CypherFile(f) for f in fnames]
