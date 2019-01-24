# -*- coding: utf-8 -*-
"""
cymod.cyproc
~~~~~~~~~~~~

This module contains classes used to load Cypher queries from files in the file 
system, and to generate Cypher to represent data contained in 
:obj:`pandas.DataFrame` objects.
"""
from __future__ import print_function
import re

class CypherQuerySource(object):
    """Container for information about a Cypher query's original source."""

    def __init__(self, ref, ref_type, index):
        """    
        Args:
            ref (str or :obj:`pandas.DataFrame`): A reference to the original 
                data source which the user might need for debugging purposes.
            form (str): The form/ type of the original data source. Either 
                'cypher' or 'tabular' (i.e. a pandas DataFrame).
            index (int): An indication of where in the original data source the 
                query came from. In the case of a cypher file, this will be the
                query number. For tabular data it will be the row number.            
        """
        self.ref = ref
        self.ref_type= ref_type
        self.index = index

    @property
    def ref_type(self):
        return self._ref_type

    @ref_type.setter
    def ref_type(self, val):
        if val not in ["cypher", "tabular"]:
            raise ValueError("CypherQuerySource.ref_type must be either " \
            "'cypher' or 'tabular'")
        self._ref_type = val

    def __repr__(self):
        return "ref_type: {0}\nindex: {1}\nref: {2}" \
        .format(self.ref_type, self.index, str(self.ref))

class CypherQuery(object):
    """Container for data speficying an individual Cypher query."""
    
    def __init__(self, statement, params=None, source=None):
        """
        Args:
            statement (str): An individual Cypher statement.
            params (dict, optional): Cypher parameters relevant for query.
            source (:obj:`CypherQuerySource`, optional): Data specifying origin
                of query, useful for debugging purposes.            
        """
        self.statement = statement
        self.params = params
        self.source = source    


class CypherFile(object):
    """Reads, parses and reports CypherQuery objects for a given file name.

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
        _cached_data (tuple of :obj:`CypherQuery`): Data cache used to avoid 
            the need to re-read each time we use a CypherFile object to access 
            data. 
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
    def priority(self):
        """int: Priority with which queries in file should be loaded."""
        try:
            return int(self.params['priority'])
        except TypeError:
            # case where priority is not specified in file
            return None

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

        except IOError as e:
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
                containing Cypher parameters -- and a string containing
                queries. If no parameters are found in the file, the
                first element in the
                tuple will be None.
        """
        dat = self._remove_comments_and_newlines()
        re_prefix = r'\s*\{[\s*\S*]*\}\s*'
        patterns = [re.compile(re_prefix+clause)
                    for clause
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
        return {'params': dat[0], 'queries': queries_list}