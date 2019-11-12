# -*- coding: utf-8 -*-
"""
cymod.cyproc
~~~~~~~~~~~~

This module contains classes used to load Cypher queries from files in the file 
system, and to generate Cypher to represent data contained in 
:obj:`pandas.DataFrame` objects.
"""
from __future__ import print_function
import os
import re
import json
import warnings

import six

from cymod.cybase import CypherQuery, CypherQuerySource


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
        priority (int): Priority with which the file's queries should be loaded
            with respect to other files. Priority 0 files will be loaded first.
    """

    def __init__(self, filename):
        # Matches either things in quotes or //...\n. Form a group on latter
        self._comment_pattern = re.compile(
            r"\"[^\"\r\n]*\"|\"[^\"\r\n]*$|(\/\/.*(?:$|\n))"
        )
        self.filename = filename
        self.priority = 0
        self.query_start_clauses = ["START", "MATCH", "MERGE", "CREATE"]
        self._cached_data = self._parse_queries()

    @property
    def queries(self):
        """tuple of :obj:`CypherQuery`: Cypher queries identified in file."""
        if not (self._cached_data):
            self._cached_data = self._parse_queries()

        return tuple(self._cached_data)

    def _read_cypher(self):
        """Read entire (unprocessed) Cypher file.

        Other methods should be used to process this data into separate queries
        and potentially check their validity.

        Returns:
            str: Unprocessed data from Cypher file.
        """
        try:
            with open(self.filename, "r") as f:
                dat = f.read()
                return dat

        except IOError as e:
            print("Could not open Cypher file. ", e)
            raise

    def _remove_comments_and_newlines(self):
        """Remove comment lines and new line characters from Cypher file data.

        Returns:
            str: Processed data from Cypher file.
        """

        def comment_replacer(m):
            """Use match object to construct replacement string."""
            if m.group(1):
                return ""
            else:
                # If no group 1 found return complete match
                return m.group(0)

        dat = self._read_cypher()
        dat = re.sub(self._comment_pattern, comment_replacer, dat)
        dat = re.sub("\n", " ", dat)
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
        re_prefix = r"\s*\{[\s*\S*]*\}\s*"
        patterns = [
            re.compile(re_prefix + clause) for clause in self.query_start_clauses
        ]

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
            param_dict = json.loads(params)
            try:
                # set priority attributre if given in file. o/w is 0.
                self.priority = param_dict["priority"]
                # remove priority from remaining parameters
                del param_dict["priority"]
            except KeyError:
                pass

            return param_dict, queries
        except UnboundLocalError:
            return {}, dat

    def _match_params_to_statement(self, statement, all_params):
        """Return a dict of parameter_name: parameter value pairs.

        A given Cypher file can contain any number of parameters, not all of
        which will be relevant for a particular query. Given a query and a 
        dictionary containing all of a file's parameters, this function 
        returns a dict whose keys are all the parameters required by that query
        and whose values are those provided in their Cypher file, None if not 
        provided.

        Args:
            statement (str): A string encoding a Cypher statement.
            app_params (dict): Complete list of parameters included in the 
                Cypher file

        Returns:
            dict: Parameters and values relevant for `statement`
        """
        relevant_dict = {}
        r = re.compile(r"(\$)([a-zA-Z1-9_]*)")
        for match in r.finditer(statement):
            param_name = match.group(2)  # param name excluding $ sign
            relevant_dict[param_name] = None
            if param_name in all_params.keys():
                relevant_dict[param_name] = all_params[param_name]
        if relevant_dict:
            return relevant_dict
        else:
            # params should be empty dict if no matches found
            return {}

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
        query_string_list = [
            q.lstrip() + ";" for q in queries.split(";") if q.replace(" ", "")
        ]

        query_list = []
        for i, statement in enumerate(query_string_list):
            query_list.append(
                CypherQuery(
                    statement,
                    params=self._match_params_to_statement(statement, dat[0]),
                    source=CypherQuerySource(self.filename, "cypher", i),
                )
            )

        if len(query_list) == 0:
            warnings.warn("No queries found in " + self.filename, UserWarning)

        return query_list

    def __repr__(self):
        # filename, prioriry, queries
        fname_str = "file name: " + self.filename
        priority_str = "priority: " + str(self.priority)
        queries_str = "queries:\n"
        for q in self.queries:
            queries_str += repr(q) + "\n"
        return "[\n" + fname_str + "\n" + priority_str + "\n" + queries_str + "]"


class CypherFileFinder(object):
    """Searcher to find Cypher files in the provided root directory.

    Reaches outwards from a specified root directory an collects all Cypher
    files within reach.

    Args:
        root_dir (str): File system path to root directory to search for Cypher
            files.
        cypher_exts (:obj:`list` of :obj:`str`, optional): A list of strings 
            specifying file extensions which should be taken to denote a file 
            containing Cypher queries. Defaults to [".cypher", ".cql", ".cyp"].
        cypher_file_suffix (str): Suffix at the end of file names 
                (excluding file extension) which indicates file should be 
                loaded into the database. E.g. if files ending '_w.cql' 
                should be loaded, use cypher_file_suffix='_w'. Defaults to 
                None. 
    """

    def __init__(
        self, root_dir, cypher_exts=[".cypher", ".cql", ".cyp"], cypher_file_suffix=None
    ):
        self.root_dir = root_dir
        self.cypher_exts = cypher_exts
        self.cypher_file_suffix = cypher_file_suffix

    def _get_cypher_files(self):
        """Get all applicable Cypher files in directory hierarchy.

        Returns:
            :obj:`list` of :obj:`CypherFile`: A list of Cypher file objects
                ready for subsequent processing.

        """
        fnames = []
        for dirpath, subdirs, files in os.walk(self.root_dir):
            for f in files:
                if f.endswith(tuple(self.cypher_exts)):
                    if self.cypher_file_suffix:
                        test_suffix = f.split(".")[0][-len(self.cypher_file_suffix) :]
                        if test_suffix == self.cypher_file_suffix:
                            fnames.append(os.path.join(dirpath, f))
                    else:
                        # if no fname_suffix specified, all all cypher files
                        fnames.append(os.path.join(dirpath, f))

        return [CypherFile(f) for f in fnames]

    def iterfiles(self, priority_sorted=False):
        """Yields CypherFile objects representing discovered files."""
        # TODO Refactor so there is never a complete list of files processed
        # as is currently done in self._get_cypher_files()
        files = self._get_cypher_files()
        if priority_sorted:
            files.sort(key=lambda file: file.priority)
        while files:
            yield files.pop(0)

    def __repr__(self):
        s = "[\n"
        for f in self.iterfiles():
            s += f.filename + "\n"
        return s + "]"
