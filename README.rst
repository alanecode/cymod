============================================================
graphloader
============================================================

Tool to read in files containing cypher queries



Parameters
(compulsory):
-p: password (should not be printed to screen, use getpass https://docs.python.org/2/library/getpass.html)

(optional):
- h: host (defaults to localhost)
- d: root directory to search for files (defaults to current directory)
-params: name of cypher parameters (json) file, defaults to none, throwing error if queries require parameters.
