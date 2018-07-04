============================================================
cymod
============================================================

Tool to read in files containing cypher queries



Parameters
(compulsory):
-p: password (should not be printed to screen, use getpass https://docs.python.org/2/library/getpass.html)

(optional):
- h: host (defaults to localhost)
- d: root directory to search for files (defaults to current directory)
-params: name of cypher parameters (json) file, defaults to none, throwing error if queries require parameters.


Purpose
____________________________________________________________

Whereas py2neo is useful as an abstraction away from needing to write cypher
when interacting with a database in Python, what cymod aims to do is /support/
the use of cypher as a language which is useful for the description of
complicated models.

TODO
____________________________________________________________


Python 2.7 conversion
------------------------------------------------------------

- To enable the use of cymod within Java applications (such as Repast models)
  it is necessarry for them to be able to run in Jython -- the implementation
  of Python which runs in the JVM. As of June 2018, Jython only supports Python
  2.7. Therefore it is necessary to make the cymod codebase work with Python
  2.7.
  
Repurpose code for convenient generation of cypher in Java 
------------------------------------------------------------

I want to be able to generate cypher from files within a Java application. This
will require some refactoring.

Move _get_graph and refresh_graph into a ServerGraphLoader object which
inherits from GraphLoader

GraphLoader will no longer need hostname, username, password or refresh_graph
parameters.
