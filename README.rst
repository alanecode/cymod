============================================================
Cymod
============================================================

A tool to support the description and development of state-and-transition 
models using the Cypher_ graph query language.

Purpose
-------

State-and-transition models (STMs_) are valuable tools for capturing and 
communicating qualitative ecological and geomorphic knowledge. However, 
they can become unwieldy when the richness of the available qualitative 
knowledge renders them difficult to represent in their entirety on a 
2-dimensional surface.

Cymod aims to simplify the development and use of detailed STMs by allowing
modellers to specify components of their models piecemeal in individual Cypher
files. Cymod can then to do the work of wiring these components together into a
complete model, stored in a Neo4j_ graph database.

Example usage
-------------

Having prepared a set of Cypher files specifying an STM in the
``./cypher-files`` directory, a modeller can load those files into the graph
database with the following Python commands:

.. code-block:: python

   from cymod import ServerGraphLoader

   # Initialise a ServerGraphLoader object using your Neo4j credentials
   gl = ServerGraphLoader(user="username", password="password")

   # Read cypher queries from files in cypher-files directory
   gl.load_cypher("cypher-files")

   # Run these queries against the graph specified in the neo4j
   # configuration file
   gl.commit()

.. _Cypher: https://neo4j.com/developer/cypher/
.. _Neo4j: https://neo4j.com/
.. _STMs: http://doi.org/10.1007/978-3-319-46709-2_9

Supported Python versions
-------------------------

Tested against Python 2.7, 3.5, 3.6, and 3.7.
