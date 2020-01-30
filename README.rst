============================================================
Cymod
============================================================

.. image:: https://zenodo.org/badge/123963584.svg
   :target: https://zenodo.org/badge/latestdoi/123963584

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

.. _Cypher: https://neo4j.com/developer/cypher/
.. _Neo4j: https://neo4j.com/
.. _STMs: http://doi.org/10.1007/978-3-319-46709-2_9#

Installation
------------

Cymod can be installed from PyPI_ via ``pip`` using the following shell
command:

.. code-block:: bash

    pip install cymod


.. _PyPI: https://pypi.org/

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

Supported Python versions
-------------------------

Tested against Python 2.7, 3.5, 3.6, and 3.7.

Contributing
------------

If you have any questions about usage or suggestions for improvement
please `open an issue`_ on the `Cymod GitHub page`_.

.. _`open an issue`: https://help.github.com/en/github/managing-your-work-on-github/creating-an-issue
.. _`Cymod GitHub page`: https://github.com/lanecodes/cymod/issues

Versioning
----------

We use `semantic versioning`_.

.. _`semantic versioning`: https://semver.org/

Authors
-------

- `Andrew Lane <https://github.com/lanecodes>`_

License
-------

This project is licensed under the MIT License. See the `LICENSE <LICENSE>`_
file for details.

How to cite
-----------

Please visit the `Zenodo page`_ for this
project to generate and export bibliographic information in the citation style
of your choice.

.. _`Zenodo page`: http://doi.org/10.5281/zenodo.3630631

Acknowledgements
----------------

This tool was developed by Andrew Lane during the course of his PhD in the
Department of Geography at King's College London. During this time Andrew was
supported by an Engineering and Physical Sciences Research Council (EPSRC) PhD
research studentship (EPSRC grant reference number: EP/LO15854/1).
