PyStellarDB
===========

PyStellarDB is a Python API for executing Transwarp Exetended OpenCypher(TEoC) and Hive query.
It is base on PyHive(https://github.com/dropbox/PyHive)

Usage
=====

PLAIN Mode (No security is configured)
---------------------------------------
.. code-block:: python

    from pystellardb import stellar_hive

    conn = stellar_hive.StellarConnection(host="localhost", port=10000, graph_name='pokemon')
    cur = conn.cursor()
    cur.execute('config query.lang cypher')
    cur.execute('use graph pokemon')
    cur.execute('match p = (a)-[f]->(b) return a,f,b limit 1')

    print cur.fetchall()


LDAP Mode
---------
.. code-block:: python

    from pystellardb import stellar_hive

    conn = stellar_hive.StellarConnection(host="localhost", port=10000, username='hive', password='123456', auth='LDAP', graph_name='pokemon')
    cur = conn.cursor()
    cur.execute('config query.lang cypher')
    cur.execute('use graph pokemon')
    cur.execute('match p = (a)-[f]->(b) return a,f,b limit 1')

    print cur.fetchall()


Kerberos Mode
-------------
.. code-block:: python

    from pystellardb import stellar_hive

    conn = stellar_hive.StellarConnection(host="localhost", port=10000, kerberos_service_name='hive', auth='KERBEROS', graph_name='pokemon')
    cur = conn.cursor()
    cur.execute('config query.lang cypher')
    cur.execute('use graph pokemon')
    cur.execute('match p = (a)-[f]->(b) return a,f,b limit 1')

    print cur.fetchall()


Execute Hive Query
------------------
.. code-block:: python

    from pystellardb import stellar_hive

    # If `graph_name` parameter is None, it will execute a Hive query and return data just as PyHive does
    conn = stellar_hive.StellarConnection(host="localhost", port=10000, database='default')
    cur = conn.cursor()
    cur.execute('SELECT * FROM default.abc limit 10')


Dependencies
============

Required:
------------

- Python 2.7+ / Python 3

System SASL
------------

Different systems require different packages to be installed to enable SASL support
in Impyla. Some examples of how to install the packages on different distributions
follow.

Ubuntu:

.. code-block:: bash

    apt-get install libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit
    apt-get install python-dev gcc              #Update python and gcc if needed

RHEL/CentOS:

.. code-block:: bash

    yum install cyrus-sasl-md5 cyrus-sasl-plain cyrus-sasl-gssapi cyrus-sasl-devel
    yum install gcc-c++ python-devel.x86_64     #Update python and gcc if needed


Requirements
============

Install using

- ``pip install 'pystellardb[hive]'`` for the Hive interface.

PyHive works with

- For Hive: `HiveServer2 <https://cwiki.apache.org/confluence/display/Hive/Setting+up+HiveServer2>`_ daemon


Testing
=======

On his way