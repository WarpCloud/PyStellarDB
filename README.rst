PyStellarDB
===========

PyStellarDB is a Python API for executing Transwarp Exetended OpenCypher(TEoC) and Hive query.
It could also generate a RDD object which could be used in PySpark.
It is base on PyHive(https://github.com/dropbox/PyHive) and PySpark(https://github.com/apache/spark/)

PySpark RDD
===========

We hack a way to generate RDD object using the same method in `sc.parallelize(data)`.
It could cause memory panic if the query returns a large amount of data.

Users could use a workaround if you do need huge data:

1. If you are querying a graph, refer to StellarDB manual of Chapter 4.4.5 to save the query data into a temporary table.

2. If you are querying a SQL table, save your query result into a temporary table.

3. Find the HDFS path of the temporary table generated in Step 1 or Step 2.

4. Use API like `sc.newAPIHadoopFile()` to generate RDD.

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

    # Make sure you have the correct realms infomation about the KDC server in /etc/krb5.conf
    # Make sure you have the correct keytab file in your environment
    # Run kinit command:
    # In Linux: kinit -kt FILE_PATH_OF_KEYTABL PRINCIPAL_NAME
    # In Mac: kinit -t FILE_PATH_OF_KEYTABL -f PRINCIPAL_NAME

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


Execute Graph Query and change to a PySpark RDD object
------------------------------------------------------
.. code-block:: python

    from pyspark import SparkContext
    from pystellardb import stellar_hive
    
    sc = SparkContext("local", "Demo App")

    conn = stellar_hive.StellarConnection(host="localhost", port=10000, graph_name='pokemon')
    cur = conn.cursor()
    cur.execute('config query.lang cypher')
    cur.execute('use graph pokemon')
    cur.execute('match p = (a)-[f]->(b) return a,f,b limit 10')

    rdd = cur.toRDD(sc)

    def f(x): print(x)

    rdd.map(lambda x: (x[0].toJSON(), x[1].toJSON(), x[2].toJSON())).foreach(f)

    # Every line of this query is in format of Tuple(VertexObject, EdgeObject, VertexObject)
    # Vertex and Edge object has a function of toJSON() which can print the object in JSON format


Execute Hive Query and change to a PySpark RDD object
-----------------------------------------------------
.. code-block:: python

    from pyspark import SparkContext
    from pystellardb import stellar_hive
    
    sc = SparkContext("local", "Demo App")

    conn = stellar_hive.StellarConnection(host="localhost", port=10000)
    cur = conn.cursor()
    cur.execute('select * from default_db.default_table limit 10')

    rdd = cur.toRDD(sc)

    def f(x): print(x)

    rdd.foreach(f)

    # Every line of this query is in format of Tuple(Column, Column, Column)

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