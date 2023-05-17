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

Ubuntu:

.. code-block:: bash

    apt-get install libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit
    apt-get install python-dev gcc              #Update python and gcc if needed

RHEL/CentOS:

.. code-block:: bash

    yum install cyrus-sasl-md5 cyrus-sasl-plain cyrus-sasl-gssapi cyrus-sasl-devel
    yum install gcc-c++ python-devel.x86_64     #Update python and gcc if needed

    # if pip3 install fails with a message like 'Can't connect to HTTPS URL because the SSL module is not available'
    # you may need to update ssl & reinstall python

    # 1. Download a higher version of openssl, e.g: https://www.openssl.org/source/openssl-1.1.1k.tar.gz
    # 2. Install openssl: ./config && make && make install
    # 3. Link openssl: echo /usr/local/lib64/ > /etc/ld.so.conf.d/openssl-1.1.1.conf
    # 4. Update dynamic lib: ldconfig -v
    # 5. Uninstall Python & Download a new Python source package
    # 6. vim Modules/Setup, search '_socket socketmodule.c', uncomment
    #    _socket socketmodule.c
    #    SSL=/usr/local/ssl
    #    _ssl _ssl.c \
    #            -DUSE_SSL -I$(SSL)/include -I$(SSL)/include/openssl \
    #            -L$(SSL)/lib -lssl -lcrypto
    #
    # 7. Install Python: ./configure && make && make install

Windows:

.. code-block:: bash

    # There are 3 ways of installing sasl for python on windows
    # 1. (recommended) Download a .whl version of sasl from https://www.lfd.uci.edu/~gohlke/pythonlibs/#sasl
    # 2. (recommended) If using anaconda, use conda install sasl.
    # 3. Install Microsoft Visual C++ 9.0/14.0 buildtools for python2.7/3.x, then pip install sasl.

Notices
=======

Pystellardb >= 0.9 contains beeline installation to /usr/local/bin/beeline.

Requirements
============

Install using

- ``pip install 'pystellardb[hive]'`` for the Hive interface.

PyHive works with

- For Hive: `HiveServer2 <https://cwiki.apache.org/confluence/display/Hive/Setting+up+HiveServer2>`_ daemon


Windows Kerberos Configuration
==============================

Windows Kerberos configuration can be a little bit tricky and may need a few instructions.
First, you'll need to install & configure Kerberos for Windows.
Get it from http://web.mit.edu/kerberos/dist/

After installation, configure the environment variables.
Make sure the position of your Kerberos variable is ahead of JDK variable, avoid using kinit command located in JDK path.

Find /etc/krb5.conf on your KDC, copy it into krb5.ini on Windows with some modifications.
e.g.(krb5.conf on KDC):

.. code-block:: bash

    [logging]
    default = FILE:/var/log/krb5libs.log
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmind.log

    [libdefaults]
    default_realm = DEFAULT
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    allow_weak_crypto = true
    udp_preference_limit = 32700
    default_ccache_name = FILE:/tmp/krb5cc_%{uid}

    [realms]
    DEFAULT = {
    kdc = host1:1088
    kdc = host2:1088
    }

Modify it, delete [logging] and default_ccache_name in [libdefaults]:

.. code-block:: bash

    [libdefaults]
    default_realm = DEFAULT
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    allow_weak_crypto = true
    udp_preference_limit = 32700

    [realms]
    DEFAULT = {
    kdc = host1:1088
    kdc = host2:1088
    }

Above is your krb5.ini for Kerberos on Windows. Put it at 3 places:

    C:\ProgramData\MIT\Kerberos5\krb5.ini

    C:\Program Files\MIT\Kerberos\krb5.ini

    C:\Windows\krb5.ini


Finally, configure hosts file at: C:/Windows/System32/drivers/etc/hosts
Add ip mappings of host1, host2 in the previous example. e.g.

.. code-block:: bash

    10.6.6.96     host1
    10.6.6.97     host2

Now, you can try running kinit in your command line!

Testing
=======

On his way
