"""DB-API implementation backed by PyHive

See http://www.python.org/dev/peps/pep-0249/

Many docstrings in this file are based on the PEP, which is in the public domain.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

from pyhive import hive
from pystellardb import graph_types
from pystellardb import stellar_rdd

from TCLIService import TCLIService
from TCLIService import ttypes
# Make all exceptions visible in this module per DB-API
from pyhive.exc import *  # noqa
from builtins import range
import contextlib
import getpass
import logging
import sys
import json
import thrift.protocol.TBinaryProtocol
import thrift.transport.TSocket
import thrift.transport.TTransport

_logger = logging.getLogger(__name__)


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database. See class :py:class:`Connection` for
    arguments.

    :returns: a :py:class:`Connection` object.
    """
    return StellarConnection(*args, **kwargs)


class StellarConnection(object):
    """Wraps a Thrift session"""
    def __init__(self,
                 host=None,
                 port=None,
                 username=None,
                 database='default',
                 auth=None,
                 configuration=None,
                 kerberos_service_name=None,
                 password=None,
                 thrift_transport=None,
                 graph_name=None):
        """Connect to HiveServer2

        :param host: What host HiveServer2 runs on
        :param port: What port HiveServer2 runs on. Defaults to 10000.
        :param auth: The value of hive.server2.authentication used by HiveServer2.
            Defaults to ``NONE``.
        :param configuration: A dictionary of Hive settings (functionally same as the `set` command)
        :param kerberos_service_name: Use with auth='KERBEROS' only
        :param password: Use with auth='LDAP' or auth='CUSTOM' only
        :param thrift_transport: A ``TTransportBase`` for custom advanced usage.
            Incompatible with host, port, auth, kerberos_service_name, and password.
        :param graph_name: The name of graph to manipulate.

        The way to support LDAP and GSSAPI is originated from cloudera/Impyla:
        https://github.com/cloudera/impyla/blob/255b07ed973d47a3395214ed92d35ec0615ebf62
        /impala/_thrift_api.py#L152-L160
        """
        username = username or getpass.getuser()
        configuration = configuration or {}

        if (password is not None) != (auth in ('LDAP', 'CUSTOM')):
            raise ValueError(
                "Password should be set if and only if in LDAP or CUSTOM mode; "
                "Remove password or use one of those modes")
        if (kerberos_service_name is not None) != (auth == 'KERBEROS'):
            raise ValueError(
                "kerberos_service_name should be set if and only if in KERBEROS mode"
            )
        if thrift_transport is not None:
            has_incompatible_arg = (host is not None or port is not None
                                    or auth is not None
                                    or kerberos_service_name is not None
                                    or password is not None)
            if has_incompatible_arg:
                raise ValueError(
                    "thrift_transport cannot be used with "
                    "host/port/auth/kerberos_service_name/password")

        if thrift_transport is not None:
            self._transport = thrift_transport
        else:
            if port is None:
                port = 10000
            if auth is None:
                auth = 'NONE'
            socket = thrift.transport.TSocket.TSocket(host, port)
            if auth == 'NOSASL':
                # NOSASL corresponds to hive.server2.authentication=NOSASL in hive-site.xml
                self._transport = thrift.transport.TTransport.TBufferedTransport(
                    socket)
            elif auth in ('LDAP', 'KERBEROS', 'NONE', 'CUSTOM'):
                # Defer import so package dependency is optional
                import sasl
                import thrift_sasl

                if auth == 'KERBEROS':
                    # KERBEROS mode in hive.server2.authentication is GSSAPI in sasl library
                    sasl_auth = 'GSSAPI'
                else:
                    sasl_auth = 'PLAIN'
                    if password is None:
                        # Password doesn't matter in NONE mode, just needs to be nonempty.
                        password = 'x'

                def sasl_factory():
                    sasl_client = sasl.Client()
                    sasl_client.setAttr('host', host)
                    if sasl_auth == 'GSSAPI':
                        sasl_client.setAttr('service', kerberos_service_name)
                    elif sasl_auth == 'PLAIN':
                        sasl_client.setAttr('username', username)
                        sasl_client.setAttr('password', password)
                    else:
                        raise AssertionError
                    sasl_client.init()
                    return sasl_client

                self._transport = thrift_sasl.TSaslClientTransport(
                    sasl_factory, sasl_auth, socket)
            else:
                # All HS2 config options:
                # https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2#SettingUpHiveServer2-Configuration
                # PAM currently left to end user via thrift_transport option.
                raise NotImplementedError(
                    "Only NONE, NOSASL, LDAP, KERBEROS, CUSTOM "
                    "authentication are supported, got {}".format(auth))

        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(
            self._transport)
        self._client = TCLIService.Client(protocol)
        # oldest version that still contains features we care about
        # "V6 uses binary type for binary payload (was string) and uses columnar result set"
        protocol_version = ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6

        self._graph_schema = None

        try:
            self._transport.open()
            open_session_req = ttypes.TOpenSessionReq(
                client_protocol=protocol_version,
                configuration=configuration,
                username=username,
            )
            response = self._client.OpenSession(open_session_req)
            hive._check_status(response)
            assert response.sessionHandle is not None, "Expected a session from OpenSession"
            self._sessionHandle = response.sessionHandle
            assert response.serverProtocolVersion == protocol_version, \
                "Unable to handle protocol version {}".format(response.serverProtocolVersion)

            # parse schema if the graph_name is assigned
            if graph_name is not None:
                with contextlib.closing(self.cursor()) as cursor:
                    cursor.execute('set query.lang=cypher')
                    cursor.execute('DESCRIBE GRAPH {} RAW'.format(graph_name))

                    schemaInJson = cursor.fetchone()[0]

                    self._graph_schema = graph_types.GraphSchema.parseSchemaFromJson(
                        schemaInJson)
            else:
                assert response.serverProtocolVersion == protocol_version, \
                    "Unable to handle protocol version {}".format(response.serverProtocolVersion)
                with contextlib.closing(self.cursor()) as cursor:
                    cursor.execute('set query.lang=sql')
                    cursor.execute('USE `{}`'.format(database))
        except:
            self._transport.close()
            raise

    def __enter__(self):
        """Transport should already be opened by __init__"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Call close"""
        self.close()

    def close(self):
        """Close the underlying session and Thrift transport"""
        req = ttypes.TCloseSessionReq(sessionHandle=self._sessionHandle)
        response = self._client.CloseSession(req)
        self._transport.close()
        hive._check_status(response)

    def commit(self):
        """Hive does not support transactions, so this does nothing."""
        pass

    def cursor(self, *args, **kwargs):
        """Return a new :py:class:`Cursor` object using the connection."""
        return StellarCursor(self, *args, **kwargs)

    @property
    def client(self):
        return self._client

    @property
    def sessionHandle(self):
        return self._sessionHandle

    def rollback(self):
        raise NotSupportedError(
            "Hive does not have transactions")  # pragma: no cover

    def getGraphSchema(self):
        return self._graph_schema


class StellarCursor(hive.Cursor):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """
    def __init__(self, connection, arraysize=1000):
        super(StellarCursor, self).__init__(connection, arraysize)
        self._column_comments = []
        self._graph_schema = connection.getGraphSchema()

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        This attribute will be ``None`` for operations that do not return rows or if the cursor has
        not had an operation invoked via the :py:meth:`execute` method yet.

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        if self._operationHandle is None or not self._operationHandle.hasResultSet:
            return None
        if self._description is None:
            req = ttypes.TGetResultSetMetadataReq(self._operationHandle)
            response = self._connection.client.GetResultSetMetadata(req)
            hive._check_status(response)
            columns = response.schema.columns
            self._description = []
            # If it's a cypher query, column comment is not null
            self._column_comments = [
                col.comment for col in response.schema.columns
                if col.comment is not None
            ]

            for col in columns:
                primary_type_entry = col.typeDesc.types[0]
                if primary_type_entry.primitiveEntry is None:
                    # All fancy stuff maps to string
                    type_code = ttypes.TTypeId._VALUES_TO_NAMES[
                        ttypes.TTypeId.STRING_TYPE]
                else:
                    type_id = primary_type_entry.primitiveEntry.type
                    type_code = ttypes.TTypeId._VALUES_TO_NAMES[type_id]
                self._description.append(
                    (col.columnName.decode('utf-8')
                     if sys.version_info[0] == 2 else col.columnName,
                     type_code.decode('utf-8') if sys.version_info[0] == 2 else
                     type_code, None, None, None, None, True))
        return self._description

    def fetchone(self):
        row = super(StellarCursor, self).fetchone()

        # Empty data / graph name not assigned / Not a cypher query
        if row is None or self._graph_schema is None or len(
                self._column_comments) == 0:
            return row
        else:
            parsed_row = []

            for i in range(0, len(self._column_comments)):
                parsed_row.append(
                    self._convertData(self._column_comments[i], row[i]))

            return tuple(parsed_row)

    def _convertData(self, type, data):
        """Convert Crux type to Readable type"""
        if type == 'boolean':
            return bool(data)
        elif type == 'int':
            return int(data)
        elif type == 'long':
            return int(data)
        elif type == 'float' or type == 'double':
            return float(data)
        elif type == 'CruxType:Node' or type == 'GraphNode':
            return graph_types.Vertex.parseVertexFromJson(data)
        elif type == 'CruxType:Relation' or type == 'GraphRelation':
            return graph_types.Edge.parseEdgeFromJson(self._graph_schema, data)
        elif type == 'CruxType:Path':
            return graph_types.Path.parsePathFromJson(self._graph_schema, data)
        elif type.startswith('CruxType:List'):
            return self._parseList(type, data)
        elif type.startswith('CruxType:Map'):
            return self._parseMap(type, data)
        else:
            return data

    def _parseList(self, type, data):
        """Parse 'CruxType:List' type"""
        parsed_data = json.loads(data)
        newType = type[len('CruxType:List') + 1:type.find('>')]

        return [self._convertData(newType, json.dumps(entry)) for entry in parsed_data]

    def _parseMap(self, type, data):
        """Parse 'CruxType:Map' type"""
        parsed_data = json.loads(data)
        newTypes = type[len('CruxType:Map') + 1:-2].split(',')

        result = {}

        for entry in parsed_data.keys():
            key = self._convertData(newTypes[0], entry)
            result[key] = self._convertData(newTypes[1], parsed_data[entry])

        return result

    def toRDD(self, sc, parallelism=1):
        """
        Transform to RDD
        param sc: SparkContext
        param parallelism: RDD parallelism
        """
        return stellar_rdd.transformToRDD(self, sc, parallelism)
