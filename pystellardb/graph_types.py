"""
Define vertex, edge, path and schema data structure
"""

from __future__ import absolute_import
import abc
from future.utils import with_metaclass
import json
import logging
import binascii

_logger = logging.getLogger(__name__)


class GraphElement(with_metaclass(abc.ABCMeta, object)):
    """Base class of vertex and edge"""
    def __init__(self, label):
        self._label = label
        self._fields = {}
        self._tags = []

    def getLabel(self):
        return self._label

    def getField(self, name):
        return self._fields[name]

    def hasField(self, name):
        return name in self._fields

    def getFields(self):
        return self._fields

    def setFeild(self, name, value):
        self._fields[name] = value

    def getTags(self):
        return self._tags

    def setTags(self, newTags):
        self._tags = newTags


class Vertex(GraphElement):
    """
    Vertex consists of a user-defined id, a label, a number of properties, and a list of tags.
    Empty properties and tags will be execluded in JSON string.
    """
    def __init__(self, uid, label):
        super(Vertex, self).__init__(label)
        self._uid = uid

    def getUserId(self):
        return self._uid

    def toJSON(self):
        m = {
            'type': 'vertex',
            'label': self._label,
            'uid': self._uid,
        }

        if self._tags is not None and len(self._tags) > 0:
            m['tags'] = self._tags

        if self._fields is not None and len(self._fields) > 0:
            m['properties'] = self._fields

        return m

    def __str__(self):
        return json.dumps(self.toJSON())

    @staticmethod
    def parseVertexFromJson(json_str):
        """Parse a Vertex object from a JSON string"""
        m = json.loads(json_str)

        return Vertex.parseVertexFromDict(m)

    @staticmethod
    def parseVertexFromDict(m):
        """Parse a Vertex object from a dict"""
        if 'labels' not in m:
            raise ValueError("Could not find label in JSON")

        if '__uid' not in m['properties']:
            raise ValueError("Could not find uid in JSON")

        vertex = Vertex(m['properties']['__uid'], m['labels'][0])

        for key in m['properties'].keys():
            if key != '__uid' and key != '__tags':
                vertex.setFeild(key, m['properties'][key])

        vertex.setTags(m['properties']['__tags'])

        return vertex

    @staticmethod
    def parseUidFromRK(rk):
        """Parse user-defined id from vertex row key in byte array"""
        return ''.join([chr(x) for x in rk[:-2]])

    @staticmethod
    def parseLabelIdxFromRK(rk):
        """Parse label index from vertex row key in byte array"""
        label_in_little_endian = rk[-2:]
        #reverse to big endian
        label_in_little_endian.reverse()
        return int(binascii.hexlify(bytearray(label_in_little_endian)), 16)


class Edge(GraphElement):
    """
    Edge consists of a start node, an end node, a label, and a list of tags.
    If multiple edges exist with the same start node and end node, an extra user-defined edge id should be assigned.
    Empty properties and tags will be execluded in JSON string.
    """
    def __init__(self, label):
        super(Edge, self).__init__(label)
        self._startNode = None
        self._endNode = None
        self._uid = ""

    def getStartNode(self):
        return self._startNode

    def setStartNode(self, startNode):
        self._startNode = startNode

    def getEndNode(self):
        return self._endNode

    def setEndNode(self, endNode):
        self._endNode = endNode

    def getUid(self):
        return self._uid

    def setUid(self, uid):
        self._uid = uid

    def toJSON(self):
        m = {
            'type': 'edge',
            'label': self._label,
            'euid': self._uid,
            'startNode': self._startNode.toJSON(),
            'endNode': self._endNode.toJSON(),
        }

        if self._tags is not None and len(self._tags) > 0:
            m['tags'] = self._tags

        if self._fields is not None and len(self._fields) > 0:
            m['properties'] = self._fields

        return m

    def __str__(self):
        return json.dumps(self.toJSON())

    @staticmethod
    def parseEdgeFromJson(schema, json_str):
        """Parse an Edge object from a JSON string"""
        m = json.loads(json_str)

        return Edge.parseEdgeFromDict(schema, m)

    @staticmethod
    def parseEdgeFromDict(schema, m):
        """Parse an Edge object from a dict"""
        if 'labels' not in m:
            raise ValueError("Could not find label in JSON")

        edge = Edge(m['labels'][0])

        # parse start node
        if 'startKey' not in m:
            raise ValueError("Could not find start node entity key in JSON")

        startUid = Vertex.parseUidFromRK(m['startKey'])
        startLabelIdx = Vertex.parseLabelIdxFromRK(m['startKey'])
        startLabel = schema.getVertexLabel(startLabelIdx)

        if startLabel is None:
            raise ValueError(
                'Could not find start node label with label index `{}`'.format(
                    startLabelIdx))

        edge.setStartNode(Vertex(startUid, startLabel))

        # parse end node
        if 'endKey' not in m:
            raise ValueError("Could not find end node entity key in JSON")

        endUid = Vertex.parseUidFromRK(m['endKey'])
        endLabelIdx = Vertex.parseLabelIdxFromRK(m['endKey'])
        endLabel = schema.getVertexLabel(endLabelIdx)

        if endLabel is None:
            raise ValueError(
                'Could not find end node label with label index `{}`'.format(
                    endLabelIdx))

        edge.setEndNode(Vertex(endUid, endLabel))

        # parse extra edge id
        if '__uid' in m['properties']:
            edge.setUid(m['properties']['__uid'])

        # parse properties
        for key in m['properties'].keys():
            if key != '__uid' and key != '__tags':
                edge.setFeild(key, m['properties'][key])

        # parse tags
        edge.setTags(m['properties']['__tags'])

        return edge


class Path(object):
    """A Path object consists of a list of Vertex and Edge objects"""
    def __init__(self, elems):
        self._elems = elems

    def getElements(self):
        return self._elems

    def length(self):
        return len(self._elems)

    def __str__(self):
        return str([str(entry) for entry in self._elems])

    @staticmethod
    def parsePathFromJson(schema, json_str):
        m = json.loads(json_str)
        elems = []

        for i in range(0, len(m)):
            if i % 2 == 0:
                elems.append(Vertex.parseVertexFromDict(m[i]))
            else:
                elems.append(Edge.parseEdgeFromDict(schema, m[i]))

        return Path(elems)


class GraphSchema(object):
    """
    A schema of graph contains a list of meta info:
    schema_version: Version of schema
    graph_name: Name of graph, unique in a cluster
    shard_number: Shard number of graph
    index_type: Type of index, could only be 'native' now
    replication_number: Replication number of graph, could be 1, 3, or other odd integer
    """
    def __init__(self, schema_version, graph_name, shard_number, index_type,
                 replication_number):
        self._schema_version = schema_version
        self._graph_name = graph_name
        self._shard_number = shard_number
        self._index_type = index_type
        self._replication_number = replication_number
        self._vertex_schemas = []
        self._edge_schemas = []

        # since V8
        self._encryption_type = None
        self._index_separated = None

    def setVertexSchemas(self, schema):
        self._vertex_schemas = schema

    def setEdgeSchemas(self, schema):
        self._edge_schemas = schema

    def setEncryptionType(self, enc_type):
        self._encryption_type = enc_type

    def setIndexSeparate(self, idx_sep):
        self._index_separated = idx_sep

    def getVertexLabel(self, labelIndex):
        for schema in self._vertex_schemas:
            if schema['label.index'] == labelIndex:
                return schema['label.value']

        return None

    def getEdgeLabel(self, labelIndex):
        for schema in self._edge_schemas:
            if schema['label.index'] == labelIndex:
                return schema['label.value']

        return None

    def toJSON(self):
        m = {
            '__VERSION': self._schema_version,
            'graph.name': self._graph_name,
            'graph.replication.number': self._replication_number,
            'graph.shard.number': self._shard_number,
            'graph.index.type': self._index_type,
            'vertex.tables': self._vertex_schemas,
            'edge.tables': self._edge_schemas,
        }

        # since V8
        if self._encryption_type:
            m['graph.encryption.type'] = self._encryption_type

        # since V8
        if self._index_separated:
            m['graph.index.separated'] = self._index_separated

        return m

    def __str__(self):
        return json.dumps(self.toJSON())

    @staticmethod
    def parseSchemaFromJson(json_str):
        """Parse a schema from JSON string"""
        m = json.loads(json_str)

        if '__VERSION' not in m:
            raise ValueError('Could not find `__VERSION` in graph schema')

        if 'graph.index.type' not in m:
            raise ValueError(
                'Could not find `graph.index.type` in graph schema')

        if 'graph.name' not in m:
            raise ValueError('Could not find `graph.name` in graph schema')

        if 'graph.replication.number' not in m:
            raise ValueError(
                'Could not find `graph.replication.number` in graph schema')

        if 'graph.shard.number' not in m:
            raise ValueError(
                'Could not find `graph.shard.number` in graph schema')

        if 'vertex.tables' not in m:
            raise ValueError('Could not find `vertex.tables` in graph schema')

        if 'edge.tables' not in m:
            raise ValueError('Could not find `edge.tables` in graph schema')

        result = GraphSchema(m['__VERSION'], m['graph.name'], int(m['graph.shard.number']),
                             m['graph.index.type'],
                             int(m['graph.replication.number']))

        result.setVertexSchemas(m['vertex.tables'])
        result.setEdgeSchemas(m['edge.tables'])

        # since V8
        if 'graph.encryption.type' in m:
            result.setEncryptionType(m['graph.encryption.type'])

        # since V8
        if 'graph.index.separated' in m:
            result.setIndexSeparate(m['graph.index.separated'])

        return result
