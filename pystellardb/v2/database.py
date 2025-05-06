"""
Define database class for operations on a single graph.
"""

from __future__ import absolute_import
from types import TracebackType
import logging
import json
from typing import Any, Union

from ..graph_types import GraphSchema
from .connection import Connection, CypherResult

_logger = logging.getLogger(__name__)

class Graph(object):
    """
    Database class for operations on a single graph.
    """

    def __init__(self, graph_name: str, connection: Connection):
        self.graph_name = graph_name
        self.connection = connection
        self.graph_schema: GraphSchema = None
        self._init_connection()

    def __enter__(self):
        """
        Enter the database context.
        """
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the database context.
        """
        if self.connection:
            self.connection.close()
        self.connection = None

    def _init_connection(self):
        """
        Initialize the connection to the database.
        """
        
        self.connection.execute('config query.lang cypher')
        
        """Try to bind cursor with graph name."""
        try:
            self.connection.execute(f'use graph {self.graph_name}')
            self._get_graph_schema()
        except Exception as e:
            _logger.debug(f"graph {self.graph_name} not found")

    def _get_graph_schema(self):
        """
        Get the graph schema.
        """
        query_result = self.connection.execute('DESCRIBE GRAPH {} RAW'.format(self.graph_name))
        schemaInJson = query_result.fetchone()[0]
        query_result.close()
        self.graph_schema = GraphSchema.parseSchemaFromJson(schemaInJson)
        
        # get schema from data
        try:
            query_result = self.connection.execute('manipulate graph {} get_schema_from_data'.format(self.graph_name))
            self.graph_labels = query_result.fetchone()[0]
            query_result.close()
            self.graph_labels = json.loads(self.graph_labels)
        except:
            pass

    def execute(self, query: str, *args, **kwargs) -> CypherResult:
        """
        Execute a query on the database.
        """
        query_result = self.connection.execute(query, self.graph_schema)

        if (query.strip().lower().startswith('create graph')):
            self._get_graph_schema()
            
        return query_result

    def create(self, cypher: str):
        """
        Create a graph.
        """
        create_result = self.connection.execute(cypher)
        create_result.close()
        self._get_graph_schema()
    
    def drop(self):
        """
        Drop the graph.
        """
        drop_result = self.connection.execute('DROP GRAPH {}'.format(self.graph_name))
        drop_result.close()
        self.graph_schema = None

    def get_graph_schema(self) -> Union[GraphSchema, None]:
        """
        Get the graph schema.
        """
        return self.graph_schema
    
    def get_labels(self) -> Any:
        """
        Get the labels of the graph.
        """
        return self.graph_labels