
from ._version import get_versions
from .v2 import Connection, Graph, CypherResult


__version__ = get_versions()['version']
del get_versions

__all__ = [
    '__version__',
    'stellar_hive',
    'stellar_rdd',
    'graph_types',
    'Connection',
    'Graph',
    'CypherResult',
]