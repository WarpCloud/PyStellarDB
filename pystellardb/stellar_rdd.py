"""
Create a function to transform StellarCursor data to RDD
"""

from __future__ import absolute_import
import abc
from future.utils import with_metaclass
import logging

try:
    import pyspark
    from pyspark import RDD, SparkContext
    from pyspark.serializers import BatchedSerializer
except ImportError:
    pyspark = None

_logger = logging.getLogger(__name__)


def transformToRDD(cursor, sc, parallelism=1):
    """
    Transform a StellarCursor to a Python RDD object

    param cursor: StellarCursor
    param sc: SparkContext
    param parallelism: Parallelism of RDD
    """
    if not pyspark:
        raise ImportError("Could not import pyspark! Please run `pip install pyspark` first in your environment!")

    # Get all data from cursor
    data = cursor.fetchall()

    # Set parallelism
    parallelism = max(1, parallelism)

    def reader_func(temp_filename):
        return sc._jvm.PythonRDD.readRDDFromFile(sc._jsc, temp_filename, parallelism)

    def createRDDServer():
        return sc._jvm.PythonParallelizeServer(sc._jsc.sc(), parallelism)

    batchSize = max(1, min(len(data) // parallelism, 1024))
    serializer = BatchedSerializer(sc._unbatched_serializer, batchSize)

    jrdd = sc._serialize_to_jvm(data, serializer, reader_func, createRDDServer)

    return RDD(jrdd, sc, serializer)
