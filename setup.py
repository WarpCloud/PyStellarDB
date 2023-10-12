#!/usr/bin/env python

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import versioneer


# class PyTest(TestCommand):
#     def finalize_options(self):
#         TestCommand.finalize_options(self)
#         self.test_args = []
#         self.test_suite = True

#     def run_tests(self):
#         # import here, cause outside the eggs aren't loaded
#         import pytest
#         errno = pytest.main(self.test_args)
#         sys.exit(errno)


with open('README.rst') as readme:
    long_description = readme.read()

setup(
    name="PyStellarDB",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Python interface to StellarDB",
    long_description=long_description,
    url='https://github.com/WarpCloud/PyStellarDB',
    author="Zhiping Wang",
    author_email="zhiping.wang@transwarp.io",
    license="Apache License, Version 2.0",
    python_requires='>=2.7',
    packages=find_packages(),
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
    ],
    install_requires=[
        'future',
        'python-dateutil',
        'pyhive',
        'sasl',
        'thrift',
        'thrift-sasl>=0.3.0'
    ],
    extras_require={
        'presto': ['requests>=1.0.0'],
        'hive': ['sasl>=0.2.1', 'thrift>=0.10.0'],
        'sqlalchemy': ['sqlalchemy>=1.3.0'],
        'kerberos': ['requests_kerberos>=0.12.0'],
        'pyspark': ['pyspark>=2.4.0']
    },
    tests_require=[
        'mock>=1.0.0',
        'pytest',
        'pytest-cov',
        'requests>=1.0.0',
        'requests_kerberos>=0.12.0',
        'sasl>=0.2.1',
        'sqlalchemy>=1.3.0',
        'thrift>=0.10.0',
    ],
    package_data={
        '': ['*.rst'],
    },
    entry_points={}
)
