#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

import os
import sys

# Add /usr/local/include to the path for macs, fixes easy_install for several packages (like gevent and pyyaml)
if sys.platform == 'darwin':
    os.environ['C_INCLUDE_PATH'] = '/usr/local/include'

version = '0.1.5-dev'

setup(  name = 'coi-services',
        version = version,
        description = 'OOI ION COI Services',
        url = 'https://github.com/ooici/coi-services',
        download_url = 'http://sddevrepo.oceanobservatories.org/releases/',
        license = 'Apache 2.0',
        author = 'Michael Meisinger',
        author_email = 'mmeisinger@ucsd.edu',
        keywords = ['ooici','ioncore', 'pyon', 'coi'],
        packages = find_packages(),
        dependency_links = [
            'http://sddevrepo.oceanobservatories.org/releases/',
            'https://github.com/ooici/coverage-model/tarball/master#egg=coverage-model',
            'https://github.com/ooici/marine-integrations/tarball/master#egg=marine_integrations-1.0',
            'https://github.com/ooici/pyon/tarball/v0.1.7#egg=pyon-1.0',
            'https://github.com/ooici/utilities/tarball/master#egg=utilities-9999'
        ],
        test_suite = 'pyon',
        install_requires = [
            'pyzmq==2.2.0',
            'utilities',
            'coverage-model',
            'marine-integrations',
            'pyon',
            'Flask==0.9',
            'python-dateutil==1.5',
            'WebTest==1.4.0',
            'requests==0.13.5',
            'seawater==2.0.1',
            'matplotlib==1.1.1',
            'Pydap==3.1.RC1',
            'netCDF4>=1.0',
            'elasticpy==0.11',
            'pyparsing==1.5.6', 
            'snakefood==1.4',
            'xlrd==0.8.0',
        ],
        entry_points = """
            [pydap.handler]
            coverage = ion.util.pydap.handlers.coverage:Handler
        """,
     )
