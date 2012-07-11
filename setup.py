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

version = '0.1'

setup(  name = 'coi-services',
        version = version,
        description = 'OOI ION COI Services',
        url = 'https://github.com/ooici/coi-services',
        download_url = 'http://ooici.net/releases',
        license = 'Apache 2.0',
        author = 'Michael Meisinger',
        author_email = 'mmeisinger@ucsd.edu',
        keywords = ['ooici','ioncore', 'pyon', 'coi'],
        packages = find_packages(),
        dependency_links = [
            'http://ooici.net/releases',
            'https://github.com/ooici/marine-integrations/tarball/master#egg=marine_integrations-1.0',
            'https://github.com/ooici/pyon/tarball/master#egg=pyon'
        ],
        test_suite = 'pyon',
        install_requires = [
            'marine-integrations',
            'pyon',
            'Flask==0.8',
            'dateutil==1.5',
            'WebTest',
            'requests',
            'seawater',
            'matplotlib==1.1.0',
            'Pydap>=3.0.1',
            'netCDF4>=0.9.8',
            'cdat_lite>=6.0rc2',
            'elasticpy==0.9a',
            'pyparsing==1.5.6', 
            'snakefood==1.4',
        ],
     )
