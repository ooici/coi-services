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

version = '2.0.16-dev'

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
        ],
        test_suite = 'pyon',
        install_requires = [
            'pyzmq==2.2.0',
            'coverage-model',
            'ion-functions',
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
            'ntplib',
            'xlrd==0.8.0',
            'apscheduler==2.1.0'
        ],
        entry_points = """
            [pydap.handler]
            coverage = ion.util.pydap.handlers.coverage:Handler
        """,
     )
