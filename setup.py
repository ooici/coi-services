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

version = '3.0.1-dev'

setup(  name = 'coi-services',
        version = version,
        description = 'OOINet System',
        url = 'https://github.com/ooici/coi-services',
        download_url = 'http://sddevrepo.oceanobservatories.org/releases/',
        license = 'Apache 2.0',
        author = 'Michael Meisinger',
        author_email = 'mmeisinger@ucsd.edu',
        keywords = ['ooi','ooinet', 'pyon'],
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
            'seawater==2.0.1',
            'pygsw==0.0.10',
            'matplotlib==1.1.1',
            'Pydap==3.3.RC1',
            'netCDF4>=1.0',
            'pyparsing==1.5.6', 
            'ntplib',
            'xlrd==0.8.0',
            'xlwt==0.7.5',
            'apscheduler==2.1.0',
            'pyproj==1.9.3',
            'udunitspy==0.0.6',
        ],
        entry_points = """
            [pydap.handler]
            coverage = ion.util.pydap.handlers.coverage:Handler
        """,
     )

