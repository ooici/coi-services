#!/usr/bin/env python

"""
@package ion.idk.test.test_package
@file ion/idk/test/test_package.py
@author Bill French
@brief test file package process
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

from os.path import basename, dirname
from os import makedirs
from os.path import exists
import sys

from nose.plugins.attrib import attr
from mock import Mock
import unittest

from pyon.util.log import log
from ion.idk.metadata import Metadata
from ion.idk.driver_generator import DriverGenerator

from ion.idk.exceptions import NotPython
from ion.idk.exceptions import NoRoot
from ion.idk.exceptions import FileNotFound

from ion.idk.egg_generator import DriverFileList
from ion.idk.egg_generator import DependencyList
from ion.idk.egg_generator import EggGenerator


ROOTDIR="/tmp/test_package.idk_test"
# /tmp is a link on OS X
if exists("/private/tmp"):
    ROOTDIR = "/private%s" % ROOTDIR
TESTDIR="%s/mi/foo" % ROOTDIR


@unittest.skip('Skip until moved to MI repo')
@attr('UNIT', group='mi')
class IDKPackageNose(unittest.TestCase):
    """
    Base class for IDK Package Tests
    """    
    def setUp(self):
        """
        Setup the test case
        """
        # Our test path needs to be in the python path for SnakeFood to work.
        sys.path = ["%s/../.." % TESTDIR] + sys.path
        
        self.write_basefile()
        self.write_implfile()
        self.write_nosefile()
        self.write_resfile()
        
    def write_basefile(self):
        """
        Create all of the base python modules.  These files live in the same root
        and should be reported as internal dependencies
        """
        
        destdir = dirname(self.basefile())
        if not exists(destdir):
            makedirs(destdir)
        
        ofile = open(self.basefile(), "w")
        ofile.write( "class MiFoo():\n")
        ofile.write( "    def __init__():\n")
        ofile.write( "        pass\n\n")
        ofile.close()
        
        # base2.py is a simple python module with no dependencies
        initfile = self.basefile().replace("base.py", 'base2.py')
        ofile = open(initfile, "w")
        ofile.write( "import mi.base4\n")
        ofile.close()

        # base3.py has an external dependency
        initfile = self.basefile().replace("base.py", 'base3.py')
        ofile = open(initfile, "w")
        ofile.write( "import string\n\n")
        ofile.close()

        # base4.py has an circular dependency
        initfile = self.basefile().replace("base.py", 'base4.py')
        ofile = open(initfile, "w")
        ofile.write( "import base2\n\n")
        ofile.close()
        
        # We need out init file
        initfile = self.basefile().replace("base.py", '__init__.py')
        ofile = open(initfile, "w")
        ofile.write("")
        ofile.close()
        
    def write_implfile(self):
        """
        The impl.py file is the target of our test.  All tests will report the
        dependencies of this file.
        """
        destdir = dirname(self.implfile())
        if not exists(destdir):
            makedirs(destdir)
        
        # Write a base file
        ofile = open(self.implfile(), "w")
        
        # Test various forms of import. MiFoo is a class defined in base.py
        # The rest are py file imports.
        ofile.write( "from mi.base import MiFoo\n")
        ofile.write( "import mi.base2\n")
        ofile.write( "from mi import base3\n\n")
        ofile.close()
        
        # Add a pyc file to ignore
        initfile = self.implfile().replace("impl.py", 'impl.pyc')
        ofile = open(initfile, "w")
        ofile.close()
        
        # Ensure we have an import in an __init__ py file
        initfile = self.implfile().replace("impl.py", '__init__.py')
        ofile = open(initfile, "w")
        ofile.close()
        
    def write_nosefile(self):
        """
        The test.py file is the target of our test.  All tests will report the
        dependencies of this file.
        """
        destdir = dirname(self.nosefile())
        if not exists(destdir):
            makedirs(destdir)
        
        # Write a base test file
        ofile = open(self.nosefile(), "w")
        ofile.close()

        # Ensure we have an import in an __init__ py file
        initfile = self.nosefile().replace("test_process.py", '__init__.py')
        ofile = open(initfile, "w")
        ofile.close()

    def write_resfile(self):
        """
        The impl.py file is the target of our test.  All tests will report the
        dependencies of this file.
        """
        destdir = dirname(self.resfile())
        log.debug(self.resfile())
        if not exists(destdir):
            makedirs(destdir)
        
        # Write a base file
        ofile = open(self.resfile(), "w")
        
        # Test various forms of import. MiFoo is a class defined in base.py
        # The rest are py file imports.
        ofile.write( "hello world\n")
        ofile.close()
        
    def basefile(self):
        """
        The main base python file imported by the target file.
        """
        return "%s/../%s" % (TESTDIR, "base.py")
        
    def implfile(self):
        """
        The main python we will target for the tests
        """
        return "%s/%s" % (TESTDIR, "impl.py")
        
    def nosefile(self):
        """
        The main test python we will target for the tests
        """
        return "%s/%s" % (TESTDIR, "test/test_process.py")
        
    def resfile(self):
        """
        The main test resource we will target for the tests
        """
        return "%s/%s" % (TESTDIR, "res/test_file")


@unittest.skip('Skip until moved to MI repo')
@attr('UNIT', group='mi')
class TestDependencyList(IDKPackageNose):
    """
    Test the DependencyList object that uses the snakefood module.  
    """    
    def test_exceptions(self):
        """
        Test all of the failure states for DependencyList
        """
        generator = None
        try:
            generator = DependencyList("this_file_does_not_exist.foo")
        except FileNotFound, e:
            self.assertTrue(e)
        self.assertFalse(generator)
        
        generator = None
        try:
            generator = DependencyList("/etc/hosts")
        except NotPython, e:
            self.assertTrue(e)
        self.assertFalse(generator)
        
        
    def test_internal_dependencies(self):
        """
        Test internal the dependency lists.  This should include
        all of the files we created in setUp()
        """
        generator = DependencyList(self.implfile())
        root_list = generator.internal_roots()
        dep_list = generator.internal_dependencies()
            
        self.assertTrue(ROOTDIR in root_list)
        
        internal_deps = [
                          "mi/base.py", 
                          "mi/base2.py", 
                          "mi/base3.py",
                          "mi/base4.py",
                          "mi/foo/impl.py",
                        ]
            
        self.assertEqual(internal_deps, dep_list)
        
    def test_internal_dependencies_with_init(self):
        """
        Test internal the dependency lists.  This should include
        all of the files we created in setUp()
        """
        generator = DependencyList(self.implfile(), include_internal_init = True)
        root_list = generator.internal_roots()
        dep_list = generator.internal_dependencies()
        
        self.assertTrue(ROOTDIR in root_list)
        
        internal_deps = [
                          "mi/__init__.py",
                          "mi/base.py", 
                          "mi/base2.py", 
                          "mi/base3.py",
                          "mi/base4.py",
                          "mi/foo/__init__.py",
                          "mi/foo/impl.py", 
                         ]
        
        self.assertEqual(internal_deps, dep_list)

    def test_internal_test_dependencies_with_init(self):
        """
        Test internal the dependency lists for the unit test.
        """
        generator = DependencyList(self.nosefile(), include_internal_init = True)
        root_list = generator.internal_roots()
        dep_list = generator.internal_dependencies()

        self.assertTrue(ROOTDIR in root_list)

        internal_deps = [
            "mi/__init__.py",
            "mi/foo/__init__.py",
            "mi/foo/test/__init__.py",
            "mi/foo/test/test_process.py",
            ]

        self.assertEqual(internal_deps, dep_list)

    def test_external_dependencies(self):
        """
        Test external the dependency lists.  This should exclude
        all of the files we created in setUp()
        """
        generator = DependencyList(self.implfile())
        root_list = generator.external_roots()
        dep_list = generator.external_dependencies()
        
        self.assertFalse(ROOTDIR in root_list)

        self.assertFalse("mi/base4.py" in dep_list)
        self.assertFalse("mi/base3.py" in dep_list)
        self.assertFalse("mi/base2.py" in dep_list)
        self.assertFalse("mi/foo/impl.py" in dep_list)
        self.assertFalse("mi/base.py" in dep_list)
        self.assertTrue("string.py" in dep_list)
        
    def test_all_dependencies(self):
        """
        Test the full dependency lists.  This should exclude
        all of the files we created in setUp()
        """
        generator = DependencyList(self.implfile())
        root_list = generator.all_roots()
        dep_list = generator.all_dependencies()
        
        self.assertTrue(ROOTDIR in root_list)
        
        self.assertTrue("mi/base4.py" in dep_list)
        self.assertTrue("mi/base3.py" in dep_list)
        self.assertTrue("mi/base2.py" in dep_list)
        self.assertTrue("mi/foo/impl.py" in dep_list)
        self.assertTrue("mi/base.py" in dep_list)
        self.assertTrue("string.py" in dep_list)


@unittest.skip('Skip until moved to MI repo')
@attr('UNIT', group='mi')
class TestDriverFileList(IDKPackageNose):
    """
    Test the driver file list object.  The driver file list is what is
    stored in the driver egg
    """
    def test_extra_list(self):
        """
        Find all the files in the driver directory
        """
        rootdir = dirname(TESTDIR)
        filelist = DriverFileList(Metadata(), ROOTDIR, self.implfile(), self.nosefile())
        self.assertTrue(filelist)
        
        known_files = [
            '%s/res/test_file' % TESTDIR
        ]
        
        files = filelist._extra_files()

        log.debug(sorted(files))
        log.debug(sorted(known_files))

        self.assertEqual(sorted(files), sorted(known_files))
        
        
    def test_list(self):
        """
        Test the full file manifest
        """
        filelist = DriverFileList(Metadata(), ROOTDIR, self.implfile(), self.nosefile())
        self.assertTrue(filelist)

        known_files = [
                      'mi/__init__.py',
                      'mi/base.py',
                      'mi/base2.py',
                      'mi/base3.py',
                      'mi/base4.py',
                      'mi/foo/__init__.py',
                      'mi/foo/impl.py',
                      'mi/foo/res/test_file',
                      'mi/foo/test/__init__.py',
                      'mi/foo/test/test_process.py',
        ]
        
        files = filelist.files()
        log.debug( "F: %s" % files)

        self.assertEqual(sorted(files), sorted(known_files))
        pass
        
        