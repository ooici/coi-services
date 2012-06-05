#!/usr/bin/env python

"""
@file coi-services/ion/idk/egg_generator.py
@author Bill French
@brief Generate egg for a driver.  This uses snakefood to build
a dependecy list and includes all files in the driver directory.
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import re
import os
import sys
from os.path import basename, dirname
from operator import itemgetter
import pprint

from ion.idk.config import Config
from ion.idk.metadata import Metadata
from ion.idk.driver_generator import DriverGenerator
from pyon.util.log import log

from ion.idk.exceptions import NotPython
from ion.idk.exceptions import NoRoot
from ion.idk.exceptions import FileNotFound

from snakefood.util import iter_pyfiles, setup_logging, def_ignores, is_python
from snakefood.depends import output_depends, read_depends
from snakefood.find import find_dependencies
from snakefood.find import find_imports
from snakefood.find import ERROR_IMPORT, ERROR_SYMBOL, ERROR_UNUSED
from snakefood.fallback.collections import defaultdict
from snakefood.roots import *


class DependencyList:
    """
    Build a list of dependency classes for a python module.  This uses the snakefood
    module to build out the list.  This class does not output __init__.py files by
    default, but passing the include_interanl_init option to the constructor can
    change this behavior.
    
    Usage:
    
    deplist = DependencyList(target_file, True)
    
    # All dependency files.
    all_deps = deplist.all_dependencies()
    
    # Internal dependencies
    int_deps = deplist.internal_dependencies()
    
    # External dependencies
    extern_deps = deplist.external_dependencies()
    """
    def __init__(self, filename, include_internal_init = False):
        if not os.path.isfile(filename):
            raise FileNotFound(filename)
            
        if not is_python(filename):
            raise NotPython(filename)
            
        self.include_internal_init = include_internal_init
            
        self.dependency_list = None
        self.file_roots = None
        
        self.filename = filename
        
    def fetch_dependencies(self):
        """
        Fetch all dependencies and follow the target file.
        This was inspired by the snakefood library
        snakefood-1.4-py2.7.egg/snakefood/gendeps.py
        """
        # No need to run this twice
        if self.dependency_list: return self.dependency_list
        
        log.info("Fetching internal dependecies: %s" % self.filename)
        
        depends = find_imports(self.filename, 1, 0)
        
        # Get the list of package roots for our input files and prepend them to the
        # module search path to insure localized imports.
        inroots = find_roots([self.filename], [])
        self.file_roots = inroots
        
        if not inroots:
            raise NoRoot
        
        for file in inroots:
            log.debug("Root found: %s" % file)
        sys.path = inroots + sys.path
            
        #log.debug("Using the following import path to search for modules:")
        #for dn in sys.path:
        #    log.debug(" --  %s" % dn)
        inroots = frozenset(inroots)

        # Find all the dependencies.
        log.debug("Processing file:")
        allfiles = defaultdict(set)
        allerrors = []
        processed_files = set()
        ignorefiles = []
        alldependencies = []

        fiter = iter_pyfiles([self.filename], ignorefiles, False)
        while 1:
            newfiles = set()

            for fn in fiter:
                log.debug("  post-filter: %s" % fn)
                processed_files.add(fn)
    
                if is_python(fn):
                    files, errors = find_dependencies(fn, 0, 0)
                    log.debug("dependency file count: %d" % len(files))
                    allerrors.extend(errors)
                else:
                    # If the file is not a source file, we don't know how to get the
                    # dependencies of that (without importing, which we want to
                    # avoid).
                    files = []
            
                # When packages are the source of dependencies, remove the __init__
                # file.  This is important because the targets also do not include the
                # __init__ (i.e. when "from <package> import <subpackage>" is seen).
                if basename(fn) == '__init__.py':
                    fn = dirname(fn)

                # no dependency.
                from_ = relfile(fn, ignorefiles)
                if from_ is None:
                    log.debug("from_ empty.  Move on")
                    continue
                infrom = from_[0] in inroots
                log.debug( "  from: %s" % from_[0])
                log.debug( "  file: %s" % from_[1])
                allfiles[from_].add((None, None))
    
                # Add the dependencies.
                for dfn in files:
                    xfn = dfn
                    if basename(xfn) == '__init__.py':
                        xfn = dirname(xfn)
        
                    to_ = relfile(xfn, ignorefiles)
                    into = to_[0] in inroots
                    log.debug( "  from: %s" % from_[1])
                    log.debug( "  to: %s" % to_[1])

                    if dfn in alldependencies:
                        log.debug("Already added %s to dependency list" % dfn)
                    else:
                        log.debug("Add %s to dependency list" % dfn)
                        allfiles[from_].add(to_)
                        newfiles.add(dfn)
                        alldependencies.append(dfn)

                    
            if not newfiles:
                log.debug("No more new files.  all done")
                break
            else:
                fiter = iter(sorted(newfiles))
                
        # Output the list of roots found.
        log.debug("Found roots:")

        found_roots = set()
        for key, files in allfiles.iteritems():
            found_roots.add(key[0])
            found_roots.update(map(itemgetter(0),files))
        if None in found_roots:
            found_roots.remove(None)
        for root in sorted(found_roots):
            log.debug("  %s" % root)

        
        self.dependency_list = allfiles
        return self.dependency_list;
                
        
    def internal_dependencies(self):
        """
        Return a list of internal dependencies for self.filename
        """
        filelist = self.fetch_dependencies()
        
        initlist = []
        if self.include_internal_init:
            initlist = self._init_set(filelist, self.file_roots[0])
            
        log.debug( "Internal dependencies:" )
        return sorted(initlist + self._dependency_set(filelist, True, False))
        
        
    def external_dependencies(self):
        """
        Return a list of external dependencies for self.filename
        """
        filelist = self.fetch_dependencies()
        
        log.debug( "External dependencies:" )
        return self._dependency_set(filelist, False, True)
        
        
    def all_dependencies(self):
        """
        Return all dependencies for self.filename
        """
        filelist = self.fetch_dependencies()
        
        log.debug( "All dependencies:" )
        return self._dependency_set(filelist)
        
        
    def internal_roots(self):
        """
        Return a list of internal dependency roots for self.filename
        """
        filelist = self.fetch_dependencies()
        
        log.debug( "Internal roots:" )
        return self._root_set(filelist, True, False)
        
        
    def external_roots(self):
        """
        Return a list of external dependency roots for self.filename
        """
        filelist = self.fetch_dependencies()
        
        log.debug( "External roots:" )
        return self._root_set(filelist, False, True)
        
        
    def all_roots(self):
        """
        Return all dependency roots for self.filename
        """
        filelist = self.fetch_dependencies()
        
        log.debug( "All roots:" )
        return self._root_set(filelist)
        
        
    def _dependency_set(self, filelist, internal = None, external = None):
        """
        Return a set of files that are in the dependency list.  The internal and external
        flags speficy the list contents.  Internal set returns all files that have the same
        root as the target file.  External is the opposite.
        """
        result = []
        for key, files in filelist.iteritems():
            inroot = key[0] in self.file_roots
            if(not(internal or external) or (internal and inroot) or (external and not inroot)):
                result.append(key[1])
        
        result = sorted(result)
        for file in result:
            log.debug("   %s" % file)
        
        return result
        
    
    def _root_set(self, filelist, internal = None, external = None):
        """
        Return a set of files that are in the dependency list.  The internal and external
        flags speficy the list contents.  Internal set returns all files that have the same
        root as the target file.  External is the opposite.
        """
        result = []
        for key, files in filelist.iteritems():
            inroot = key[0] in self.file_roots
            if(not(internal or external) or (internal and inroot) or (external and not inroot)):
                if not key[0] in result:
                    result.append(key[0])
                
        result = sorted(result)
        for root in result:
            log.debug("   %s" % root)
        
        return result
    
    def _init_set(self, filelist, root):
        """
        Return a set of init files for the filelist passed in.
        """
        #log.debug("Build __init__.py list")
        result = []
        for key, files in filelist.iteritems():
            if key[0] == root:
                for initfile in self._get_init_files(key[1], root):
                    if not initfile in result:
                        result.append(initfile)
        
        result = sorted(result)
        for file in result:
            log.debug(" add init file %s" % file)
        
        return result
    
    def _get_init_files(self, pyfile, root):
        """
        recursively step through a python module path and find all the __init__.py files
        """
        result = []
        current_path = dirname(pyfile);
        while current_path:
            initfile = "%s/__init__.py" % current_path
            abspath = "%s/%s" % (root, initfile)
            log.debug( "  -- Does %s exist?" % abspath )
            if os.path.exists(abspath):
                result.append(initfile)
            current_path = dirname(current_path) 
            
        return result
        

class DriverFileList:
    """
    Build list of files that are associated to a driver.  It uses the DependencyList
    object to get all python files.  Then it will look in the target module directory
    for additional files.
    """
    def __init__(self, metadata, basedir, driver_file = None, driver_test_file = None):
        driver_generator = DriverGenerator(metadata)
        
        self.basedir = basedir

        if driver_file:
            self.driver_file = driver_file
        else:
            self.driver_file = driver_generator.driver_path()

        if driver_test_file:
            self.driver_test_file = driver_test_file
        else:
            self.driver_test_file = driver_generator.driver_test_path()
        
        self.driver_dependency = DependencyList(self.driver_file, include_internal_init=True)
        self.test_dependency = DependencyList(self.driver_test_file, include_internal_init=True)

    def files(self):
        basep = re.compile(self.basedir)
        rootp = re.compile('^/')
        result = []
        
        log.debug( "F: %s" % self.driver_file)
        driver_files = self.driver_dependency.internal_dependencies()
        test_files = self.test_dependency.internal_dependencies()
        extra_files = self._extra_files()
        files = self._extra_files() + self.driver_dependency.internal_dependencies() + self.test_dependency.internal_dependencies()

        for fn in files:
            if not fn in result:
                f = basep.sub('', fn)
                f = rootp.sub('', f)
                result.append(f)
                
        return result
        
    def _extra_files(self):
        result = []
        p = re.compile('\.(py|pyc)$')
        
        for root, dirs, names in os.walk(dirname(self.driver_file)):
            for filename in names:
                # Ignore python files
                if not p.search(filename):
                    result.append("%s/%s" % (root, filename))
                
        return result
    
class EggGenerator:
    """
    Generate driver egg
    """

    def __init__(self, metadata):
        """
        @brief Constructor
        @param metadata IDK Metadata object
        """
        


if __name__ == '__main__':
    pass

