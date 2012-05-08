#!/usr/bin/env python

"""
@file coi-services/ion/idk/driver_generator.py
@author Bill French
@brief Generate directory structure and code stubs for a driver
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'


import os
import re
from string import Template

import yaml

from pyon.util.config import Config
from ion.idk.metadata import Metadata

DEBUG = False

class DriverGenerator:
    """
    Generate driver code, tests and directory structure
    """

    ###
    #    Configurations
    ###
    def base_dir(self):
        """
        @brief base directory for the new driver
        @retval dir name
        """
        if DEBUG:
            print "base_dir() = " + self.IDK_CFG.idk.dev_base_path.replace('~', os.getenv("HOME"))
        return self.IDK_CFG.idk.dev_base_path.replace('~', os.getenv("HOME"))

    def template_dir(self):
        """
        @brief directory where code templates are stored
        @retval template dir name
        """
        if DEBUG:
            print "template_dir() = " + "/".join([self.base_dir(), self.IDK_CFG.idk.coi_repo_dir_name, "ion/idk/templates"])
        return "/".join([self.base_dir(), self.IDK_CFG.idk.coi_repo_dir_name, "ion/idk/templates"]) 


    def driver_test_dir(self):
        """
        @brief directory to store the new driver test code
        @retval driver test dir name
        """
        if DEBUG:
            print "driver_test_dir() = " + "/".join([self.driver_dir(), "test"])
        return "/".join([self.driver_dir(), "test"])

    def resource_dir(self):
        """
        @brief directory to store the driver resources
        @retval driver resource dir name
        """
        if DEBUG:
            print "resource_dir() = " + "/".join([self.driver_dir(), "resource"])
        return "/".join([self.driver_dir(), "resource"])

    def driver_filename(self):
        """
        @brief file name of the new driver
        @retval driver filename
        """
        if DEBUG:
            print "driver_filename() = " + "%s_v%02d_driver.py" % (self.metadata.name.lower(), self.driver_version())
        return "%s_v%02d_driver.py" % (self.metadata.name.lower(), self.driver_version())

    def driver_full_name(self):
        """
        @brief full path and filename to the driver code
        @retval driver path
        """
        return "/".join([self.driver_dir(), self.driver_filename()])

    def driver_dir(self):
        """
        @brief full path to the driver code
        @retval driver path
        """
        if DEBUG:
            print "driver_dir() = " + "/".join([self.base_dir(), 
                  self.IDK_CFG.idk.mi_repo_dir_name, self.IDK_CFG.idk.driver_dir, 
                  'instrument', self.metadata.instrument_class, self.metadata.name.lower()])
        return "/".join([self.base_dir(), self.IDK_CFG.idk.mi_repo_dir_name, 
               self.IDK_CFG.idk.driver_dir, 'instrument', 
               self.metadata.instrument_class, self.metadata.name.lower()])


    def driver_test_filename(self):
        """
        @brief file name of the new driver tests
        @retval driver test filename
        """
        if DEBUG:
            print "driver_test_filename() = " + self.driver_filename().replace('driver', 'driver_test')
        return self.driver_filename().replace('driver', 'driver_test')

    def driver_test_full_name(self):
        """
        @brief full-path file name of the new driver tests
        @retval full-path driver test filename
        """
        if DEBUG:
            print "/".join([self.driver_test_dir(), self.driver_test_filename()])
        return "/".join([self.driver_test_dir(), self.driver_test_filename()])


    def template_dir(self): 
        """
        @brief path to the driver template dir
        @retval driver test code template path
        """
        if DEBUG:
            print "template_dir() = " + "/".join([self.base_dir(), self.IDK_CFG.idk.coi_repo_dir_name, "ion/idk/templates"])
        return "/".join([self.base_dir(), self.IDK_CFG.idk.coi_repo_dir_name, "ion/idk/templates"])

    def test_template(self):
        """
        @brief path to the driver test code template
        @retval driver test code template path
        """
        if DEBUG:
            print "test_template() = " + "/".join([self.template_dir(), "driver_test.tmpl"])
        return "/".join([self.template_dir(), "driver_test.tmpl"])

    def driver_template(self):
        """
        @brief path to the driver code template
        @retval driver code template path
        """
        if DEBUG:
            print "driver_template() = " + "/".join([self.template_dir(), "driver.tmpl"])
        return "/".join([self.template_dir(), "driver.tmpl"])

    def test_modulename(self):
        """
        @brief module name of the new driver tests
        @retval driver test module name
        """
        if DEBUG:
            print "test_modulename() = " + ".".join([self.IDK_CFG.idk.driver_dir, 
                  'instrument', self.metadata.instrument_class, self.metadata.name.lower(), 
                  'test', self.driver_test_filename().replace('/', '.').replace('.py', '')])
        return ".".join([self.IDK_CFG.idk.driver_dir, 'instrument', self.metadata.instrument_class, 
               self.metadata.name.lower(), 'test', self.driver_test_filename().replace('/', '.').replace('.py', '')])



    def driver_modulename(self):
        """
        @brief module name of the new driver tests
        @retval driver test module name
        """

        if DEBUG:
            print "driver_modulename() = " + ".".join([self.IDK_CFG.idk.driver_dir, 
                  'instrument', self.metadata.instrument_class, self.metadata.name.lower(), 
                  self.driver_filename().replace('/', '.').replace('.py', '')])
        return ".".join([self.IDK_CFG.idk.driver_dir, 'instrument', 
               self.metadata.instrument_class, self.metadata.name.lower(), 
               self.driver_filename().replace('/', '.').replace('.py', '')])

    def driver_version(self):

        if(self.metadata.version and self.metadata.version > 0):
            return self.metadata.version
        else:
            return self._get_next_version()


    ###
    #   Private Methods
    ###
    def __init__(self, metadata, force = False):
        """
        @brief Constructor
        @param metadata IDK Metadata object
        """
        self.metadata = metadata
        self.force = force

        conf_paths = ['../coi-services/res/config/idk.yml', '../coi-services/res/config/idk.local.yml']
        self.IDK_CFG = Config(conf_paths, ignore_not_found=True).data

    def _touch_init(self, dir):
        """
        @brief touch a python __init.py__ file if it doesn't exist

        @todo NEED to add in a __init__.py for the instrument_class dir

        """
        file = dir + "/__init__.py";

        if(not os.path.exists(file)):
            touch = open(file, "w")
            touch.close()


    def _get_template(self, template_file):
        """
        @brief return a string.Template object constructed with the contents of template_file
        @param template_file path to a file that containes a template
        @retval string.Template object
        """
        try:
            infile = open(template_file)
            tmpl_str = infile.read()
            return Template(tmpl_str)
        except IOError:
            return Template("")


    def _driver_template_data(self):
        """
        @brief dictionary containing a map of substitutions for the driver code generation
        @retval data mapping for driver generation
        """
        return {
            'driver_module': self.driver_modulename(),
            'file': self.driver_full_name(),
            'author': self.metadata.author,
            'driver_name': self.metadata.name,
            'release_notes': self.metadata.notes
        }


    def _test_template_data(self):
        """
        @brief dictionary containing a map of substitutions for the driver test code generation
        @retval data mapping for driver test generation
        """
        return {
            'test_module': self.test_modulename(),
            'driver_module': self.driver_modulename(),
            'driver_dir': self.driver_dir(),
            'driver_path': self.driver_dir(),
            'file': self.driver_full_name(),
            'author': self.metadata.author,
            'driver_name': self.metadata.name
        }


    def _get_next_version(self):
        """
        @brief Get the next available version number for a driver
        @retval version number
        """
        if(not os.path.exists(self.driver_dir())):
            return 1

        for file in os.listdir(self.driver_dir()):
            pass

        return 1


    ###
    #   Public Methods
    ###
    def build_directories(self):
        """
        @brief Build directory structure for the new driver
        """
        print( " -- Build directories --" )

        if not os.path.exists(self.driver_dir()):
            os.makedirs(self.driver_dir())

        if not os.path.exists(self.driver_test_dir()):
            os.makedirs(self.driver_test_dir())

        if not os.path.exists(self.resource_dir()):
            os.makedirs(self.resource_dir())

        self._touch_init(self.driver_dir())
        self._touch_init(self.driver_test_dir())


    def generate_code(self):
        """
        @brief Generate code files for the driver and tests
        """
        print( " -- Generating code --" )
        self.generate_driver_code()
        self.generate_test_code()


    def generate_driver_code(self):
        """
        @brief Generate stub driver code
        """
        if(os.path.exists(self.driver_full_name()) and not self.force):
            print "Warning: driver exists (" + self.driver_full_name() + ") not overwriting"
        else:
            template = self._get_template(self.driver_template())
            ofile = open( self.driver_full_name(), 'w' )
            code = template.substitute(self._driver_template_data())
            ofile.write(code)
            ofile.close()


    def generate_test_code(self):
        """
        @brief Generate stub driver test code
        """
        if(os.path.exists(self.driver_test_full_name()) and not self.force):
            print "Warning: driver test file exists (" + self.driver_test_full_name() + ") not overwriting"
        else:
            template = self._get_template(self.test_template())
            ofile = open( self.driver_test_full_name(), 'w' )
            code = template.substitute(self._test_template_data())
            ofile.write(code)
            ofile.close()


    def display_report(self):
        """
        @brief Display a report of the files created to STDOUT
        """
        print( "*** Generation Complete ***" )
        print(" - Driver File: " + self.driver_dir() + "/" + self.driver_filename())
        print(" - Test File: " + self.driver_test_dir() + "/" + self.driver_test_filename())
        print(" - Resource Directory: " + self.resource_dir())


    def generate(self):
        """
        @brief Main method for generating drivers.  Assumption: this is run from the console.
        """
        print( "*** Generating Driver Code ***" )

        self.build_directories()
        self.generate_code()
        self.display_report()


if __name__ == '__main__':
    metadata = Metadata()
    driver = DriverGenerator( metadata )

    driver.generate()

