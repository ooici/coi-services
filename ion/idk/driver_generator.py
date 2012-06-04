#!/usr/bin/env python

"""
@file coi-services/ion/idk/driver_generator.py
@author Bill French
@brief Generate directory structure and code stubs for a driver
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'


import os
import sys
import re
from string import Template

import yaml

from ion.idk.config import Config
from ion.idk.metadata import Metadata
from pyon.util.log import log

from ion.idk.exceptions import DriverParameterUndefined
from ion.idk.exceptions import MissingTemplate

class DriverGenerator:
    """
    Generate driver code, tests and directory structure
    """

    ###
    #    Configurations
    ###
    def driver_test_dir(self):
        """
        @brief directory to store the new driver test code
        @retval driver test dir name
        """
        return os.path.join(self.driver_dir(), "test")

    def resource_dir(self):
        """
        @brief directory to store the driver resources
        @retval driver resource dir name
        """
        return os.path.join(self.driver_dir(), "resource")

    def driver_filename(self):
        """
        @brief file name of the new driver
        @retval driver filename
        """
        return "driver.py"

    def driver_make_dir(self):
        """
        @brief full path to the driver make dir
        @retval driver make path
        """
        if not self.metadata.driver_make:
            raise DriverParameterUndefined("driver_make undefined in metadata")
        
        return os.path.join(Config().base_dir(),
                            "mi", "instrument",
                            self.metadata.driver_make.lower())
        
    def driver_model_dir(self):
        """
        @brief full path to the driver model
        @retval driver model path
        """
        if not self.metadata.driver_model:
            raise DriverParameterUndefined("driver_model undefined in metadata")
        
        return os.path.join(self.driver_make_dir(), self.metadata.driver_model.lower())
    
    def driver_dir(self):
        """
        @brief full path to the driver code
        @retval driver path
        """
        if not self.metadata.driver_name:
            raise DriverParameterUndefined("driver_name undefined in metadata")
        
        return os.path.join(self.driver_model_dir(),self.metadata.driver_name.lower())

    def driver_path(self):
        """
        @brief full path and filename to the driver code
        @retval driver path
        """
        return os.path.join(self.driver_dir(), self.driver_filename())


    def driver_test_filename(self):
        """
        @brief file name of the new driver tests
        @retval driver test filename
        """
        return "test_driver.py"

    def driver_test_path(self):
        """
        @brief full-path file name of the new driver tests
        @retval full-path driver test filename
        """
        return os.path.join(self.driver_test_dir(), self.driver_test_filename())

    def template_dir(self): 
        """
        @brief path to the driver template dir
        @retval driver test code template path
        """
        return Config().template_dir()

    def test_template(self):
        """
        @brief path to the driver test code template
        @retval driver test code template path
        """
        return os.path.join(self.template_dir(), "driver_test.tmpl")

    def driver_template(self):
        """
        @brief path to the driver code template
        @retval driver code template path
        """
        return os.path.join(self.template_dir(), "driver.tmpl")

    def test_modulename(self):
        """
        @brief module name of the new driver tests
        @retval driver test module name
        """
        return self.driver_test_path().replace(Config().base_dir() + "/",'').replace('/','.').replace('.py','')


    def driver_modulename(self):
        """
        @brief module name of the new driver tests
        @retval driver test module name
        """
        return self.driver_path().replace(Config().base_dir() + "/",'').replace('/','.').replace('.py','')

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
            raise MissingTemplate(msg="Missing: %s" % template_file)


    def _driver_template_data(self):
        """
        @brief dictionary containing a map of substitutions for the driver code generation
        @retval data mapping for driver generation
        """
        return {
            'driver_module': self.driver_modulename(),
            'file': self.driver_path(),
            'author': self.metadata.author,
            'driver_make': self.metadata.driver_make,
            'driver_model': self.metadata.driver_model,
            'driver_name': self.metadata.driver_name,
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
            'file': self.driver_path(),
            'author': self.metadata.author,
            'driver_make': self.metadata.driver_make,
            'driver_model': self.metadata.driver_model,
            'driver_name': self.metadata.driver_name
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

        self._touch_init(self.driver_make_dir())
        self._touch_init(self.driver_model_dir())
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
        if(os.path.exists(self.driver_path()) and not self.force):
            msg = "Warning: driver exists (" + self.driver_path() + ") not overwriting"
            sys.stderr.write(msg)
            log.warn(msg)
        else:
            log.info("Generate driver code from template %s to file %s" % (self.driver_template(), self.driver_path()))
            
            template = self._get_template(self.driver_template())
            ofile = open( self.driver_path(), 'w' )
            code = template.substitute(self._driver_template_data())
            ofile.write(code)
            ofile.close()


    def generate_test_code(self):
        """
        @brief Generate stub driver test code
        """
        if(os.path.exists(self.driver_test_path()) and not self.force):
            print "Warning: driver test file exists (" + self.driver_test_path() + ") not overwriting"
        else:
            template = self._get_template(self.test_template())
            ofile = open( self.driver_test_path(), 'w' )
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

