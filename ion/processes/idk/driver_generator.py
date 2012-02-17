#!/usr/bin/env python

"""
@file coi-services/ion/processes/idk/driver_generator.py
@author Bill French
@brief Generate directory structure and code stubs for a driver
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'


import os
import re
from string import Template

import yaml

from ion.processes.idk.metadata import Metadata


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
        return os.environ['HOME'] + "/Workspace/code/wfrench"

    def template_dir(self):
        """
        @brief directory where code templates are stored
        @retval template dir name
        """
        return self.base_dir() + "/coi-services/ion/processes/idk/templates"

    def driver_dir(self):
        """
        @brief directory to store the new driver code
        @retval driver dir name
        """
        return self.base_dir() + "/coi-services/ion/services/mi/drivers/" + self.metadata.name

    def test_dir(self):
        """
        @brief directory to store the new driver test code
        @retval driver test dir name
        """
        return self.driver_dir() + "/test"

    def resource_dir(self):
        """
        @brief directory to store the driver resources
        @retval driver resource dir name
        """
        return self.driver_dir() + "/resource"

    def driver_filename(self):
        """
        @brief file name of the new driver
        @retval driver filename
        """
        # TODO: add code version drivers
        return self.metadata.name + "_driver.py"

    def driver_path(self):
        """
        @brief full path to the driver code
        @retval driver path
        """
        return self.driver_dir() + "/" + self.driver_filename()

    def driver_relative_path(self):
        """
        @brief relative path in the code base.
        @retval relative driver path
        """
        path = self.driver_path() + "/" + self.driver_filename()
        return path.replace(self.base_dir() + '/coi-services/', '')

    def test_filename(self):
        """
        @brief file name of the new driver tests
        @retval driver test filename
        """
        # TODO: add code version drivers
        return self.metadata.name + "_driver_test.py"

    def test_template(self):
        """
        @brief path to the driver test code template
        @retval driver test code template path
        """
        return self.template_dir() + "/driver_test.tmpl"

    def driver_template(self):
        """
        @brief path to the driver code template
        @retval driver code template path
        """
        return self.template_dir() + "/driver.tmpl"

    def test_modulename(self):
        """
        @brief module name of the new driver tests
        @retval driver test module name
        """
        test_file = self.test_dir() + "/" + self.test_filename()
        module_name = test_file.replace(self.base_dir() + '/coi-services/', '')
        module_name = module_name.replace('/', '.')
        module_name = module_name.replace('.py', '')

        return module_name


    def driver_modulename(self):
        """
        @brief module name of the new driver tests
        @retval driver test module name
        """
        driver_file = self.driver_dir() + "/" + self.driver_filename()
        module_name = driver_file.replace(self.base_dir() + '/coi-services/', '')
        module_name = module_name.replace('/', '.')
        module_name = module_name.replace('.py', '')

        return module_name


    ###
    #   Private Methods
    ###
    def __init__(self, metadata):
        """
        @brief Constructor
        @param metadata IDK Metadata object
        """
        self.metadata = metadata

    def _touch_init(self, dir):
        """
        @brief touch a python __init.py__ file if it doesn't exist
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
        infile = open(template_file)
        tmpl_str = infile.read()

        return Template(tmpl_str)


    def _driver_template_data(self):
        """
        @brief dictionary containing a map of substitutions for the driver code generation
        @retval data mapping for driver generation
        """
        return {
            'driver_module': self.driver_modulename(),
            'driver_class': self.metadata.driver_class,
            'file': self.driver_relative_path(),
            'author': self.metadata.author,
            'driver_name': self.metadata.name
        }


    def _test_template_data(self):
        """
        @brief dictionary containing a map of substitutions for the driver test code generation
        @retval data mapping for driver test generation
        """
        return {
            'test_module': self.test_modulename(),
            'driver_module': self.driver_modulename(),
            'driver_class': self.metadata.driver_class,
            'file': self.driver_relative_path(),
            'author': self.metadata.author,
            'driver_name': self.metadata.name
        }


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

        if not os.path.exists(self.test_dir()):
            os.makedirs(self.test_dir())

        if not os.path.exists(self.resource_dir()):
            os.makedirs(self.resource_dir())

        self._touch_init(self.driver_dir())
        self._touch_init(self.test_dir())


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
        template = self._get_template(self.driver_template())
        ofile = open( self.driver_dir() + "/" + self.driver_filename(), 'w' )
        code = template.substitute(self._driver_template_data())
        ofile.write(code)
        ofile.close()


    def generate_test_code(self):
        """
        @brief Generate stub driver test code
        """
        template = self._get_template(self.test_template())
        ofile = open( self.test_dir() + "/" + self.test_filename(), 'w' )
        code = template.substitute(self._test_template_data())
        ofile.write(code)
        ofile.close()


    def display_report(self):
        """
        @brief Display a report of the files created to STDOUT
        """
        print( "*** Generation Complete ***" )
        print(" - Driver File: " + self.driver_dir() + "/" + self.driver_filename())
        print(" - Test File: " + self.test_dir() + "/" + self.test_filename())
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

