#!/usr/bin/env python

"""
@file coi-services/ion/processes/idk/driver_generator.py
@author Bill French
@brief Generate directory structure and code stubs for a driver
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'


import os

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
        return os.environ['HOME'] + "/Workspace/code/testing"

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

    def test_filename(self):
        """
        @brief file name of the new driver
        @retval driver test filename
        """
        # TODO: add code version drivers
        return self.metadata.name + "_driver_test.py"


    ###
    #   Private Methods
    ###
    def __init__(self, metadata):
        """
        @brief Constructor
        @param metadata IDK Metadata object
        """
        self.metadata = metadata


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


    def generate_code(self):
        """
        @brief Generate code files for the driver and tests
        """
        # TODO: Add code generation instead of just touching files.
        print( " -- Generating code --" )
        driver_infile = open( self.driver_dir() + "/" + self.driver_filename(), 'w' )
        test_infile = open( self.test_dir() + "/" + self.test_filename(), 'w' )

        driver_infile.close()
        test_infile.close()


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
    metadata = Metadata( name = 'sbe37', author = 'foo', email = 'goo', notes = 'foosd' );
    driver = DriverGenerator( metadata )

    driver.generate()

