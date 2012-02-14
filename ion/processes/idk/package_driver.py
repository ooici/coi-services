"""
@file coi-services/ion/idk/package_driver.py
@author Bill French
@brief Main script class for running the package_driver process
"""

from ion.processes.idk.metadata import *
from ion.processes.idk.nose_test import *

class PackageDriver():

    def __init__(self):
        self.metadata = Metadata()

    def run_qualification_tests(self):
        """
        """
        test = NoseTest(self.metadata)
        test.run()

        return True

    def generate_manifest_file(self):
        """
        """

    def package_driver(self):
        """
        """

    def run(self):
        print( "*** Starting Driver Packaging Process***" )

        if( self.run_qualification_tests() ):
            self.generate_manifest_file()
            self.package_driver()
        else:
            print "Qualification tests have fail!  No package created."


if __name__ == '__main__':
    app = PackageDriver()
    app.run()
