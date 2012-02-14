"""
@file coi-services/ion/idk/nose_test.py
@author Bill French
@brief Helper class to invoke nose tests
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import sys

from ion.processes.idk.metadata import Metadata
from ion.processes.idk.comm_config import CommConfig

class NoseTest():
    """
    Helper class to invoke nose tests for drivers.
    """

    ###
    #   Private Methods
    ###
    def __init__(self, metadata, log_file = None):
        """
        @brief Constructor
        @param metadata IDK Metadata object
        @param log_file File to store test results.  If none specified log to STDOUT
        """
        self.metadata = metadata
        if(not self.metadata.name):
            raise Exception('No drivers initialized.  run start_driver')

        if( log_file ):
            self.log_fh = open(log_file, "w")
        else:
            self.log_fh = sys.stdout

        self.comm_config = CommConfig.get_config_from_type(metadata, 'ethernet')
        if(not self.comm_config):
            raise Exception('No comm config file found!')

        self._output_header()

    def _log(self, message):
        """
        @brief Log a test message either to stdout or a log file.
        @param message message to be outputted
        """
        self.log_fh.write(message + "\n")

    def _output_header(self):
        """
        @brief Output message for everytime we run tests.  It contains config info, metadata and a header
        @param message message to be outputted
        """
        self._log( "****************************************" )
        self._log( "***   Starting Drive Test Process    ***" )
        self._log( "****************************************\n" )

        self._output_metadata()
        self._output_comm_config()

    def _output_metadata(self):
        self._log( "Metadata =>\n\n" + self.metadata.serialize())

    def _output_comm_config(self):
        self._log( "Comm Config =>\n\n" + self.comm_config.serialize())

    ###
    #   Public Methods
    ###
    def run(self):
        """
        @brief Run it.
        """
        self.run_unit()
        self.run_integration()
        self.run_qualification()

    def run_unit(self):
        """
        @brief Run unit tests for a driver
        """
        self._log("*** Starting Unit Tests ***")

    def run_integration(self):
        """
        @brief Run integration tests for a driver
        """
        self._log("*** Starting Integration Tests ***")

    def run_qualification(self):
        """
        @brief Run qualification test for a driver
        """
        self._log("*** Starting Qualification Tests ***")


if __name__ == '__main__':
    metadata = Metadata()
    test = NoseTest(metadata)

    test.run()
