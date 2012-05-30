"""
@file coi-services/ion/idk/nose_test.py
@author Bill French
@brief Helper class to invoke nose tests
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import sys

import nose

from ion.idk.metadata import Metadata
from ion.idk.comm_config import CommConfig
from ion.idk.driver_generator import DriverGenerator

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
        if(not self.metadata.driver_name):
            raise Exception('No drivers initialized.  run start_driver')

        if( log_file ):
            self.log_fh = open(log_file, "w")
        else:
            self.log_fh = sys.stdout

        self.comm_config = CommConfig.get_config_from_type(metadata, 'ethernet')
        if(not self.comm_config):
            raise Exception('No comm config file found!')

        self.test_runner = nose.core.TextTestRunner(stream=self.log_fh)

    def __del__(self):
        """
        @brief Destructor.  If stdout has been reassigned then we need to reset it back.  It appears the nose.core.TextTestRunner
        takes STDOUT from us which seems weird (a bug?)
        """
        sys.stdout = sys.__stdout__

    def _log(self, message):
        """
        @brief Log a test message either to stdout or a log file.
        @param message message to be outputted
        """
        self.log_fh.write(message + "\n")

    def _output_metadata(self):
        self._log( "Metadata =>\n\n" + self.metadata.serialize())

    def _output_comm_config(self):
        self._log( "Comm Config =>\n\n" + self.comm_config.serialize())

    def _driver_test_module(self):
        generator = DriverGenerator(self.metadata)
        return generator.test_modulename()

    def _qualification_test_module(self):
        return self._driver_test_module()

    def _unit_test_class(self):
        return 'UnitFromIDK'

    def _int_test_class(self):
        return 'IntFromIDK'

    def _qual_test_class(self):
        return 'QualFromIDK'

    ###
    #   Public Methods
    ###
    def run(self):
        """
        @brief Run all tests
        @retval False if any test has failed, True if all successful
        """
        self.report_header()
        if(not self.run_unit()):
            self._log( "\n\n!!!! ERROR: Unit Tests Failed !!!!")
            return False
        elif(not self.run_integration()):
            self._log( "\n\n!!!! ERROR: Integration Tests Failed !!!!")
            return False
        elif(not self.run_qualification()):
            self._log( "\n\n!!!! ERROR: Qualification Tests Failed !!!!")
            return False
        else:
            self._log( "\n\nAll tests have passed!")
            return True

    def report_header(self):
        """
        @brief Output report header containing system information.  i.e. metadata stored, comm config, etc.
        @param message message to be outputted
        """
        self._log( "****************************************" )
        self._log( "***   Starting Drive Test Process    ***" )
        self._log( "****************************************\n" )

        self._output_metadata()
        self._output_comm_config()

    def run_unit(self):
        """
        @brief Run unit tests for a driver
        """
        self._log("*** Starting Unit Tests ***")
        self._log(" ==> module: " + self._driver_test_module())
        args=[ sys.argv[0], '-s', '-v', '-a', 'UNIT']
        module = "%s:%s" % (self._driver_test_module(), self._unit_test_class())

        return nose.run(defaultTest=module, testRunner=self.test_runner, argv=args, exit=False)

    def run_integration(self):
        """
        @brief Run integration tests for a driver
        """
        self._log("*** Starting Integration Tests ***")
        self._log(" ==> module: " + self._driver_test_module())
        args=[ sys.argv[0], '-s', '-v', '-a', 'INT']
        module = "%s:%s" % (self._driver_test_module(), self._int_test_class())

        return nose.run(defaultTest=module, testRunner=self.test_runner, argv=args, exit=False)

    def run_qualification(self):
        """
        @brief Run qualification test for a driver
        """
        self._log("*** Starting Qualification Tests ***")
        self._log(" ==> module: " + self._qualification_test_module())
        args=[ sys.argv[0], '-s', '-v', '-a', 'QUAL', '-v' ]
        module = "%s:%s" % (self._qualification_test_module(), self._qual_test_class())

        return nose.run(defaultTest=module, testRunner=self.test_runner, argv=args, exit=False)


if __name__ == '__main__':
    metadata = Metadata()
    test = NoseTest(metadata)

    test.run()
