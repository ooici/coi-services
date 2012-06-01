"""
@file coi-services/ion/idk/which_driver.py
@author Bill French
@brief Main script class for running the which_driver process
"""

from ion.idk.metadata import Metadata
from ion.idk.config import Config
from pyon.util.log import log

from ion.idk import prompt

class WhichDriver():
    """
    Main class for running the which driver process.
    """

    def fetch_metadata(self):
        """
        @brief collect metadata from the user
        """
        self.metadata = Metadata()

    def run(self):
        """
        @brief Run it.
        """
        self.fetch_metadata()
        if self.metadata.driver_name:
            print "%s/%s/%s" % (self.metadata.driver_make,
                                self.metadata.driver_model,
                                self.metadata.driver_name)


if __name__ == '__main__':
    app = SwitchDriver()
    app.run()
