"""
@file coi-services/ion/idk/switch_driver.py
@author Bill French
@brief Main script class for running the switch_driver process
"""

from ion.idk.metadata import Metadata
from ion.idk.comm_config import CommConfig
from ion.idk.config import Config
from pyon.util.log import log

from ion.idk import prompt

class SwitchDriver():
    """
    Main class for running the switch driver process.
    """

    def fetch_metadata(self):
        """
        @brief collect metadata from the user
        """
        self.driver_make = prompt.text( 'Driver Make' )
        self.driver_model = prompt.text( 'Driver Model' )
        self.driver_name = prompt.text( 'Driver Name' )
        
        self.metadata = Metadata(self.driver_make, self.driver_model, self.driver_name)

    def fetch_comm_config(self):
        """
        @brief collect connection information for the logger from the user
        """
        config_path = "%s/%s" % (self.metadata.driver_dir(), CommConfig.config_filename())
        self.comm_config = CommConfig.get_config_from_console(config_path)
        self.comm_config.get_from_console()

    def run(self):
        """
        @brief Run it.
        """
        print( "*** Starting Switch Driver Process***" )

        self.fetch_metadata()
        self.fetch_comm_config()
        self.metadata.link_current_metadata()


if __name__ == '__main__':
    app = SwitchDriver()
    app.run()
