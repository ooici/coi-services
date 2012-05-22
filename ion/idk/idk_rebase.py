"""
@file coi-services/ion/idk/driver_generator.py
@author Bill French
@brief Main script class for running the reinitializing 
       the IDK environment 
"""

from ion.idk.config import Config
from ion.idk.logger import Log

class IDKRebase():
    """
    Main class for running the rebase process.
    """

    def run(self):
        """
        @brief Run it.
        """
        Log.info( "*** IDK Rebase Process***" )
        Config().rebase()

if __name__ == '__main__':
    app = IDKRebase()
    app.run()
