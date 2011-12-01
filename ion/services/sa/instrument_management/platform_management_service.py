#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


#from pyon.public import Container
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound
from pyon.datastore.datastore import DataStore
#from pyon.net.endpoint import RPCClient
from pyon.util.log import log



from interface.services.sa.iplatform_management_service import BasePlatformManagementService

class PlatformManagementService(BasePlatformManagementService):



    ##########################################################################
    #
    # PHYSICAL PLATFORM
    #
    ##########################################################################



    def create_platform_device(self, platform_device_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {platform_device_id: ''}
        #
        pass

    def update_platform_device(self, platform_device_id='', platform_device_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_platform_device(self, platform_device_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # platform_device_info: {}
        #
        pass

    def delete_platform_device(self, platform_device_id='', reason=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_platform_device(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # platform_device_info_list: []
        #
        pass






    ##########################################################################
    #
    # LOGICAL PLATFORM 
    #
    ##########################################################################

    def create_platform(self, platform_info={}):
        """
        method docstring
        """
        # Validate the input filter and augment context as required

        # Define YAML/params from https://confluence.oceanobservatories.org/display/CIDev/R2+Resource+Page+for+Platform
        # Metadata from Steve Foley:
        # Anchor system, weight (water/air), serial, model, power characteristics (battery capacity), comms characteristics (satellite  systems, schedule, GPS), clock stuff), deploy length, data capacity, processing capacity

        #  Create platform resource and set initial state

        # Create associations

        pass

    def update_platform(self, platform_id='', platform_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_platform(self, platform_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # platform: {}
        #
        pass

    def delete_platform(self, platform_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_platform(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # platform_info_list: []
        #
        pass




    ##########################################################################
    #
    # PLATFORM AGENT
    #
    ##########################################################################

    def create_platform_agent(self, platform_agent_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {platform_agent_id: ''}
        #
        pass

    def update_platform_agent(self, platform_agent_id='', platform_agent_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_platform_agent(self, platform_agent_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # platform_agent: {}
        #
        pass

    def delete_platform_agent(self, platform_agent_id='', reason=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_platform_agent(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # platform_agent_info_list: []
        #
        pass

    def assign_platform_agent(self, platform_agent_id='', platform_id='', platform_agent_instance={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_agent(self, platform_agent_id='', platform_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass
