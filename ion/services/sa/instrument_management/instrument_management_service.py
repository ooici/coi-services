#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


#from pyon.public import Container
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound
from pyon.datastore.datastore import DataStore
#from pyon.net.endpoint import RPCClient
from pyon.util.log import log



from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService

class InstrumentManagementService(BaseInstrumentManagementService):


    
    ##########################################################################
    #
    # INSTRUMENT DRIVER
    #
    ##########################################################################

    def create_instrument_driver(self, instrument_driver_info={}):
        """
        method docstring
        """

        # Validate the input filter and augment context as required
        try:
            find_res = self.clients.resource_registry.find([
                    ("type_", DataStore.EQUAL, "InstrumentDriver"), 
                    DataStore.AND, 
                    ("name", DataStore.EQUAL, name)
                    ])
            driver_info = find_res[0]
        except NotFound:
            # New after all.  PROCEED.
            pass
        else:
            raise BadRequest("An instrument driver named '%s' already exists" % instrument_driver_info[name])

        #FIXME: more validation?


        #persist
        instrument_driver_obj = IonObject("InstrumentDriver", instrument_driver_info)
        create_tuple = self.clients.resource_registry.create(account_obj)
        instrument_driver_id = create_tuple[0]

        # Return a resource ref
        return create_tuple


    def update_instrument_driver(self, instrument_driver_id='', instrument_driver_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_instrument_driver(self, instrument_driver_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_driver_info: {}
        #
        pass

    def delete_instrument_driver(self, instrument_driver_id='', reason=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instrument_driver(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_driver_info_list: []
        #
        pass




    
    ##########################################################################
    #
    # INSTRUMENT MODEL
    #
    ##########################################################################

    def create_instrument(self, instrument_info={}):
        """
        method docstring
        """
        # Validate the input filter and augment context as required

        # Define YAML/params from
        # Instrument metadata draft: https://confluence.oceanobservatories.org/display/CIDev/R2+Resource+Page+for+Instrument+Instance

        # Create instrument resource, set initial state, persist

        # Create associations

        # Return a resource ref


        pass

    def update_instrument(self, instrument_id='', instrument_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_instrument(self, instrument_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_info: {}
        #
        pass

    def delete_instrument(self, instrument_id='', reason=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instrument(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_info_list: []
        #
        pass







    ##########################################################################
    #
    # PHYSICAL INSTRUMENT
    #
    ##########################################################################



    def create_instrument_device(self, instrument_device_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {instrument_device_id: ''}
        #
        pass

    def update_instrument_device(self, instrument_device_id='', instrument_device_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_instrument_device(self, instrument_device_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_device_info: {}
        #
        pass

    def delete_instrument_device(self, instrument_device_id='', reason=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instrument_device(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_device_info_list: []
        #
        pass

    def assign_instrument_device(self, instrument_id='', instrument_device_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_instrument_device(self, instrument_id='', instrument_device_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def activate_instrument_device(self, instrument_device_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass


    def request_direct_access(self, instrument_id=''):
        """
        method docstring
        """
        # Validate request; current instrument state, policy, and other

        # Retrieve and save current instrument settings

        # Request DA channel, save reference

        # Return direct access channel
        pass

    def stop_direct_access(self, instrument_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass



    
    ##########################################################################
    #
    # LOGICAL INSTRUMENT
    #
    ##########################################################################

    def create_instrument(self, instrument_info={}):
        """
        method docstring
        """
        # Validate the input filter and augment context as required

        # Define YAML/params from
        # Instrument metadata draft: https://confluence.oceanobservatories.org/display/CIDev/R2+Resource+Page+for+Instrument+Instance

        # Create instrument resource, set initial state, persist

        # Create associations

        # Return a resource ref
        pass

    def update_instrument(self, instrument_id='', instrument_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_instrument(self, instrument_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_info: {}
        #
        pass

    def delete_instrument(self, instrument_id='', reason=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instrument(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_info_list: []
        #
        pass

    def request_direct_access(self, instrument_id=''):
        """
        method docstring
        """
        # Validate request; current instrument state, policy, and other

        # Retrieve and save current instrument settings

        # Request DA channel, save reference

        # Return direct access channel
        pass

    def stop_direct_access(self, instrument_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass








    ##########################################################################
    #
    # INSTRUMENT AGENT
    #
    ##########################################################################


    def create_instrument_agent(self, instrument_agent={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {instrument_agent_id: ''}
        #
        pass

    def update_instrument_agent(self, instrument_agent_id='', instrument_agent={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_instrument_agent(self, instrument_agent_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_agent: {}
        #
        pass

    def delete_instrument_agent(self, instrument_agent_id='', reason=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instrument_agent(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_agent_info_list: []
        #
        pass

    def assign_instrument_agent(self, instrument_agent_id='', instrument_id='', instrument_agent_instance={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_instrument_agent(self, instrument_agent_id='', instrument_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass



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
