#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService

class InstrumentManagementService(BaseInstrumentManagementService):


    def create_instrument(self, instrument={}):
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

    def update_instrument(self, instrument={}):
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
        # instrument: {}
        #
        pass

    def delete_instrument(self, instrument_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instruments(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_list: []
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

    def request_command(self, instrument_id='', command={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_instrument_device(self, instrument_device={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {instrument_device_id: ''}
        #
        pass

    def update_instrument_device(self, instrument_device={}):
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
        # instrument_device: {}
        #
        pass

    def delete_instrument_device(self, instrument_device_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instrument_devices(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_device_list: []
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

    def create_instrument_agent(self, instrument_agent={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {instrument_agent_id: ''}
        #
        pass

    def update_instrument_agent(self, instrument_agent={}):
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

    def delete_instrument_agent(self, instrument_agent_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instrument_agents(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_agent_list: []
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

    def create_platform(self, platform={}):
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

    def update_platform(self, platform={}):
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

    def find_platforms(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # platform_list: []
        #
        pass

    def create_platform_agent(self, platform_agent={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {platform_agent_id: ''}
        #
        pass

    def update_platform_agent(self, platform_agent={}):
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

    def delete_platform_agent(self, platform_agent_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_platform_agents(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # platform_agent_list: []
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
