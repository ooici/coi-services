#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService

class InstrumentManagementService(BaseInstrumentManagementService):


    def define_instrument(self, instrument={}):
        # Validate the input filter and augment context as required

        # Define YAML/params from
        # Instrument metadata draft: https://confluence.oceanobservatories.org/display/CIDev/R2+Resource+Page+for+Instrument+Instance

        # Create instrument resource, set initial state, persist

        # Create associations

        # Return a resource ref
        pass

    def get_instrument(self, resourceId=''):
        # Return Value
        # ------------
        # instrument: {}
        #
        pass

    def activate_instrument(self, resourceId='', agentId=''):
        # Return Value
        # ------------
        # {status: true}
        #
        pass

    def update_instrument(self, instrument={}):
        # Return Value
        # ------------
        # {status: true}
        #
        pass

    def find_instrument(self, filters={}):
        # Return Value
        # ------------
        # results: []
        #
        pass

    def define_platform(self, platform={}):
        # Validate the input filter and augment context as required

        # Define YAML/params from https://confluence.oceanobservatories.org/display/CIDev/R2+Resource+Page+for+Platform
        # Metadata from Steve Foley:
        # Anchor system, weight (water/air), serial, model, power characteristics (battery capacity), comms characteristics (satellite  systems, schedule, GPS), clock stuff), deploy length, data capacity, processing capacity

        #  Create platform resource and set initial state

        # Create associations

        # Return a resource ref
        pass

    def get_platform(self, resourceId=''):
        # Return Value
        # ------------
        # result: {}
        #
        pass

    def find_platform(self, filters={}):
        # Return Value
        # ------------
        # results: []
        #
        pass

    def retire_platform(self, resourceId=''):
        # Return Value
        # ------------
        # {status: true}
        #
        pass

    def request_direct_access(self, resourceId=''):
        # Validate request; current instrument state, policy, and other

        # Retrieve and save current instrument settings

        # Request DA channel, save reference

        # Return direct access channel
        pass

    def request_command(self, resourceId=''):
        # Return Value
        # ------------
        # {status: true}
        #
        pass
  