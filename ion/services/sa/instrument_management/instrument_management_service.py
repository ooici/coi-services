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
    
    # find whether a resource with the same type and name already exists
    def _check_name(self, resource_type, name):
        try:
            found_res, _ = self.clients.resource_registry.find_resources(resource_type, None, name, True)
        except NotFound:
            # New after all.  PROCEED.
            pass
        else:
            if 0 < len(found_res):
                raise BadRequest("%s resource named '%s' already exists" % (resource_type, name))
        
    # try to get a resource
    def _get_resource(self, resource_type, resource_id):
        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("%s %s does not exist" % (resource_type, resource_id))
        return resource

    # return a valid message from a create
    def _return_create(self, resource_label, resource_id):
        retval = {}
        retval[resource_label] = resource_id
        return retval

    # return a valid message from an update
    def _return_update(self, success_bool):
        retval = {}
        retval["success"] = success_bool
        return retval

    # return a valid message from a read
    def _return_read(self, resource_type, resource_label, resource_id):
        retval = {}
        resource = self._get_resource(resource_type, resource_id)
        retval[resource_label] = resource
        return retval

    
    ##########################################################################
    #
    # INSTRUMENT AGENT
    #
    ##########################################################################

    def create_instrument_agent(self, instrument_agent_info={}):
        """
        method docstring
        """
        # Validate the input filter and augment context as required
        self._check_name("InstrumentAgent", name)

        #FIXME: more validation?

        #persist
        instrument_agent_obj = IonObject("InstrumentAgent", instrument_agent_info)
        instrument_agent_id, _ = self.clients.resource_registry.create(instrument_agent_obj)

        return self._return_create("instrument_agent_id", instrument_agent_id)


    def update_instrument_agent(self, instrument_agent_id='', instrument_agent_info={}):
        """
        method docstring
        """
        instrument_agent_obj = self._get_resource("InstrumentAgent", instrument_agent_id)        

        # Validate the input 
        
        #if the name is being changed, make sure it's not being changed to a duplicate
        self._check_name("InstrumentAgent", instrument_agent_info.name)

        # update some list of fields
        
        #persist
        self.clients.resource_registry.update(instrument_agent_obj)


        return self._return_update(True)

        

    def read_instrument_agent(self, instrument_agent_id=''):
        """
        method docstring
        """

        return self._return_read("InstrumentAgent", "instrument_agent_info", instrument_agent_id)


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
    # INSTRUMENT MODEL
    #
    ##########################################################################

    def create_instrument_model(self, instrument_model_info={}):
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

    def update_instrument_model(self, instrument_id='', instrument_model_info={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_instrument_model(self, instrument_model_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_model_info: {}
        #
        pass

    def delete_instrument_model(self, instrument_model_id='', reason=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instrument_model(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # instrument_model_info_list: []
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





