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

    # return a valid message from a delete
    def _return_delete(self, success_bool):
        retval = {}
        retval["success"] = success_bool
        return retval

    # return a valid message from an activate
    def _return_activate(self, success_bool):
        retval = {}
        retval["success"] = success_bool
        return retval

    

    
    ##########################################################################
    #
    # PLATFORM AGENT
    #
    ##########################################################################

    def create_platform_agent(self, platform_agent_info=None):
        """
        method docstring
        """
        # Validate the input filter and augment context as required
        self._check_name("PlatformAgent", platform_agent_info.name)

        #FIXME: more validation?

        #persist
        platform_agent_obj = IonObject("PlatformAgent", platform_agent_info)
        platform_agent_id, _ = self.clients.resource_registry.create(platform_agent_obj)

        #start LCS
        self.clients.resource_registry.execute_lifecycle_transition(resource_id=platform_agent_id, 
                                                                    lcstate='ACTIVE')

        return self._return_create("platform_agent_id", platform_agent_id)


    def update_platform_agent(self, platform_agent_id='', platform_agent_info={}):
        """
        method docstring
        """
        platform_agent_obj = self._get_resource("PlatformAgent", platform_agent_id)        

        # Validate the input 
        
        #if the name is being changed, make sure it's not being changed to a duplicate
        self._check_name("PlatformAgent", platform_agent_info.name)

        # update some list of fields
        
        #persist
        self.clients.resource_registry.update(platform_agent_obj)


        return self._return_update(True)

        

    def read_platform_agent(self, platform_agent_id=''):
        """
        method docstring
        """
        return self._return_read("PlatformAgent", "platform_agent_info", platform_agent_id)



    def delete_platform_agent(self, platform_agent_id=''):
        """
        method docstring
        """

        platform_agent_obj = self._get_resource("PlatformAgent", platform_agent_id)        
        
        self.clients.resource_registry.delete(platform_agent_obj)
        
        return self._return_delete(True)

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
        # platform_agent_info_list: []
        #
        raise NotImplementedError()
        pass



    def assign_platform_agent(self, platform_agent_id='', platform_id='', platform_agent_instance={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        pass

    def unassign_platform_agent(self, platform_agent_id='', platform_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        pass





    ##########################################################################
    #
    # PHYSICAL PLATFORM
    #
    ##########################################################################



    def create_platform_device(self, platform_device_info=None):
        """
        method docstring
        """
        # Define YAML/params from
        # Platform metadata draft: https://confluence.oceanobservatories.org/display/CIDev/R2+Resource+Page+for+Platform+Instance

        # Validate the input filter and augment context as required
        self._check_name("PlatformDevice", platform_device_info.name)

        #FIXME: more validation?

        # Create platform resource, set initial state, persist
        platform_device_obj = IonObject("PlatformDevice", platform_device_info)
        platform_device_id, _ = self.clients.resource_registry.create(platform_device_obj)

        #self.clients.resource_registry.execute_lifecycle_transition(resource_id=platform_agent_id, 
        #                                                            lcstate='ACTIVE')

        # Create data product (products?)

        # associate data product with platform
        #_ = self.clients.resource_registry.create_association(org_id, AT.hasExchangeSpace, xs_id)

        return self._return_create("platform_device_id", platform_device_id)



    def update_platform_device(self, platform_id='', platform_device_info=None):
        """
        method docstring
        """

        platform_device_obj = self._get_resource("Platformdevice", platform_device_id)        

        # Validate the input 
        
        #if the name is being changed, make sure it's not being changed to a duplicate
        self._check_name("Platformdevice", platform_device_info.name)

        # update some list of fields
        
        #persist
        self.clients.resource_registry.update(platform_device_obj)

        return self._return_update(True)

    

    def read_platform_device(self, platform_device_id=''):
        """
        method docstring
        """
        return self._return_read("PlatformDevice", "platform_device_info", platform_device_id)


    def delete_platform_device(self, platform_device_id=''):
        """
        method docstring
        """
        platform_device_obj = self._get_resource("PlatformDevice", platform_device_id)        
        
        self.clients.resource_registry.delete(platform_device_obj)
        
        return self._return_delete(True)



    def find_platform_devices(self, filters=None):
        """
        method docstring
        """
        # Return Value
        # ------------
        # platform_device_info_list: []
        #
        raise NotImplementedError()
        pass

    def assign_platform_device(self, platform_id='', platform_device_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        pass

    def unassign_platform_device(self, platform_id='', platform_device_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        pass

    def activate_platform_device(self, platform_device_id=''):
        """
        method docstring
        """
        
        #FIXME: validate somehow

        self.clients.resource_registry.execute_lifecycle_transition(resource_id=platform_device_id, 
                                                                    lcstate='ACTIVE')

        self._return_activate(True)





    
    ##########################################################################
    #
    # LOGICAL PLATFORM
    #
    ##########################################################################


    def create_platform_logical(self, platform_logical_info=None):
        """
        method docstring
        """
        # Define YAML/params from
        # Platform metadata draft: https://confluence.oceanobservatories.org/display/CIDev/R2+Resource+Page+for+Platform+Instance

        # Validate the input filter and augment context as required
        self._check_name("PlatformLogical", platform_logical_info.name)

        #FIXME: more validation?

        # Create platform resource, set initial state, persist
        platform_logical_obj = IonObject("PlatformLogical", platform_logical_info)
        platform_logical_id, _ = self.clients.resource_registry.create(platform_logical_obj)
        self.clients.resource_registry.execute_lifecycle_transition(resource_id=platform_logical_id, 
                                                                    lcstate='ACTIVE')


        # Create data product (products?)

        # associate data product with platform
        #_ = self.clients.resource_registry.create_association(org_id, AT.hasExchangeSpace, xs_id)

        return self._return_create("platform_logical_id", platform_logical_id)



    def update_platform_logical(self, platform_id='', platform_logical_info=None):
        """
        method docstring
        """

        platform_logical_obj = self._get_resource("Platformlogical", platform_logical_id)        

        # Validate the input 
        
        #if the name is being changed, make sure it's not being changed to a duplicate
        self._check_name("Platformlogical", platform_logical_info.name)

        # update some list of fields
        
        #persist
        self.clients.resource_registry.update(platform_logical_obj)

        return self._return_update(True)

    

    def read_platform_logical(self, platform_logical_id=''):
        """
        method docstring
        """
        return self._return_read("PlatformLogical", "platform_logical_info", platform_logical_id)


    def delete_platform_logical(self, platform_logical_id=''):
        """
        method docstring
        """
        platform_logical_obj = self._get_resource("PlatformLogical", platform_logical_id)        
        
        self.clients.resource_registry.delete(platform_logical_obj)
        
        return self._return_delete(True)


    def find_platform_logical(self, filters=None):
        """
        method docstring
        """
        # Return Value
        # ------------
        # platform_logical_info_list: []
        #
        raise NotImplementedError()
        pass



