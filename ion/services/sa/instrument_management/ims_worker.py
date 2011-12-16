#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

from pyon.core.exception import BadRequest, NotFound
#from pyon.core.bootstrap import IonObject

######
"""
now TODO

 - implement find methods


Later TODO

 - 

"""
######




class IMSworker(object):
    
    def __init__(self, clients):
        self.clients = clients

        self.RR    = self.clients.resource_registry
        self.DAMS  = self.clients.data_acquisition_management_service

        self.iontype  = self._primary_object_name()
        self.ionlabel = self._primary_boject_label()


    ##################################################
    #
    #    STUFF THAT SHOULD BE OVERRIDDEN
    #
    ##################################################

    def _primary_object_name(self):
        return "YOU MUST SET THIS" #like InstrumentAgent

    def _primary_object_label(self):
        return "YOU MUST SET THIS" #like instrument_agent

    def _pre_create(self, obj):
        return 

    def _post_create(self, obj_id, obj):
        return 

    def _pre_update(self, obj):
        return
    
    def _post_update(self, obj):
        return
        


    # find whether a resource with the same type and name already exists
    def _check_name(self, resource_type, primary_object, verb):
        if not hasattr(primary_object, "name"):
            raise BadRequest("The name field was not set in the resource %s" % verb)

        name = primary_object.name
        try:
            found_res, _ = self.RR.find_resources(resource_type, None, name, True)
        except NotFound:
            # New after all.  PROCEED.
            pass
        else:
            if 0 < len(found_res):
                raise BadRequest("%s resource named '%s' already exists" % (resource_type, name))
        
    # try to get a resource
    def _get_resource(self, resource_type, resource_id):
        resource = self.RR.read(resource_id)
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
    # CRUD methods
    #
    ##########################################################################

    def create_one(self, primary_object={}):
        """
        method docstring
        """
        # Validate the input filter and augment context as required
        self._check_name(self.iontype, primary_object, "to be created")

        #FIXME: more validation?
        self._pre_create(primary_object)

        #persist
        #primary_object_obj = IonObject(self.iontype, primary_object)
        primary_object_id, _ = self.RR.create(primary_object)

        self._post_create(primary_object_id, primary_object)

        return self._return_create("%s_id" % self.ionlabel, primary_object_id)


    def update_one(self, primary_object={}):
        """
        method docstring
        """
        if not hasattr(primary_object, "_id"):
            raise BadRequest("The _id field was not set in the resource to be updated")

        #primary_object_id = primary_object._id
        #
        #primary_object_obj = self._get_resource(self.iontype, primary_object_id)        

        # Validate the input 
        self._pre_update(primary_object)
        
        #if the name is being changed, make sure it's not being changed to a duplicate
        self._check_name(self.iontype, primary_object, "to be updated")

        #persist
        self.RR.update(primary_object)

        return self._return_update(True)

        

    def read_one(self, primary_object_id=''):
        """
        method docstring
        """
        return self._return_read(self.iontype, self.ionlabel, primary_object_id)



    def delete_one(self, primary_object_id=''):
        """
        method docstring
        """

        primary_object_obj = self._get_resource(self.iontype, primary_object_id)        
        
        self.RR.delete(primary_object_obj)
        
        return self._return_delete(True)

        # Return Value
        # ------------
        # {success: true}
        #
        pass



    def find_some(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # primary_object_list: []
        #
        raise NotImplementedError()
        pass


