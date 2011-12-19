#!/usr/bin/env python

"""
@package  ion.services.sa.resource_worker
@file     ion/services/sa/resource_worker.py
@author   Ian Katz
@brief    A base class for CRUD, LCS, and association ops on any ION resource
"""

from pyon.core.exception import BadRequest, NotFound
#from pyon.core.bootstrap import IonObject
#from pyon.public import AT
from pyon.util.log import log


######
"""
now TODO

 - implement find methods


Later TODO

 -

"""
######


class ResourceWorker(object):

    def __init__(self, clients):
        self.clients = clients

        self.iontype  = self._primary_object_name()
        self.ionlabel = self._primary_boject_label()

        self.RR = self.clients.resource_registry
        self.on_worker_init()

    ##################################################
    #
    #    STUFF THAT SHOULD BE OVERRIDDEN
    #
    ##################################################

    def _primary_object_name(self):
        """
        the IonObject type that this worker controls
        """
        #like "InstrumentAgent" or (better) RT.InstrumentAgent
        return "YOU MUST SET THIS"

    def _primary_object_label(self):
        """
        the argument label that this worker controls
        """
        #like "instrument_agent"
        return "YOU MUST SET THIS"

    def on_worker_init():
        """
        called on initialization of class, after
        parent service's clients have been passed in
        """
        return

    def on_pre_create(self, obj):
        return

    def on_post_create(self, obj_id, obj):
        return

    def on_pre_update(self, obj):
        return

    def on_post_update(self, obj):
        return

    ##################################################
    #
    #   LIFECYCLE TRANSITION ... THIS IS IMPORTANT
    #
    ##################################################

    def advance_lcs(self, resource_id, newstate):
        """
        attempt to advance the lifecycle state of a resource
        @resource_id the resource id
        @newstate the new lifecycle state
        @todo check that this resource is of the same type as this worker class
        """
        necessary_method = "lcs_precondition_" + str(newstate)
        if not hasattr(self, necessary_method):
            raise NotImplementedError(
                "Lifecycle precondition method '%s' not defined for %s!"
                % (newstate, self.iontype))

        #FIXME: make sure that the resource type matches self.iontype

        precondition_fn = getattr(self, necessary_method)

        #call the precondition function and react
        if precondition_fn(self, resource_id):
            log.debug("Moving %s resource to state %s"
                      % (self.iontype, newstate))
            self.RR.execute_lifecycle_transition(resource_id=resource_id,
                                                 lcstate=newstate)
        else:
            raise BadRequest(("Couldn't transition %s to state %s; "
                              + "failed precondition")
                             % (self.iontype, newstate))

    # so, for example if you want to transition to "NEW", you'll need this:
    #
    def lcs_precondition_NEW(self, resource_id):
        return True

    ##################################################
    #
    #    HELPER METHODS
    #
    ###################################################

    def _check_name(self, resource_type, primary_object, verb):
        """
        determine whether a resource with the same type and name already exists
        @param resource_type the IonObject type
        @param primary_object the resource to be checked
        @param verb what will happen to this  object (like "to be created")
        @raises BadRequest if name exists already or wasn't set
        """
        if not hasattr(primary_object, "name"):
            raise BadRequest("The name field was not set in the resource %s"
                             % verb)

        name = primary_object.name
        try:
            found_res, _ = self.RR.find_resources(resource_type,
                                                  None,
                                                  name,
                                                  True)
        except NotFound:
            # New after all.  PROCEED.
            pass
        else:
            if 0 < len(found_res):
                raise BadRequest("%s resource named '%s' already exists"
                                 % (resource_type, name))

    def _get_resource(self, resource_type, resource_id):
        """
        try to get a resource
        @param resource_type the IonObject type
        @param resource_id
        @raises NotFound
        """
        resource = self.RR.read(resource_id)
        if not resource:
            raise NotFound("%s %s does not exist"
                           % (resource_type, resource_id))
        return resource

    def _return_create(self, resource_label, resource_id):
        """
        return a valid response to a create operation
        @param resource_label what goes in the return value name
        @param resource_id what goes in the return value's value
        """
        retval = {}
        retval[resource_label] = resource_id
        return retval

    def _return_update(self, success_bool):
        """
        return a valid response to an update operation
        @param success_bool whether to report success
        """
        retval = {}
        retval["success"] = success_bool
        return retval

    def _return_read(self, resource_type, resource_label, resource_id):
        """
        return a valid response from a read operation
        @param resource_type the IonObject type
        @param resource_label what goes in the return value name
        @param resource_id the ID of the resource to be returned
        """
        retval = {}
        resource = self._get_resource(resource_type, resource_id)
        retval[resource_label] = resource
        return retval

    # return a valid message from a delete
    def _return_delete(self, success_bool):
        """
        return a valid response from a delete operation
        @param success_bool whether to report success
        """
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
        create a single object of the predefined type
        @param primary_object an IonObject resource of the proper type
        @retval the resource ID
        """
        # make sure ID isn't set
        if hasattr(primary_object, "_id"):
            raise BadRequest("ID field present in a create %s operation"
                             % self.iontype)

        # Validate the input filter and augment context as required
        self._check_name(self.iontype, primary_object, "to be created")

        #FIXME: more validation?
        self.on_pre_create(primary_object)

        #persist
        #primary_object_obj = IonObject(self.iontype, primary_object)
        primary_object_id, _ = self.RR.create(primary_object)

        self.on_post_create(primary_object_id, primary_object)

        return self._return_create("%s_id" % self.ionlabel, primary_object_id)


    def update_one(self, primary_object={}):
        """
        update a single object of the predefined type
        @param primary_object the updated resource
        """
        if not hasattr(primary_object, "_id"):
            raise BadRequest("The _id field was not set in the "
                             + "%s resource to be updated" % self.iontype)


        # Validate the input
        self.on_pre_update(primary_object)

        #if the name is being changed, make sure it's not
        # being changed to a duplicate
        self._check_name(self.iontype, primary_object, "to be updated")

        #persist
        self.RR.update(primary_object)

        self.on_post_update(primary_object)

        return self._return_update(True)


    def read_one(self, primary_object_id=''):
        """
        read a single object of the predefined type
        @param primary_object_id the id to be retrieved
        """
        return self._return_read(self.iontype,
                                 self.ionlabel,
                                 primary_object_id)


    def delete_one(self, primary_object_id=''):
        """
        delete a single object of the predefined type AND its history
        (i.e., NOT retiring!)
        @param primary_object_id the id to be deleted
        """

        primary_object_obj = self._get_resource(self.iontype,
                                                primary_object_id)
        self.RR.delete(primary_object_obj)

        return self._return_delete(True)


    def find_some(self, filters={}):
        """
        find method
        @todo receive definition of the filters object
        """
        # Return Value
        # ------------
        # primary_object_list: []
        #
        raise NotImplementedError()


    def _find_having(self, association_predicate, some_object):
        """
        find resources having ____:
          find resource IDs of the predefined type that
          have the given association attached
        @param association_predicate one of the association types
        @param some_object the object "owned" by the association type
        """
        return self.RR.find_subjects(self.iontype,
                                     association_predicate,
                                     some_object,
                                     True)

    def _find_stemming(self, primary_object_id, association_predicate, some_object_type):
        """
        find resources stemming from _____:
          find resource IDs of the given object type that
          are associated with the primary object
        @param primary_object_id the id of the primary object
        @param association_prediate the association type
        @param some_object_type the type of associated object
        """
        return self.RR.find_objects(primary_object_id,
                                    association_predicate,
                                    some_object_type,
                                    True)

    #########################################################
    #
    # ASSOCIATION METHODS
    #
    #########################################################

    def _assn_name(self, association_type):
        """
        return the name of an association type
        @param association_type the association type
        @retval the association name
        """
        return str(association_type)


    def link_resources(self, subject_id='', association_type='', object_id=''):
        """
        create an association
        @param subject_id the resource ID of the predefined type
        @param association_type the predicate
        @param object_id the resource ID of the type to be joined
        @todo check for errors
        """

        associate_success = self.RR.create_association(subject_id,
                                                       association_type,
                                                       object_id)

        log.debug("Create %s Association: %s"
                  % (self._assn_name(association_type),
                     str(associate_success)))
        return associate_success


    def unlink_resources(self, subject_id='', association_type='', object_id=''):
        """
        delete an association
        @param subject_id the resource ID of the predefined type
        @param association_type the predicate
        @param object_id the resource ID of the type to be joined
        @todo check for errors
        """

        assoc = self.RR.get_association(subject=subject_id,
                                        predicate=association_type,
                                        object=object_id)
        dessociate_success = self.RR.delete_association(assoc)

        log.debug("Delete %s Association: %s"
                  % (self._assn_name(association_type),
                     str(dessociate_success)))
        return dessociate_success
