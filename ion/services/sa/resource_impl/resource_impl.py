7#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.resource_impl
@file     ion/services/sa/resource_impl/resource_impl.py
@author   Ian Katz
@brief    DRY = Don't Repeat Yourself; base class for CRUD, LCS, and association ops on any ION resource
"""

from pyon.core.exception import BadRequest, NotFound, Inconsistent
#from pyon.core.bootstrap import IonObject
from pyon.public import PRED, RT, LCE
from pyon.util.log import log

import inspect


class ResourceImpl(object):

    def __init__(self, clients):
        self.clients = clients

        self.iontype  = self._primary_object_name()
        self.ionlabel = self._primary_object_label()

        if hasattr(clients, "resource_registry"):
            self.RR = self.clients.resource_registry

        self.lce_precondition = {}

        # by default allow everything
        # args s, r are "self" and "resource"; retval = ok ? "" : "err msg"
        for l in LCE:
            self.add_lce_precondition(l, (lambda r: ""))

        # do implementation-specific stuff
        self.on_impl_init()



    ##################################################
    #
    #    STUFF THAT SHOULD BE OVERRIDDEN
    #
    ##################################################

    def _primary_object_name(self):
        """
        the IonObject type that this impl controls
        """
        #like "InstrumentAgent" or (better) RT.InstrumentAgent
        raise NotImplementedError("Extender of the class must set this!")

    def _primary_object_label(self):
        """
        the argument label that this impl controls
        """
        #like "instrument_agent"
        raise NotImplementedError("Extender of the class must set this!")

    def on_impl_init(self):
        """
        called on initialization of class, after
        parent service's clients have been passed in
        """
        return

    def on_pre_create(self, obj):
        """
        hook to be run before an object is created
        @param obj the object that will become the resource
        """
        return

    def on_post_create(self, obj_id, obj):
        """
        hook to be run after an object is created
        @param obj_id the ID of the new object
        @param obj the object that has become the resource
        """
        return

    def on_pre_update(self, obj):
        """
        hook to be run before an object is updated
        @param obj the object
        """
        return

    def on_post_update(self, obj):
        """
        hook to be run after an object is updated
        @param obj the object
        """
        return

    def on_pre_delete(self, obj_id, obj):
        """
        hook to be run before an object is deleted
        @param obj_id the ID of the object
        @param obj the object
        """
        return

    def on_post_delete(self, obj_id, obj):
        """
        hook to be run after an object is deleted
        @param obj_id the ID of the object
        @param obj the object
        """
        return

    ##################################################
    #
    #   LIFECYCLE TRANSITION ... THIS IS IMPORTANT
    #
    ##################################################

    def advance_lcs(self, resource_id, transition_event):
        """
        attempt to advance the lifecycle state of a resource
        @resource_id the resource id
        @new_state the new lifecycle state
        """

        
        # check that the resource exists
        resource = self.RR.read(resource_id)
        resource_type = self._get_resource_type(resource)

        # check that we've been handed the correct object
        if not resource_type == self.iontype:
            raise BadRequest("Attempted to change lifecycle of a %s in a %s module" %
                             (resource_type, self.iontype))

        
        # check that precondition function exists
        if not transition_event in self.lce_precondition:
            raise BadRequest(
                "%s lifecycle precondition method for event '%s' not defined!"
                % (self.iontype, transition_event))

        precondition_fn = self.lce_precondition[transition_event]

        # check that the precondition is met
        errmsg = precondition_fn(resource_id)
        if not "" == errmsg:
            raise BadRequest(("Couldn't apply '%s' LCS transition to %s '%s'; "
                              + "failed precondition: %s")
                             % (transition_event, self.iontype, resource_id, errmsg))

        log.debug("Moving %s resource life cycle with transition event %s"
                  % (self.iontype, transition_event))

        ret = self.RR.execute_lifecycle_transition(resource_id=resource_id,
                                                   transition_event=transition_event)

        log.debug("Result of lifecycle transition was %s" % str(ret))
        return ret


    def add_lce_precondition(self, transition, precondition_predicate_fn):
        """
        register a precondition predicate function for a lifecycle transition
        @param destination_state the state, defined in pyon/ion/resource.pyx
        @param precondition_predicate_fn takes (self, resource_id) and returns string
                -- empty string means ok, otherwise error indicated by string
        """
        self.lce_precondition[transition] = precondition_predicate_fn


    def use_policy(self, policy_predicate_fn):
        """
        turn a policy-style function (taking resource_id, returning boolean) and have it return strings instead
        """
        def freeze():
            def wrapper(resource_id):
                if policy_predicate_fn(resource_id):
                    return ""
                else:
                    return "%s returned false" % str(policy_predicate_fn)

            return wrapper

        return freeze()

    ##################################################
    #
    #    HELPER METHODS
    #
    ###################################################

    # get the non-impl code that called the impl
    def _toplevel_call(self):

        last_fn = ""
        for frame in inspect.stack():
            if -1 == frame[1].find("impl"):
                return frame[3] + "->" + last_fn
            last_fn = frame[3]

        return "?????"

    def _check_name(self, resource_type, primary_object, verb):
        """
        determine whether a resource with the same type and name already exists
        @param resource_type the IonObject type
        @param primary_object the resource to be checked
        @param verb what will happen to this  object (like "to be created")
        @raises BadRequest if name exists already or wasn't set
        """
        if not (hasattr(primary_object, "name") and "" != primary_object.name):
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
            # should never be more than one with a given name
            if 1 < len(found_res):
                raise Inconsistent("Multiple %s resources with name '%s' exist" % (resource_type, name))

            # if creating
            if not hasattr(primary_object, "_id"):
                # must not be any matching names
                if 0 < len(found_res): 
                    raise BadRequest("%s resource named '%s' already exists with ID '%s'"
                                     % (resource_type, name, found_res[0]))
            else: #updating
            # any existing name must match the id
                if 1 == len(found_res) and primary_object._id != found_res[0]:
                    raise BadRequest("%s resource named '%s' already exists with a different ID"
                                     % (resource_type, name))
                    
                           


    def _get_resource_type(self, resource):
        """
        get the type of a resource... simple wrapper
        @param resource a resource
        """
        restype = type(resource).__name__

        return restype


    def _get_resource_type_by_id(self, resource_id):
        """
        get the type of a resource by id
        @param resource_id a resource id
        """
        assert(type("") == type(resource_id))
        return self._get_resource_type(self.RR.read(resource_id))


    def _return_create(self, resource_id):
        """
        return a valid response to a create operation
        @param resource_label what goes in the return value name
        @param resource_id what goes in the return value's value
        """
        #resource_label = "%s_id" % self.ionlabel
        return resource_id

    def _return_read(self, resource_id):
        """
        return a valid response from a read operation
        @param resource_type the IonObject type
        @param resource_label what goes in the return value name
        @param resource_id the ID of the resource to be returned
        """
        #resource_label = "%s_id" % self.ionlabel
        resource = self.RR.read(resource_id)
        return resource

    def _return_find(self, resource_ids):
        """
        return a valid response from a read operation
        @param resource_type the IonObject type
        @param resource_label what goes in the return value name
        @param resource_id the ID of the resource to be returned
        """
        #retval["%s_list" % self.resource_label] = resource_ids
        return resource_ids

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

        # Validate the input filter and augment context as required
        self._check_name(self.iontype, primary_object, "to be created")

        #FIXME: more validation?
        self.on_pre_create(primary_object)

        #persist
        #primary_object_obj = IonObject(self.iontype, primary_object)
        primary_object_id, _ = self.RR.create(primary_object)

        self.on_post_create(primary_object_id, primary_object)

        return self._return_create(primary_object_id)


    def update_one(self, primary_object={}):
        """
        update a single object of the predefined type
        @param primary_object the updated resource
        """
        if not hasattr(primary_object, "_id") or "" == primary_object._id:
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

        return


    def read_one(self, primary_object_id=''):
        """
        read a single object of the predefined type
        @param primary_object_id the id to be retrieved
        """
        return self._return_read(primary_object_id)

    def delete_one(self, primary_object_id=''):
        """
        delete a single object of the predefined type AND its history
        (i.e., NOT retiring!)
        @param primary_object_id the id to be deleted
        """

        primary_object_obj = self.RR.read(primary_object_id)

        self.on_pre_delete(primary_object_id, primary_object_obj)
        
        self.RR.delete(primary_object_id)

        self.on_post_delete(primary_object_id, primary_object_obj)

        return


    def find_some(self, filters={}):
        """
        find method
        @todo receive definition of the filters object
        """
        results, _ = self.RR.find_resources(self.iontype, None, None, False)
        return self._return_find(results)


    def _find_having(self, association_predicate, some_object):
        """
        find resources having ____:
          find resource IDs of the predefined type that
          have the given association attached
        @param association_predicate one of the association types
        @param some_object the object "owned" by the association type
        """
        #log.debug("_find_having, from %s" % self._toplevel_call())
        ret, _ = self.RR.find_subjects(self.iontype,
                                       association_predicate,
                                       some_object,
                                       False)
        return ret

    def _find_stemming(self, primary_object_id, association_predicate, some_object_type):
        """
        find resources stemming from _____:
          find resource IDs of the given object type that
          are associated with the primary object
        @param primary_object_id the id of the primary object
        @param association_prediate the association type
        @param some_object_type the type of associated object
        """
        #log.debug("_find_stemming, from %s" % self._toplevel_call())
        ret, _ = self.RR.find_objects(primary_object_id,
                                      association_predicate,
                                      some_object_type,
                                      False)
        return ret


    def _find_having_single(self, association_predicate, some_object):
        """
        enforces exclusivity: 0 or 1 association allowed
        """
        ret = self._find_having(association_predicate, some_object)

        if 1 < len(ret):
            raise Inconsistent("More than one %s point to %s '%s'" % (association_predicate,
                                                                      self.iontype,
                                                                      some_object))
        return ret

    def _find_stemming_single(self, primary_object_id, association_predicate, some_object_type):
        """
        enforces exclusivity: 0 or 1 association allowed
        """
        
        ret = self._find_stemming(primary_object_id, association_predicate, some_object_type)

        if 1 < len(ret):
            raise Inconsistent("%s '%s' has more than one %s:" % (self.iontype,
                                                                  primary_object_id,
                                                                  association_predicate,
                                                                  some_object_type))
        return ret



    def find_having_attachment(self, attachment_id):
        """
        find resource having the specified attachment
        @param attachment_id the id of the attachment
        """
        return self._find_having(PRED.hasAttachment, attachment_id)

    def find_stemming_attachment(self, resource_id):
        """
        find attachments attached to the specified resource
        @param resource_id the id of the resource
        """
        return self._find_stemming(resource_id, PRED.hasAttachment, RT.Attachment)
        

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

    def _resource_link_exists(self, subject_id='', association_type='', object_id=''):
        return 0 < len(self.RR.find_associations(subject_id,  association_type,  object_id, id_only=True))
   
    def _link_resources_lowlevel(self, subject_id='', association_type='', object_id='', check_duplicates=True):
        """
        create an association
        @param subject_id the resource ID of the predefined type
        @param association_type the predicate
        @param object_id the resource ID of the type to be joined
        @param check_duplicates whether to check for an existing association of this exact subj-pred-obj
        @todo check for errors: does RR check for bogus ids?
        """

        assert(type("") == type(subject_id) == type(object_id))

        if check_duplicates:
            if self._resource_link_exists(subject_id, association_type, object_id):
                log.debug("Create %s Association from '%s': ALREADY EXISTS"
                          % (self._assn_name(association_type),
                             self._toplevel_call()))
                return None

        associate_success = self.RR.create_association(subject_id,
                                                       association_type,
                                                       object_id)

        log.debug("Create %s Association from '%s': %s"
                  % (self._assn_name(association_type),
                     self._toplevel_call(),
                      str(associate_success)))
        return associate_success
        
        
        
        
    def _link_resources(self, subject_id='', association_type='', object_id=''):
        # just link, and check duplicates
        self._link_resources_lowlevel(subject_id, association_type, object_id, True)

    def _link_resources_single_object(self, subject_id='', association_type='', object_id='', raise_exn=True):
        """
        create an association where only one object at a time can exist
         if there is an existing association, the choice is left to the user whether to raise exception
         or quietly remove/replace the existing one.

        @param subject_id the resource ID of the predefined type
        @param association_type the predicate
        @param object_id the resource ID of the type to be joined
        @param raise_exn whether a BadRequest error should be raised if a duplicate is attempted
        @todo check for errors: does RR check for bogus ids?
        """

        # see if there are any other objects of this type and pred on this subject
        obj_type = self._get_resource_type_by_id(object_id)
        existing_links = self._find_stemming(subject_id, association_type, obj_type)
        
        if len(existing_links) > 1:
            raise Inconsistent("Multiple %s-%s objects found on the same %s subject with id='%s'" %
                               (association_type, obj_type, self.iontype, subject_id))

        if len(existing_links) > 0:
            if self._resource_link_exists(subject_id, association_type, object_id):
                log.debug("Create %s Association (single object) from '%s': ALREADY EXISTS"
                          % (self._assn_name(association_type),
                             self._toplevel_call()))
                return None

            if raise_exn:
                raise BadRequest("Attempted to add a duplicate %s-%s association to a %s with id='%s'" %
                                 (association_type, obj_type, self.iontype, subject_id))
            
            self.unlink_all_objects_by_type(self, subject_id, association_type)

        return self._link_resources_lowlevel(subject_id, association_type, object_id, False)

 
    def _link_resources_single_subject(self, subject_id='', association_type='', object_id='', raise_exn=True):
        """
        create an association where only one subject at a time can exist
         if there is an existing association, the choice is left to the user whether to raise exception
         or quietly remove/replace the existing one.

        @param subject_id the resource ID of the predefined type
        @param association_type the predicate
        @param object_id the resource ID of the type to be joined
        @param raise_exn whether a BadRequest error should be raised if a duplicate is attempted
        @todo check for errors: does RR check for bogus ids?
        """

        # see if there are any other objects of this type and pred on this subject
        obj_type = self._get_resource_type_by_id(object_id)
        existing_links = self._find_having(association_type, object_id)
        
        if len(existing_links) > 1:
            raise Inconsistent("Multiple %s-%s subjects found on the same %s object with id='%s'" %
                               (self.iontype, association_type, obj_type, object_id))

        if len(existing_links) > 0:
            if self._resource_link_exists(subject_id, association_type, object_id):
                log.debug("Create %s Association (single subject) from '%s': ALREADY EXISTS"
                          % (self._assn_name(association_type),
                             self._toplevel_call()))
                return None

            if raise_exn:
                raise BadRequest("Attempted to add a duplicate %s-%s association on a %s object with id='%s'" %
                                 (self.iontype, association_type, obj_type, subject_id))

            self._unlink_resources(self, subject_id, association_type, existing_links[0])


        return self._link_resources_lowlevel(subject_id, association_type, object_id, False)


    def _unlink_resources(self, subject_id='', association_type='', object_id=''):
        """
        delete an association
        @param subject_id the resource ID of the predefined type
        @param association_type the predicate
        @param object_id the resource ID of the type to be joined
        @todo check for errors
        """

        assert(type("") == type(subject_id) == type(object_id))

        assoc = self.RR.get_association(subject=subject_id,
                                        predicate=association_type,
                                        object=object_id)
        dessociate_success = self.RR.delete_association(assoc)

        log.debug("Delete %s Association through %s: %s"
                  % (self._assn_name(association_type),
                     self._toplevel_call(),
                     str(dessociate_success)))
        return dessociate_success

    def _unlink_all_objects_by_association_type(self, subject_id='', association_type=''):
        """
        delete all assocations of a given type
        """
        log.debug("Deleting all %s object associations from subject with id='%s'" % 
                  (association_type, subject_id))
        associations = self.RR.find_associations(subject=subject_id, predicate=association_type)
        
        for a in associations:
            self.RR.delete_association(a)

        
    def _unlink_all_subjects_by_assocation_type(self, association_type='', object_id=''):
        """
        delete all assocations of a given type
        """
        log.debug("Deleting all %s associations to object with id='%s'" % 
                  (association_type, object_id))
        associations = self.RR.find_associations(object=object_id, predicate=association_type)
        
        for a in associations:
            self.RR.delete_association(a)

        


    def link_attachment(self, resource_id='', attachment_id=''):
        """
        associate an attachment with a resource.  any resource can have one
        @param resource_id a resource id
        @param attachment_id a resource id
        """
        return self._link_resources(resource_id, PRED.hasAttachment, attachment_id)


    def unlink_attachment(self, resource_id='', attachment_id=''):
        """
        dissociate an attachment with a resource. 
        @param resource_id a resource id
        @param attachment_id a resource id
        """
        return self._unlink_resources(resource_id, PRED.hasAttachment, attachment_id)

