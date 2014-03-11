#!/usr/bin/env python

__author__ = 'Ian Katz, Michael Meisinger'

import re
from ooi import logging
from ooi.logging import log

from pyon.util.containers import get_ion_ts, DotDict
from pyon.core.exception import BadRequest, Inconsistent, NotFound
from pyon.core.registry import getextends
from pyon.ion.resource import LCE, RT, PRED
from pyon.util.config import Config

# Common resource type and association definitions
errc_lookups = None


class EnhancedResourceRegistryClient(object):
    """
    This class provides enhanced resource registry client functionality by wrapping the "real" client.

    Specifically, this class adds more succinct interaction with the resource registry in assign and find operations.

    This class analyzes the allowable resource/predicate relations to allow the following:
     * assigning/unassigning one resource to another and letting this class figure out the allowed predicate
     * assigning and validating that only one subject (or object) association exists
     * finding objects or subjects between two resource types and letting the class figure out the allowed predicate
     * finding a single object or subject and letting the class do the error checking for len(results) == 1
     * all of the above find ops, but with resource_id instead of full resource

    Examples:
     # assigning
     self.assign_instrument_model_to_instrument_agent(instrument_model_id, instrument_agent_id)
     self.assign_one_instrument_model_to_instrument_device(instrument_model_id, instrument_device_id)
     self.assign_instrument_device_to_one_platform_device(instrument_device_id, platform_device_id)
     self.unassign_instrument_model_from_instrument_device(instrument_model_id, instrument_device_id)

     # find objects
     self.find_instrument_models_of_instrument_device(instrument_device_id) # returns list
     self.find_instrument_model_of_instrument_device(instrument_device_id)  # returns IonObject or raises NotFound
     self.find_instrument_devices_by_instrument_model(instrument_model_id)  # returns list
     self.find_instrument_device_by_instrument_model(instrument_model_id)   # returns IonObject or raises NotFound

     # find subjects
     self.find_instrument_model_ids_of_instrument_device(instrument_device_id) # returns list
     self.find_instrument_model_id_of_instrument_device(instrument_device_id)  # returns string or raises NotFound
     self.find_instrument_device_ids_by_instrument_model(instrument_model_id)  # returns list
     self.find_instrument_device_id_by_instrument_model(instrument_model_id)   # returns string or raises NotFound

    Breaking Ambiguity:
     assign/unassign method names can also include "_with_has_model" ("_with_", and the predicate type with underscores)

     find method name can include "_using_has_model" ("_using_", and the predicate type with underscores)
    """

    def __init__(self, rr_client):
        self.id = id(self)
        log.debug("EnhancedResourceRegistryClient init")
        self.RR = rr_client

        global errc_lookups
        if not errc_lookups:
            errc_lookups = self._build_lookups()
        self.__dict__.update(errc_lookups)

        self._cached_dynamics = {}

        self._cached_predicates = {}
        self._cached_predicates = {}
        self._cached_resources = {}
        self._all_cached_resources = {}

        log.debug("done init")

    @classmethod
    def _build_lookups(cls):
        lookup_dict = {}
        log.debug("Generating lookup tables for %s resources and their labels", len(RT))
        lookup_dict["resource_to_label"] = dict([(v, cls._uncamel(v)) for v in RT.values() if type("") == type(v)])
        lookup_dict["label_to_resource"] = dict([(cls._uncamel(v), v) for v in RT.values() if type("") == type(v)])

        log.debug("Generating lookup tables for %s predicates and their labels", len(PRED.values()))
        lookup_dict["predicate_to_label"] = dict([(v, cls._uncamel(v)) for v in PRED.values() if type("") == type(v)])
        lookup_dict["label_to_predicate"] = dict([(cls._uncamel(v), v) for v in PRED.values() if type("") == type(v)])

        log.debug("Generating predicate lookup table")
        lookup_dict["predicates_for_subj_obj"] = cls._build_predicate_list()

        return lookup_dict

    @classmethod
    def _build_predicate_list(cls):
        """
        Create a master dict of dicts of lists in self.predicates_for_subj_obj

        self.predicates_for_subj_obj[RT.SubjectType][RT.ObjectType] = [PRED.typeOfPred1, PRED.typeOfPred2]
        """
        pred_lookup = {}

        # if no extends are found, just return the base type as a list
        def my_getextends(iontype):
            try:
                return getextends(iontype)
            except KeyError:
                return [iontype]

        # read associations yaml and expand all domain/range pairs
        assoc_defs = Config(["res/config/associations.yml"]).data['AssociationDefinitions']
        for ad in assoc_defs:
            predicate = ad['predicate']
            domain = ad['domain']
            range = ad['range']

            for d in domain:
                for ad in my_getextends(d):
                    for r in range:
                        for ar in my_getextends(r):
                            pred_lookup.setdefault(ad, {}).setdefault(ar, set()).add(predicate)

        return pred_lookup

    def __getattr__(self, item):
        """
        anything we can't puzzle out gets passed along to the real RR client
        """
        # don't waste time looking up function names twice
        if item in self._cached_dynamics:
            return self._cached_dynamics[item]

        dynamic_fns = [
            self._make_dynamic_assign_function,   # understand assign_x_x_to_y_y_with_some_predicate(o, s) functions
            self._make_dynamic_assign_single_object_function,   # understand assign_one_x_x_to_y_y_with_some_predicate(o, s) functions
            self._make_dynamic_assign_single_subject_function,   # understand assign_x_x_to_one_y_y_with_some_predicate(o, s) functions
            self._make_dynamic_unassign_function, # understand unassign_x_x_to_y_y_with_some_predicate(o, s) functions
            self._make_dynamic_find_objects_function,  # understand find_x_xs_by_y_y_using_some_predicate(s) functions
            self._make_dynamic_find_subjects_function, # understand find_x_xs_by_y_y_using_some_predicate(o) functions
            self._make_dynamic_find_object_function,   # understand find_x_x_by_y_y_using_some_predicate(s) functions
            self._make_dynamic_find_subject_function,  # understand find_x_x_by_y_y_using_some_predicate(o) functions
            self._make_dynamic_find_object_ids_function,  # understand find_x_x_ids_by_y_y_using_some_predicate(s) functions
            self._make_dynamic_find_subject_ids_function, # understand find_x_x_ids_by_y_y_using_some_predicate(o) functions
            self._make_dynamic_find_object_id_function,   # understand find_x_x_id_by_y_y_using_some_predicate(s) functions
            self._make_dynamic_find_subject_id_function,  # understand find_x_x_id_by_y_y_using_some_predicate(o) functions
        ]

        # try parsing against all the dynamic functions to see if one works
        for gen_fn in dynamic_fns:
            fn = gen_fn(item)
            if fn is None:
                log.trace("dynamic function match fail")
            else:
                log.trace("dynamic function match for %s", item)
                self._cached_dynamics[item] = fn
                return fn

        log.trace("Getting %s attribute from self.RR", item)
        if not hasattr(self.RR, item):
            raise AttributeError(("The method '%s' could not be parsed as a dynamic function and does not exist " +
                                 "in the Resource Registry Client (%s)") % (item, type(self.RR).__name__))
        ret = getattr(self.RR, item)
        log.trace("Got attribute from self.RR: %s", type(ret).__name__)

        self._cached_dynamics[item] = ret
        return ret


    def create(self, resource_obj=None, specific_type=None):
        """
        create a single object of the predefined type
        @param resource_obj an IonObject resource of the proper type
        @param specific_type the name of an Ion type (e.g. RT.Resource)
        @retval the resource ID
        """
        if resource_obj is None:
            resource_obj = {}

        # Validate the input
        self._check_type(resource_obj, specific_type, "to be created")
        self._check_name(resource_obj, "to be created")

        #persist
        #primary_object_obj = IonObject(self.iontype, primary_object)
        resource_id, _ = self.RR.create(resource_obj)

        return resource_id


    def read(self, resource_id='', specific_type=None):
        """
        update a single object of the predefined type
        @param resource_id the id to be deleted
        @param specific_type the name of an Ion type (e.g. RT.Resource)
        """
        if resource_id in self._all_cached_resources:
            resource_obj = self._all_cached_resources[resource_id]
            self._check_type(resource_obj, specific_type, "to be read")
            log.debug("Returning cached %s object", specific_type)
            return resource_obj

        resource_obj = self.RR.read(resource_id)

        self._check_type(resource_obj, specific_type, "to be read")

        if specific_type in self._cached_resources:
            log.debug("Adding cached %s object", specific_type)
            self._add_resource_to_cache(specific_type, resource_obj)

        return resource_obj


    def read_mult(self, resource_ids=None, specific_type=None):
        if resource_ids is None:
            resource_ids = []

        found_resources = [self._all_cached_resources.get(rid, None) for rid in resource_ids]
        missing_resources = [resource_ids[i] for i, robj in enumerate(found_resources) if robj is None]

        if not missing_resources:
            for robj in found_resources:
                self._check_type(robj, specific_type, "to be read")
            return found_resources

        # normal case, check return types
        if not specific_type in self._cached_resources:
            ret = self.RR.read_mult(resource_ids)
            if None is not specific_type:
                if not all([r.type_ == specific_type for r in ret]):
                    raise BadRequest("Expected %s resources from read_mult, but received different type" %
                                     specific_type)
            return ret

        log.debug("Returning cached %s resources", specific_type)
        cache = self._cached_resources[specific_type]

        # fill in any holes that we can
        misses = [x for x in resource_ids if x not in cache.by_id]
        if misses:
            log.debug("Attempting to fill in %s cache misses", len(misses))
            misses_objs = self.RR.read_mult(misses)
            for mo in misses_objs:
                if None is not mo:
                    self._add_resource_to_cache(specific_type, mo)


        return [cache.by_id.get(r, None) for r in resource_ids]


    def update(self, resource_obj=None, specific_type=None):
        """
        update a single object of the predefined type
        @param resource_obj the updated resource
        @param specific_type the name of an Ion type (e.g. RT.Resource)
        """
        if None == resource_obj: resource_obj = {}

        self._check_type(resource_obj, specific_type, "to be updated")

        if not hasattr(resource_obj, "_id") or "" == resource_obj._id:
            raise BadRequest("The _id field was not set in the "
            + "%s resource to be updated" % type(resource_obj).__name__)

        #if the name is being changed, make sure it's not
        # being changed to a duplicate
        self._check_name(resource_obj, "to be updated")

        #persist
        return self.RR.update(resource_obj)


    def retire(self, resource_id='', specific_type=None):
        """
        alias for LCS retire -- the default "delete operation" in ION

        @param resource_id the id to be deleted
        @param specific_type the name of an Ion type (e.g. RT.Resource)
        """

        if None is not specific_type:
            resource_obj = self.RR.read(resource_id)
            self._check_type(resource_obj, specific_type, "to be retired")

        self.RR.retire(resource_id)

        return


    def delete(self, resource_id):

        raise NotImplementedError("TODO: remove me")


    def pluck_delete(self, resource_id='', specific_type=None):
        """
        delete a single object of the predefined type
        AND its history
        AND any associations to/from it
        (i.e., NOT retiring!)
        @param resource_id the id to be deleted
        @param specific_type the name of an Ion type (e.g. RT.Resource)
        """

        #primary_object_obj = self.RR.read(primary_object_id)

        if None is not specific_type:
            resource_obj = self.RR.read(resource_id)
            self._check_type(resource_obj, specific_type, "to be deleted")

        self.pluck(resource_id)

        self.RR.delete(resource_id)


    def delete_association(self, subject_id='', association_type='', object_id=''):
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
        self.RR.delete_association(assoc)


    def find_resource_by_name(self, resource_type, name, id_only=False):
        rsrcs = self.find_resources_by_name(resource_type, name, id_only)

        if 1 == len(rsrcs):
            return rsrcs[0]
        elif 1 < len(rsrcs):
            raise Inconsistent("Expected 1 %s with name '%s', got %d" %
                               (resource_type, name, len(rsrcs)))
        else:
            raise NotFound("Expected 1 %s with name '%s', got %d" %
                           (resource_type, name, len(rsrcs)))


    def find_resources_by_name(self, resource_type, name, id_only=False):
        assert name
        if resource_type not in self._cached_resources:
            log.warn("Using find_resources_by_name on resource type %s, which was not cached", resource_type)
            ret, _ = self.RR.find_resources(restype=resource_type, name=name, id_only=id_only)
            return ret

        if not name in self._cached_resources[resource_type].by_name:
            log.debug("The %s resource with name '%s' was not in the cache", resource_type, name)
            return []

        log.debug("Returning object(s) from cache")
        objs = self._cached_resources[resource_type].by_name[name]
        if id_only:
            return [obj._id for obj in objs]
        else:
            return objs


    def find_subjects(self, subject_type='', predicate='', object='', id_only=False):
        assert subject_type != ''
        assert predicate != ''
        object_id, object_type = self._extract_id_and_type(object)

        if not self.has_cached_predicate(predicate):
            ret, _ = self.RR.find_subjects(subject_type=subject_type,
                                           predicate=predicate,
                                           object=object_id,
                                           id_only=id_only)
            return ret

        log.debug("Using %s cached results for 'find (%s) subjects'", len(self._cached_predicates[predicate]), predicate)

        def filter_fn(assoc):
            if object != assoc.o:
                return False

            if "" != subject_type and subject_type != assoc.st:
                return False

            return True

        log.debug("Checking object_id=%s, subject_type=%s", object_id, subject_type)
        preds = self._cached_predicates[predicate]
        time_search_start = get_ion_ts()
        subject_ids = [a.s for a in self.filter_cached_associations(predicate, filter_fn)]
        time_search_stop = get_ion_ts()
        total_time = int(time_search_stop) - int(time_search_start)
        log.debug("Processed %s %s predicates for %s subjects in %s seconds",
                 len(preds),
                 predicate,
                 len(subject_ids),
                 total_time / 1000.0)


        if id_only:
            return subject_ids
        else:
            log.debug("getting full subject IonObjects with read_mult")
            return self.read_mult(subject_ids, subject_type)


    def find_objects(self, subject, predicate, object_type='', id_only=False):
        subject_id, subject_type = self._extract_id_and_type(subject)

        if not self.has_cached_predicate(predicate):
            ret, _ = self.RR.find_objects(subject=subject_id,
                                         predicate=predicate,
                                         object_type=object_type,
                                         id_only=id_only)
            return ret

        log.debug("Using %s cached results for 'find (%s) objects'", len(self._cached_predicates[predicate]), predicate)

        def filter_fn(assoc):
            if subject_id != assoc.s:
                return False

            if "" != object_type and object_type != assoc.ot:
                return False

            return True

        log.debug("Checking subject_id=%s, object_type=%s", subject_id, object_type)
        preds = self._cached_predicates[predicate]
        time_search_start = get_ion_ts()
        object_ids = [a.o for a in self.filter_cached_associations(predicate, filter_fn)]
        time_search_stop = get_ion_ts()
        total_time = int(time_search_stop) - int(time_search_start)
        log.debug("Processed %s %s predicates for %s objects in %s seconds",
                  len(preds),
                  predicate,
                  len(object_ids),
                  total_time / 1000.0)

        if id_only:
            return object_ids
        else:
            log.debug("getting full object IonObjects with read_mult")
            return self.read_mult(object_ids)


    def find_subject(self, subject_type='', predicate='', object='', id_only=False):
        assert subject_type != ''
        assert predicate != ''
        object_id, object_type = self._extract_id_and_type(object)

        idstring = ""
        if id_only: idstring = " ID"
        findop_name = "Find %s subject%s by %s object using predicate %s" % (subject_type,
                                                                             idstring,
                                                                             object_type,
                                                                             predicate)

        return self._find_subject_(findop_name, subject_type, predicate, object_id, object_type, id_only)

    def _find_subject_(self, findop_name, subject_type, predicate, object_id, object_type, id_only):
        objs  = self.find_subjects(subject_type=subject_type,
                                   predicate=predicate,
                                   object=object_id,
                                   id_only=id_only)


        if 1 == len(objs):
            return objs[0]
        elif 1 < len(objs):
            raise Inconsistent("Expected 1 %s as subject of %s '%s', got %d in '%s'" %
                              (subject_type, object_type, str(object_id), len(objs), findop_name))
        else:
            raise NotFound("Expected 1 %s as subject of %s '%s' in '%s'" %
                           (subject_type, object_type, str(object_id), findop_name))


    def find_object(self, subject, predicate, object_type='', id_only=False):
        subject_id, subject_type = self._extract_id_and_type(subject)

        idstring = ""
        if id_only: idstring = " ID"
        findop_name = "Find %s subject%s by %s object using predicate %s" % (subject_type,
                                                                             idstring,
                                                                             object_type,
                                                                             predicate)

        return self._find_object_(findop_name, subject_id, subject_type, predicate, object_type, id_only)


    def _find_object_(self, findop_name, subject_id, subject_type, predicate, object_type, id_only):

        objs = self.find_objects(subject=subject_id,
                                 predicate=predicate,
                                 object_type=object_type,
                                 id_only=id_only)

        if 1 == len(objs):
            return objs[0]
        elif 1 < len(objs):
            raise Inconsistent("Expected 1 %s as object of %s '%s', got %d in '%s'" %
                              (object_type, subject_type, str(subject_id), len(objs), findop_name))
        else:
            raise NotFound("Expected 1 %s as object of %s '%s' in '%s'" %
                            (object_type, subject_type, str(subject_id), findop_name))


    def delete_object_associations(self, subject_id='', association_type=''):
        """
        delete all assocations of a given type that are attached as objects to the given subject
        """
        log.debug("Deleting all %s object associations from subject with id='%s'",
                  association_type,
                  subject_id)
        associations = self.RR.find_associations(subject=subject_id, predicate=association_type)

        for a in associations:
            self.RR.delete_association(a)


    def delete_subject_associations(self, association_type='', object_id=''):
        """
        delete all assocations of a given type that are attached as subjects to the given object
        """
        log.debug("Deleting all %s associations to object with id='%s'",
                  association_type,
                  object_id)
        associations = self.RR.find_associations(object=object_id, predicate=association_type)

        for a in associations:
            self.RR.delete_association(a)


    def advance_lcs(self, resource_id, transition_event):
        """
        attempt to advance the lifecycle state of a resource
        @resource_id the resource id
        @new_state the new lifecycle state
        """

        assert(type("") == type(resource_id))
        assert(type(LCE.PLAN) == type(transition_event))


        if LCE.RETIRE == transition_event:
            log.debug("Using RR.retire")
            ret = self.RR.retire(resource_id)
            return ret
        else:
            log.debug("Moving resource life cycle with transition event=%s", transition_event)

            ret = self.RR.execute_lifecycle_transition(resource_id=resource_id,
                                                       transition_event=transition_event)

            log.info("lifecycle transition=%s resulted in lifecycle state=%s", transition_event, str(ret))

        return ret


    def cache_predicate(self, predicate):
        """
        Save all associations of a given predicate type to memory, for in-memory find_subjects/objects ops

        This is a PREFETCH operation, and EnhancedResourceRegistryClient objects that use the cache functionality
        should NOT be persisted across service calls.
        """
        #log.debug("Caching predicates: %s", predicate)
        if self.has_cached_predicate(predicate):
            #log.debug("Reusing prior cached predicate %s", predicate)
            return

        time_caching_start = get_ion_ts()
        preds = self.RR.find_associations(predicate=predicate, id_only=False)
        time_caching_stop = get_ion_ts()

        total_time = int(time_caching_stop) - int(time_caching_start)

        log.debug("Cached predicate %s with %s resources in %s seconds", predicate, len(preds), total_time / 1000.0)
        self._cached_predicates[predicate] = preds


    def filter_cached_associations(self, predicate, is_match_fn):
        if not self.has_cached_predicate(predicate):
            raise BadRequest("Attempted to filter cached associations of uncached predicate '%s'" % predicate)

        return [a for a in self._cached_predicates[predicate] if is_match_fn(a)]

    def get_cached_associations(self, predicate):
        return self.filter_cached_associations(predicate, lambda x: True)

    def _add_resource_to_cache(self, resource_type, resource_obj):
        self._cached_resources[resource_type].by_id[resource_obj._id] = resource_obj
        self._cached_resources[resource_type].by_name.setdefault(resource_obj.name, []).append(resource_obj)
        self._all_cached_resources[resource_obj._id] = resource_obj


    def cache_resources(self, resource_type, specific_ids=None):
        """
        Save all resources of a given type to memory, for in-memory lookup ops

        This is a PREFETCH operation, and EnhancedResourceRegistryClient objects that use the cache functionality
        should NOT be kept across service calls.
        """
        #log.info("Caching resources: %s", resource_type)
        #log.debug("This cache is %s", self)
        time_caching_start = get_ion_ts()

        resource_objs = []
        if specific_ids is None:
            resource_objs, _ = self.RR.find_resources(restype=resource_type, id_only=False)
        else:
            assert type(specific_ids) is list
            if specific_ids:
                resource_objs = self.RR.read_mult(specific_ids)

        lookups = DotDict()
        lookups.by_id =   {}
        lookups.by_name = {}
        self._cached_resources[resource_type] = lookups

        for r in resource_objs:
            self._add_resource_to_cache(resource_type, r)

        time_caching_stop = get_ion_ts()

        total_time = int(time_caching_stop) - int(time_caching_start)

        #log.info("Cached %s %s resources in %s seconds", len(resource_objs), resource_type, total_time / 1000.0)


    def has_cached_predicate(self, predicate):
        return predicate in self._cached_predicates


    def has_cached_resource(self, resource_type):
        return resource_type in self._cached_resources


    def clear_cached_predicate(self, predicate=None):
        if None is predicate:
            self._cached_predicates = {}
        elif predicate in self._cached_predicates:
            del self._cached_predicates[predicate]


    def clear_cached_resource(self, resource_type=None):
        if None is resource_type:
            self._cached_resources = {}
            self._all_cached_resources = {}
        elif resource_type in self._cached_resources:
            del self._cached_resources[resource_type]
            del_list = [i for i, o in self._all_cached_resources.iteritems() if o.type_ == resource_type]
            for i in del_list:
                del self._all_cached_resources[i]


    @classmethod
    def _uncamel(cls, name):
        """
        convert CamelCase to camel_case, from http://stackoverflow.com/a/1176023/2063546
        """
        log.trace("name is %s: '%s'" % (type(name).__name__, name))
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


    def _extract_id_and_type(self, id_or_obj):
        """
        figure out whether a subject/object is an IonObject or just an ID
        """
        if hasattr(id_or_obj, "_id"):
            log.debug("find_object for IonObject")
            the_id = id_or_obj._id
            the_type = type(id_or_obj).__name__
        else:
            the_id = id_or_obj
            the_type = "(Unspecified IonObject)"
            if log.isEnabledFor(logging.DEBUG):
                try:
                    the_obj = self.RR.read(the_id)
                    the_type = type(the_obj).__name__
                except:
                    pass


        return the_id, the_type




    def _parse_function_name_for_subj_pred_obj(self, genre, fn_name, regexp, required_fields=None, group_names=None):
        """
        parse a function name into subject/predicate/object, as well as their CamelCase equivalents

        extracts subject, object, and predicate from a function name.  predicate is optional, and if missing
        then this function will attempt to look it up in the list of predicates for the given subject and
        object.  the function raises error messages if the function name is parsed correctly but yields no
        matches in the RT and PRED lists.

        @param genre string, an identifier for what kind of function we're parsing, used for debug messages
        @param fn_name string, the function name coming from getattr
        @param regexp string, the regexp (containing groups) to parse the fn_name
        @param required_fields list, the list of what groups should be "not None" to accept the parse
        @param group_names dict mapping of "subject", "object", and "predicate" to their group names
        """
        if None is group_names: group_names = {}
        if None is required_fields: required_fields = []

        log.trace("Attempting parse %s as %s", fn_name, genre)

        m = re.match(regexp, fn_name)
        if None is m: return None

        for r in required_fields:
            if None is m.group(r): return None

        log.debug("parsed '%s' as %s", fn_name, genre)

        ret = {}
        for name, idx in group_names.iteritems():
            if None is idx:
                ret[name] = None
            else:
                ret[name] = m.group(idx)


        obj  = ret["object"]
        subj = ret["subject"]
        pred = ret["predicate"]

        if not subj in self.label_to_resource:
            log.debug("Attempted to use dynamic %s with unknown subject '%s'", genre, subj)
            return None

        if not obj in self.label_to_resource:
            log.debug("Attempted to use dynamic %s with unknown object '%s'", genre, obj)
            return None

        isubj = self.label_to_resource[subj]
        iobj  = self.label_to_resource[obj]

        # code won't execute because getextends(Resource) puts ALL resources in a domain position
#        if isubj not in self.predicates_for_subj_obj:
#            log.debug("Dynamic %s wanted 1 predicate choice for associating %s to %s, no domain" %
#                      (genre, subj, obj))
#            return None

        if iobj not in self.predicates_for_subj_obj[isubj]:
            log.debug("Dynamic %s wanted 1 predicate choice for associating %s to %s, no range" %
                      (genre, subj, obj))
            return None

        if pred is not None:
            log.debug("supplied pred is %s", pred)
            if not pred in self.label_to_predicate:
                raise BadRequest("Attempted to use dynamic %s between %s and %s with unknown predicate '%s'" %
                                 (genre, isubj, iobj, pred))
                #return None

            ipred = self.label_to_predicate[pred]
            if not ipred in self.predicates_for_subj_obj[isubj][iobj]:
                raise BadRequest("Attempted to use dynamic %s between %s and %s with disallowed predicate '%s'" %
                                 (genre, isubj, ipred, ipred))
        else:
            log.debug("no supplied predicate, picking from choices: %s" % self.predicates_for_subj_obj[isubj][iobj])
            if len(self.predicates_for_subj_obj[isubj][iobj]) != 1:
                raise BadRequest("Dynamic %s wanted 1 predicate choice for associating %s to %s, got %s" %
                               (genre, subj, obj, self.predicates_for_subj_obj[isubj][iobj]))


            ipred = self.predicates_for_subj_obj[isubj][iobj][0]

        ret["RT.object"] = iobj
        ret["RT.subject"] = isubj
        ret["PRED.predicate"] = ipred

        return ret


    def _make_dynamic_assign_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("assign function w/pred",
                                                             item,
                                                             r"(assign_)(\w+)(_to_)(\w+)(_with_)(\w+)",
                                                             [2,3,4,5,6],
                                                             {"subject": 4, "predicate": 6, "object": 2})
        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to create associations %s -> %s -> %s", isubj, ipred, iobj)
        def freeze():
            def ret_fn(obj_id, subj_id):
                log.debug("Dynamically creating association %s -> %s -> %s", isubj, ipred, iobj)
                log.debug("%s -> %s -> %s", subj_id, ipred, obj_id)
                self.RR.create_association(subj_id, ipred, obj_id)

            return ret_fn

        ret = freeze()
        return ret


    def _make_dynamic_assign_single_subject_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("assign single subject function w/pred",
                                                             item,
                                                             r"(assign_)(\w+)(_to_one_)(\w+)(_with_)(\w+)",
                                                             [2,3,4,5,6],
                                                             {"subject": 4, "predicate": 6, "object": 2})
        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to create associations (1)%s -> %s -> %s", isubj, ipred, iobj)
        def freeze():
            def ret_fn(obj_id, subj_id):
                log.debug("Dynamically creating association (1)%s -> %s -> %s", isubj, ipred, iobj)
                log.debug("%s -> %s -> %s", subj_id, ipred, obj_id)
                # see if there are any other objects of this type and pred on this subject
                existing_subjs = self.find_subjects(isubj, ipred, obj_id, id_only=True)

                if len(existing_subjs) > 1:
                    raise Inconsistent("Multiple %s-%s subjects found associated to the same %s object with id='%s'" %
                                       (isubj, ipred, iobj, obj_id))

                if len(existing_subjs) > 0:
                    try:
                        self.RR.get_association(subj_id, ipred, obj_id)
                    except NotFound:
                        raise BadRequest("Attempted to add a second %s-%s association to a %s with id='%s'" %
                                         (isubj, ipred, iobj, obj_id))
                    else:
                        log.debug("Create %s Association (single subject): ALREADY EXISTS", ipred)
                        return

                self.RR.create_association(subj_id, ipred, obj_id)

            return ret_fn

        ret = freeze()
        return ret


    def _make_dynamic_assign_single_object_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("assign single object function w/pred",
                                                             item,
                                                             r"(assign_one_)(\w+)(_to_)(\w+)(_with_)(\w+)",
                                                             [2,3,4,5,6],
                                                             {"subject": 4, "predicate": 6, "object": 2})
        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to create associations %s -> %s -> (1)%s", isubj, ipred, iobj)
        def freeze():
            def ret_fn(obj_id, subj_id):
                log.debug("Dynamically creating association %s -> %s -> (1)%s", isubj, ipred, iobj)
                log.debug("%s -> %s -> %s", subj_id, ipred, obj_id)

                # see if there are any other objects of this type and pred on this subject
                existing_objs = self.find_objects(subj_id, ipred, iobj, id_only=True)

                if len(existing_objs) > 1:
                    raise Inconsistent("Multiple %s-%s objects found with the same %s subject with id='%s'" %
                                       (ipred, iobj, isubj, subj_id))

                if len(existing_objs) > 0:
                    try:
                        log.debug("get_association gives")
                        log.debug(self.RR.get_association(subj_id, ipred, obj_id))
                    except NotFound:
                        raise BadRequest("Attempted to add a second %s-%s association to a %s with id='%s'" %
                                         (ipred, iobj, isubj, subj_id))
                    else:
                        log.debug("Create %s Association (single object): ALREADY EXISTS", ipred)
                        return

                self.RR.create_association(subj_id, ipred, obj_id)

            return ret_fn

        ret = freeze()
        return ret


    def _make_dynamic_unassign_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("unassign function w/pred",
                                                             item,
                                                             r"(unassign_)(\w+)(_from_)(\w+)(_with_)(\w+)",
                                                             [2,3,4,5,6],
                                                             {"subject": 4, "predicate": 6, "object": 2})

        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to delete associations %s -> %s -> %s", isubj, ipred, iobj)
        def freeze():
            def ret_fn(obj_id, subj_id):
                log.debug("Dynamically deleting association %s -> %s -> %s", isubj, ipred, iobj)
                log.debug("%s -> %s -> %s", subj_id, ipred, obj_id)
                self.delete_association(subj_id, ipred, obj_id)

            return ret_fn

        ret = freeze()
        return ret


    def _make_dynamic_find_objects_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("find objects w/pred function",
                                                             item,
                                                             r"(find_)(\w+)(s_of_)(\w+)(_using_)(\w+)",
                                                             [2,3,4,5,6],
                                                             {"subject": 4, "predicate": 6, "object": 2})
        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to find objects %s -> %s -> %s", isubj, ipred, iobj)
        def freeze():
            def ret_fn(subj):
                log.debug("Dynamically finding objects %s -> %s -> %s", isubj, ipred, iobj)
                log.debug("%s -> %s -> %s", subj, ipred, iobj)
                subj_id, _ = self._extract_id_and_type(subj)
                ret = self.find_objects(subject=subj_id, predicate=ipred, object_type=iobj, id_only=False)
                return ret

            return ret_fn

        ret = freeze()
        return ret


    def _make_dynamic_find_subjects_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("find subjects w/pred function",
                                                             item,
                                                             r"(find_)(\w+)(s_by_)(\w+)(_using_)(\w+)",
                                                             [2,3,4,5,6],
                                                             {"subject": 2, "predicate": 6, "object": 4})
        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to find subjects %s <- %s <- %s", iobj, ipred, isubj)
        def freeze():
            def ret_fn(obj):
                log.debug("Dynamically finding subjects %s <- %s <- %s", iobj, ipred, isubj)
                log.debug("%s <- %s <- %s", obj, ipred, isubj)
                obj_id, _ = self._extract_id_and_type(obj)
                ret = self.find_subjects(subject_type=isubj, predicate=ipred, object=obj_id, id_only=False)
                return ret

            return ret_fn

        ret = freeze()
        return ret


    def _make_dynamic_find_object_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("find object w/pred function",
                                                             item,
                                                             r"(find_)(\w+)(_of_)(\w+)(_using_)(\w+)",
                                                             [2,3,4,5,6],
                                                             {"subject": 4, "predicate": 6, "object": 2})

        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to find object %s -> %s -> %s", isubj, ipred, iobj)
        def freeze():
            def ret_fn(subj_id):
                log.debug("Dynamically finding object %s -> %s -> %s", isubj, ipred, iobj)
                log.debug("%s -> %s -> %s", subj_id, ipred, iobj)
                ret = self._find_object_(item, subj_id, isubj, ipred, iobj, False)
                return ret

            return ret_fn

        ret = freeze()
        return ret


    def _make_dynamic_find_subject_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("find subject w/pred function",
                                                             item,
                                                             r"(find_)(\w+)(_by_)(\w+)(_using_)(\w+)",
                                                             [2,3,4,5,6],
                                                             {"subject": 2, "predicate": 6, "object": 4})
        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to find subject %s <- %s <- %s", iobj, ipred, isubj)
        def freeze():
            def ret_fn(obj_id):
                log.debug("Dynamically finding subject %s <- %s <- %s", iobj, ipred, isubj)
                log.debug("%s <- %s <- %s", isubj, ipred, obj_id)
                ret = self._find_subject_(item, isubj, ipred, obj_id, iobj, False)
                return ret

            return ret_fn

        ret = freeze()
        return ret



    def _make_dynamic_find_object_ids_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("find object_ids w/pred function",
                                                             item,
                                                             r"(find_)(\w+)(_ids_of_)(\w+)(_using_)(\w+)",
                                                             [2,3,4,5,6],
                                                             {"subject": 4, "predicate": 6, "object": 2})

        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to find object_ids %s -> %s -> %s", isubj, ipred, iobj)
        def freeze():
            def ret_fn(subj):
                log.debug("Dynamically finding object_ids %s -> %s -> %s", isubj, ipred, iobj)
                log.debug("%s -> %s -> %s", subj, ipred, iobj)
                subj_id, _ = self._extract_id_and_type(subj)
                ret = self.find_objects(subject=subj_id, predicate=ipred, object_type=iobj, id_only=True)
                return ret

            return ret_fn

        ret = freeze()
        return ret


    def _make_dynamic_find_subject_ids_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("find subject_ids w/pred function",
                                                             item,
                                                             r"(find_)(\w+)(_ids_by_)(\w+)(_using_)(\w+)",
                                                             [2,3,4,5,6],
                                                             {"subject": 2, "predicate": 6, "object": 4})
        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to find subject_ids %s <- %s <- %s", iobj, ipred, isubj)
        def freeze():
            def ret_fn(obj):
                log.debug("Dynamically finding subject_ids %s <- %s <- %s", iobj, ipred, isubj)
                log.debug("%s <- %s <- %s", isubj, ipred, obj)
                obj_id, _ = self._extract_id_and_type(obj)
                ret = self.find_subjects(subject_type=isubj, predicate=ipred, object=obj_id, id_only=True)
                return ret

            return ret_fn

        ret = freeze()
        return ret


    def _make_dynamic_find_object_id_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("find object_id w/pred function",
                                                             item,
                                                             r"(find_)(\w+)(_id_of_)(\w+)(_using_)(\w+)?",
                                                             [2,3,4,5,6],
                                                             {"subject": 4, "predicate": 6, "object": 2})

        if None is inputs:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to find object_id %s -> %s -> %s", isubj, ipred, iobj)
        def freeze():
            def ret_fn(subj_id):
                log.debug("Dynamically finding object_id %s -> %s -> %s", isubj, ipred, iobj)
                log.debug("%s -> %s -> %s", subj_id, ipred, iobj)
                ret = self._find_object_(item, subj_id, isubj, ipred, iobj, True)
                return ret

            return ret_fn

        ret = freeze()
        return ret


    def _make_dynamic_find_subject_id_function(self, item):
        inputs = self._parse_function_name_for_subj_pred_obj("find subject_id w/pred function",
                                                             item,
                                                             r"(find_)(\w+)(_id_by_)(\w+)(_using_)(\w+)?",
                                                             [2,3,4,5,6],
                                                             {"subject": 2, "predicate": 6, "object": 4})
        if inputs is None:
            return None

        isubj = inputs["RT.subject"]
        iobj  = inputs["RT.object"]
        ipred = inputs["PRED.predicate"]

        log.debug("Making function to find subject_id %s <- %s <- %s", iobj, ipred, isubj)
        def freeze():
            def ret_fn(obj_id):
                log.debug("Dynamically finding subject_id %s <- %s <- %s", iobj, ipred, isubj)
                log.debug("%s <- %s <- %s", isubj, ipred, obj_id)
                ret = self._find_subject_(item, isubj, ipred, obj_id, iobj, True)
                return ret

            return ret_fn

        ret = freeze()
        return ret


    def _check_type(self, resource_obj, specific_type, verb):
        """
        determine whether the given resource matches the given type (if indeed given)
        @param resource_obj the IonObject resource to be checked
        @param specific_type a string type, or None
        @param verb what will happen to this  object (like "to be created")
        @raises BadRequest if name exists already or wasn't set
        """

        if specific_type is None:
            return

        resource_type = resource_obj.type_
        if resource_type != specific_type:
            raise BadRequest("Expected a %s for the resource %s, but received type %s" %
                            (specific_type, verb, resource_type))



    def _check_name(self, resource_obj, verb):
        """
        determine whether a resource with the same type and name already exists
        @param resource_obj the IonObject resource to be checked
        @param verb what will happen to this  object (like "to be created")
        @raises BadRequest if name exists already or wasn't set
        """

        resource_type = resource_obj.type_

        if not (hasattr(resource_obj, "name") and "" != resource_obj.name):
            raise BadRequest("The name field was not set in the resource %s" % verb)


    def pluck(self, resource_id=''):
        """
        delete all associations to/from a resource
        """

        # find all associations where this is the subject
        _, obj_assns = self.RR.find_objects(subject=resource_id, id_only=True)

        # find all associations where this is the object
        _, sbj_assns = self.RR.find_subjects(object=resource_id, id_only=True)

        log.debug("pluck will remove %s subject associations and %s object associations",
                  len(sbj_assns), len(obj_assns))

        for assn in obj_assns:
            log.debug("pluck deleting object association %s", assn)
            self.RR.delete_association(assn)

        for assn in sbj_assns:
            log.debug("pluck deleting subject association %s", assn)
            self.RR.delete_association(assn)

        debug = False

        if debug:
            # find all associations where this is the subject
            _, obj_assns = self.RR.find_objects(subject=resource_id, id_only=True)

            # find all associations where this is the object
            _, sbj_assns = self.RR.find_subjects(object=resource_id, id_only=True)

            log.debug("post-deletions, pluck found %s subject associations and %s object associations",
                      len(sbj_assns), len(obj_assns))
