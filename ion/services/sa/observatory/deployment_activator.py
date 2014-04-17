#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.deployment_activator
@author   Ian Katz
"""
import copy

from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.core.exception import BadRequest, NotFound
from pyon.ion.resource import PRED, RT, OT
from ion.core.ooiref import OOIReferenceDesignator

from ooi.logging import log
from pyon.util.containers import get_ion_ts
from ion.services.sa.observatory.observatory_util import ObservatoryUtil

import constraint


class DeploymentResourceCollector(object):
    """
    A deployment resource collector pulls in all devices, sites, and models related to a deployment.

    its primary purpose is to collect( ) after which you'll be able to access what it collected.

    various lookup tables exist to prevent too many hits on the resource registry.
    """

    def __init__(self, clients, RR2=None):
        """
        @param clients dict of clients from a service
        @param deployment_obj the deployment to activate
        @param allow_children whether to allow children of a device to be provided
        @param include_children whether to search for child devices from root device
        @param RR2 a reference to an enhanced RR client
        """

        self.clients = clients
        self.enhanced_rr = RR2

        if None is self.enhanced_rr:
            self.enhanced_rr = EnhancedResourceRegistryClient(self.clients.resource_registry)

        if not isinstance(self.enhanced_rr, EnhancedResourceRegistryClient):
            raise AssertionError("Type of self.enhanced_rr is %s not %s" %
                                 (type(self.enhanced_rr), type(EnhancedResourceRegistryClient)))

        self.on_init()


    def on_init(self):
        # return stuff
        self._device_models = {}
        self._site_models   = {}
        self._device_tree   = {}
        self._site_tree     = {}

        # cache for things that RR2 can't do well without a resource type
        self._model_lookup = {}
        self._type_lookup  = {}
        self._typecache_hits  = 0
        self._typecache_miss  = 0
        self._modelcache_hits = 0
        self._modelcache_miss = 0


    # functions to return the result of the collect( ) operation
    def collected_device_ids(self):
        return self._device_models.keys()

    def collected_site_ids(self):
        return self._site_models.keys()

    def collected_models_by_site(self):
        return copy.copy(self._site_models)

    def collected_models_by_device(self):
        return copy.copy(self._device_models)

    def collected_site_tree(self):
        return copy.copy(self._site_tree)

    def collected_device_tree(self):
        return copy.copy(self._device_tree)


    def typecache_add(self, resource_id, resource_type):
        """
        add an entry to the internal cache of resource types by id
        """
        assert resource_type in RT
        log.trace("typecache_add type %s", resource_type)
        self._type_lookup[resource_id] = resource_type

    def read_using_typecache(self, resource_id):
        """
        RR2.read needs a type to do a cache lookup... so keep a cache of object types, because
        sometimes we don't know if an ID is for a platform or instrument model/device/site
        """
        if resource_id in self._type_lookup:
            self._typecache_hits += 1
            log.trace("Typeache HIT/miss = %s / %s", self._typecache_hits, self._typecache_miss)
            return self.enhanced_rr.read(resource_id, self._type_lookup[resource_id])

        self._typecache_miss += 1
        log.trace("Typeache hit/MISS = %s / %s", self._typecache_hits, self._typecache_miss)
        rsrc_obj = self.enhanced_rr.read(resource_id)
        self.typecache_add(resource_id, rsrc_obj._get_type())
        return rsrc_obj


    def get_resource_type(self, resource_id):
        """
        get the type of a resource.  faster than an RR op because we cache.
        """
        return self.read_using_typecache(resource_id)._get_type()


    def find_models_fromcache(self, resource_id):
        """
        Find any models assiciatiated with a device/site id.  use a local cache

        returns a list OR a string, depending on resource type
        """
        if resource_id in self._model_lookup:
            self._modelcache_hits += 1
            log.trace("Modelcache HIT/miss = %s / %s", self._modelcache_hits, self._modelcache_miss)
            return self._model_lookup[resource_id]

        self._modelcache_miss += 1
        log.trace("Modelcache hit/MISS = %s / %s", self._modelcache_hits, self._modelcache_miss)

        rsrc_type = self.get_resource_type(resource_id)
        if RT.PlatformDevice == rsrc_type:
            model = self.enhanced_rr.find_platform_model_id_of_platform_device_using_has_model(resource_id)
        elif RT.PlatformSite == rsrc_type:
            model = self.enhanced_rr.find_platform_model_ids_of_platform_site_using_has_model(resource_id)
        elif RT.InstrumentDevice == rsrc_type:
            model = self.enhanced_rr.find_instrument_model_id_of_instrument_device_using_has_model(resource_id)
        elif RT.InstrumentSite == rsrc_type:
            model = self.enhanced_rr.find_instrument_model_ids_of_instrument_site_using_has_model(resource_id)
        else:
            raise AssertionError("Got unexpected type from which to find a model: %s" % rsrc_type)

        self._model_lookup[resource_id] = model

        return model


    """
    Functions to manage the RR2 cache
    """
    def _predicates_to_cache(self):
        return [
            PRED.hasDeployment,
            PRED.hasDevice,
            PRED.hasSite,
            PRED.hasModel,
            ]

    def _resources_to_cache(self):
        return [
            RT.InstrumentDevice,
            RT.InstrumentSite,
            RT.PlatformDevice,
            RT.PlatformSite,
        ]

    def _update_cached_predicates(self):
        # cache some predicates for in-memory lookups
        preds = self._predicates_to_cache()
        log.debug("updating cached predicates: %s" % preds)
        time_caching_start = get_ion_ts()
        for pred in preds:
            log.debug(" - %s", pred)
            self.enhanced_rr.cache_predicate(pred)
        time_caching_stop = get_ion_ts()

        total_time = int(time_caching_stop) - int(time_caching_start)

        log.info("Cached %s predicates in %s seconds", len(preds), total_time / 1000.0)

    def _update_cached_resources(self):
        # cache some resources for in-memory lookups
        rsrcs = self._resources_to_cache()
        log.debug("updating cached resources: %s" % rsrcs)
        time_caching_start = get_ion_ts()
        for r in rsrcs:
            log.debug(" - %s", r)
            self.enhanced_rr.cache_resources(r)
        time_caching_stop = get_ion_ts()

        total_time = int(time_caching_stop) - int(time_caching_start)

        log.info("Cached %s resource types in %s seconds", len(rsrcs), total_time / 1000.0)

    def _fetch_caches(self):
        if any([not self.enhanced_rr.has_cached_predicate(x) for x in self._predicates_to_cache()]):
            self._update_cached_predicates()

        if any([not self.enhanced_rr.has_cached_resource(x) for x in self._resources_to_cache()]):
            self._update_cached_resources()


    def _clear_caches(self):
        log.warn("Clearing caches")
        for r in self._resources_to_cache():
            self.enhanced_rr.clear_cached_resource(r)

        for p in self._predicates_to_cache():
            self.enhanced_rr.clear_cached_predicate(p)

    def _include_children(self):
        deployment_context_type = type(self.deployment_obj.context).__name__

        # a bundled, integrated platform
        if deployment_context_type in (OT.RemotePlatformDeploymentContext, OT.MobileAssetDeploymentContext):
            return True
        # a one-to-one deployment of an instrument onto an RSN platform
        if OT.CabledInstrumentDeploymentContext == deployment_context_type:
            return False
        # single node, with instruments optionally
        if OT.CabledNodeDeploymentContext == deployment_context_type:
            return True
        raise BadRequest("Can't activate deployments of unimplemented context type '%s'" % deployment_context_type)
    

    def _build_tree(self, root_id, assn_type, leaf_types, known_leaves):
        """
        Recursively build a tree of resources based on various types and allowed relationships between resources

        """

        # copy this list
        leftover_leaves = filter(lambda x: x != root_id, known_leaves)

        root_obj = self.read_using_typecache(root_id)

        # the base value
        tree = {}
        tree["_id"] = root_id
        tree["name"] = root_obj.name
        tree["children"] = {}
        tree["type"] = self._type_lookup[root_id]
        if PRED.hasDevice == assn_type:
            tree["model"] = self.find_models_fromcache(root_id)
            tree["model_name"] = self.enhanced_rr.read(tree["model"]).name
            assert type("") == type(tree["model"]) == type(tree["model_name"])
        elif PRED.hasSite == assn_type:
            tree["models"] = self.find_models_fromcache(root_id)
            tree["model_names"] = [self.read_using_typecache(m).name for m in tree["models"]]
            tree["uplink_port"] = root_obj.planned_uplink_port.reference_designator
            assert type([]) == type(tree["models"]) == type(tree["model_names"])
        else:
            raise AssertionError("Got unexpected predicate %s" % assn_type)

        # build list of children
        children  = []
        for lt in leaf_types:
            immediate_children = self.enhanced_rr.find_objects(root_id, assn_type, lt, True)

            # filter list to just the allowed stuff if we aren't meant to find new all children
            if not self._include_children():
                immediate_children = filter(lambda x: x in known_leaves, immediate_children)

            children += immediate_children

        # recurse on children
        for c in children:
            child_tree, leftover_leaves = self._build_tree(c, assn_type, leaf_types, leftover_leaves)
            tree["children"][c] = child_tree

        return tree, leftover_leaves


    def _attempt_site_tree_build(self, site_ids):
        """
        attempt to build a site tree that includes all provided site ids.  return None if None

        we do this by just trying each site id as the root and seeing if the resuling tree includes all site_ids
        """
        #TODO: optimize, because each id that shows up in the children can't be a valid one to try as root
        # presumably the ids in "leftovers" are valid candidates

        log.debug("Attempting site tree build")
        for d in site_ids:
            tree, leftovers = self._build_tree(d, PRED.hasSite, [RT.InstrumentSite, RT.PlatformSite], site_ids)
            if 0 == len(leftovers):
                return tree
        return None


    def _attempt_device_tree_build(self, device_ids):
        """
        attempt to build a device tree that includes all provided device ids.  return None if None

        we do this by just trying each device id as the root and seeing if the resuling tree includes all device_ids
        """
        #TODO: optimize, because each id that shows up in the children can't be a valid one to try as root
        # presumably the ids in "leftovers" are valid candidates

        log.debug("Attempting device tree build")
        for d in device_ids:
            tree, leftovers = self._build_tree(d, PRED.hasDevice, [RT.InstrumentDevice, RT.PlatformDevice], device_ids)
            if 0 == len(leftovers):
                return tree
        return None


    def collect_deployment_resources(self, deployment_obj):
        """
        Get all the resources involved for a deployment.  store them several ways.
        """
        self.deployment_obj = deployment_obj

        # fetch caches just in time
        self._fetch_caches()

        deployment_id = self.deployment_obj._id

        device_models = {}
        site_models = {}


        # functions to add resources to the lookup tables as well as the return values
        def add_site_models(site_id, model_ids):
            if site_id in site_models:
                log.info("Site '%s' was already collected in deployment '%s'", site_id, deployment_id)
                if model_ids != site_models[site_id]:
                    log.warn("Device '%s' being assigned a different model.  old=%s, new=%s",
                             site_id, site_models[site_id], model_ids)
            site_models[site_id] = model_ids
            self._model_lookup[site_id] = model_ids

        def add_device_model(device_id, model_id):
            if device_id in device_models:
                log.info("Device '%s' was already collected in deployment '%s'", device_id, deployment_id)
                if model_id != device_models[device_id]:
                    log.warn("Device '%s' being assigned a different model.  old='%s', new='%s'",
                             device_id, device_models[device_id], model_id)
            device_models[device_id] = model_id
            self._model_lookup[device_id] = model_id

        def add_sites(site_ids, model_type):
            for s in site_ids:
                models = self.enhanced_rr.find_objects(s, PRED.hasModel, model_type, id_only=True)
                for m in models: self.typecache_add(m, model_type)
                log.trace("Found %s %s objects of site", len(models), model_type)
                add_site_models(s, models)

        def add_devices(device_ids, model_type):
            for d in device_ids:
                model = self.enhanced_rr.find_object(d, PRED.hasModel, model_type, id_only=True)
                self.typecache_add(model, model_type)
                log.trace("Found 1 %s object of device", model_type)
                add_device_model(d, model)


        def collect_specific_resources(site_type, device_type, model_type):
            # check this deployment -- specific device types -- for validity
            # return a list of pairs (site, device) to be associated
            #log.debug("Collecting resources: site=%s device=%s model=%s", site_type, device_type, model_type)
            new_site_ids = self.enhanced_rr.find_subjects(site_type,
                                                    PRED.hasDeployment,
                                                    deployment_id,
                                                    True)
            log.debug("Found %s %s", len(new_site_ids), site_type)

            new_device_ids = self.enhanced_rr.find_subjects(device_type,
                                                      PRED.hasDeployment,
                                                      deployment_id,
                                                      True)
            log.debug("Found %s %s", len(new_device_ids), device_type)

            add_sites(new_site_ids, model_type)
            add_devices(new_device_ids, model_type)

            for s in new_site_ids:   self.typecache_add(s, site_type)
            for d in new_device_ids: self.typecache_add(d, device_type)


        def collect_models_from_tree(a_tree, tree_pred_type):
            """
            a tree is something made by self._build_tree
            tree_pred_type is hasDevice or hasSite

            adds all device or site models to device_models or site_models as appropriate
            """
            log.debug("collect_models_from_tree of type '%s' with: %s", tree_pred_type, a_tree)
            assert(type({}) == type(a_tree))
            assert(type(PRED.hasResource) == type(tree_pred_type))
            if PRED.hasDevice == tree_pred_type:
                add_device_model(a_tree["_id"], a_tree["model"])
            elif PRED.hasSite == tree_pred_type:
                add_site_models(a_tree["_id"], a_tree["models"])
            else:
                raise BadRequest("Tried to collect_models_from_tree(a_tree, %s) -- bad predicate" % tree_pred_type)

            for _, c in a_tree["children"].iteritems():
                collect_models_from_tree(c, tree_pred_type)


        # collect platforms, verify that only one platform device exists in the deployment
        collect_specific_resources(RT.PlatformSite, RT.PlatformDevice, RT.PlatformModel)
        collect_specific_resources(RT.InstrumentSite, RT.InstrumentDevice, RT.InstrumentModel)

        log.debug("build the trees to get the entire picture")
        site_tree   = self._attempt_site_tree_build(site_models.keys())
        device_tree = self._attempt_device_tree_build(device_models.keys())

        log.debug("collect models from device tree")
        if site_tree: collect_models_from_tree(site_tree, PRED.hasSite)
        log.debug("collect models from site tree")
        if device_tree: collect_models_from_tree(device_tree, PRED.hasDevice)

        # various validation
        if len(device_models) > len(site_models):
            raise BadRequest("Devices in this deployment outnumber sites (%s to %s).  Device tree: %s Site tree: %s" %
                             (len(device_models), len(site_models), device_tree, site_tree))

        if 0 == len(device_models):
            raise BadRequest("No devices were found in the deployment")

        if not site_tree:
            raise BadRequest("Sites in this deployment were not all part of the same tree")

        if not device_tree:
            raise BadRequest("Devices in this deployment were not part of the same tree")

        if not self._include_children() and 0 < len(device_tree["children"]):
            raise BadRequest("This type of deployment does not allow child devices, but they were included")

        log.info("Got site tree: %s" % site_tree)
        log.info("Got device tree: %s" % device_tree)

        # store output values
        self._device_models = device_models
        self._site_models   = site_models
        self._device_tree   = device_tree
        self._site_tree     = site_tree



class DeploymentPlanner(object):
    """
    A deployment activator validates that a set of devices will map to a set of sites in one unique way

    its primary purpose is to prepare( ) after which you'll be able to access what associations must be made (and unmade)

    it makes use of the deployment resource collector
    """

    def __init__(self, clients=None, enhanced_rr=None):
        self.clients = clients
        self.enhanced_rr = enhanced_rr

        if not enhanced_rr:
            self.enhanced_rr = EnhancedResourceRegistryClient(self.clients.resource_registry)
        self.outil = ObservatoryUtil(self, enhanced_rr=self.enhanced_rr)

        self.resource_collector= DeploymentResourceCollector(self.clients, self.enhanced_rr)
        #self.resource_collector = resource_collector.create(self.deployment_obj)
        


    # these are the output accessors
    def hasdevice_associations_to_delete(self):
        return self._hasdevice_associations_to_delete[:]

    def hasdevice_associations_to_create(self):
        return self._hasdevice_associations_to_create[:]



    def prepare_activation(self, deployment_obj):
        """
        Prepare (validate) a deployment for activation, returning lists of what associations need to be added
        and which ones need to be removed.
        """
    
        
        self.deployment_obj = deployment_obj
        
        self.outil = ObservatoryUtil(self, enhanced_rr=self.enhanced_rr)
        
        #retrieve the site tree information using the OUTIL functions; site info as well has site children
        site_ids = self.enhanced_rr.find_subjects(subject_type=RT.PlatformSite, predicate=PRED.hasDeployment, object=self.deployment_obj._id, id_only=True)
        if not site_ids:
           site_ids = self.enhanced_rr.find_subjects(subject_type=RT.InstrumentSite, predicate=PRED.hasDeployment, object=self.deployment_obj._id, id_only=True)
        if site_ids:
            self.site_resources, self.site_children = self.outil.get_child_sites( parent_site_id=site_ids[0], id_only=False)

        self.resource_collector.collect_deployment_resources(self.deployment_obj)

        pairs_to_add = self._prepare_using_portassignment_trees()

        log.info("Pairs to add: %s", pairs_to_add)

        #figure out if any of the devices in the new mapping are already mapped and need to be removed
        pairs_to_remove = []
        pairs_to_ignore = []
        for (s, d) in pairs_to_add:
            rm_pair, ignore_pair = self._find_existing_relationship(s, d)
            if rm_pair:
                pairs_to_remove.append(rm_pair)
            if ignore_pair:
                pairs_to_ignore.append(ignore_pair)

        log.info("Pairs to ignore (will be removed from add list): %s", pairs_to_ignore)

        # make sure that anything being removed is not also being added
        pairs_to_add = filter(lambda x: x not in pairs_to_ignore, pairs_to_add)
        
        self._hasdevice_associations_to_create = pairs_to_add
        self._hasdevice_associations_to_delete = pairs_to_remove

        log.info("Pairs to remove: %s", pairs_to_remove)
        
        return pairs_to_remove, pairs_to_add


    def _prepare_using_portassignment_trees(self):
        # return a list of (site, device) pairs
        # badrequest if not all devices get consumed

        site_tree   = self.resource_collector.collected_site_tree()
        device_tree = self.resource_collector.collected_device_tree()

        merged_tree_pairs, leftover_devices = self._merge_trees(site_tree, device_tree)

        if leftover_devices:
            log.info("Merging site and device trees resulted unassigned devices: %s", leftover_devices)
            raise BadRequest("Merging site and device trees resulted in %s unassigned devices" % len(leftover_devices))

        return merged_tree_pairs


    def _resource_ids_in_tree(self, some_tree):
        # get all the resource ids stored in a tree
        def all_children_h(acc, t):
            acc.append(t["_id"])
            for c, ct in t["children"].iteritems():
                acc = all_children_h(acc[:], ct)

            return acc
        return all_children_h([], some_tree)


    def _merge_trees(self, site_tree, device_tree):
        # return a list of (site, device) pairs and a list of unmatched devices

        portref_of_device = self.deployment_obj.port_assignments

        def _merge_helper(acc, site_ptr, dev_ptr, unmatched_list):
            """
            given 2 trees, try to match up all their children.  assume roots already matched
            """
            dev_id  = dev_ptr["_id"]
            site_id = site_ptr["_id"]
            if not dev_ptr["model"] in site_ptr["models"]:
                log.warning("Attempted to assign device '%s' to a site '%s' that doesn't support its model", dev_id, site_id)

            if dev_id in unmatched_list: unmatched_list.remove(dev_id)
            acc.append((site_id, dev_id))
            log.debug('Add to matched list  site_id:  %s   dev_id: %s', site_id, dev_id)


            site_of_portref = {}
            # create a dict of reference_designator on sites so that devices can be matched
            dev_site_obj = self.site_resources[site_id]
            site_of_portref[dev_site_obj.reference_designator] = site_id
            if site_id in self.site_children:
                for child in self.site_children[site_id]:
                    dev_site_obj = self.site_resources[child]
                    site_of_portref[dev_site_obj.reference_designator] = child

            for child_dev_id, child_dev_ptr in dev_ptr["children"].iteritems():

                if not child_dev_id in portref_of_device:
                    child_dev_obj = self.enhanced_rr.read(child_dev_id)
                    log.error("Activation cancelled. No platform port information specified for device %s. Exiting deployment", child_dev_id)

                    raise BadRequest("Activation cancelled. No port assignments provided in the deployment resource: %s", self.deployment_obj)
                    #log.warning("No platform port information specified for device %s" % child_dev_id)
                dev_port = portref_of_device[child_dev_id]

                #check that a PlatformPort object is provided
                if dev_port.type_ != OT.PlatformPort:
                    log.warning("No platform port information specified for device %s" % child_dev_id)

                ooi_rd = OOIReferenceDesignator(dev_port.reference_designator)
                if ooi_rd.error:
                   log.warning("Invalid OOIReferenceDesignator ( %s ) specified for device %s", dev_port.reference_designator, child_dev_id)
                if not ooi_rd.port:
                    log.warning("Invalid OOIReferenceDesignator ( %s ) specified for device %s, could not retrieve port", dev_port.reference_designator, child_dev_id)

                if dev_port.reference_designator in site_of_portref:
                    child_site_id = site_of_portref[dev_port.reference_designator]
                    child_site_obj = self.enhanced_rr.read(child_site_id)
                    child_site_ptr = site_ptr["children"][child_site_id]
                    acc, unmatched_list = _merge_helper(acc[:], child_site_ptr, child_dev_ptr, unmatched_list[:])

                else:
                    log.warning("Couldn't find a port on site %s (%s) called '%s'", site_ptr["name"],  site_id, dev_port)

                    # this check is to match the ref_designator in the deployment object with the ref_designator in the target site
                    #todo add ref_designators to the Sites in preload to match intended deployments



            return acc, unmatched_list

        unmatched_devices = self._resource_ids_in_tree(device_tree)

        return _merge_helper([], site_tree, device_tree, unmatched_devices)



    def _find_existing_relationship(self, site_id, device_id, site_type=None, device_type=None):
        # look for an existing relationship between the site_id and another device.
        # if this site/device pair already exists, we leave it alone
        assert(type("") == type(site_id) == type(device_id))

        log.debug("checking %s/%s pair for deployment", site_type, device_type)
        #return a pair that should be REMOVED, or None

        if site_type is None:
            site_type = self.resource_collector.get_resource_type(site_id)

        if device_type is None:
            device_type = self.resource_collector.get_resource_type(device_id)


        log.debug("checking existing %s hasDevice %s links", site_type, device_type)

        ret_remove = None
        ret_ignore = None

        try:
            found_device_id = self.enhanced_rr.find_object(site_id, PRED.hasDevice, device_type, True)

            if found_device_id == device_id:
                ret_ignore = (site_id, device_id)
            else:
                ret_remove = (site_id, found_device_id)
                log.info("%s '%s' already hasDevice %s", site_type, site_id, device_type)

        except NotFound:
            pass

        return ret_remove, ret_ignore

