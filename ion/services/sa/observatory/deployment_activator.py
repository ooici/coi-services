#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.deployment_activator
@author   Ian Katz
"""
import copy

from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.core.exception import BadRequest, NotFound
from pyon.ion.resource import PRED, RT, OT

from ooi.logging import log
from pyon.util.containers import get_ion_ts

import constraint

CSP_DEVICE_PREFIX = "Csp-Device"
def mk_csp_var(label):
    return "%s_%s" % (CSP_DEVICE_PREFIX, label)

def unpack_csp_var(var):
    return var.split("_")[1]

class DeploymentOperatorFactory(object):
    """
    Deployment operators all require a deployment object and a set of options describing how their devices
    are included.  This class builds them appropriately based on deployment objects
    """

    def __init__(self, clients, RR2=None):
        self.clients = clients
        self.RR2 = RR2 #enhanced resource registry client

    def creation_type(self):
        raise NotImplementedError("this function should be overridden, and return the class to be instantiated")

    def create(self, deployment_obj):
        deployment_context_type = type(deployment_obj.context).__name__

        new_object_type = self.creation_type()

        # a bundled, integrated platform
        if OT.RemotePlatformDeploymentContext == deployment_context_type:
            return new_object_type(self.clients,
                                   deployment_obj,
                                   allow_children=True,
                                   include_children=True,
                                   RR2=self.RR2)

        # a one-to-one deployment of an instrument onto an RSN platform
        if OT.CabledInstrumentDeploymentContext == deployment_context_type:
            return new_object_type(self.clients,
                                   deployment_obj,
                                   allow_children=True,
                                   include_children=True,
                                   RR2=self.RR2)

        # single node, with instruments optionally
        if OT.CabledNodeDeploymentContext == deployment_context_type:
            return new_object_type(self.clients,
                                   deployment_obj,
                                   allow_children=True,
                                   include_children=True,
                                   RR2=self.RR2)

        raise BadRequest("Can't activate deployments of unimplemented context type '%s'" % deployment_context_type)

class DeploymentActivatorFactory(DeploymentOperatorFactory):

    def creation_type(self):
        return DeploymentActivator


class DeploymentResourceCollectorFactory(DeploymentOperatorFactory):

    def creation_type(self):
        return DeploymentResourceCollector






class DeploymentOperator(object):
    """
    A deployment operator (in the functional addition/subtraction/comparison operator sense) understands attributes
    of a deployment, as determined by several static parameters. (these are based on the deployment context).

    This is the base class
    """

    def __init__(self, clients, deployment_obj, allow_children, include_children, RR2=None):
        """
        @param clients dict of clients from a service
        @param deployment_obj the deployment to activate
        @param allow_children whether to allow children of a device to be provided
        @param include_children whether to search for child devices from root device
        @param RR2 a reference to an enhanced RR client
        """

        self.clients = clients
        self.RR2 = RR2

        #sanity
        if include_children: assert allow_children
        self.allow_children = allow_children
        self.include_children = include_children

        if None is self.RR2:
            self.RR2 = EnhancedResourceRegistryClient(self.clients.resource_registry)

        if not isinstance(self.RR2, EnhancedResourceRegistryClient):
            raise AssertionError("Type of self.RR2 is %s not %s" %
                                 (type(self.RR2), type(EnhancedResourceRegistryClient)))

        self.deployment_obj = deployment_obj

        self.on_init()

    def on_init(self):
        pass


class DeploymentResourceCollector(DeploymentOperator):
    """
    A deployment resource collector pulls in all devices, sites, and models related to a deployment.

    its primary purpose is to collect( ) after which you'll be able to access what it collected.

    various lookup tables exist to prevent too many hits on the resource registry.
    """
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
            return self.RR2.read(resource_id, self._type_lookup[resource_id])

        self._typecache_miss += 1
        log.trace("Typeache hit/MISS = %s / %s", self._typecache_hits, self._typecache_miss)
        rsrc_obj = self.RR2.read(resource_id)
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
            model = self.RR2.find_platform_model_id_of_platform_device_using_has_model(resource_id)
        elif RT.PlatformSite == rsrc_type:
            model = self.RR2.find_platform_model_ids_of_platform_site_using_has_model(resource_id)
        elif RT.InstrumentDevice == rsrc_type:
            model = self.RR2.find_instrument_model_id_of_instrument_device_using_has_model(resource_id)
        elif RT.InstrumentSite == rsrc_type:
            model = self.RR2.find_instrument_model_ids_of_instrument_site_using_has_model(resource_id)
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
            self.RR2.cache_predicate(pred)
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
            self.RR2.cache_resources(r)
        time_caching_stop = get_ion_ts()

        total_time = int(time_caching_stop) - int(time_caching_start)

        log.info("Cached %s resource types in %s seconds", len(rsrcs), total_time / 1000.0)

    def _fetch_caches(self):
        if any([not self.RR2.has_cached_prediate(x) for x in self._predicates_to_cache()]):
            self._update_cached_predicates()

        if any([not self.RR2.has_cached_resource(x) for x in self._resources_to_cache()]):
            self._update_cached_resources()


    def _clear_caches(self):
        log.warn("Clearing caches")
        for r in self._resources_to_cache():
            self.RR2.clear_cached_resource(r)

        for p in self._predicates_to_cache():
            self.RR2.clear_cached_predicate(p)




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
        if PRED.hasDevice == assn_type:
            tree["model"] = self.find_models_fromcache(root_id)
            tree["model_name"] = self.RR2.read(tree["model"]).name
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
            immediate_children = self.RR2.find_objects(root_id, assn_type, lt, True)

            # filter list to just the allowed stuff if we aren't meant to find new all children
            if not self.include_children:
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



    def collect(self):
        """
        Get all the resources involved for a deployment.  store them several ways.
        """

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
                models = self.RR2.find_objects(s, PRED.hasModel, model_type, id_only=True)
                for m in models: self.typecache_add(m, model_type)
                log.trace("Found %s %s objects of site", len(models), model_type)
                add_site_models(s, models)

        def add_devices(device_ids, model_type):
            for d in device_ids:
                model = self.RR2.find_object(d, PRED.hasModel, model_type, id_only=True)
                self.typecache_add(model, model_type)
                log.trace("Found 1 %s object of device", model_type)
                add_device_model(d, model)


        def collect_specific_resources(site_type, device_type, model_type):
            # check this deployment -- specific device types -- for validity
            # return a list of pairs (site, device) to be associated
            #log.debug("Collecting resources: site=%s device=%s model=%s", site_type, device_type, model_type)
            new_site_ids = self.RR2.find_subjects(site_type,
                                                    PRED.hasDeployment,
                                                    deployment_id,
                                                    True)
            log.debug("Found %s %s", len(new_site_ids), site_type)

            new_device_ids = self.RR2.find_subjects(device_type,
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
            raise BadRequest("Devices in this deployment outnumber sites (%s to %s)" % (len(device_models), len(site_models)))

        if 0 == len(device_models):
            raise BadRequest("No devices were found in the deployment")

        if not site_tree:
            raise BadRequest("Sites in this deployment were not all part of the same tree")

        if not device_tree:
            raise BadRequest("Devices in this deployment were not part of the same tree")

        if not self.allow_children and 0 < len(device_tree["children"]):
            raise BadRequest("This type of deployment does not allow child devices, but they were included")

        log.info("Got site tree: %s" % site_tree)
        log.info("Got device tree: %s" % device_tree)

        # store output values
        self._device_models = device_models
        self._site_models   = site_models
        self._device_tree   = device_tree
        self._site_tree     = site_tree



class DeploymentActivator(DeploymentOperator):
    """
    A deployment activator validates that a set of devices will map to a set of sites in one unique way

    its primary purpose is to prepare( ) after which you'll be able to access what associations must be made (and unmade)

    it makes use of the deployment resource colelctor
    """

    def on_init(self):
        resource_collector_factory = DeploymentResourceCollectorFactory(self.clients, self.RR2)
        self.resource_collector = resource_collector_factory.create(self.deployment_obj)

        self._hasdevice_associations_to_delete = []
        self._hasdevice_associations_to_create = []

    # these are the output accessors
    def hasdevice_associations_to_delete(self):
        return self._hasdevice_associations_to_delete[:]

    def hasdevice_associations_to_create(self):
        return self._hasdevice_associations_to_create[:]

    # for debugging purposes
    def _csp_solution_to_string(self, soln):
        ret = "%s" % type(soln).__name__

        for k, s in soln.iteritems():
            d = unpack_csp_var(k)
            log.trace("reading device %s", d)
            dev_obj = self.resource_collector.read_using_typecache(d)
            log.trace("reading site %s", s)
            site_obj = self.resource_collector.read_using_typecache(s)
            ret = "%s, %s '%s' -> %s '%s'" % (ret, dev_obj._get_type(), d, site_obj._get_type(), s)
        return ret


    def prepare(self):
        """
        Prepare (validate) a deployment for activation, returning lists of what associations need to be added
        and which ones need to be removed.
        """

        #TODO: check for already deployed?


        log.debug("about to collect deployment components")
        self.resource_collector.collect()


        if not self.deployment_obj.port_assignments:
            log.info("No port assignments, so using CSP")
            pairs_to_add = self._prepare_using_csp()
        else:
            log.info("Merging trees with port assignments")
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


    def _prepare_using_portassignment_trees(self):
        # return a list of (site, device) pairs
        # badrequest if not all devices get consumed

        site_tree   = self.resource_collector.collected_site_tree()
        device_tree = self.resource_collector.collected_device_tree()

        merged_tree_pairs, leftover_devices = self._merge_trees(site_tree, device_tree)
        if 0 < len(leftover_devices):
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
                raise BadRequest("Attempted to assign device '%s' to a site that doesn't support its model" % dev_id)

            if dev_id in unmatched_list: unmatched_list.remove(dev_id)
            acc.append((site_id, dev_id))

            site_of_portref = dict([(v["uplink_port"], k) for k, v in site_ptr["children"].iteritems()])

            for child_dev_id, child_dev_ptr in dev_ptr["children"].iteritems():
                if not child_dev_id in portref_of_device:
                    raise BadRequest("No reference_designator specified for device %s" % child_dev_id)
                dev_port = portref_of_device[child_dev_id]
                if not dev_port in site_of_portref:
                    raise BadRequest("Couldn't find a port on site %s (%s) called '%s'" % (site_ptr["name"],
                                                                                           site_id,
                                                                                           dev_port))
                child_site_id = site_of_portref[dev_port]
                child_site_ptr = site_ptr["children"][child_site_id]
                acc, unmatched_list = _merge_helper(acc[:], child_site_ptr, child_dev_ptr, unmatched_list[:])

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
            found_device_id = self.RR2.find_object(site_id, PRED.hasDevice, device_type, True)

            if found_device_id == device_id:
                ret_ignore = (site_id, device_id)
            else:
                ret_remove = (site_id, found_device_id)
                log.info("%s '%s' already hasDevice %s", site_type, site_id, device_type)

        except NotFound:
            pass

        return ret_remove, ret_ignore



    def _prepare_using_csp(self):
        """
        use the previously collected resoures in a CSP problem
        """
        site_tree   = self.resource_collector.collected_site_tree()
        device_tree = self.resource_collector.collected_device_tree()
        device_models = self.resource_collector.collected_models_by_device()
        site_models = self.resource_collector.collected_models_by_site()

        log.debug("Collected %s device models, %s site models", len(device_models), len(site_models))

        # csp solver can't handle multiple platforms, because it doesn't understand hierarchy.
        #             (parent-platformsite---hasmodel-a, child-platformsite---hasmodel-b)
        # would match (parent-platformdevice-hasmodel-b, child-platformdevice-hasmodel-a)
        #
        # we can avoid this by simply restricting the deployment to 1 platform device/site in this case

#        n_pdev = sum(RT.PlatformDevice == self.resource_collector.get_resource_type(d) for d in device_models.keys())
#        if 1 < n_pdev:
#            raise BadRequest("Deployment activation without port_assignment is limited to 1 PlatformDevice, got %s" % n_pdev)
#
#        n_psite = sum(RT.PlatformSite == self.resource_collector.get_resource_type(d) for d in site_models.keys())
#        if 1 < n_psite:
#            raise BadRequest("Deployment activation without port_assignment is limited to 1 PlatformSite, got %s" % n_psite)

        solutions = self._get_deployment_csp_solutions(device_tree, site_tree, device_models, site_models)

        if 1 > len(solutions):
            raise BadRequest("The set of devices could not be mapped to the set of sites, based on matching " +
                             "models") # and streamdefs")

        if 1 < len(solutions):
            log.info("Found %d possible ways to map device and site", len(solutions))
            log.trace("Here is the %s of all of them:", type(solutions).__name__)
            for i, s in enumerate(solutions):
                log.trace("Option %d: %s" , i+1, self._csp_solution_to_string(s))
            raise BadRequest(("The set of devices could be mapped to the set of sites in %s ways based only " +
                              "on matching models, and no port assignments were specified") % len(solutions))

        log.info("Found one possible way to map devices and sites.  Best case scenario!")
        # return list of site_id, device_id
        return [(solutions[0][mk_csp_var(device_id)], device_id) for device_id in device_models.keys()]



    def _get_deployment_csp_solutions(self, device_tree, site_tree, device_models, site_models):

        log.debug("creating a CSP solver to match devices and sites")
        problem = constraint.Problem()

        def safe_get_parent(child_site_id):
            try:
                return self.RR2.find_subject(RT.PlatformSite,
                                             PRED.hasSite,
                                             child_site_id,
                                             id_only=True)
            except NotFound:
                return None

        log.debug("adding variables to CSP - the devices to be assigned, and their range (possible sites)")
        for device_id in device_models.keys():
            device_model = device_models[device_id]
            assert type(device_model) == type('')
            assert all([type('') == type(s) for s in site_models])
            possible_sites = [s for s in site_models.keys()
                              if device_model in site_models[s]]

            if not possible_sites:
                log.info("Device model: %s", device_model)
                log.info("Site models: %s", site_models)
                raise BadRequest("No sites in the deployment match the model of device '%s'" % device_id)

            device_var = mk_csp_var(device_id)
            problem.addVariable(device_var, possible_sites)

            # add parent-child constraints
            try:
                parent_device_id = self.RR2.find_subject(RT.PlatformDevice, PRED.hasDevice, device_id, id_only=True)

                problem.addConstraint(lambda child_site, parent_site: parent_site == safe_get_parent(child_site),
                                      [device_var, mk_csp_var(parent_device_id)])


            except NotFound:
                log.debug("Device '%s' has no parent", device_id) # no big deal




        log.debug("adding the constraint that all the variables have to pick their own site")
        problem.addConstraint(constraint.AllDifferentConstraint(),
            [mk_csp_var(device_id) for device_id in device_models.keys()])

        log.debug("performing CSP solve")
        # this will be a list of solutions, each a dict of var -> value
        return problem.getSolutions()




