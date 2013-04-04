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
                                   allow_children=False,
                                   include_children=False,
                                   RR2=self.RR2)

        # single node, with instruments optionally
        if OT.CabledNodeDeploymentContext == deployment_context_type:
            return new_object_type(self.clients,
                                   deployment_obj,
                                   allow_children=True,
                                   include_children=False,
                                   RR2=self.RR2)

class DeploymentActivatorFactory(DeploymentOperatorFactory):

    def creation_type(self):
        return DeploymentActivator


class DeploymentResourceCollectorFactory(DeploymentOperatorFactory):

    def creation_type(self):
        return DeploymentResourceCollector






class DeploymentOperator(object):

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

    def on_init(self):
        self._device_models = {}
        self._site_models = {}

        self._model_lookup = {}
        self._type_lookup = {}

        self._typecache_hits = 0
        self._typecache_miss = 0

        self._modelcache_hits = 0
        self._modelcache_miss = 0


    def collected_device_ids(self):
        return self._device_models.keys()

    def collected_site_ids(self):
        return self._site_models.keys()

    def collected_models_by_site(self):
        return copy.copy(self._site_models)

    def collected_models_by_device(self):
        return copy.copy(self._device_models)

    def typecache_add(self, resource_id, resource_type):
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
        return self.read_using_typecache(resource_id)._get_type()

    def find_models_fromcache(self, resource_id):
        if resource_id in self._type_lookup:
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

        # copy this list
        leftover_leaves = filter(lambda x: x != root_id, known_leaves)
#        leftover_leaves = known_leaves[:]
#        if root_id in leftover_leaves:
#            leftover_leaves.remove(root_id)

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

        children  = []
        for lt in leaf_types:
            immediate_children = self.RR2.find_objects(root_id, assn_type, lt, True)

            # filter list to just the allowed stuff if we aren't meant to find new all children
            if not self.include_children:
                immediate_children = filter(lambda x: x in known_leaves, immediate_children)

            children += immediate_children

        for c in children:
            child_tree, leftover_leaves = self._build_tree(c, assn_type, leaf_types, leftover_leaves)
            tree["children"][c] = child_tree

        return tree, leftover_leaves


    def _attempt_site_tree_build(self, site_ids):
        log.debug("Attempting site tree build")
        for d in site_ids:
            tree, leftovers = self._build_tree(d, PRED.hasSite, [RT.InstrumentSite, RT.PlatformSite], site_ids)
            if 0 == len(leftovers):
                return tree
        return None


    def _attempt_device_tree_build(self, device_ids):
        log.debug("Attempting device tree build")
        for d in device_ids:
            tree, leftovers = self._build_tree(d, PRED.hasDevice, [RT.InstrumentDevice, RT.PlatformDevice], device_ids)
            if 0 == len(leftovers):
                return tree
        return None


    def collect(self):
        # fetch caches just in time
        self._fetch_caches()

        deployment_id = self.deployment_obj._id

        device_models = {}
        site_models = {}

        # collect all devices in this deployment

        def add_sites(site_ids, model_type):
            for s in site_ids:
                models = self.RR2.find_objects(s, PRED.hasModel, model_type, id_only=True)
                for m in models: self.typecache_add(m, model_type)
                log.trace("Found %s %s objects of site", len(models), model_type)
                if s in site_models:
                    log.warn("Site '%s' was already collected in deployment '%s'", s, deployment_id)
                site_models[s] = models
                self._model_lookup[s] = models

        def add_devices(device_ids, model_type):
            for d in device_ids:
                model = self.RR2.find_object(d, PRED.hasModel, model_type, id_only=True)
                self.typecache_add(model, model_type)
                log.trace("Found 1 %s object of device", model_type)
                if d in device_models:
                    log.warn("Device '%s' was already collected in deployment '%s'", d, deployment_id)
                device_models[d] = model
                self._model_lookup[d] = model


        def collect_specific_resources(site_type, device_type, model_type):
            # check this deployment -- specific device types -- for validity
            # return a list of pairs (site, device) to be associated
            log.debug("Collecting resources: site=%s device=%s model=%s", site_type, device_type, model_type)
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

        # collect platforms, verify that only one platform device exists in the deployment
        collect_specific_resources(RT.PlatformSite, RT.PlatformDevice, RT.PlatformModel)
        collect_specific_resources(RT.InstrumentSite, RT.InstrumentDevice, RT.InstrumentModel)

        site_tree   = self._attempt_site_tree_build(site_models.keys())
        device_tree = self._attempt_device_tree_build(device_models.keys())

        if not site_tree:
            raise BadRequest("Sites in this deployment were not all part of the same tree")

        if not device_tree:
            raise BadRequest("Devices in this deployment were not part of the same tree")

        log.info("Got site tree: %s" % site_tree)
        log.info("Got device tree: %s" % device_tree)


        self._device_models = device_models
        self._site_models = site_models



class DeploymentActivator(DeploymentOperator):

    def on_init(self):
        resource_collector_factory = DeploymentResourceCollectorFactory(self.clients, self.RR2)
        self.resource_collector = resource_collector_factory.create(self.deployment_obj)

        self._hasdevice_associations_to_delete = []
        self._hasdevice_associations_to_create = []

    def hasdevice_associations_to_delete(self):
        return self._hasdevice_associations_to_delete[:]

    def hasdevice_associations_to_create(self):
        return self._hasdevice_associations_to_create[:]

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


    def prepare(self, will_launch=True):
        """
        Prepare (validate) an agent for launch, fetching all associated resources

        @param will_launch - whether the running status should be checked -- set false if just generating config
        """

        #TODO: check for already deployed?


        self.resource_collector.collect()

        log.debug("about to collect deployment components")
        device_models = self.resource_collector.collected_models_by_device()
        site_models = self.resource_collector.collected_models_by_site()

        log.debug("Collected %s device models, %s site models", len(device_models), len(site_models))

        solutions = self._get_deployment_csp_solutions(device_models, site_models)

        pairs_add = []
        pairs_rem = []

        if 1 > len(solutions):
            raise BadRequest("The set of devices could not be mapped to the set of sites, based on matching " +
                             "models") # and streamdefs")
        elif 1 < len(solutions):
            log.info("Found %d possible ways to map device and site", len(solutions))
            log.trace("Here is the %s of all of them:", type(solutions).__name__)
            for i, s in enumerate(solutions):
                log.trace("Option %d: %s" , i+1, self._csp_solution_to_string(s))
        else:
            log.info("Found one possible way to map devices and sites.  Best case scenario!")

            #figure out if any of the devices in the new mapping are already mapped and need to be removed
            #then add the new mapping to the output array
            for device_id in device_models.keys():
                site_id = solutions[0][mk_csp_var(device_id)]
                old_pair = self._find_existing_relationship(site_id, device_id)
                if old_pair:
                    pairs_rem.append(old_pair)
                new_pair = (site_id, device_id)
                pairs_add.append(new_pair)



        log.debug("falling back on port assignments")

        self._hasdevice_associations_to_create = sorted(pairs_add)
        self._hasdevice_associations_to_delete = sorted(pairs_rem)




    def _find_existing_relationship(self, site_id, device_id, site_type=None, device_type=None):
        assert(type("") == type(site_id) == type(device_id))

        log.debug("checking %s/%s pair for deployment", site_type, device_type)
        #return a pair that should be REMOVED, or None

        if site_type is None:
            site_type = self.resource_collector.get_resource_type(site_id)

        if device_type is None:
            device_type = self.resource_collector.get_resource_type(device_id)


        log.debug("checking existing hasDevice links from site")

        ret = None

        try:
            found_device_id = self.RR2.find_object(site_id, PRED.hasDevice, device_type, True)

            if found_device_id != device_id:
                ret = (site_id, found_device_id)
                log.info("%s '%s' is already hasDevice with a %s", site_type, site_id, device_type)

        except NotFound:
            pass

        return ret


    def _get_deployment_csp_solutions(self, device_models, site_models):

        log.debug("creating a CSP solver to match devices and sites")
        problem = constraint.Problem()

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
                raise BadRequest("No sites were found in the deployment")

            problem.addVariable(mk_csp_var(device_id), possible_sites)

        log.debug("adding the constraint that all the variables have to pick their own site")
        problem.addConstraint(constraint.AllDifferentConstraint(),
            [mk_csp_var(device_id) for device_id in device_models.keys()])

        log.debug("performing CSP solve")
        # this will be a list of solutions, each a dict of var -> value
        return problem.getSolutions()




