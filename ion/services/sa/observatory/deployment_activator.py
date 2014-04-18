#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.deployment_activator
@author   Ian Katz
"""
import copy

from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT, OT
from interface.objects import PortTypeEnum
from ion.core.ooiref import OOIReferenceDesignator

from ooi.logging import log
from ion.services.sa.observatory.observatory_util import ObservatoryUtil



class DeploymentPlanner(object):
    """
    A deployment activator validates that a set of devices will map to a set of sites in one unique way

    its primary purpose is to prepare( ) after which you'll be able to access what associations must be made (and unmade)

    """

    def __init__(self, clients=None, enhanced_rr=None):
        self.clients = clients
        self.enhanced_rr = enhanced_rr

        if not enhanced_rr:
            self.enhanced_rr = EnhancedResourceRegistryClient(self.clients.resource_registry)
        self.outil = ObservatoryUtil(self, enhanced_rr=self.enhanced_rr)

        #self.resource_collector= DeploymentResourceCollector(self.clients, self.enhanced_rr)
        #self.resource_collector = resource_collector.create(self.deployment_obj)

    def _find_top_site_device(self, deployment_id):
        top_site = ''
        top_device = ''
        #retrieve the site tree information using the OUTIL functions; site info as well has site children
        deploy_items_objs, _ = self.clients.resource_registry.find_subjects(predicate=PRED.hasDeployment, object=deployment_id, id_only=False)
        log.debug("site_ids associated to this deployment: %s", deploy_items_objs)
        for obj in deploy_items_objs:
            rsrc_type = obj.type_
            log.debug("resource type associated to this deployment:: %s", rsrc_type)
            if RT.PlatformDevice == rsrc_type or RT.InstrumentDevice == rsrc_type:
                top_device = obj
            elif RT.PlatformSite == rsrc_type or RT.InstrumentSite == rsrc_type:
                top_site = obj
            else:
                log.error('Deployment may only link to devices and sites. Deployment: %s', str(self.deployment_obj))

        if not top_device or not top_site:
            log.error('Deployment must associate to both site and device. Deployment: %s', str(self.deployment_obj))
            raise BadRequest('Deployment must associate to both site and device. Deployment: %s', str(self.deployment_obj))

        return top_site, top_device


    def _find_pairs_to_remove(self):
        #figure out if any of the devices in the new mapping are already mapped and need to be removed
        pairs_to_remove = []
        pairs_to_ignore = []
        for (s, d) in self.match_list:
            rm_pair, ignore_pair = self._find_existing_relationship(s, d)
            if rm_pair:
                pairs_to_remove.append(rm_pair)
            if ignore_pair:
                pairs_to_ignore.append(ignore_pair)

        log.info("Pairs to ignore (will be removed from add list): %s", pairs_to_ignore)

        # make sure that anything being removed is not also being added
        self.match_list = filter(lambda x: x not in pairs_to_ignore, self.match_list)

        log.info("Pairs to remove: %s", pairs_to_remove)
        self.remove_list = pairs_to_remove


    def _find_existing_relationship(self, site_id, device_id, site_type=None, device_type=None):
        # look for an existing relationship between the site_id and another device.
        # if this site/device pair already exists, we leave it alone
        assert(type("") == type(site_id) == type(device_id))

        log.debug("checking %s/%s pair for deployment", site_type, device_type)
        #return a pair that should be REMOVED, or None

        if site_type is None and site_id in self.site_resources:
            site_type = self.site_resources[site_id].type_

        if device_type is None and device_id in self.device_resources:
            device_type = self.device_resources[device_id].type_

        log.debug("checking existing %s hasDevice %s links", site_type, device_type)

        ret_remove = None
        ret_ignore = None

        try:
            found_device_id = self.enhanced_rr.find_object(site_id, PRED.hasDevice, device_type, True)

            if found_device_id == device_id:
                ret_ignore = (site_id, device_id)
            else:
                ret_remove = (site_id, found_device_id)
                log.warning("%s '%s' already hasDevice %s", site_type, site_id, device_type)

        except NotFound:
            pass

        return ret_remove, ret_ignore


    def _get_site_ref_designator_map(self):
        # create a map of site ids to their reference designator codes to facilitate matching
        site_ref_designator_map = {}
        for id, site_obj in self.site_resources.iteritems():
            site_ref_designator_map[site_obj.reference_designator] = id
        log.debug("prepare_activation site_ref_designator_map: %s", site_ref_designator_map)
        return site_ref_designator_map


    def _get_device_resources(self, device_tree):
        # create a map of device ids to their full resource object to assit with lookup and validation
        device_objs = self.clients.resource_registry.read_mult(device_tree.keys())
        log.debug("prepare_activation device_objectss: %s", device_objs)
        for device_obj in device_objs:
            self.device_resources[device_obj._id] = device_obj


    def _get_models(self):
        # retrieve all hasModel associations from the registry then filter
        models_tuples = {}
        assoc_list = self.outil._get_predicate_assocs(PRED.hasModel)
        for assoc in assoc_list:
            # only include these subject types in the map
            if assoc.st in [RT.InstrumentDevice, RT.InstrumentSite, RT.PlatformDevice, RT.PlatformSite]:
                if assoc.s not in models_tuples:
                    models_tuples[assoc.s] = []
                # a site may support more than one model so map to a list of models
                models_tuples[assoc.s].append((assoc.st, assoc.o, assoc.ot))
                if assoc.s not in self.models_map:
                    self.models_map[assoc.s] = []
                self.models_map[assoc.s].append(assoc.o)
        log.debug("models_map: %s", self.models_map )


    def _validate_models(self, site_id, device_id):
        # validate that the device and the site models are compatible
        if device_id in self.models_map:
            device_model_list = self.models_map[device_id]
            # devices should only be associated to one model
            if len(device_model_list) != 1:
                log.error("Device not associated to one distinct model. Device id: %s", device_id)

            elif  device_model_list and device_model_list[0] not in  self.models_map[site_id]:
                log.error("Device and Site to not share a compatible model. Device id: %s   Site id: %s", site_id)
        else:
            log.error("Device not associated with a device model. Device id: %s", device_id)
            raise NotFound("Device not associated with a device model. Device id: %s", device_id)


    def _validate_port_assignments(self, device_id, platform_port):
        deployment_context_type = type(self.deployment_obj.context).__name__

        self._validate_ooi_reference_designator(device_id, platform_port)

        # a one-to-one deployment of a device onto an RSN platform
        if OT.CabledInstrumentDeploymentContext == deployment_context_type or \
            OT.CabledNodeDeploymentContext == deployment_context_type:

            # validate IP address for a cabled node deployment
            from socket import inet_aton
            try:
                inet_aton(platform_port.ip_address)
            except :
                log.error('IP address validation failed for device. Device id: %s', device_id)

        # validate port_type based on deployment context
        # a platform device deployment should have UPLINK port type
        if OT.RemotePlatformDeploymentContext == deployment_context_type or \
            OT.CabledNodeDeploymentContext == deployment_context_type:
            if device_id in self.device_resources and self.device_resources[device_id].type_ is RT.PlatformDevice:
                if platform_port.port_type != PortTypeEnum.UPLINK:
                    log.warning('Type of port for platform port assignment should be UPLINK.  Device id: %s', device_id)

        #validate that parent_id is provided
        if not platform_port.parent_id:
            log.warning('Id of parent device should be provided in port assignment information. Device id: %s', device_id)


    def _validate_ooi_reference_designator(self, device_id, device_port):
        ooi_rd = OOIReferenceDesignator(device_port.reference_designator)
        if ooi_rd.error:
           log.warning("Invalid OOIReferenceDesignator ( %s ) specified for device %s", device_port.reference_designator, device_id)
        if not ooi_rd.port:
            log.warning("Invalid OOIReferenceDesignator ( %s ) specified for device %s, could not retrieve port", device_port.reference_designator, device_id)


    def get_deployment_sites_devices(self, deployment_obj):
        # retrieve all site and device ids related to this deployment
        site_ids = []
        device_ids = []
        self.outil = ObservatoryUtil(self, enhanced_rr=self.enhanced_rr)

        top_site, top_device = self._find_top_site_device(deployment_obj._id)

        site_resources, site_children = self.outil.get_child_sites( parent_site_id=top_site._id, id_only=False)
        site_ids = site_resources.keys()

        # get_site_devices returns a tuple that includes all devices linked to deployment sites
        site_devices = self.outil.get_site_devices(site_ids)
        for site, tuple_list in site_devices.iteritems():
            for (site_type, device_id, device_type) in tuple_list:
                device_ids.append(device_id)

        return site_ids, device_ids


    def prepare_activation(self, deployment_obj):
        """
        Prepare (validate) a deployment for activation, returning lists of what associations need to be added
        and which ones need to be removed.
        """

        self.match_list = []
        self.remove_list = []
        self.unmatched_device_list = []

        self.models_map = {}

        self.top_device = ''
        self.top_site = ''
        self.deployment_obj = deployment_obj
        self.site_resources = {}
        self.device_resources = {}

        self.outil = ObservatoryUtil(self, enhanced_rr=self.enhanced_rr)

        # retrieve the site tree information using the OUTIL functions; site info as well has site children
        self.top_site, self.top_device = self._find_top_site_device(deployment_obj._id)
        # must have a site and a device to continue
        if not self.top_site or not self.top_device:
            return [], []

        log.debug("port_assignments: %s", self.deployment_obj.port_assignments )

        # retrieve all models to use in match validation
        self._get_models()

        self.site_resources, site_children = self.outil.get_child_sites( parent_site_id=self.top_site._id, id_only=False)

        log.debug("site_resources: %s", self.site_resources)
        log.debug("site_children: %s", site_children)

        site_ref_designator_map = self._get_site_ref_designator_map()

        # retrieve the device tree from outil then cache the device resources
        device_tree = self.outil.get_child_devices(device_id=self.top_device._id)
        self._get_device_resources(device_tree)

        def _match_devices(device_id):

            # there will not be a port assignment for the top device
            if device_id == self.top_device._id:
                self._validate_models(self.top_site._id, self.top_device._id)
                self.match_list.append((self.top_site._id, self.top_device._id))

            tuple_list = device_tree[device_id]

            for (pt, child_id, ct) in tuple_list:
                log.debug("  tuple  - pt: %s  child_id: %s  ct: %s", pt, child_id, ct)

                # match this child device then if it has children, call _match_devices with this id

                # check that this device is represented in device tree and in port assignments
                if child_id in self.device_resources and child_id in self.deployment_obj.port_assignments:
                    platform_port = self.deployment_obj.port_assignments[child_id]
                    log.debug("device platform_port: %s", platform_port)

                    # validate PlatformPort info for this device
                    self._validate_port_assignments(child_id, platform_port)

                    if platform_port.reference_designator in site_ref_designator_map:
                        matched_site = site_ref_designator_map[platform_port.reference_designator]
                        self._validate_models(matched_site, child_id)
                        log.info("match_list append site: %s  device: %s", matched_site, child_id)
                        self.match_list.append((matched_site, child_id))

                        #recurse on the children of this device
                        _match_devices(child_id)

                # otherwise cant be matched to a site
                else:
                    self.unmatched_device_list.append(child_id)

        _match_devices(self.top_device._id)

        # check for hasDevice relations to remove and existing hasDevice relations
        self. _find_pairs_to_remove()

        if self.unmatched_device_list:
            log.warning("Devices not matched to sites: %s  ", self.unmatched_device_list)

        return self.remove_list, self.match_list



