#!/usr/bin/env python

"""Service managing marine facility sites and deployments"""

import string
import time

import logging

from collections import defaultdict
from pyon.core.governance import ORG_MANAGER_ROLE, DATA_OPERATOR, OBSERVATORY_OPERATOR, INSTRUMENT_OPERATOR, GovernanceHeaderValues, has_org_role

from ooi.logging import log
from pyon.core.exception import NotFound, BadRequest, Inconsistent
from pyon.public import CFG, IonObject, RT, PRED, LCS, LCE, OT
from pyon.ion.resource import ExtendedResourceContainer

from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.sa.instrument.status_builder import AgentStatusBuilder
from ion.services.sa.observatory.deployment_activator import DeploymentPlanner
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from ion.services.sa.observatory.observatory_util import ObservatoryUtil
from ion.services.sa.observatory.asset_tracking import AssetTracking
from ion.services.sa.observatory.deployment_util import DeploymentUtil
from ion.services.sa.product.data_product_management_service import DataProductManagementService
from ion.processes.event.device_state import DeviceStateManager
from ion.util.geo_utils import GeoUtils
from ion.util.related_resources_crawler import RelatedResourcesCrawler
from ion.util.datastore.resources import ResourceRegistryUtil

from interface.services.sa.iobservatory_management_service import BaseObservatoryManagementService
from interface.objects import OrgTypeEnum, ComputedValueAvailability, ComputedIntValue, ComputedListValue, ComputedDictValue, AggregateStatusType, DeviceStatusType, TemporalBounds, DatasetWindow
from interface.objects import MarineFacilityOrgExtension, NegotiationStatusEnum, NegotiationTypeEnum, ProposalOriginatorEnum, GeospatialBounds

from datetime import datetime
import calendar


INSTRUMENT_OPERATOR_ROLE  = 'INSTRUMENT_OPERATOR'
OBSERVATORY_OPERATOR_ROLE = 'OBSERVATORY_OPERATOR'
DATA_OPERATOR_ROLE        = 'DATA_OPERATOR'
STATUS_UNKNOWN = {1:1, 2:1, 3:1, 4:1}


class ObservatoryManagementService(BaseObservatoryManagementService):

    def on_init(self):
        self.override_clients(self.clients)
        self.agent_status_builder = AgentStatusBuilder(process=self)


        self.HIERARCHY_DEPTH = {RT.InstrumentSite: 3,
                                RT.PlatformSite: 2,
                                RT.Subsite: 1,
                                RT.Observatory: 0,
                                }

        self.HIERARCHY_LOOKUP = [RT.Observatory,
                                 RT.Subsite,
                                 RT.PlatformSite,
                                 RT.InstrumentSite]

        #todo: add lcs methods for these??
#        # set up all of the policy interceptions
#        if self.container and self.container.governance_controller:
#            reg_precondition = self.container.governance_controller.register_process_operation_precondition
#            reg_precondition(self, 'execute_observatory_lifecycle',
#                             self.RR2.policy_fn_lcs_precondition("observatory_id"))
#            reg_precondition(self, 'execute_subsite_lifecycle',
#                             self.RR2.policy_fn_lcs_precondition("subsite_id"))
#            reg_precondition(self, 'execute_platform_site_lifecycle',
#                             self.RR2.policy_fn_lcs_precondition("platform_site_id"))
#            reg_precondition(self, 'execute_instrument_site_lifecycle',
#                             self.RR2.policy_fn_lcs_precondition("instrument_site_id"))


    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """
        self.RR2   = EnhancedResourceRegistryClient(new_clients.resource_registry)

        #shortcut names for the import sub-services
        if hasattr(new_clients, "resource_registry"):
            self.RR = new_clients.resource_registry

        if hasattr(new_clients, "instrument_management"):
            self.IMS = new_clients.instrument_management

        if hasattr(new_clients, "data_process_management"):
            self.PRMS = new_clients.data_process_management

    def _calc_geospatial_point_center(self, site):
        siteTypes = [RT.Site, RT.Subsite, RT.Observatory, RT.PlatformSite, RT.InstrumentSite]
        if site and site.type_ in siteTypes:
            # if the geospatial_bounds is set then calculate the geospatial_point_center
            for constraint in site.constraint_list:
                if constraint.type_ == OT.GeospatialBounds:
                    site.geospatial_point_center = GeoUtils.calc_geospatial_point_center(constraint)

    ##########################################################################
    #
    # CRUD OPS
    #
    ##########################################################################


    def create_marine_facility(self, org=None):
        """Create an Org (domain of authority) that realizes a marine facility. This Org will have
        set up roles for a marine facility. Shared resources, such as a device can only be
        registered in one marine facility Org, and additionally in many virtual observatory Orgs. The
        marine facility operators will have more extensive permissions and will supercede virtual
        observatory commands

        @param org    Org
        @retval org_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        log.debug("ObservatoryManagementService.create_marine_facility(): %s", org)

        # create the org
        org.org_type = OrgTypeEnum.MARINE_FACILITY
        org_id = self.clients.org_management.create_org(org)

        #Instantiate initial set of User Roles for this marine facility
        instrument_operator_role = IonObject(RT.UserRole,
                                             governance_name=INSTRUMENT_OPERATOR_ROLE,
                                             name='Facility Operator',   #previously Instrument Operator
                                             description='Operate and post events related to Facility Platforms and Instruments')
        self.clients.org_management.add_user_role(org_id, instrument_operator_role)
        observatory_operator_role = IonObject(RT.UserRole,
                                             governance_name=OBSERVATORY_OPERATOR_ROLE,
                                             name='Facility Manager',   # previously Observatory Operator
                                             description='Change Facility configuration, post Site-related events')
        self.clients.org_management.add_user_role(org_id, observatory_operator_role)
        data_operator_role = IonObject(RT.UserRole,
                                       governance_name=DATA_OPERATOR_ROLE,
                                       name='Facility Data Operator',  # previously Data Operator
                                       description='Manipulate and post events related to Facility Data products')
        self.clients.org_management.add_user_role(org_id, data_operator_role)

        return org_id

    def create_virtual_observatory(self, org=None):
        """Create an Org (domain of authority) that realizes a virtual observatory. This Org will have
        set up roles for a virtual observatory. Shared resources, such as a device can only be
        registered in one marine facility Org, and additionally in many virtual observatory Orgs. The
        marine facility operators will have more extensive permissions and will supercede virtual
        observatory commands

        @param org    Org
        @retval org_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        log.debug("ObservatoryManagementService.create_virtual_observatory(): %s", org)

        # create the org
        org.org_type = OrgTypeEnum.VIRTUAL_OBSERVATORY
        org_id = self.clients.org_management.create_org(org)

        return org_id

    def create_observatory(self, observatory=None, org_id=""):
        """Create a Observatory resource. An observatory  is coupled
        with one Org. The Org is created and associated as part of this call.

        @param observatory    Observatory
        @retval observatory_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        # if the geospatial_bounds is set then calculate the geospatial_point_center
        self._calc_geospatial_point_center(observatory)

        # create the marine facility
        observatory_id = self.RR2.create(observatory, RT.Observatory)

        if org_id:
            self.assign_resource_to_observatory_org(observatory_id, org_id)

        return observatory_id

    def read_observatory(self, observatory_id=''):
        """Read a Observatory resource

        @param observatory_id    str
        @retval observatory    Observatory
        @throws NotFound    object with specified id does not exist
        """
        return self.RR2.read(observatory_id, RT.Observatory)

    def update_observatory(self, observatory=None):
        """Update a Observatory resource

        @param observatory    Observatory
        @throws NotFound    object with specified id does not exist
        """
        # if the geospatial_bounds is set then calculate the geospatial_point_center
        self._calc_geospatial_point_center(observatory)

        return self.RR2.update(observatory, RT.Observatory)

    def delete_observatory(self, observatory_id=''):
        """Delete a Observatory resource

        @param observatory_id    str
        @throws NotFound    object with specified id does not exist
        """
        return self.RR2.lcs_delete(observatory_id, RT.Observatory)

    def force_delete_observatory(self, observatory_id=''):
        return self.RR2.force_delete(observatory_id, RT.Observatory)

    def create_subsite(self, subsite=None, parent_id=''):
        """Create a Subsite resource. A subsite is a frame of reference within an observatory. Its parent is
        either the observatory or another subsite.

        @param subsite    Subsite
        @param parent_id    str
        @retval subsite_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        # if the geospatial_bounds is set then calculate the geospatial_point_center
        self._calc_geospatial_point_center(subsite)

        subsite_id = self.RR2.create(subsite, RT.Subsite)

        if parent_id:
            self.assign_site_to_site(subsite_id, parent_id)

        return subsite_id

    def read_subsite(self, subsite_id=''):
        """Read a Subsite resource

        @param subsite_id    str
        @retval subsite    Subsite
        @throws NotFound    object with specified id does not exist
        """
        return self.RR2.read(subsite_id, RT.Subsite)

    def update_subsite(self, subsite=None):
        """Update a Subsite resource

        @param subsite    Subsite
        @throws NotFound    object with specified id does not exist
        """
        # if the geospatial_bounds is set then calculate the geospatial_point_center
        self._calc_geospatial_point_center(subsite)

        return self.RR2.update(subsite, RT.Subsite)

    def delete_subsite(self, subsite_id=''):
        """Delete a subsite resource, removes assocations to parents

        @param subsite_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.RR2.lcs_delete(subsite_id, RT.Subsite)

    def force_delete_subsite(self, subsite_id=''):
        self.RR2.force_delete(subsite_id, RT.Subsite)



    def create_platform_site(self, platform_site=None, parent_id=''):
        """Create a PlatformSite resource. A platform_site is a frame of reference within an observatory. Its parent is
        either the observatory or another platform_site.

        @param platform_site    PlatformSite
        @param parent_id    str
        @retval platform_site_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """

        # if the geospatial_bounds is set then calculate the geospatial_point_center
        self._calc_geospatial_point_center(platform_site)

        platform_site_id = self.RR2.create(platform_site, RT.PlatformSite)

        if parent_id:
            self.RR2.assign_site_to_one_site_with_has_site(platform_site_id, parent_id)

        return platform_site_id

    def read_platform_site(self, platform_site_id=''):
        """Read a PlatformSite resource

        @param platform_site_id    str
        @retval platform_site    PlatformSite
        @throws NotFound    object with specified id does not exist
        """
        return self.RR2.read(platform_site_id, RT.PlatformSite)

    def update_platform_site(self, platform_site=None):
        """Update a PlatformSite resource

        @param platform_site    PlatformSite
        @throws NotFound    object with specified id does not exist
        """
        # if the geospatial_bounds is set then calculate the geospatial_point_center
        self._calc_geospatial_point_center(platform_site)

        return self.RR2.update(platform_site, RT.PlatformSite)

    def delete_platform_site(self, platform_site_id=''):
        """Delete a PlatformSite resource, removes assocations to parents

        @param platform_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.RR2.lcs_delete(platform_site_id, RT.PlatformSite)

    def force_delete_platform_site(self, platform_site_id=''):
        self.RR2.force_delete(platform_site_id, RT.PlatformSite)


    def create_instrument_site(self, instrument_site=None, parent_id=''):
        """Create a InstrumentSite resource. A instrument_site is a frame of reference within an observatory. Its parent is
        either the observatory or another instrument_site.

        @param instrument_site    InstrumentSite
        @param parent_id    str
        @retval instrument_site_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        # if the geospatial_bounds is set then calculate the geospatial_point_center
        self._calc_geospatial_point_center(instrument_site)

        instrument_site_id = self.RR2.create(instrument_site, RT.InstrumentSite)

        if parent_id:
            self.RR2.assign_site_to_one_site_with_has_site(instrument_site_id, parent_id)

        return instrument_site_id

    def read_instrument_site(self, instrument_site_id=''):
        """Read a InstrumentSite resource

        @param instrument_site_id    str
        @retval instrument_site    InstrumentSite
        @throws NotFound    object with specified id does not exist
        """
        return self.RR2.read(instrument_site_id, RT.InstrumentSite)

    def update_instrument_site(self, instrument_site=None):
        """Update a InstrumentSite resource

        @param instrument_site    InstrumentSite
        @throws NotFound    object with specified id does not exist
        """
        # if the geospatial_bounds is set then calculate the geospatial_point_center
        self._calc_geospatial_point_center(instrument_site)

        return self.RR2.update(instrument_site, RT.InstrumentSite)

    def delete_instrument_site(self, instrument_site_id=''):
        """Delete a InstrumentSite resource, removes assocations to parents

        @param instrument_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.RR2.lcs_delete(instrument_site_id, RT.InstrumentSite)

    def force_delete_instrument_site(self, instrument_site_id=''):
        self.RR2.force_delete(instrument_site_id, RT.InstrumentSite)



    def create_deployment(self, deployment=None, site_id="", device_id=""):
        """
        Create a Deployment resource. Represents a (possibly open-ended) time interval
        grouping one or more resources within a given context, such as an instrument
        deployment on a platform at an observatory site.
        """

        deployment_id = self.RR2.create(deployment, RT.Deployment)

        #Verify that site and device exist, add links if they do
        if site_id:
            site_obj = self.RR2.read(site_id)
            if site_obj:
                self.assign_site_to_deployment(site_id=site_id, deployment_id=deployment_id)

        if device_id:

            device_obj = self.RR2.read(device_id)
            if device_obj:
                self.assign_device_to_deployment(device_id=device_id, deployment_id=deployment_id)

        return deployment_id

    def update_deployment(self, deployment=None):
        # Overwrite Deployment object
        self.RR2.update(deployment, RT.Deployment)

    def read_deployment(self, deployment_id=''):
        deployment_obj = self.RR2.read(deployment_id, RT.Deployment)

        return deployment_obj

    def delete_deployment(self, deployment_id=''):
        """
        Delete a Deployment resource
        """
        self.RR2.lcs_delete(deployment_id, RT.Deployment)

    def force_delete_deployment(self, deployment_id=''):
        self.RR2.force_delete(deployment_id, RT.Deployment)


    ############################
    #
    #  ASSOCIATIONS
    #
    ############################


    def assign_site_to_site(self, child_site_id='', parent_site_id=''):
        """Connects a child site (any subtype) to a parent site (any subtype)

        @param child_site_id    str
        @param parent_site_id    str
        @throws NotFound    object with specified id does not exist
        """

        self.RR2.assign_site_to_site_with_has_site(child_site_id, parent_site_id)


    def unassign_site_from_site(self, child_site_id='', parent_site_id=''):
        """Disconnects a child site (any subtype) from a parent site (any subtype)

        @param child_site_id    str
        @param parent_site_id    str
        @throws NotFound    object with specified id does not exist
        """

        self.RR2.unassign_site_from_site_with_has_site(child_site_id, parent_site_id)


    def assign_device_to_site(self, device_id='', site_id=''):
        """Connects a device (any type) to a site (any subtype)

        @param device_id    str
        @param site_id    str
        @throws NotFound    object with specified id does not exist
        """

        self.RR2.assign_device_to_site_with_has_device(device_id, site_id)

    def unassign_device_from_site(self, device_id='', site_id=''):
        """Disconnects a device (any type) from a site (any subtype)

        @param device_id    str
        @param site_id    str
        @throws NotFound    object with specified id does not exist
        """

        self.RR2.unassign_device_from_site_with_has_device(device_id, site_id)

    def _update_device_add_geo_add_temporal(self, device_id='', site_id='', deployment_obj=''):
        """Assigns to device:
               temporal extent from deployment
               geo location from site

        @param device_id    str
        @param site_id    str
        @param deployment_obj Deployment
        @throws NotFound    object with specified id does not exist
        """
        device_obj = self.RR.read(device_id)
        site_obj = self.RR.read(site_id)
        for constraint in site_obj.constraint_list:
            if constraint.type_ == OT.GeospatialBounds:
                device_obj.geospatial_bounds = GeoUtils.calc_geo_bounds_for_geo_bounds_list(
                    [device_obj.geospatial_bounds, constraint])
        for constraint in deployment_obj.constraint_list:
            if constraint.type_ == OT.TemporalBounds:
                device_obj.temporal_bounds = GeoUtils.calc_temp_bounds_for_temp_bounds_list(
                    [device_obj.temporal_bounds, constraint])
        self.RR.update(device_obj)

    def _update_device_remove_geo_update_temporal(self, device_id='', temporal_constraint=None):
        """Remove the geo location and update temporal extent (end) from the device

        @param device_id    str
        @param site_id    str
        @throws NotFound    object with specified id does not exist
        """
        device_obj = self.RR.read(device_id)
        bounds = GeospatialBounds(geospatial_latitude_limit_north=float(0),
                                  geospatial_latitude_limit_south=float(0),
                                  geospatial_longitude_limit_west=float(0),
                                  geospatial_longitude_limit_east=float(0),
                                  geospatial_vertical_min=float(0),
                                  geospatial_vertical_max=float(0))
        device_obj.geospatial_bounds = bounds
        if temporal_constraint:
            device_obj.temporal_bounds.end_datetime = GeoUtils.calc_temp_bounds_for_temp_bounds_list(
                [device_obj.temporal_bounds, temporal_constraint])
        self.RR.update(device_obj)

    def _get_bounds_from_object(self, obj=''):
        temporal   = None
        geographic = None
        for constraint in obj.constraint_list:
            if constraint.type_ == OT.TemporalBounds:
                temporal = constraint
            if constraint.type_ == OT.GeospatialBounds:
                geographic = constraint
        return temporal, geographic

    def assign_device_to_network_parent(self, child_device_id='', parent_device_id=''):
        """Connects a device (any type) to parent in the RSN network

        @param child_device_id    str
        @param parent_device_id    str
        @throws NotFound    object with specified id does not exist
        """

        self.RR2.assign_device_to_one_device_with_has_network_parent(parent_device_id, child_device_id)


    def unassign_device_from_network_parent(self, child_device_id='', parent_device_id=''):
        """Disconnects a child device (any type) from parent in the RSN network

        @param child_device_id    str
        @param parent_device_id    str
        @throws NotFound    object with specified id does not exist
        """

        self.RR2.unassign_device_from_device_with_has_network_parent(parent_device_id, child_device_id)



    def assign_instrument_model_to_instrument_site(self, instrument_model_id='', instrument_site_id=''):
        self.RR2.assign_instrument_model_to_instrument_site_with_has_model(instrument_model_id, instrument_site_id)

    def unassign_instrument_model_from_instrument_site(self, instrument_model_id='', instrument_site_id=''):
        self.RR2.unassign_instrument_model_from_instrument_site_with_has_model(instrument_model_id, instrument_site_id)

    def assign_platform_model_to_platform_site(self, platform_model_id='', platform_site_id=''):
        self.RR2.assign_platform_model_to_platform_site_with_has_model(platform_model_id, platform_site_id)

    def unassign_platform_model_from_platform_site(self, platform_model_id='', platform_site_id=''):
        self.RR2.unassign_platform_model_from_platform_site_with_has_model(platform_model_id, platform_site_id)

    def assign_resource_to_observatory_org(self, resource_id='', org_id=''):
        if not org_id:
            raise BadRequest("Org id not given")
        if not resource_id:
            raise BadRequest("Resource id not given")

        #log.trace("assign_resource_to_observatory_org: org_id=%s, resource_id=%s ", org_id, resource_id)
        self.clients.org_management.share_resource(org_id, resource_id)

    def unassign_resource_from_observatory_org(self, resource_id='', org_id=''):
        if not org_id:
            raise BadRequest("Org id not given")
        if not resource_id:
            raise BadRequest("Resource id not given")

        self.clients.org_management.unshare_resource(org_id, resource_id)


    ##########################################################################
    #
    # DEPLOYMENTS
    #
    ##########################################################################


    def _get_deployment_assocs(self, deployment_id):
        res_ids, assocs = self.RR.find_subjects(predicate=PRED.hasDeployment, object=deployment_id, id_only=True)
        assoc_by_type = dict(Site=[], Device=[])
        for a in assocs:
            if a.st not in assoc_by_type:
                assoc_by_type[a.st] = []
            assoc_by_type[a.st].append(a)
            if a.st.endswith("Device"):
                assoc_by_type["Device"].append(a)
            if a.st.endswith("Site"):
                assoc_by_type["Site"].append(a)
        return assoc_by_type

    def assign_device_to_deployment(self, device_id='', deployment_id=''):
        device = self.RR.read(device_id)
        dep_assocs = self._get_deployment_assocs(deployment_id)
        if dep_assocs["Device"]:
            raise BadRequest("Deployment %s - Cannot have more than 1 Device" % deployment_id)
        if device.type_ == RT.InstrumentDevice:
            self.RR2.assign_deployment_to_instrument_device_with_has_deployment(deployment_id, device_id)
            if dep_assocs["Site"] and dep_assocs["Site"][0].st != RT.InstrumentSite:
                raise BadRequest("Deployment %s - Device %s (%s) incompatible with associated Site %s (%s)" % (
                    deployment_id, device_id, device.type_, dep_assocs["Site"][0].s, dep_assocs["Site"][0].st))
        elif device.type_ == RT.PlatformDevice:
            self.RR2.assign_deployment_to_platform_device_with_has_deployment(deployment_id, device_id)
            if dep_assocs["Site"] and dep_assocs["Site"][0].st != RT.PlatformSite:
                raise BadRequest("Deployment %s - Device %s (%s) incompatible with associated Site %s (%s)" % (
                    deployment_id, device_id, device.type_, dep_assocs["Site"][0].s, dep_assocs["Site"][0].st))
        else:
            raise BadRequest("Illegal resource type to assign to Deployment: %s" % device.type_)

    def unassign_device_from_deployment(self, device_id='', deployment_id=''):
        device = self.RR.read(device_id)
        if device.type_ == RT.InstrumentDevice:
            self.RR2.unassign_deployment_from_instrument_device_with_has_deployment(deployment_id, device_id)
        elif device.type_ == RT.PlatformDevice:
            self.RR2.unassign_deployment_from_platform_device_with_has_deployment(deployment_id, device_id)
        else:
            raise BadRequest("Illegal resource type to assign to Deployment: %s" % device.type_)

    def assign_site_to_deployment(self, site_id='', deployment_id=''):
        site = self.RR.read(site_id)
        dep_assocs = self._get_deployment_assocs(deployment_id)
        if dep_assocs["Site"]:
            raise BadRequest("Deployment %s - Cannot have more than 1 Site" % deployment_id)
        if site.type_ == RT.InstrumentSite:
            self.RR2.assign_deployment_to_instrument_site_with_has_deployment(deployment_id, site_id)
            if dep_assocs["Device"] and dep_assocs["Device"][0].st != RT.InstrumentDevice:
                raise BadRequest("Deployment %s - Site %s (%s) incompatible with associated Device %s (%s)" % (
                    deployment_id, site_id, site.type_, dep_assocs["Device"][0].s, dep_assocs["Device"][0].st))
        elif site.type_ == RT.PlatformSite:
            self.RR2.assign_deployment_to_platform_site_with_has_deployment(deployment_id, site_id)
            if dep_assocs["Device"] and dep_assocs["Device"][0].st != RT.PlatformDevice:
                raise BadRequest("Deployment %s - Site %s (%s) incompatible with associated Device %s (%s)" % (
                    deployment_id, site_id, site.type_, dep_assocs["Device"][0].s, dep_assocs["Device"][0].st))
        else:
            raise BadRequest("Illegal resource type to assign to Deployment: %s" % site.type_)

    def unassign_site_from_deployment(self, site_id='', deployment_id=''):
        site = self.RR.read(site_id)
        if site.type_ == RT.InstrumentSite:
            self.RR2.unassign_deployment_from_instrument_site_with_has_deployment(deployment_id, site_id)
        elif site.type_ == RT.PlatformSite:
            self.RR2.unassign_deployment_from_platform_site_with_has_deployment(deployment_id, site_id)
        else:
            raise BadRequest("Illegal resource type to assign to Deployment: %s" % site.type_)

    def activate_deployment(self, deployment_id='', activate_subscriptions=False):
        """
        Make the devices on this deployment the primary devices for the sites
        """
        dep_util = DeploymentUtil(self.container)

        # Verify that the deployment exists
        deployment_obj = self.RR2.read(deployment_id)
        log.info("Activating deployment %s '%s'", deployment_id, deployment_obj.name)

        # Find an existing primary deployment
        dep_site_id, dep_dev_id = dep_util.get_deployment_relations(deployment_id)
        active_dep = dep_util.get_site_primary_deployment(dep_site_id)
        if active_dep and active_dep._id == deployment_id:
            raise BadRequest("Deployment %s already active for site %s" % (deployment_id, dep_site_id))

        self.deploy_planner = DeploymentPlanner(self.clients)

        pairs_to_remove, pairs_to_add = self.deploy_planner.prepare_activation(deployment_obj)
        log.debug("activate_deployment  pairs_to_add: %s", pairs_to_add)
        log.debug("activate_deployment  pairs_to_remove: %s", pairs_to_remove)

        if not pairs_to_add:
            log.warning('No Site and Device pairs were added to activate this deployment')

        temp_constraint = dep_util.get_temporal_constraint(deployment_obj)

        # process any removals
        for site_id, device_id in pairs_to_remove:
            log.info("Unassigning hasDevice; device '%s' from site '%s'", device_id, site_id)
            self.unassign_device_from_site(device_id, site_id)
            log.info("Removing geo and updating temporal attrs for device '%s'", device_id)
            self._update_device_remove_geo_update_temporal(device_id, temp_constraint)

            # Sever the connection between dev/site and the primary deployment
            assocs = self.clients.resource_registry.find_associations(device_id, PRED.hasPrimaryDeployment, deployment_id)
            for assoc in assocs:
                self.RR.delete_association(assoc)
            assocs = self.clients.resource_registry.find_associations(site_id, PRED.hasPrimaryDeployment, deployment_id)
            for assoc in assocs:
                self.RR.delete_association(assoc)

        # process the additions
        for site_id, device_id in pairs_to_add:
            log.info("Setting primary device '%s' for site '%s'", device_id, site_id)
            self.assign_device_to_site(device_id, site_id)
            log.info("Adding geo and updating temporal attrs for device '%s'", device_id)
            self._update_device_add_geo_add_temporal(device_id, site_id, deployment_obj)
            site_obj = self.RR2.read(site_id)
            dev_obj = self.RR2.read(device_id)

            # Make this deployment Primary for every device and site
            self.RR.create_association(subject=device_id, predicate=PRED.hasPrimaryDeployment, object=deployment_id, assoc_type=RT.Deployment)
            self.RR.create_association(subject=site_id,   predicate=PRED.hasPrimaryDeployment, object=deployment_id, assoc_type=RT.Deployment)

            # Add a withinDeployment association from Device to Deployment
            # so the entire history of a Device can be found.
            self.RR.create_association(subject=device_id, predicate=PRED.withinDeployment, object=deployment_id, assoc_type=RT.Deployment)

            sdps, _  = self.RR.find_objects(subject=site_id, predicate=PRED.hasOutputProduct, object_type=RT.DataProduct, id_only=False)
            sdps_ids = [s._id for s in sdps] # Get a list of Site Data Product IDs
            sdps_streams, _ = self.RR.find_objects_mult(subjects=sdps_ids, predicate=PRED.hasStream, id_only=False)

            dpds, _ = self.RR.find_objects(subject=device_id, predicate=PRED.hasOutputProduct, object_type=RT.DataProduct, id_only=False)
            dps_ids = [d._id for d in dpds] # Get a list of device data product ids
            dps_streams, _ = self.RR.find_objects_mult(subjects=dps_ids, predicate=PRED.hasStream, id_only=False)

            # Match SDPs to DDPs to get dataset_id and update the dataset_windows.
            if not sdps_ids and log.isEnabledFor(logging.DEBUG):
                log.debug("Not updating data_windows on Site '%s'... no SiteDataProducts were found." % site_id)

            for sdp in sdps:
                if not sdp.ingest_stream_name:
                    log.warning("Unable to pair site data product %s without an ingest stream name", sdp.name)
                    continue # Ingest stream name isn't defined

                for dpd in dpds:
                    # breakpoint(locals(), globals())
                    if sdp.ingest_stream_name == dpd.ingest_stream_name:

                        # Update the window list in the resource
                        site_dataset_id = self.RR2.find_object(sdp._id, PRED.hasDataset, id_only=True)
                        device_dataset_id = self.RR2.find_object(dpd._id, PRED.hasDataset, id_only=True)
                        bounds = TemporalBounds(start_datetime=temp_constraint.start_datetime, end_datetime=str(calendar.timegm(datetime(2038,1,1).utctimetuple())))
                        window = DatasetWindow(dataset_id=device_dataset_id, bounds=bounds)
                        sdp.dataset_windows.append(window)
                        self.clients.data_product_management.update_data_product(sdp)

                        # TODO: Once coverages support None for open intervals on complex, we'll change it
                        # in the man time, 2038 is pretty far out, and the world will end shortly after, so
                        # it's pretty good for an arbitrary point in the future
                        start = int(temp_constraint.start_datetime) + 2208988800
                        end   = calendar.timegm(datetime(2038,1,1).utctimetuple()) + 2208988800

                        self.clients.dataset_management.add_dataset_window_to_complex(device_dataset_id, (start, end), site_dataset_id)

                        dp_params = self.clients.data_product_management.get_data_product_parameters(dpd._id, id_only=False)
                        # print [d.name for d in dp_params]
                        for param in dp_params:
                            if 'lat' in param.name and param.parameter_type == 'sparse':
                                # Update sparse lat/lon data with site lat/lon
                                site_obj = self.RR.read(site_id)

                                # Search for GeospatialBounds bbox constraint
                                for constraint in site_obj.constraint_list:
                                    if constraint.type_ == OT.GeospatialBounds:
                                        # Get the midpoint of the site geospatial bounds
                                        mid_point = GeoUtils.calc_geospatial_point_center(constraint)

                                        # Create granule using midpoint
                                        stream_def_id, _ = self.RR.find_objects(subject=dpd, predicate=PRED.hasStreamDefinition, id_only=True)
                                        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id[0])
                                        rdt['time'] = [start]
                                        rdt['lat'] = [mid_point['lat']]
                                        rdt['lon'] = [mid_point['lon']]

                                        ParameterHelper.publish_rdt_to_data_product(dpd, rdt)

        if deployment_obj.lcstate != LCS.DEPLOYED:
            self.RR.execute_lifecycle_transition(deployment_id, LCE.DEPLOY)
        else:
            log.warn("Deployment %s was already DEPLOYED when activated", deployment_obj._id)

        if active_dep:
            log.info("activate_deployment(): Deactivating prior Deployment %s at site %s" % (active_dep._id, dep_site_id))
            # Set Deployment end date
            olddep_tc = dep_util.get_temporal_constraint(active_dep)
            newdep_tc = dep_util.get_temporal_constraint(deployment_obj)
            if float(olddep_tc.end_datetime) > float(newdep_tc.start_datetime):
                # Set to new deployment start date
                dep_util.set_temporal_constraint(active_dep, end_time=newdep_tc.start_datetime)
                self.RR.update(active_dep)

            # Change LCS
            if active_dep.lcstate == LCS.DEPLOYED:
                self.RR.execute_lifecycle_transition(active_dep._id, LCE.INTEGRATE)
            else:
                log.warn("Prior Deployment %s was not in DEPLOYED lcstate", active_dep._id)


    def deactivate_deployment(self, deployment_id=''):
        """Remove the primary device designation for the deployed devices at the sites

        @param deployment_id    str
        @throws NotFound    object with specified id does not exist
        @throws BadRequest    if devices can not be undeployed
        """

        #Verify that the deployment exists
        deployment_obj = self.RR2.read(deployment_id)
        dep_util = DeploymentUtil(self.container)

        if deployment_obj.lcstate != LCS.DEPLOYED:
            log.warn("deactivate_deployment(): Deployment %s is not DEPLOYED" % deployment_id)
#            raise BadRequest("This deployment is not active")

        # get all associated components
        self.deploy_planner = DeploymentPlanner(self.clients)
        site_ids, device_ids = self.deploy_planner.get_deployment_sites_devices(deployment_obj)

        dep_util.set_temporal_constraint(deployment_obj, end_time=DeploymentUtil.DATE_NOW)
        self.RR.update(deployment_obj)
        temp_constraint = dep_util.get_temporal_constraint(deployment_obj)

        # delete only associations where both site and device have passed the filter
        for s in site_ids:
            dataset_ids = []
            ds, _ = self.RR.find_objects(s, PRED.hasDevice, id_only=True)
            for d in ds:
                if d in device_ids:
                    a = self.RR.get_association(s, PRED.hasDevice, d)
                    self.RR.delete_association(a)
                    log.info("Removing geo and updating temporal attrs for device '%s'", d)
                    self._update_device_remove_geo_update_temporal(d, temp_constraint)
                    try:
                        self.RR.execute_lifecycle_transition(d, LCE.INTEGRATE)
                    except BadRequest:
                        log.warn("Could not set device %s lcstate to INTEGRATED", d)

                    primary_d = self.RR.find_associations(subject=d,  predicate=PRED.hasPrimaryDeployment, object=deployment_id)
                    if primary_d:
                        self.RR.delete_association(primary_d[0])
                    primary_s = self.RR.find_associations(subject=s,  predicate=PRED.hasPrimaryDeployment, object=deployment_id)
                    if primary_s:
                        self.RR.delete_association(primary_s[0])

                    # Get Dataset IDs for a Device
                    dps, _         = self.RR.find_objects(subject=d, predicate=PRED.hasOutputProduct, id_only=True)
                    dataset_ids, _ = self.RR.find_objects_mult(subjects=dps, predicate=PRED.hasDataset, id_only=True)

            dataset_ids = list(set(dataset_ids))

            # Get the Deployment time bounds as datetime objects
            temporal, geographc = self._get_bounds_from_object(obj=deployment_obj)

            # Set the ending of the appropriate dataset_windows.  Have to search by dataset_id because we are
            # not creating any new resources for the dataset_window logic!
            site_dps, _ = self.RR.find_objects(s, PRED.hasOutputProduct, id_only=True)
            for dp in site_dps:
                site_data_product = self.RR.read(dp)
                # This is assuming that data_windows is ALWAYS kept IN ORDER (Ascending).
                # There should NEVER be a situation where there are two dataset_window
                # attribute missing an 'ending' value.  If there is, it wasn't deactivated
                # properly.
                for window in site_data_product.dataset_windows:
                    if window.dataset_id in dataset_ids:
                        
                        # Set up the tuples of start and stops
                        old_start = int(window.bounds.start_datetime) + 2208988800
                        old_end   = int(window.bounds.end_datetime)   + 2208988800
                        new_start = old_start
                        new_end   = int(temporal.end_datetime)        + 2208988800

                        # Update the data product resource
                        window.bounds.end_datetime = temporal.end_datetime
                        site_dataset_id = self.RR2.find_object(site_data_product._id, PRED.hasDataset, id_only=True)
                        device_dataset_id = window.dataset_id

                        # Update the dataset 
                        self.clients.dataset_management.update_dataset_window_for_complex(device_dataset_id, (old_start, old_end), (new_start, new_end), site_dataset_id)

                        break
                self.clients.data_product_management.update_data_product(site_data_product)


        # This should set the deployment resource to retired.
        # Michael needs to fix the RR retire logic so it does not
        # retire all associations before we can use it. Currently we switch
        # back to INTEGRATE.
        #self.RR.execute_lifecycle_transition(deployment_id, LCE.RETIRE)
        # mark deployment as not deployed (developed seems appropriate)
        if deployment_obj.lcstate == LCS.DEPLOYED:
            self.RR.execute_lifecycle_transition(deployment_id, LCE.INTEGRATE)
        else:
            log.warn("Deployment %s was not in DEPLOYED lcstate", deployment_id)

    def prepare_deployment_support(self, deployment_id=''):
        extended_resource_handler = ExtendedResourceContainer(self)

        resource_data = extended_resource_handler.create_prepare_resource_support(deployment_id, OT.DeploymentPrepareSupport)

        #Fill out service request information for creating a instrument agent instance
        extended_resource_handler.set_service_requests(resource_data.create_request, 'observatory_management',
            'create_deployment', { "deployment":  "$(deployment)" })

        #Fill out service request information for creating a instrument agent instance
        extended_resource_handler.set_service_requests(resource_data.update_request, 'observatory_management',
            'update_deployment', { "deployment":  "$(deployment)" })

        #Fill out service request information for assigning a InstrumentDevice
        extended_resource_handler.set_service_requests(resource_data.associations['DeploymentHasInstrumentDevice'].assign_request, 'observatory_management',
            'assign_device_to_deployment', {"device_id":  "$(instrument_device_id)",
                                            "deployment_id":  deployment_id })

        #Fill out service request information for assigning a PlatformDevice
        extended_resource_handler.set_service_requests(resource_data.associations['DeploymentHasPlatformDevice'].assign_request, 'observatory_management',
            'assign_device_to_deployment', {"device_id":  "$(platform_device_id)",
                                            "deployment_id":  deployment_id })

        #Fill out service request information for unassigning a InstrumentDevice
        extended_resource_handler.set_service_requests(resource_data.associations['DeploymentHasInstrumentDevice'].unassign_request, 'observatory_management',
            'unassign_device_from_deployment', {"device_id":  "$(instrument_device_id)",
                                                "deployment_id":  deployment_id })

        #Fill out service request information for unassigning a PlatformDevice
        extended_resource_handler.set_service_requests(resource_data.associations['DeploymentHasPlatformDevice'].unassign_request, 'observatory_management',
            'unassign_device_from_deployment', {"device_id":  "$(platform_device_id)",
                                                "deployment_id":  deployment_id })

        #Fill out service request information for assigning a InstrumentSite
        extended_resource_handler.set_service_requests(resource_data.associations['DeploymentHasInstrumentSite'].assign_request, 'observatory_management',
            'assign_site_to_deployment', {"site_id":  "$(instrument_site_id)",
                                          "deployment_id":  deployment_id })

        #Fill out service request information for assigning a PlatformSite
        extended_resource_handler.set_service_requests(resource_data.associations['DeploymentHasPlatformSite'].assign_request, 'observatory_management',
            'assign_site_to_deployment', {"site_id":  "$(platform_site_id)",
                                          "deployment_id":  deployment_id })

        #Fill out service request information for unassigning a InstrumentSite
        extended_resource_handler.set_service_requests(resource_data.associations['DeploymentHasInstrumentSite'].unassign_request, 'observatory_management',
            'unassign_site_from_deployment', {"site_id":  "$(instrument_site_id)",
                                              "deployment_id":  deployment_id })

        #Fill out service request information for unassigning a PlatformSite
        extended_resource_handler.set_service_requests(resource_data.associations['DeploymentHasPlatformSite'].unassign_request, 'observatory_management',
            'unassign_site_from_deployment', {"site_id":  "$(platform_site_id)",
                                              "deployment_id":  deployment_id })

        return resource_data


    ##########################################################################
    #
    # FIND OPS
    #
    ##########################################################################



    def find_org_by_observatory(self, observatory_id=''):
        """
        """
        orgs,_ = self.RR.find_subjects(RT.Org, PRED.hasResource, observatory_id, id_only=False)
        return orgs


    def find_related_frames_of_reference(self, input_resource_id='', output_resource_type_list=None):

        # use the related resources crawler
        finder = RelatedResourcesCrawler()

        # generate the partial function (cached association list)
        get_assns = finder.generate_related_resources_partial(self.RR, [PRED.hasSite])

        # run 2 searches allowing all site-based resource types: one down (subj-obj), one up (obj-subj)
        full_crawllist = [RT.InstrumentSite, RT.PlatformSite, RT.Subsite, RT.Observatory]
        search_down = get_assns({PRED.hasSite: (True, False)}, full_crawllist)
        search_up = get_assns({PRED.hasSite: (False, True)}, full_crawllist)

        # the searches return a list of association objects, so compile all the ids by extracting them
        retval_ids = set([])

        # we want only those IDs that are not the input resource id
        for a in search_down(input_resource_id, -1) + search_up(input_resource_id, -1):
            if a.o not in retval_ids and a.o != input_resource_id:
                retval_ids.add(a.o)
            if a.s not in retval_ids and a.s != input_resource_id:
                retval_ids.add(a.s)


        log.trace("converting retrieved ids to objects = %s" % retval_ids)
        #initialize the dict
        retval = dict((restype, []) for restype in output_resource_type_list)

        #workaround for read_mult problem
        all_res = []
        if retval_ids: all_res = self.RR.read_mult(list(retval_ids))
        #all_res = self.RR.read_mult(retval_ids)

        # put resources in the slot based on their type
        for resource in all_res:
            typename = type(resource).__name__
            if typename in output_resource_type_list:
                retval[typename].append(resource)

        # display a count of how many resources we retrieved
        log.debug("got these resources: %s", dict([(k, len(v)) for k, v in retval.iteritems()]))

        return retval

    def find_related_sites(self, parent_resource_id='', exclude_site_types=None, include_parents=False,
                           include_devices=False, id_only=False):
        if not parent_resource_id:
            raise BadRequest("Must provide a parent parent_resource_id")
        exclude_site_types = exclude_site_types or []
        if not isinstance(exclude_site_types, list):
            raise BadRequest("exclude_site_types mut be a list, is: %s" % type(exclude_site_types))

        parent_resource = self.RR.read(parent_resource_id)

        org_id, site_id = None, None
        if parent_resource.type_ == RT.Org:
            org_id = parent_resource_id
        elif RT.Site in parent_resource._get_extends():
            site_id = parent_resource_id
        else:
            raise BadRequest("Illegal parent_resource_id type. Expected Org/Site, given:%s" % parent_resource.type_)

        RR2 = EnhancedResourceRegistryClient(self.RR)
        RR2.cache_resources(RT.Observatory)
        RR2.cache_resources(RT.PlatformSite)
        RR2.cache_resources(RT.InstrumentSite)
        if include_devices:
            RR2.cache_resources(RT.PlatformDevice)
            RR2.cache_resources(RT.InstrumentDevice)
        outil = ObservatoryUtil(self, enhanced_rr=RR2)

        site_resources, site_children = outil.get_child_sites(site_id, org_id,
                                   exclude_types=exclude_site_types, include_parents=include_parents, id_only=id_only)

        site_devices, device_resources = None, None
        if include_devices:
            site_devices = outil.get_device_relations(site_children.keys())
            device_list = list({tup[1] for key,dev_list in site_devices.iteritems() if dev_list for tup in dev_list})
            device_resources = RR2.read_mult(device_list)

            # HACK:
            dev_by_id = {dev._id: dev for dev in device_resources}
            site_resources.update(dev_by_id)


        return site_resources, site_children, site_devices, device_resources


    def get_sites_devices_status(self, parent_resource_ids=None, include_sites=False, include_devices=False, include_status=False):
        if not parent_resource_ids:
            raise BadRequest("Must provide a parent parent_resource_id")

        result_dict = {}

        RR2 = EnhancedResourceRegistryClient(self.RR)
        RR2.cache_resources(RT.Observatory)
        RR2.cache_resources(RT.PlatformSite)
        RR2.cache_resources(RT.InstrumentSite)
        RR2.cache_resources(RT.PlatformDevice)
        RR2.cache_resources(RT.InstrumentDevice)
        outil = ObservatoryUtil(self, enhanced_rr=RR2, device_status_mgr=DeviceStateManager())
        parent_resource_objs = RR2.read_mult(parent_resource_ids)
        res_by_id = dict(zip(parent_resource_ids, parent_resource_objs))

        # Loop thru all the provided site ids and create the result structure
        for parent_resource_id in parent_resource_ids:

            parent_resource = res_by_id[parent_resource_id]

            org_id, site_id = None, None
            if parent_resource.type_ == RT.Org:
                org_id = parent_resource_id
            elif RT.Site in parent_resource._get_extends():
                site_id = parent_resource_id

            site_result_dict = {}

            site_resources, site_children = outil.get_child_sites(site_id, org_id, include_parents=True, id_only=False)
            if include_sites:
                site_result_dict["site_resources"] = site_resources
                site_result_dict["site_children"] = site_children

            all_device_statuses = {}
            if include_devices or include_status:
                RR2.cache_predicate(PRED.hasSite)
                RR2.cache_predicate(PRED.hasDevice)
                all_device_statuses = outil.get_status_roll_ups(parent_resource_id)

            if include_status:
                #add code to grab the master status table to pass in to the get_status_roll_ups calc
                log.debug('get_sites_devices_status site master_status_table:   %s ', all_device_statuses)
                site_result_dict["site_status"] = all_device_statuses

                #create the aggreagate_status for each device and site

                log.debug("calculate site aggregate status")
                site_status = [all_device_statuses.get(x,{}).get('agg',DeviceStatusType.STATUS_UNKNOWN) for x in site_children.keys()]
                site_status_dict = dict(zip(site_children.keys(), site_status))

                log.debug('get_sites_devices_status  site_status_dict:   %s ', site_status_dict)
                site_result_dict["site_aggregate_status"] = site_status_dict


            if include_devices:
                log.debug("calculate device aggregate status")
                inst_status = [all_device_statuses.get(x,{}).get('agg',DeviceStatusType.STATUS_UNKNOWN) for x in all_device_statuses.keys()]
                device_agg_status_dict = dict(zip(all_device_statuses.keys(), inst_status))
                log.debug('get_sites_devices_status  device_agg_status_dict:   %s ', device_agg_status_dict)
                site_result_dict["device_aggregate_status"] = device_agg_status_dict

            result_dict[parent_resource_id] = site_result_dict

        return result_dict

    def find_site_data_products(self, parent_resource_id='', include_sites=False, include_devices=False,
                                include_data_products=False):
        if not parent_resource_id:
            raise BadRequest("Must provide a parent parent_resource_id")

        outil = ObservatoryUtil(self)
        res_dict = outil.get_site_data_products(parent_resource_id, include_sites=include_sites,
                                                     include_devices=include_devices,
                                                     include_data_products=include_data_products)

        return res_dict

    # -------------------------------------------------------------------------
    #   Marine Asset Management RESOURCES (start)
    # -------------------------------------------------------------------------
    #   AssetType
    def create_asset_type(self, asset_type=None):
        """Create a AssetType resource.

        @param asset_type           RT.AssetType
        @retval asset_type_id       str
        @throws: BadRequest 'asset_type object is empty'
        """
        if not asset_type:
            raise BadRequest('asset_type object is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        asset_type_id = at.create_asset_type(asset_type)

        return asset_type_id

    def read_asset_type(self, asset_type_id=''):
        """Read an AssetType resource.

        @param asset_type_id    str
        @retval asset_type      RT.AssetType
        @throws: BadRequest 'asset_type_id parameter is empty'
        """
        if not asset_type_id:
            raise BadRequest('asset_type_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        asset_type = at.read_asset_type(asset_type_id)

        return asset_type

    def update_asset_type(self, asset_type=None):
        """Update an AssetType resource.

        @param asset_type   RT.AssetType
        @throws: BadRequest 'asset_type object is empty'
        """
        if not asset_type:
            raise BadRequest('asset_type object is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        obj = at.update_asset_type(asset_type)

        return obj

    def delete_asset_type(self, asset_type_id=''):
        """Delete an AssetType resource.

        @param asset_type_id    str
        @throws: BadRequest     'asset_type_id parameter is empty'
        """
        if not asset_type_id:
            raise BadRequest('asset_type_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.delete_asset_type(asset_type_id)

    def force_delete_asset_type(self, asset_type_id=''):
        """Force delete an AssetType resource
        @param asset_type_id    str
        @throws: BadRequest     'asset_type_id parameter is empty'
        """
        if not asset_type_id:
            raise BadRequest('asset_type_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.force_delete_asset_type(asset_type_id)

    def update_attribute_specifications(self, resource_id='', spec_dict=None):
        """ Update attribute_specifications of resource using spec_dict provided.

        @param resource_id   str        # id of RT.Asset or RT.EventDurationType
        @param spec_dict     []         # list of attribute specification name(s)
        @throws BadRequest   'resource_id parameter is empty'
        @throws BadRequest   'spec_dict parameter is empty'
        @throws Inconsistent unable to process resource of this type
        """
        # TODO NOTE: Must abide by state restriction model
        # Updating attribute_specification is dependent on state (i.e. if in integrated or deployment state,
        # updates are not permitted unless the operator has privileges to do so.
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')

        if not spec_dict:
            raise BadRequest('spec_dict parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.update_attribute_specifications(resource_id, spec_dict)

    def delete_attribute_specification(self, resource_id='', attr_spec_names=None):
        """Delete attribute_specifications in list of attr_spec_names and return the
        TypeResource attribute_specifications dictionary for resource_id.

        @param resource_id          str     # id of RT.Asset or RT.EventDurationType
        @param attr_spec_names      []      # list of attribute specification name(s)
        @retval r_obj               {}      # dictionary of attribute specification(s)
        @throws BadRequest      'resource_id parameter is empty'
        @throws BadRequest      'attr_spec_names parameter is empty'
        """
        # TODO NOTE: Must abide by state restriction model
        # Delete attribute_specifications in list of attr_spec_names and return the
        # TypeResource attribute_specifications dictionary for resource_id.

        if not resource_id:
            raise BadRequest('resource_id parameter is empty')

        if not attr_spec_names:
            raise BadRequest('attr_spec_names parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        r_obj = at.delete_attribute_specification(resource_id, attr_spec_names)

        return r_obj

    #
    #  Asset
    #
    def create_asset(self, asset=None, asset_type_id=''):
        """Create an Asset resource. If alt_ids provided verify well formed and unique
        in namespace RT.Asset. An Asset is coupled with an AssetType. The AssetType is
        created and associated within this call if asset_type_id provided.

        @param  asset           RT.Asset
        @param  asset_type_id   str        # optional
        @param  asset_id        str
        @throws BadRequest      'asset object is empty'
        @throws Inconsistent    'multiple alt_ids not permitted for Asset resources'
        @throws Inconsistent    'malformed alt_ids provided for Asset; required format \'Asset:asset_name\''
        @throws BadRequest      'resource instance already exists (\'Asset\') with this altid: %s'
        @throws Inconsistent    'Invalid asset object'
        """
        if not asset:
            raise BadRequest('asset object is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        asset_id = at.create_asset(asset, asset_type_id)

        return asset_id

    def read_asset(self, asset_id=''):
        """Read an Asset resource
        @param  asset_id    str
        @retval asset       RT.Asset
        @throws BadRequest  'asset_id parameter is empty'
        """
        if not asset_id:
            raise BadRequest('asset_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        obj = at.read_asset(asset_id)

        return obj

    def update_asset(self, asset=None):
        """Update an Asset resource. Ensure alt_ids value (if provided) is well formed and
        unique in namespace. The asset object provided shall have asset_attrs defined and shall also have
        an association (PRED.implementsAssetType) defined or method shall fail. asset.asset_attrs and
        the association are required to perform validation and constraint checks prior to update.

        @param asset        RT.Asset
        @throws BadRequest  'asset object is empty'
        @throws BadRequest  '_id is empty'
        @throws BadRequest  'asset (id=%s) does not have association (PRED.implementsAssetType) defined'
        @throws BadRequest  'asset (id=%s) has more than one association (PRED.implementsAssetType) defined'
        @throws BadRequest  'asset type (id: \'%s\') does not have attribute_specifications'
        @throws BadRequest  'asset_update requires asset_attrs to be provided'
        @throws BadRequest  'attribute (\'%s\') not found in AssetType (id=\'%s\') AttributeSpecification '
        @throws BadRequest  'update_asset: altid returned: %s; instance using current_altid_exists: %s'
        @throws BadRequest  (numerous error messages from lower methods inside update_asset)
        @throws BadRequest  'update_asset failed'
        """
        try:
            if not asset:
                raise BadRequest('asset object is empty')

            if not asset._id:
                raise NotFound('_id is empty')

            at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
            at.update_asset(asset)

        except BadRequest, Arguments:
            raise BadRequest('update_asset: %s' % Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound('update_asset: %s' % Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent('update_asset: %s' % Arguments.get_error_message())
        except:
            raise BadRequest('update_asset failed')

        return

    def delete_asset(self, asset_id=''):
        """Delete an Asset resource

        @param asset_id     str
        @throws BadRequest  'asset_id parameter is empty'
        """
        if not asset_id:
            raise BadRequest('asset_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.delete_asset(asset_id)

        return

    def force_delete_asset(self, asset_id=''):
        """ Force delete an Asset resource

        @param asset_id     str
        @throws BadRequest  'asset_id parameter is empty'
        """
        if not asset_id:
            raise BadRequest('asset_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.force_delete_asset(asset_id)

    def get_asset_extension(self, asset_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Returns an AssetExtension object containing additional related information

        @param asset_id             str
        @param ext_associations     dict
        @param ext_exclude          list
        @param user_id              str
        @retval extended_asset      AssetExtension
        @throws BadRequest          'asset_id parameter is empty'
        """
        if not asset_id:
            raise BadRequest('asset_id parameter is empty')

        extended_resource_handler = ExtendedResourceContainer(self)
        extended_asset = extended_resource_handler.create_extended_resource_container(
            extended_resource_type=OT.AssetExtension,
            resource_id=asset_id,
            computed_resource_type=OT.BaseComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id)

        from ion.util.extresource import strip_resource_extension, get_matchers, matcher_UserInfo, matcher_MarineAsset,\
        matcher_DataProduct, matcher_DeviceModel,  matcher_Device
        matchers = get_matchers([matcher_MarineAsset, matcher_UserInfo])
        strip_resource_extension(extended_asset, matchers=matchers)

        return extended_asset

    def prepare_asset_support(self, asset_id=''):
        """Asset prepare support for UI (create, update).

        @param  asset_id        str
        @retval resource_data   resource_schema
        """
        extended_resource_handler = ExtendedResourceContainer(self)

        resource_data = extended_resource_handler.create_prepare_resource_support(asset_id, OT.AssetPrepareSupport)

        #Fill out service request information for creating a instrument agent instance
        extended_resource_handler.set_service_requests(resource_data.create_request, 'observatory_management',
            'create_asset', { "asset":  "$(asset)" })

        #Fill out service request information for creating a instrument agent instance
        extended_resource_handler.set_service_requests(resource_data.update_request, 'observatory_management',
            'update_asset', { "asset":  "$(asset)" })

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # assign event to asset (LocationEvent, OperabilityEvent, VerificationEvent, IntegrationEvent)
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        #Fill out service request information for assigning an EventDuration to Asset (LocationEvent)
        extended_resource_handler.set_service_requests(resource_data.associations['AssetHasLocationEvent'].assign_request,
                                                       'observatory_management', 'assign_event_duration_to_asset',
                                                       {"event_duration_id":  "$(event_duration_id)", "asset_id":  asset_id })

        #Fill out service request information for assigning an EventDuration to Asset (OperabilityEvent)
        extended_resource_handler.set_service_requests(resource_data.associations['AssetHasOperabilityEvent'].assign_request,
                                                       'observatory_management', 'assign_event_duration_to_asset',
                                                       {"event_duration_id":  "$(event_duration_id)", "asset_id":  asset_id })

        #Fill out service request information for assigning an EventDuration to Asset (VerificationEvent)
        extended_resource_handler.set_service_requests(resource_data.associations['AssetHasVerificationEvent'].assign_request,
                                                       'observatory_management', 'assign_event_duration_to_asset',
                                                       {"event_duration_id":  "$(event_duration_id)", "asset_id":  asset_id })

        #Fill out service request information for assigning an EventDuration to Asset (IntegrationEvent)
        extended_resource_handler.set_service_requests(resource_data.associations['AssetHasAssemblyEvent'].assign_request,
                                                       'observatory_management', 'assign_event_duration_to_asset',
                                                       {"event_duration_id":  "$(event_duration_id)", "asset_id":  asset_id })

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # unassign event to asset (LocationEvent, OperabilityEvent, VerificationEvent, IntegrationEvent)
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        #Fill out service request information for unassigning an EventDuration to Asset (LocationEvent)
        extended_resource_handler.set_service_requests(resource_data.associations['AssetHasLocationEvent'].unassign_request,
                                                       'observatory_management', 'unassign_event_duration_to_asset',
                                                       {"event_duration_id":  "$(event_duration_id)", "asset_id":  asset_id })

        #Fill out service request information for unassigning an EventDuration to Asset (OperabilityEvent)
        extended_resource_handler.set_service_requests(resource_data.associations['AssetHasOperabilityEvent'].unassign_request,
                                                       'observatory_management', 'unassign_event_duration_to_asset',
                                                       {"event_duration_id":  "$(event_duration_id)", "asset_id":  asset_id })

        #Fill out service request information for unassigning an EventDuration to Asset (VerificationEvent)
        extended_resource_handler.set_service_requests(resource_data.associations['AssetHasVerificationEvent'].unassign_request,
                                                       'observatory_management', 'unassign_event_duration_to_asset',
                                                       {"event_duration_id":  "$(event_duration_id)", "asset_id":  asset_id })

        #Fill out service request information for unassigning an EventDuration to Asset (IntegrationEvent)
        extended_resource_handler.set_service_requests(resource_data.associations['AssetHasAssemblyEvent'].unassign_request,
                                                       'observatory_management', 'unassign_event_duration_to_asset',
                                                       {"event_duration_id":  "$(event_duration_id)", "asset_id":  asset_id })

        return resource_data

    def assign_asset_type_to_asset(self, asset_type_id='',asset_id=''):
        """ Link an Asset to an AssetType

        @param asset_type_id  str
        @param asset_id       str
        @throws BadRequest    'asset_type_id parameter is empty'
        @throws BadRequest    'asset_id parameter is empty'
        """
        if not asset_type_id:
            raise BadRequest('asset_type_id parameter is empty')
        if not asset_id:
            raise BadRequest('asset_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.assign_asset_type_to_asset(asset_type_id, asset_id)

    def unassign_asset_type_from_asset(self, asset_type_id='', asset_id=''):
        """Remove link of Asset from AssetType.

        @param asset_type_id    str
        @param asset_id         str
        @throws BadRequest      'asset_type_id parameter is empty'
        @throws BadRequest      'asset_id parameter is empty'
        """
        if not asset_type_id:
            raise BadRequest('asset_type_id parameter is empty')
        if not asset_id:
            raise BadRequest('asset_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.unassign_asset_type_from_asset(asset_type_id, asset_id)

    #
    #   EventDurationType
    #

    def create_event_duration_type(self, event_duration_type=None):
        """Create a EventDurationType resource.

        @param event_duration_type      RT.EventDurationType
        @retval event_duration_type_id  str
        @throws: BadRequest 'event_duration_type parameter is empty'
        """
        if not event_duration_type:
            raise BadRequest('event_duration_type parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        event_duration_type = at.create_event_duration_type(event_duration_type)
        return event_duration_type

    def read_event_duration_type(self, event_duration_type_id=''):
        """Read an EventDurationType resource.

        @param event_duration_type_id  str
        @retval event_duration_type    RT.EventDurationType
        @throws: BadRequest 'event_duration_type_id parameter is empty'
        """
        if not event_duration_type_id:
            raise BadRequest('event_duration_type_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        event_duration_type = at.read_event_duration_type(event_duration_type_id)

        return event_duration_type

    def update_event_duration_type(self, event_duration_type=None):
        """Update an EventDurationType resource.

        @param event_duration_type  RT.EventDurationType
        @throws: BadRequest     'event_duration_type parameter is empty'
        """
        if not event_duration_type:
            raise BadRequest('event_duration_type parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.update_event_duration_type(event_duration_type)
        return

    def delete_event_duration_type(self, event_duration_type_id=''):
        """Delete an EventDurationType resource.

        @param event_duration_type_id  str
        @throws: BadRequest 'event_duration_type_id parameter is empty'
        """
        if not event_duration_type_id:
            raise BadRequest('event_duration_type_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.delete_event_duration_type(event_duration_type_id)

        return

    def force_delete_event_duration_type(self, event_duration_type_id=''):
        """Force delete an EventDurationType resource.

        @param event_duration__type_id  str
        @throws: BadRequest 'event_duration_type_id parameter is empty'
        """
        if not event_duration_type_id:
            raise BadRequest('event_duration_type_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.force_delete_event_duration_type(event_duration_type_id)

    #
    #    EventDuration
    #
    def create_event_duration(self, event_duration=None, event_duration_type_id=''):
        """Create a EventDuration resource.

        An EventDuration is created and is coupled with an EventDurationType if
        the optional event_duration_type_id is provided.

        @param event_duration           RT.EventDuration
        @param event_duration_type_id   str              # optional
        @retval event_duration_id       str
        @throws BadRequest      'event_duration parameter is empty'
        @throws Inconsistent    'multiple alt_ids not permitted for EventDuration resources'
        @throws Inconsistent    'malformed EventDuration.alt_ids provided; required format empty or \'EventDuration:event_name\'
        @throws Inconsistent    'invalid namespace (%s) provided for EventDuration resource'
        @throws BadRequest      'resource instance already exists (\'EventDuration\') with this altid: %s'
        @throws Inconsistent    'Invalid event_duration object'
        """
        if not event_duration:
            raise BadRequest('event_duration parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        event_duration_id = at.create_event_duration(event_duration, event_duration_type_id)

        return event_duration_id

    def read_event_duration(self, event_duration_id=''):
        """Read an EventDuration resource.

        @param event_duration_id  str
        @retval event_duration    RT.EventDuration
        @throws BadRequest  'event_duration_id parameter is empty'
        """
        if not event_duration_id:
            raise BadRequest('event_duration_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        event_duration = at.read_event_duration(event_duration_id)

        return event_duration

    def update_event_duration(self, event_duration=None):
        """Update an EventDuration resource and ensure alt_ids value (if provided) is well formed and
        unique in namespace. The event_duration object provided shall have event_duration_attrs
        defined and shall also have an association (PRED.implementsEventDurationType) defined or
        method shall fail. event_duration.event_duration_attrs and the association are required
        to perform validation and constraint checks prior to update.
        @param event_duration    RT.EventDuration
        @throws BadRequest      'update_event_duration failed'
        @throws BadRequest      'event_duration parameter is empty'
        @throws BadRequest      'event_duration (id=%s) does not have association (PRED.implementsEventDurationType) defined'
        @throws BadRequest      'event_duration (id=%s) has more than one association (PRED.implementsEventDurationType) defined'
        @throws BadRequest      'event_duration_update requires event_duration_attrs to be provided'
        @throws BadRequest      'event_duration_update: altid returned: %s and current_altid_exists: %s'
        @throws BadRequest      'update_event_duration failed'
        """
        try:
            if not event_duration:
                raise BadRequest('event_duration parameter is empty')

            if not event_duration._id:
                raise NotFound('_id is empty')

            at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
            at.update_event_duration(event_duration)

        except BadRequest, Arguments:
            raise BadRequest('update_event_duration: %s' % Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound('update_event_duration: %s' % Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent('update_event_duration: %s' % Arguments.get_error_message())
        except:
            raise BadRequest('update_event_duration failed')

        return

    def delete_event_duration(self, event_duration_id=''):
        """Delete an EventDuration resource.

        @param event_duration_id   str
        @throws BadRequest  'event_duration_id parameter is empty'
        """
        if not event_duration_id:
            raise BadRequest('event_duration_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.delete_event_duration(event_duration_id)

        return

    def force_delete_event_duration(self, event_duration_id=''):
        """ Force delete an EventDuration resource.

        @param event_duration_id    str
        @throws BadRequest  'event_duration_id parameter is empty'
        """
        if not event_duration_id:
            raise BadRequest('event_duration_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.force_delete_event_duration(event_duration_id)

    def assign_event_duration_type_to_event_duration(self, event_duration_type_id='', event_duration_id=''):
        """ Link an EventDuration to an EventDurationType.

        @param event_duration_type_id  str
        @param event_duration_id       str
        @throws BadRequest  'event_duration_type_id parameter is empty'
        @throws BadRequest  'event_duration_id parameter is empty'
        """
        if not event_duration_type_id:
            raise BadRequest('event_duration_type_id parameter is empty')

        if not event_duration_id:
            raise BadRequest('event_duration_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.assign_event_duration_type_to_event_duration(event_duration_type_id, event_duration_id)

    def unassign_event_duration_type_from_event_duration(self, event_duration_type_id='', event_duration_id=''):
        """Remove link of EventDuration from EventDurationType.

        @param event_duration_type_id  str
        @param event_duration_id       str
        @throws BadRequest      'event_duration_type_id parameter is empty'
        @throws BadRequest      'event_duration_id parameter is empty'
        """
        if not event_duration_type_id:
            raise BadRequest('event_duration_type_id parameter is empty')

        if not event_duration_id:
            raise BadRequest('event_duration_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.unassign_event_duration_type_from_event_duration(event_duration_type_id, event_duration_id)

    def get_event_duration_extension(self, event_duration_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Returns an EventDurationExtension object containing additional related information

        @param event_duration_id        str
        @param ext_associations         dict
        @param ext_exclude              list
        @param user_id                  str
        @retval extended_event_duration EventDurationExtension
        @throws BadRequest          'event_duration_id parameter is empty'
        """
        if not event_duration_id:
            raise BadRequest('event_duration_id parameter is empty')

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_event_duration = extended_resource_handler.create_extended_resource_container(
            extended_resource_type=OT.EventDurationExtension,
            resource_id=event_duration_id,
            computed_resource_type=OT.BaseComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id)

        from ion.util.extresource import strip_resource_extension, get_matchers, matcher_UserInfo, matcher_MarineAsset, \
        matcher_DataProduct, matcher_DeviceModel,  matcher_Device
        matchers = get_matchers([matcher_MarineAsset, matcher_UserInfo])
        strip_resource_extension(extended_event_duration, matchers=matchers)

        return extended_event_duration

    def prepare_event_duration_support(self, event_duration_id=''):
        """EventDuration prepare support for UI (create, update).

        @param  event_duration_id       str
        @retval resource_data           resource_schema
        """
        extended_resource_handler = ExtendedResourceContainer(self)

        resource_data = extended_resource_handler.create_prepare_resource_support(event_duration_id, OT.EventDurationPrepareSupport)

        #Fill out service request information for creating a instrument agent instance
        extended_resource_handler.set_service_requests(resource_data.create_request, 'observatory_management',
            'create_event_duration', { "event_duration":  "$(event_duration)" })

        #Fill out service request information for creating a instrument agent instance
        extended_resource_handler.set_service_requests(resource_data.update_request, 'observatory_management',
            'update_event_duration', { "event_duration":  "$(event_duration)" })


        """
        #Fill out service request information for assigning an EventDurationType from EventDuration
        extended_resource_handler.set_service_requests(resource_data.associations['EventDurationHasEventDurationType'].assign_request, 'observatory_management',
            'assign_event_duration_type_from_event_duration', {"event_duration_type_id":  "$(event_duration_type_id)",
                                            "event_duration_id":  event_duration_id })

        #Fill out service request information for unassigning an EventDurationType from EventDuration
        extended_resource_handler.set_service_requests(resource_data.associations['EventDurationHasEventDurationType'].unassign_request, 'observatory_management',
            'unassign_event_duration_type_from_event_duration', {"event_duration_type_id":  "$(event_duration_type_id)",
                                                "event_duration_id":  event_duration_id })
        """

        return resource_data

    def assign_event_duration_to_asset(self, event_duration_id='', asset_id=''):
        """ Link an EventDuration to an Asset.

        @param event_duration_id    str
        @param asset_id             str
        @throws BadRequest      'event_duration_id parameter is empty'
        @throws BadRequest      'asset_id parameter is empty'
        @throws NotFound        'asset instance not found'
        @throws Inconsistent    'this event duration has multiple event duration types'
        @throws BadRequest      'this event duration does not have associated event duration type'
        @throws BadRequest      'unknown EventCategoryEnum value for association category'
        @throws BadRequest      'an association (%s) already exists, cannot assign more than one association of the same type'
        @throws BadRequest      'unknown association category predicate (Event to Asset)'
        @throws BadRequest      'failed to assign association (%s)
        """
        if not event_duration_id:
            raise BadRequest('event_duration_id parameter is empty')
        if not asset_id:
            raise BadRequest('asset_id parameter is empty')

        try:
            at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
            at.assign_event_duration_to_asset(event_duration_id, asset_id)
        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('failed to assign association event duration to asset')

    def unassign_event_duration_to_asset(self, event_duration_id='', asset_id=''):
        """Remove link of EventDuration from Asset.

        @param event_duration_id    str
        @param asset_id             str
        @throws BadRequest          'event_duration_id parameter is empty'
        @throws BadRequest          'asset_id parameter is empty'
        @throws Inconsistent        'this event duration implements multiple event duration types'
        @throws BadRequest          'this event duration does not have associated event duration type'
        @throws Inconsistent        'this event duration has multiple associations with asset'
        @throws BadRequest          'this event duration is not associated with asset'
        """
        if not event_duration_id:
            raise BadRequest('event_duration_id parameter is empty')
        if not asset_id:
            raise BadRequest('asset_id parameter is empty')
        try:
            at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
            at.unassign_event_duration_to_asset(event_duration_id, asset_id)
        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('failed to unassign association (event duration from asset)')

    #
    #  Asset associations to resource
    #  (not used; remove here AND from observatory_management_service.yml)
    #
    def assign_asset_to_resource(self, asset_id='',resource_id=''):
        # Link an asset to a resource (deprecate)
        #@param asset_id         str
        #@param resource_id      str
        #@throws NotFound    object with specified id does not exist
        #@throws BadRequest  if object with specified id does not have_id or_rev attribute
        #
        if not asset_id:
            raise BadRequest('asset_id parameter is empty')
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.assign_asset_to_resource(asset_id, resource_id)

    def unassign_asset_from_resource(self, asset_id='', resource_id=''):
        #Remove link of asset from resource. (deprecate)
        #@param asset_id         str
        #@param resource_id      str
        #@throws BadRequest  if object with specified id does not have_id or_rev attribute
        #
        if not asset_id:
            raise BadRequest('asset_id parameter is empty')
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.unassign_asset_from_resource(asset_id, resource_id)
    #
    #  CodeSpace
    #
    def create_code_space(self, code_space=None):
        """Create a CodeSpace resource.

        @param code_space       RT.CodeSpace
        @retval  id             str
        @throws: BadRequest     'code_space object is empty'
        @throws: Inconsistent   'invalid code_space object'
        """
        if not code_space:
            raise BadRequest('code_space object is empty')

        try:
            at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
            id = at.create_code_space(code_space)
        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except:
            raise Inconsistent('invalid code_space object')

        return id

    def read_code_space(self, resource_id=''):
        """Read an CodeSpace resource.

        @param resource_id          str
        @retval code_space          RT.CodeSpace
        @throws BadRequest 'resource_id parameter is empty'
        @throws NotFound   'object with specified id does not exist'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        try:
            at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
            obj = at.read_code_space(resource_id)
        except:
            raise NotFound('object with specified id does not exist.')

        return obj

    def update_code_space(self, code_space=None):
        """Update an CodeSpace resource.

        @param code_space  RT.CodeSpace
        @throws BadRequest 'code_space object is empty'
        """
        if not code_space:
            raise BadRequest('code_space object is empty')
        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        obj = at.update_code_space(code_space)

        return obj

    def delete_code_space(self, resource_id=''):
        """Delete a CodeSpace resource.

        @param resource_id  str
        @throws BadRequest 'resource_id parameter is empty'
        @throws NotFound   'object with specified id does not exist.'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        try:
            at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
            at.delete_code_space(resource_id)
        except:
            raise NotFound('object with specified id does not exist.')

        return


    def force_delete_code_space(self, resource_id=''):
        """ Force delete a CodeSpace resource.

        @param resource_id      str
        @throws BadRequest      'resource_id parameter is empty'
        @throws NotFound        'object with specified id does not exist.'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')

        try:
            at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
            at.force_delete_code_space(resource_id)
        except:
            raise NotFound('object with specified id does not exist.')

        return #obj


    def read_codesets_by_name(self, resource_id='', names=None):
        """Read CodeSpace (id=resource_id) for list of codeset name(s); return list of CodeSets.

        @param resource_id      str
        @param names            []
        @throws: BadRequest     'resource_id parameter is empty'
        @throws: BadRequest     'names parameter is empty'
        @throws NotFound        'object with specified resource_id (type RT.CodeSpace) does not exist'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        if not names:
            raise BadRequest('names parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        codesets = at.read_codesets_by_name(resource_id, names)

        return codesets

    def read_codes_by_name(self, resource_id='', names=None, id_only=False):
        """Read CodeSpace with resource_id and for list of Code name(s); return list of Codes.

        @param resource_id      str
        @param names            []
        @params id_only         bool        # optional
        @throws BadRequest      'resource_id parameter is empty'
        @throws BadRequest      'names parameter is empty'
        @throws NotFound        'object with specified resource_id (type RT.CodeSpace) does not exist'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        if not names:
            raise BadRequest('names parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        codes = at.read_codes_by_name(resource_id, names, id_only)

        return codes

    def update_codes(self, resource_id='', codes=None):
        """Read CodeSpace with resource_id, update Codes identified in dictionary of codes.

        @param resource_id  str
        @param codes        {}
        @throws BadRequest  'resource_id parameter is empty'
        @throws BadRequest  'codes parameter is empty'
        @throws NotFound    'object with specified resource_id and type=RT.CodeSpace does not exist'
        @throws NotFound    'code not found in CodeSpace (with id=resource_id).
        @throws NotFound    'code provided for update with empty name.'
        @throws NotFound    'codes not found in CodeSpace (with id=resource_id).'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        if not codes:
            raise BadRequest('codes parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.update_codes(resource_id, codes)

    def update_codesets(self, resource_id='', codesets=None):
        """Read CodeSpace, with resource_id, and update codesets as identified in
        the dictionary codesets.

        @param  resource_id      str
        @param  codesets         {}
        @throws BadRequest      'resource_id parameter is empty'
        @throws BadRequest      'codesets parameter is empty'
        @throws NotFound        'object with specified resource_id and type=RT.CodeSpace does not exist'
        @throws NotFound        'CodeSet not found in CodeSpace.'
        @throws NotFound        'CodeSet provided for update with empty name.'
        @throws NotFound        'CodeSpace codesets is empty.'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        if not codesets:
            raise BadRequest('codesets parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        at.update_codesets(resource_id, codesets)

    def delete_codes(self, resource_id='', names=None):
        """Delete Codes (identified in names list) from CodeSpace; return list of Codes in CodeSpace.
        Check if code is used by code_set; if so, remove code fom code_set, update code_set and then
        delete the code.

        @param  resource_id     str
        @param  names           []
        @retval codes_list      []
        @throws BadRequest      'resource_id parameter is empty'
        @throws BadRequest      'names parameter is empty'
        @throws NotFound        'object with resource_id and type RT.CodeSpace does not exist
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        if not names:
            raise BadRequest('names parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        codes_list = at.delete_codes(resource_id, names)

        return codes_list

    def delete_codesets(self, resource_id='', names=None):
        """Delete CodeSets identified in list names; return list of CodeSets in CodeSpace.

        @param  resource_id     str
        @param  names           []
        @retval codeset_list    []
        @throws BadRequest      'resource_id parameter is empty'
        @throws BadRequest      'names parameter is empty'
        @throws NotFound        'object with resource_id and type RT.CodeSpace does not exist
        """
        #todo (* Return value scheduled to change.)
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        if not names:
            raise BadRequest('names parameter is empty')

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        codeset_list = at.delete_codesets(resource_id, names)

        return codeset_list

    ############################
    #
    #  START - Services for Marine Asset Management
    #
    ############################

    def declare_asset_tracking_resources(self, content='', content_type='', content_encoding=''):
        """Read content which defines asset management resources, instantiate resources;
        return dictionary of resource ids by category of resource type.

        @param  content              encoded blob              # binascii.b2a_hex(content)
        @param  content_type         file_descriptor.mimetype  # file descriptor type
        @param  content_encoding     'b2a_hex'                 # encoding (set to binascii.b2a_hex)
        @retval response             {}                        # dict of resource ids by category of resource type
        @throws BadRequest          'content parameter is empty'
        @throws BadRequest          'declare_asset_tracking_resources error'
        @throws BadRequest          (from _process_xls)
        @throws NotFound            (from _process_xls)
        @throws Inconsistent        (from _process_xls)
        """
        if not content:
            raise BadRequest('content parameter is empty')

        try:
            at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
            response = at._process_xls(content, content_type, content_encoding)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('declare_asset_tracking_resources error')

        return response

    def asset_tracking_report(self):
        """Query system instances of marine tracking resources (CodeSpaces,Codes,CodeSets, Assets, AssetTypes, EventDurations,
        EventDurationTypes) produce xls workbook and return encoded content.

        @retval content         binascii.b2a_hex(xls workbook)
        @throws BadRequest      'asset tracking report failed to produce xls'
        @throws BadRequest      (from _download_xls)
        @throws NotFound        (from _download_xls)
        @throws Inconsistent    (from _download_xls)
        """
        try:
            at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
            content = at._download_xls()

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('asset tracking report failed to produce xls')

        return content

    # Deprecate - helper picklists for altids (Asset and Event[Duration]s)
    def get_altids(self, res_type=''):

        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        picklist = at.get_altids(res_type)
        return picklist

        # helper picklists for altids (Asset and Event[Duration]s)
    def get_assets_picklist(self, id_only=''):
        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        picklist = at.get_picklist(RT.Asset, id_only)
        return picklist

    def get_events_picklist(self, id_only=''):
        at = AssetTracking(self,container=self.container, enhanced_rr=self.RR2, rr=self.RR, node=self.container.node)
        picklist = at.get_picklist(RT.EventDuration, id_only)
        return picklist

    # -------------------------------------------------------------------------
    #   Marine Asset Management RESOURCES (end)
    # -------------------------------------------------------------------------

    ############################
    #
    #  EXTENDED RESOURCES
    #
    ############################

    # TODO: Make every incoming call to this one
    def get_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        site_extension = None

        # Make a case decision on what what to do
        site_obj = self.RR2.read(site_id)
        site_type = site_obj._get_type()

        if site_type == RT.InstrumentSite:
            site_extension = self._get_instrument_site_extension(site_id, ext_associations, ext_exclude, user_id)

        elif site_type in (RT.Observatory, RT.Subsite):
            site_extension = self._get_platform_site_extension(site_id, ext_associations, ext_exclude, user_id)

        elif site_type == RT.PlatformSite:
            site_extension = self._get_platform_site_extension(site_id, ext_associations, ext_exclude, user_id)

        else:
            raise BadRequest("Unknown site type '%s' for site %s" % (site_type, site_id))

        from ion.util.extresource import strip_resource_extension, get_matchers, matcher_DataProduct, matcher_DeviceModel, \
            matcher_Device, matcher_UserInfo
        matchers = get_matchers([matcher_DataProduct, matcher_DeviceModel, matcher_Device, matcher_UserInfo])
        strip_resource_extension(site_extension, matchers=matchers)

        return site_extension

    # TODO: Redundant, remove operation and use get_site_extension
    def get_observatory_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        return self.get_site_extension(site_id, ext_associations, ext_exclude, user_id)

    # TODO: Redundant, remove operation and use get_site_extension
    def get_platform_station_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        return self.get_site_extension(site_id, ext_associations, ext_exclude, user_id)

    # TODO: Redundant, remove operation and use get_site_extension
    def get_platform_assembly_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        return self.get_site_extension(site_id, ext_associations, ext_exclude, user_id)

    # TODO: Redundant, remove operation and use get_site_extension
    def get_platform_component_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        return self.get_site_extension(site_id, ext_associations, ext_exclude, user_id)

    # TODO: Redundant, remove operation and use get_site_extension
    def get_instrument_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        return self.get_site_extension(site_id, ext_associations, ext_exclude, user_id)

    def _get_site_device(self, site_id, device_relations):
        site_devices = [tup[1] for tup in device_relations.get(site_id, []) if tup[2] in (RT.InstrumentDevice, RT.PlatformDevice)]
        if len(site_devices) > 1:
            log.error("Inconsistent: Site %s has multiple devices: %s", site_id, site_devices)
        if not site_devices:
            return None
        return site_devices[0]

    def _get_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Returns a site extension object containing common information, plus some helper objects

        @param site_id    str
        @param ext_associations    dict
        @param ext_exclude    list
        @retval TBD
        @throws BadRequest    A parameter is missing
        @throws NotFound    An object with the specified observatory_id does not exist
        """
        try:
            if not site_id:
                raise BadRequest("The site_id parameter is empty")

            extended_resource_handler = ExtendedResourceContainer(self)

            extended_site = extended_resource_handler.create_extended_resource_container(
                extended_resource_type=OT.SiteExtension,
                resource_id=site_id,
                computed_resource_type=OT.SiteComputedAttributes,
                ext_associations=ext_associations,
                ext_exclude=ext_exclude,
                user_id=user_id)

            RR2 = EnhancedResourceRegistryClient(self.clients.resource_registry)
            outil = ObservatoryUtil(self, enhanced_rr=RR2, device_status_mgr=DeviceStateManager())

            # Find all subsites and devices
            site_resources, site_children = outil.get_child_sites(parent_site_id=site_id, include_parents=False, id_only=False)
            site_ids = site_resources.keys() + [site_id]  # IDs of this site and all child sites
            device_relations = outil.get_device_relations(site_ids)

            # Set parent immediate child sites
            parent_site_ids = [a.s for a in RR2.filter_cached_associations(PRED.hasSite, lambda a: a.p ==PRED.hasSite and a.o == site_id)]
            if parent_site_ids:
                extended_site.parent_site = RR2.read(parent_site_ids[0])
            else:
                extended_site.parent_site = None
            extended_site.sites = [site_resources[ch_id] for ch_id in site_children[site_id]] if site_children.get(site_id, None) is not None else []

            # Set all nested child devices, remove any dups
            instrument_device_ids = list( set( [tup[1] for (parent,dlst) in device_relations.iteritems() for tup in dlst if tup[2] == RT.InstrumentDevice] ) )
            platform_device_ids =  list( set( [tup[1] for (parent,dlst) in device_relations.iteritems() for tup in dlst if tup[2] == RT.PlatformDevice] ) )

            device_ids = list(set(instrument_device_ids + platform_device_ids))
            device_objs = self.RR2.read_mult(device_ids)
            devices_by_id = dict(zip(device_ids, device_objs))

            extended_site.instrument_devices = [devices_by_id[did] for did in instrument_device_ids]
            extended_site.platform_devices = [devices_by_id[did] for did in platform_device_ids]

            # Set primary device at immediate child sites
            extended_site.sites_devices = []
            for ch_site in extended_site.sites:
                device_id = self._get_site_device(ch_site._id, device_relations)
                extended_site.sites_devices.append(devices_by_id.get(device_id, None))
            extended_site.portal_instruments = extended_site.sites_devices   # ALIAS

            # Set deployments
            RR2.cache_predicate(PRED.hasDeployment)
            deployment_assocs = RR2.filter_cached_associations(PRED.hasDeployment, lambda a: a.s in site_ids)
            deployment_ids = [a.o for a in deployment_assocs]
            deployment_objs = RR2.read_mult(list(set(deployment_ids)))
            extended_site.deployments = deployment_objs

            # Get current active deployment. May be site or parent sites
            dep_util = DeploymentUtil(self.container)
            extended_site.deployment = dep_util.get_active_deployment(site_id, is_site=True, rr2=RR2)

            # Set data products
            RR2.cache_predicate(PRED.hasSource)
            dataproduct_assocs = RR2.filter_cached_associations(PRED.hasSource, lambda a: a.o in site_ids)
            dataproduct_ids = [a.s for a in dataproduct_assocs]
            dataproduct_objs = RR2.read_mult(list(set(dataproduct_ids)))
            extended_site.data_products = dataproduct_objs

            log.debug("Building list of model objs")
            # Build a lookup for device models via hasModel predicates.
            # lookup is a 2d associative array of [subject type][subject id] -> object id
            RR2.cache_predicate(PRED.hasModel)
            lookup = {rt : {} for rt in [RT.InstrumentDevice, RT.PlatformDevice]}
            for a in RR2.filter_cached_associations(PRED.hasModel, lambda assn: assn.st in lookup):
                lookup[a.st][a.s] = a.o

            def retrieve_model_objs(rsrc_list, object_type):
            # rsrc_list is devices that need models looked up.  object_type is the resource type (a device)
            # not all devices have models (represented as None), which kills read_mult.  so, extract the models ids,
            #  look up all the model ids, then create the proper output
                model_list = [lookup[object_type].get(r._id) for r in rsrc_list]
                model_uniq = list(set([m for m in model_list if m is not None]))
                model_objs = self.RR2.read_mult(model_uniq)
                model_dict = dict(zip(model_uniq, model_objs))
                return [model_dict.get(m) for m in model_list]

            extended_site.instrument_models = retrieve_model_objs(extended_site.instrument_devices, RT.InstrumentDevice)
            extended_site.platform_models   = retrieve_model_objs(extended_site.platform_devices, RT.PlatformDevice)

            primary_device_id = self._get_site_device(site_id, device_relations)

            # Filtered subsites by type/alt type
            def fs(resource_type, filter_fn):
                both = lambda s: ((resource_type == s._get_type()) and filter_fn(s))
                return filter(both, site_resources.values())

            extended_site.platform_station_sites   = fs(RT.PlatformSite, lambda s: s.alt_resource_type == "StationSite")
            extended_site.platform_component_sites = fs(RT.PlatformSite, lambda s: s.alt_resource_type == "PlatformComponentSite")
            extended_site.platform_assembly_sites  = fs(RT.PlatformSite, lambda s: s.alt_resource_type == "PlatformAssemblySite")
            extended_site.instrument_sites         = fs(RT.InstrumentSite, lambda _: True)

            context = dict(
                extended_site=extended_site,
                enhanced_RR=RR2,
                site_device_id=primary_device_id,
                site_resources=site_resources,
                site_children=site_children,
                device_relations=device_relations,
                outil=outil
            )
            return context
        except:
            log.error('_get_site_extension failed', exc_info=True)
            raise

    def _get_platform_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Creates a SiteExtension and status for platforms and higher level sites"""
        log.debug("_get_platform_site_extension")
        context = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)
        extended_site, RR2, platform_device_id, site_resources, site_children, device_relations, outil = \
            context["extended_site"], context["enhanced_RR"], context["site_device_id"], \
            context["site_resources"], context["site_children"], context["device_relations"], context["outil"]

        statuses = outil.get_status_roll_ups(site_id, include_structure=True)
        portal_status = []
        if extended_site.portal_instruments:
            for x in extended_site.portal_instruments:
                if x:
                    portal_status.append(statuses.get(x._id,{}).get("agg", DeviceStatusType.STATUS_UNKNOWN))
                else:
                    portal_status.append(DeviceStatusType.STATUS_UNKNOWN)
            extended_site.computed.portal_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=portal_status)
        else:
            extended_site.computed.portal_status = ComputedListValue(status=ComputedValueAvailability.NOTAVAILABLE)

        site_status = []
        if extended_site.sites:
            for x in extended_site.sites:
                if x:
                    site_status.append(statuses.get(x._id,{}).get("agg", DeviceStatusType.STATUS_UNKNOWN))
                else:
                    site_status.append(DeviceStatusType.STATUS_UNKNOWN)
            extended_site.computed.site_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=site_status)
        else:
            extended_site.computed.site_status = ComputedListValue(status=ComputedValueAvailability.NOTAVAILABLE)

        # create the list of station status from the overall status list
        subset_status = []
        for site in extended_site.platform_station_sites:
            if not extended_site.sites.count(site):
                log.error(" Platform Site does not exist in the full list of sites. id: %s", site._id)
                break
            idx =   extended_site.sites.index( site )
            subset_status.append( site_status[idx] )
        extended_site.computed.station_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=subset_status)

        inst_status = []
        if extended_site.instrument_devices:
            for x in extended_site.instrument_devices:
                if x:
                    inst_status.append(statuses.get(x._id,{}).get("agg", DeviceStatusType.STATUS_UNKNOWN))
                else:
                    inst_status.append(DeviceStatusType.STATUS_UNKNOWN)
            extended_site.computed.instrument_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=inst_status)
        else:
            extended_site.computed.instrument_status = ComputedListValue(status=ComputedValueAvailability.NOTAVAILABLE)

        plat_status = []
        if extended_site.platform_devices:
            for x in extended_site.platform_devices:
                    if x:
                        plat_status.append(statuses.get(x._id,{}).get("agg", DeviceStatusType.STATUS_UNKNOWN))
                    else:
                        plat_status.append(DeviceStatusType.STATUS_UNKNOWN)
            extended_site.computed.platform_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=plat_status)
        else:
            extended_site.computed.platform_status = ComputedListValue(status=ComputedValueAvailability.NOTAVAILABLE)

        comms_rollup = statuses.get(site_id,{}).get(AggregateStatusType.AGGREGATE_COMMS,DeviceStatusType.STATUS_UNKNOWN)
        power_rollup = statuses.get(site_id,{}).get(AggregateStatusType.AGGREGATE_POWER,DeviceStatusType.STATUS_UNKNOWN)
        data_rollup = statuses.get(site_id,{}).get(AggregateStatusType.AGGREGATE_DATA,DeviceStatusType.STATUS_UNKNOWN)
        location_rollup = statuses.get(site_id,{}).get(AggregateStatusType.AGGREGATE_LOCATION,DeviceStatusType.STATUS_UNKNOWN)

        extended_site.computed.communications_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=comms_rollup)
        extended_site.computed.data_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=power_rollup)
        extended_site.computed.location_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=data_rollup)
        extended_site.computed.power_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=location_rollup)

        dep_util = DeploymentUtil(self.container)
        extended_site.deployment_info = dep_util.describe_deployments(extended_site.deployments,
                                                                      status_map=statuses)

        return extended_site

    def _get_instrument_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Creates a SiteExtension and status for instruments"""
        context = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)
        extended_site, RR2, inst_device_id, site_resources, site_children, device_relations, outil = \
            context["extended_site"], context["enhanced_RR"], context["site_device_id"], \
            context["site_resources"], context["site_children"], context["device_relations"], context["outil"]

        statuses = outil.get_status_roll_ups(site_id, include_structure=True)

        comms_rollup = statuses.get(site_id,{}).get(AggregateStatusType.AGGREGATE_COMMS,DeviceStatusType.STATUS_UNKNOWN)
        power_rollup = statuses.get(site_id,{}).get(AggregateStatusType.AGGREGATE_POWER,DeviceStatusType.STATUS_UNKNOWN)
        data_rollup = statuses.get(site_id,{}).get(AggregateStatusType.AGGREGATE_DATA,DeviceStatusType.STATUS_UNKNOWN)
        location_rollup = statuses.get(site_id,{}).get(AggregateStatusType.AGGREGATE_LOCATION,DeviceStatusType.STATUS_UNKNOWN)

        extended_site.computed.communications_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=comms_rollup)
        extended_site.computed.data_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=power_rollup)
        extended_site.computed.location_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=data_rollup)
        extended_site.computed.power_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=location_rollup)

        instrument_status = []
        if  extended_site.instrument_devices:
            for x in extended_site.instrument_devices:
                if x:
                    instrument_status.append(statuses.get(x._id,{}).get("agg", DeviceStatusType.STATUS_UNKNOWN))
                else:
                    instrument_status.append(DeviceStatusType.STATUS_UNKNOWN)
            extended_site.computed.instrument_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=instrument_status)
        else:
            extended_site.computed.instrument_status = ComputedListValue(status=ComputedValueAvailability.NOTAVAILABLE)

        extended_site.computed.platform_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=[])
        extended_site.computed.site_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=[])
        extended_site.computed.portal_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=[])

        dep_util = DeploymentUtil(self.container)
        extended_site.deployment_info = dep_util.describe_deployments(extended_site.deployments,
                                                                      status_map=statuses)

        return extended_site

    def get_deployment_extension(self, deployment_id='', ext_associations=None, ext_exclude=None, user_id=''):
        if not deployment_id:
            raise BadRequest("The deployment_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)
        extended_deployment = extended_resource_handler.create_extended_resource_container(
            extended_resource_type=OT.DeploymentExtension,
            resource_id=deployment_id,
            computed_resource_type=OT.DeploymentComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id)

        if not extended_deployment.device or not extended_deployment.site \
            or not hasattr(extended_deployment.device, '_id') \
            or not hasattr(extended_deployment.site, '_id'):
            return extended_deployment
            #raise Inconsistent('deployment %s should be associated with a device and a site' % deployment_id)

        log.debug('have device: %r\nand site: %r', extended_deployment.device.__dict__, extended_deployment.site.__dict__)
        RR2 = EnhancedResourceRegistryClient(self.clients.resource_registry)
        finder = RelatedResourcesCrawler()
        get_assns = finder.generate_related_resources_partial(RR2, [PRED.hasDevice])
        # search from PlatformDevice to subplatform or InstrumentDevice
        search_down = get_assns({PRED.hasDevice: (True, False)}, [RT.InstrumentDevice, RT.PlatformDevice])

        # collect ids of devices below deployment target
        platform_device_ids = set()
        instrument_device_ids = set()
        # make sure main device in deployment is in the list
        if extended_deployment.device.type_==RT.InstrumentDevice:
            instrument_device_ids.add(extended_deployment.device._id)
        else:
            platform_device_ids.add(extended_deployment.device._id)
        for a in search_down(extended_deployment.device._id, -1):
            if a.o != extended_deployment.device._id:
                if a.ot == RT.InstrumentDevice:
                    instrument_device_ids.add(a.o)
                else: # a.ot == RT.PlatformDevice:
                    platform_device_ids.add(a.o)

        # get sites (portals)
        extended_deployment.computed.portals = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=[])
        subsite_ids = set()
        device_by_site = { extended_deployment.site._id: extended_deployment.device._id }
        for did in platform_device_ids:
            related_sites = RR2.find_platform_site_ids_by_platform_device_using_has_device(did)
            for sid in related_sites:
                subsite_ids.add(sid)
                device_by_site[sid] = did
        for did in instrument_device_ids:
            related_sites = RR2.find_instrument_site_ids_by_instrument_device_using_has_device(did)
            for sid in related_sites:
                subsite_ids.add(sid)
                device_by_site[sid] = did

        # sort the objects into the lists to be displayed
        ids = list(platform_device_ids|instrument_device_ids|subsite_ids)
        device_by_id = { extended_deployment.device._id: extended_deployment.device }
        objs = self.RR.read_mult(ids)
        for obj in objs:
            if obj.type_==RT.InstrumentDevice:
                extended_deployment.instrument_devices.append(obj)
            elif obj.type_==RT.PlatformDevice:
                extended_deployment.platform_devices.append(obj)
            else: # InstrumentSite or PlatformSite
                extended_deployment.computed.portals.value.append(obj)

        # get associated models for all devices
        devices = list(platform_device_ids|instrument_device_ids)
        assocs = self.RR.find_associations(anyside=list(devices), id_only=False)
        ## WORKAROUND find_associations doesn't support anyside + predicate,
        # so must use anyside to find a list of values and filter for predicate later
        workaround = []
        for a in assocs:
            if a.p==PRED.hasModel:
                workaround.append(a)
        assocs = workaround
        ## end workaround

        model_id_by_device = { a.s: a.o for a in assocs }
        model_ids = set( [ a.o for a in assocs ])
        models = self.RR.read_mult( list(model_ids) )
        model_by_id = { o._id: o for o in models }

        extended_deployment.instrument_models = [ model_by_id[model_id_by_device[d._id]] for d in extended_deployment.instrument_devices ]
        extended_deployment.platform_models = [ model_by_id[model_id_by_device[d._id]] for d in extended_deployment.platform_devices ]
        for p in extended_deployment.computed.portals.value:
            if p._id in device_by_site and device_by_site[p._id] in device_by_id:
                extended_deployment.portal_instruments.append( device_by_id[device_by_site[p._id]] )


        # TODO -- all status values
        #
        #status: !ComputedIntValue
        ## combined list of sites and their status
        ##@ResourceType=InstrumentSite,PlatformSite
        #portal_status: !ComputedListValue
        ## status of device lists
        #instrument_status: !ComputedListValue
        #platform_status: !ComputedListValue

        from ion.util.extresource import strip_resource_extension, get_matchers, matcher_DataProduct, matcher_DeviceModel, \
            matcher_Device, matcher_UserInfo
        matchers = get_matchers([matcher_DataProduct, matcher_DeviceModel, matcher_Device, matcher_UserInfo])
        strip_resource_extension(extended_deployment, matchers=matchers)

        return extended_deployment




    #-----------------------------------------------
    #  COMPUTED RESOURCES
    #-----------------------------------------------
    def get_marine_facility_extension(self, org_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Returns an MarineFacilityOrgExtension object containing additional related information

        @param org_id               str
        @param ext_associations     dict
        @param ext_exclude          list
        @retval observatory    ObservatoryExtension
        @throws BadRequest     A parameter is missing
        @throws NotFound       An object with the specified observatory_id does not exist
        """

        if not org_id:
            raise BadRequest("The org_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_org = extended_resource_handler.create_extended_resource_container(
            extended_resource_type=OT.MarineFacilityOrgExtension,
            resource_id=org_id,
            computed_resource_type=OT.MarineFacilityOrgComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id,
            negotiation_status=NegotiationStatusEnum.OPEN)

        RR2 = EnhancedResourceRegistryClient(self.RR)
        RR2.cache_predicate(PRED.hasModel)
        RR2.cache_predicate(PRED.hasDevice)
        outil = ObservatoryUtil(self, enhanced_rr=RR2, device_status_mgr=DeviceStateManager())

        #Fill out service request information for requesting data products
        extended_org.data_products_request.service_name = 'resource_registry'
        extended_org.data_products_request.service_operation = 'find_objects'
        extended_org.data_products_request.request_parameters = {
            'subject': org_id,
            'predicate': 'hasResource',
            'object_type': 'DataProduct',
            'id_only': False,
            'limit': 10,
            'skip': 0
        }

        #Fill out service request information for requesting marine tracking resources - Assets
        extended_org.assets_request.service_name = 'resource_registry'
        extended_org.assets_request.service_operation = 'find_objects'
        extended_org.assets_request.request_parameters = {
            'subject': org_id,
            'predicate': 'hasResource',
            'object_type': 'Asset',
            'id_only': False,
            'limit': 10,
            'skip': 0
        }

        #Fill out service request information for requesting marine tracking resources - AssetTypes
        extended_org.asset_types_request.service_name = 'resource_registry'
        extended_org.asset_types_request.service_operation = 'find_objects'
        extended_org.asset_types_request.request_parameters = {
            'subject': org_id,
            'predicate': 'hasResource',
            'object_type': 'AssetType',
            'id_only': False,
            'limit': 10,
            'skip': 0
        }

        #Fill out service request information for requesting marine tracking resources - EventDuration
        extended_org.event_durations_request.service_name = 'resource_registry'
        extended_org.event_durations_request.service_operation = 'find_objects'
        extended_org.event_durations_request.request_parameters = {
            'subject': org_id,
            'predicate': 'hasResource',
            'object_type': 'EventDuration',
            'id_only': False,
            'limit': 10,
            'skip': 0
        }

        #Fill out service request information for requesting marine tracking resources - EventDurationTypes
        extended_org.event_duration_types_request.service_name = 'resource_registry'
        extended_org.event_duration_types_request.service_operation = 'find_objects'
        extended_org.event_duration_types_request.request_parameters = {
            'subject': org_id,
            'predicate': 'hasResource',
            'object_type': 'EventDurationType',
            'id_only': False,
            'limit': 10,
            'skip': 0
        }


        # extended object contains list of member ActorIdentity, so need to change to user info
        rr_util = ResourceRegistryUtil(self.container)
        extended_org.members = rr_util.get_actor_users(extended_org.members)

        #Convert Negotiations to OrgUserNegotiationRequest
        extended_org.open_requests = self._convert_negotiations_to_requests(extended_org, extended_org.open_requests)
        extended_org.closed_requests = self._convert_negotiations_to_requests(extended_org, extended_org.closed_requests)

        # lookup all hasModel predicates
        # lookup is a 2d associative array of [subject type][subject id] -> object id (model)
        lookup = dict([(rt, {}) for rt in [RT.InstrumentDevice, RT.PlatformDevice]])
        for a in RR2.filter_cached_associations(PRED.hasModel, lambda assn: assn.st in lookup):
            if a.st in lookup:
                lookup[a.st][a.s] = a.o

        def retrieve_model_objs(rsrc_list, object_type):
            # rsrc_list is devices that need models looked up.  object_type is the resource type (a device)
            # not all devices have models (represented as None), which kills read_mult.  so, extract the models ids,
            #  look up all the model ids, then create the proper output
            model_list = [lookup[object_type].get(r._id) for r in rsrc_list]
            model_uniq = list(set([m for m in model_list if m is not None]))
            model_objs = self.clients.resource_registry.read_mult(model_uniq)
            model_dict = dict(zip(model_uniq, model_objs))
            return [model_dict.get(m) for m in model_list]

        extended_org.instrument_models = retrieve_model_objs(extended_org.instruments, RT.InstrumentDevice)
        extended_org.platform_models = retrieve_model_objs(extended_org.platforms, RT.PlatformDevice)


        statuses = outil.get_status_roll_ups(org_id, include_structure=True)

        site_status = []
        if extended_org.sites:
            for x in extended_org.sites:
                if x:
                    site_status.append(statuses.get(x._id,{}).get("agg", DeviceStatusType.STATUS_UNKNOWN))
                else:
                    site_status.append(DeviceStatusType.STATUS_UNKNOWN)
            extended_org.computed.site_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=site_status)
        else:
            extended_org.computed.site_status = ComputedListValue(status=ComputedValueAvailability.NOTAVAILABLE)

        inst_status = []
        if extended_org.instruments:
            for x in extended_org.instruments:
                if x:
                    inst_status.append(statuses.get(x._id,{}).get("agg", DeviceStatusType.STATUS_UNKNOWN))
                else:
                    inst_status.append(DeviceStatusType.STATUS_UNKNOWN)
            extended_org.computed.instrument_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=inst_status)
        else:
            extended_org.computed.instrument_status = ComputedListValue(status=ComputedValueAvailability.NOTAVAILABLE)

        plat_status = []
        if extended_org.platforms:
            for x in extended_org.platforms:
                if x:
                    plat_status.append(statuses.get(x._id,{}).get("agg", DeviceStatusType.STATUS_UNKNOWN))
                else:
                    plat_status.append(DeviceStatusType.STATUS_UNKNOWN)
            extended_org.computed.platform_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=plat_status)
        else:
            extended_org.computed.platform_status = ComputedListValue(status=ComputedValueAvailability.NOTAVAILABLE)


        subset = []
        for site in extended_org.station_sites:
            if site.alt_resource_type=='StationSite':
                subset.append(site)
        extended_org.station_sites = subset

        station_status = []
        if extended_org.station_sites:
            for x in extended_org.station_sites:
                if x:
                    station_status.append(statuses.get(x._id,{}).get("agg", DeviceStatusType.STATUS_UNKNOWN))
                else:
                    station_status.append(DeviceStatusType.STATUS_UNKNOWN)
            extended_org.computed.station_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=station_status)
        else:
            extended_org.computed.station_status = ComputedListValue(status=ComputedValueAvailability.NOTAVAILABLE)

        comms_rollup = statuses.get(org_id,{}).get(AggregateStatusType.AGGREGATE_COMMS,DeviceStatusType.STATUS_UNKNOWN)
        power_rollup = statuses.get(org_id,{}).get(AggregateStatusType.AGGREGATE_POWER,DeviceStatusType.STATUS_UNKNOWN)
        data_rollup = statuses.get(org_id,{}).get(AggregateStatusType.AGGREGATE_DATA,DeviceStatusType.STATUS_UNKNOWN)
        location_rollup = statuses.get(org_id,{}).get(AggregateStatusType.AGGREGATE_LOCATION,DeviceStatusType.STATUS_UNKNOWN)

        extended_org.computed.communications_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=comms_rollup)
        extended_org.computed.data_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=power_rollup)
        extended_org.computed.location_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=data_rollup)
        extended_org.computed.power_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=location_rollup)

        dep_util = DeploymentUtil(self.container)
        extended_org.deployment_info = dep_util.describe_deployments(extended_org.deployments,
                                                                     status_map=statuses)

        from ion.util.extresource import strip_resource_extension, get_matchers, matcher_DataProduct, matcher_DeviceModel, \
            matcher_Device, matcher_UserInfo, matcher_MarineAsset
        matchers = get_matchers([matcher_DataProduct, matcher_DeviceModel, matcher_Device, matcher_UserInfo, matcher_MarineAsset])
        strip_resource_extension(extended_org, matchers=matchers)

        return extended_org

    def _get_root_platforms(self, RR2, platform_device_list):
        # get all relevant assocation objects
        filter_fn = lambda a: a.o in platform_device_list

        # get child -> parent dict
        lookup = dict([(a.o, a.s) for a in RR2.filter_cached_associations(PRED.hasDevice, filter_fn)])

        # root platforms have no parent, or a parent that's not in our list
        return [r for r in platform_device_list if (r not in lookup or (lookup[r] not in platform_device_list))]

    # return a table of device statuses for all given device ids
    def _get_master_status_table(self, RR2, site_tree_ids):
        platformdevice_tree_ids = []
        for s in site_tree_ids:
            platformdevice_tree_ids   += RR2.find_objects(s, PRED.hasDevice, RT.PlatformDevice, True)

        plat_roots = self._get_root_platforms(RR2, platformdevice_tree_ids)

        # build id -> aggstatus lookup table
        master_status_table = {}
        for plat_root_id in plat_roots:
            agg_status, _ = self.agent_status_builder.get_cumulative_status_dict(plat_root_id)
            if None is agg_status:
                log.warn("Can't get agg status for platform %s, ignoring", plat_root_id)
            else:
                for k, v in agg_status.iteritems():
                    master_status_table[k] = v

        return master_status_table


    # based on ALL the site ids in this tree, return a site rollup list corresponding to each site in the site_id_list
    def _get_site_rollup_list(self, RR2, master_status_table, site_id_list):

        # get rollup for each site
        master_status_rollup_list = []
        for s in site_id_list:
            #_, underlings = self.outil.get_child_sites(parent_site_id=s, id_only=True)
            master_status_rollup_list.append(self.agent_status_builder._crush_status_dict(
                self._get_site_rollup_dict(RR2, master_status_table, s)))

        return master_status_rollup_list

    # based on return a site rollup dict corresponding to a site in the site_id_list
    def _get_site_rollup_dict(self, RR2, master_status_table, site_id):

        outil = ObservatoryUtil(self, enhanced_rr=RR2)
        attr1, underlings = outil.get_child_sites(parent_site_id=site_id, id_only=True)

        def collect_all_children(site_id, child_site_struct, child_list):
            #walk the tree of site children and put all site ids (all the way down the hierarchy) into one list
            children = child_site_struct.get(site_id, [])
            for child in children:
                child_list.append(child)
                #see if this child has children
                more_children = child_site_struct.get(child, [])
                if more_children:
                    collect_all_children(child, child_site_struct, child_list)

            log.debug('collect_all_children  child_list:  %s', child_list)
            child_list = list( set(child_list ) )
            return child_list

        site_aggregate = {}
        all_site_ids = [site_id]
        all_site_ids = collect_all_children(site_id, underlings,  all_site_ids)

        site_aggregate = {}
        #all_site_ids = underlings.keys()
        all_device_ids = []
        for s in all_site_ids:
            all_device_ids += RR2.find_objects(s, PRED.hasDevice, RT.PlatformDevice, True)
            all_device_ids += RR2.find_objects(s, PRED.hasDevice, RT.InstrumentDevice, True)

        log.debug("Calculating cumulative rollup values for all_device_ids = %s", all_device_ids)
        for k, v in AggregateStatusType._str_map.iteritems():
            aggtype_list = [master_status_table.get(d, {}).get(k, DeviceStatusType.STATUS_UNKNOWN) for d in all_device_ids]
            log.debug("aggtype_list for %s is %s", v, zip(all_device_ids, aggtype_list))
            site_aggregate[k] = self.agent_status_builder._crush_status_list(aggtype_list)

        return site_aggregate



    def _get_platform_rollup_list(self, RR2, master_status_table, platform_id_list):
        finder = RelatedResourcesCrawler()
        get_assns = finder.generate_related_resources_partial(RR2, [PRED.hasDevice])
        full_crawllist = [RT.InstrumentDevice, RT.PlatformDevice]
        search_down = get_assns({PRED.hasDevice: (True, False)}, full_crawllist)


        # get rollup for each platform device
        master_status_rollup_list = []

        for p in platform_id_list:

            # the searches return a list of association objects, so compile all the ids by extracting them
            underlings = set([])
            # we want only those IDs that are not the input resource id
            for a in search_down(p, -1):
                underlings.add(a.o)
            underlings.add(p)

            master_status_rollup_list.append(self.agent_status_builder._crush_status_list(
                [self.agent_status_builder._crush_status_dict(master_status_table.get(k, {})) for k in underlings]
            ))

        return master_status_rollup_list


    def _convert_negotiations_to_requests(self, extended_marine_facility=None, negotiations=None):
        assert isinstance(extended_marine_facility, MarineFacilityOrgExtension)
        assert isinstance(negotiations, list)

        #Get all associations for user info
        assoc_list = self.clients.resource_registry.find_associations(predicate=PRED.hasInfo, id_only=False)

        ret_list = []
        followup_list = defaultdict(list)

        for neg in negotiations:

            request = IonObject(OT.OrgUserNegotiationRequest, ts_updated=neg.ts_updated, negotiation_id=neg._id,
                negotiation_type=NegotiationTypeEnum._str_map[neg.negotiation_type],
                negotiation_status=NegotiationStatusEnum._str_map[neg.negotiation_status],
                originator=ProposalOriginatorEnum._str_map[neg.proposals[-1].originator],
                request_type=neg.proposals[-1].type_,
                description=neg.description, reason=neg.reason,
                org_id=neg.proposals[-1].provider)

            # since this is a proxy for the Negotiation object, simulate its id to help the UI deal with it
            request._id = neg._id

            actor_assoc = [ a for a in assoc_list if a.s == neg.proposals[-1].consumer ]
            if actor_assoc:
                member_assoc = [ m for m in extended_marine_facility.members if m._id == actor_assoc[0].o ]
                if member_assoc:
                    request.user_id = member_assoc[0]._id
                    request.name = member_assoc[0].name
                else:
                    followup_list[actor_assoc[0].o].append(request)

            ret_list.append(request)

        # assign names/user_ids to any requests that weren't in the members list, likely enroll requests
        if len(followup_list):
            user_infos = self.clients.resource_registry.read_mult(followup_list.keys())
            udict = {}
            for u in user_infos:
                udict[u._id] = u

            for k, v in followup_list.iteritems():
                for request in v:
                    request.user_id = k
                    request.name    = udict[k].name

        return ret_list

    def check_deployment_activation_policy(self, process, message, headers):
        try:
            gov_values = GovernanceHeaderValues(headers=headers, process=process, resource_id_required=False)

        except Inconsistent, ex:
            return False, ex.message

        resource_id = message.get("deployment_id", None)
        if not resource_id:
            return False, '%s(%s) has been denied - no deployment_id argument provided' % (process.name, gov_values.op)

        # Allow actor to activate/deactivate deployment in an org where the actor has the appropriate role
        orgs,_ = self.clients.resource_registry.find_subjects(subject_type=RT.Org, predicate=PRED.hasResource, object=resource_id, id_only=False)
        for org in orgs:
            if (has_org_role(gov_values.actor_roles, org.org_governance_name, [ORG_MANAGER_ROLE, OBSERVATORY_OPERATOR])):
                log.error("returning true: "+str(gov_values.actor_roles))
                return True, ''

        return False, '%s(%s) has been denied since the user is not a member in any org to which the deployment id %s belongs ' % (process.name, gov_values.op, resource_id)
