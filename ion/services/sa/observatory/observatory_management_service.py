#!/usr/bin/env python

"""Service managing marine facility sites and deployments"""

import string
import time

import csv
from ion.util.xlsparser import XLSParser
from xlwt import Workbook
import binascii
import uuid
import re

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
from ion.services.sa.observatory.deployment_util import DeploymentUtil
from ion.services.sa.product.data_product_management_service import DataProductManagementService
from ion.processes.event.device_state import DeviceStateManager
from ion.util.geo_utils import GeoUtils
from ion.util.related_resources_crawler import RelatedResourcesCrawler
from ion.util.datastore.resources import ResourceRegistryUtil

from interface.services.sa.iobservatory_management_service import BaseObservatoryManagementService
from interface.objects import OrgTypeEnum, ComputedValueAvailability, ComputedIntValue, ComputedListValue, ComputedDictValue, AggregateStatusType, DeviceStatusType, TemporalBounds, DatasetWindow
from interface.objects import MarineFacilityOrgExtension, NegotiationStatusEnum, NegotiationTypeEnum, ProposalOriginatorEnum, GeospatialBounds
from interface.objects import EventCategoryEnum, ValueTypeEnum

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

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    #    Marine Asset Management - Helper utilities
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _get_type_resource_by_name(self, res_name, res_type):
        if not res_name:
            raise BadRequest('res_name parameter is empty')
        if not res_type:
            raise BadRequest('res_type parameter is empty')
        if res_type != RT.AssetType and res_type != RT.EventDurationType:
            raise BadRequest('invalid res_type value (%s)' % res_type)

        res_objs, res_keys = self.container.resource_registry.find_resources_ext(alt_id_ns=res_type, alt_id=res_name, id_only=False)
        type_resource = ''
        if res_keys:
            if len(res_keys) == 1:
                type_resource = res_objs[0]

        return type_resource

    def _get_resource_by_name(self, res_name, res_type):
        if not res_name:
            raise BadRequest('res_name parameter is empty')
        if not res_type:
            raise BadRequest('res_type parameter is empty')
        if res_type != RT.Asset and res_type != RT.EventDuration:
            raise BadRequest('invalid res_type value (%s)' % res_type)

        res_objs, res_keys = self.container.resource_registry.find_resources_ext(alt_id_ns=res_type, alt_id=res_name, id_only=False)
        resource = ''
        if res_objs:
            if len(res_objs) == 1:
                resource = res_objs[0]
        return resource

    def _get_type_resource_key_by_name(self, res_name, res_type):
        if not res_name:
            raise BadRequest('res_name parameter is empty')
        if not res_type:
            raise BadRequest('res_type parameter is empty')
        if res_type != RT.AssetType and res_type != RT.EventDurationType:
            raise BadRequest('invalid res_type value (%s)' % res_type)

        _, res_keys = self.container.resource_registry.find_resources_ext(alt_id_ns=res_type, alt_id=res_name, id_only=False)
        type_resource_key = ''
        if res_keys:
            if len(res_keys) == 1:
                type_resource_key = res_keys[0]

        return type_resource_key

    def _get_resource_key_by_name(self, res_name, res_type):
        if not res_name:
            raise BadRequest('res_name parameter is empty')
        if not res_type:
            raise BadRequest('res_type parameter is empty')

        if res_type != RT.Asset and res_type != RT.EventDuration:
            raise BadRequest('invalid res_type value (%s)' % res_type)

        _, res_keys = self.container.resource_registry.find_resources_ext(alt_id_ns=res_type, alt_id=res_name, id_only=False)
        resource_key = ''
        if res_keys:
            if len(res_keys) == 1:
                resource_key = res_keys[0]

        return resource_key

    def create_value(self, value=None):

        # no DateValue processing
        # no complex object processing:
        #    'LocatedValue', 'ObjectValue', 'ObjectReferenceValue', 'ResourceReferenceValue'
        #
        constructor_map = {bool.__name__ : OT.BooleanValue, int.__name__ : OT.IntegerValue,
                           float.__name__ : OT.RealValue, str.__name__ : OT.StringValue}

        if not type(value).__name__ in constructor_map :
            raise BadRequest("The type of value provided not supported")

        return IonObject(constructor_map[type(value).__name__],value=value)

    def create_complex_value(self, type=None, value=None):


        constructor_map = {'CodeValue' : OT.CodeValue, 'DateValue' : OT.DateValue,
                           'TimeValue' : OT.TimeValue, 'DateTimeValue' : OT.DateTimeValue }

        if not value :
            raise BadRequest("value parameter is empty")
        if not type :
            raise BadRequest("type parameter is empty")
        if type not in constructor_map :
            raise BadRequest("type provided is not supported")

        return IonObject(constructor_map[type],value=value)

    def isfloat(self, value):
        try:
            float(value)
            return True
        except:
            return False

    def isint(self, value):
        try:
            int(value)
            return True
        except:
            return False

    def isbool(self, value):
        try:
            if value:
                if value.lower() == 'true' or value.lower() == '1':
                    return True
            #bool(value)
            #return True
        except:
            return False

    def _get_org_ids(self, org_altids):
        # helper
        # For a list of org's altids, obtaining the corresponding org id for each and return in org_ids[]
        # todo are orgs always in namespace 'PRE'? doubt it
        org_ids = []
        if org_altids:
            for org_alias in org_altids:
                exist_ids, _ = self.container.resource_registry.find_resources_ext(alt_id_ns="PRE",alt_id=org_alias, id_only=True)
                if exist_ids:
                    id = exist_ids[0]
                    org_ids.append(id)

        return org_ids

    def get_list_csv(self, string):
        """For a comma separated string, return list of values.

        @param   string         ''          # input string of comma separated values
        @retval  return_list    []          # list of values
        """

        # helper
        return_list = []
        if string is not None:
            if string:
                if ',' in string:
                    tmp = string[:]
                    tmp_list = tmp.split(',')
                    for id in tmp_list:
                        _id = id.strip()
                        if _id not in return_list:
                            return_list.append(_id)
                else:
                    tmp = string[:]
                    tmp_val = tmp.strip()
                    return_list.append(tmp_val)

        return return_list

    def valid_cardinality(self, cardinality):
        """For a given cardinality verify it is one of valid cardinality value, return boolean.

        @param   cardinality        ''          # input string of comma separated values
        @retval  is_valid           bool        # boolean indicating validity of cardinality
        """
        # helper
        is_valid = False
        valid_cardinality = ['1..1', '0..1', '1..N', '0..N']
        if cardinality:
            cardinality = (cardinality.upper()).strip()
            if cardinality in valid_cardinality:
                is_valid = True

        return is_valid



    def get_picklist(self, res_type='', id_only=''):
        picklist = []
        bcheck = True
        if id_only:
            id_only = id_only.lower()
            if id_only == 'false':
                bcheck = False
        if res_type in RT:
            namespace = res_type
            res_objs, _ = self.container.resource_registry.find_resources_ext(alt_id_ns=namespace, id_only=False)
            if res_objs:
                for obj in res_objs:
                    tuple = []
                    if obj.name:
                        tuple.append(obj.name)
                        tuple.append(obj._id)
                        if not bcheck:
                            tuple.append(obj.alt_ids)
                        picklist.append(tuple)
        return picklist

    # helper picklists for altids (Asset and Event[Duration]s)
    def get_altids(self, res_type=''):
        picklist = []

        if res_type in RT:
            namespace = res_type
            res_objs, _ = self.container.resource_registry.find_resources_ext(alt_id_ns=namespace, id_only=False)
            if res_objs:
                for obj in res_objs:
                    tuple = []
                    if obj.name:
                        picklist.append(obj.alt_ids)
        return picklist

    def _get_item_value(self, item, item_id):
        """For a specific item (i.e. xlsx row) read item_id, strip resulting value.

        @param  item            {}      # dictionary for xlsx row
        @param  item_id         ''      # category to be processed
        @retval item_value      ''      # resulting value for item_id in item
        """
        item_value = ''
        if item_id in item:
            if item[item_id]:
                item_value = item[item_id]
                if item_value:
                    item_value = item_value.strip()

        return item_value

    def _get_item_value_boolean(self, item, item_id):
        """For a specific boolean item (i.e. xlsx row) read item_id; default is FALSE.

        @param  item            {}      # dictionary for xlsx row
        @param  item_id         ''      # category to be processed
        @retval item_value      ''      # resulting value for item_id in item
        """
        item_value = 'FALSE'
        if item_id in item:
            if item[item_id]:
                item_value = item[item_id]
                if item_value:
                    item_value = item_value.upper().strip()
                    if item_value != 'TRUE' and item_value != 'FALSE':            # look for '1' or '0'
                        if item_value == '1':
                            item_value = 'TRUE'
                        else:
                            item_value = 'FALSE'

        return item_value


    # -------------------------------------------------------------------------
    #   Marine Asset Management RESOURCES
    # -------------------------------------------------------------------------
    #   AssetType
    def create_asset_type(self, asset_type=None):
        """Create a AssetType resource.

        @param asset_type  RT.AssetType
        @retval asset_type_id str
        @throws: BadRequest if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        if not asset_type:
            raise BadRequest('asset_type object is empty')

        try:
            id = self.RR2.create(asset_type, RT.AssetType)
            return id
        except:
            #log.error('unknown exception [create_asset_type]', exc_info=True)
            raise BadRequest('Failed to create_asset_type')


    def read_asset_type(self, asset_type_id=''):
        """Read an AssetType resource.

        @param asset_type_id    str
        @retval asset_type      RT.AssetType
        @throws NotFound  object with specified id does not exists
        @throws: BadRequest if object does not have _id or _rev attribute
        """
        if not asset_type_id:
            raise BadRequest("The asset_type_id parameter is empty")

        return self.RR2.read(asset_type_id, RT.AssetType)

    def update_asset_type(self, asset_type=None):
        """Update an AssetType resource.

        @param asset_type   RT.AssetType
        @throws NotFound    object with specified id does not exist
        @throws: BadRequest if object does not have _id or _rev attribute
        """
        if not asset_type:
            raise BadRequest('asset_type object is empty')

        return self.RR2.update(asset_type, RT.AssetType)

    def delete_asset_type(self, asset_type_id=''):
        """Delete an AssetType resource.

        @param asset_type_id    str
        @throws NotFound        object with specified id does not exist
        @throws: BadRequest     if object does not have _id or _rev attribute
        """
        if not asset_type_id:
            raise BadRequest("The asset_type_id parameter is empty")

        try:
            self.RR2.retire(asset_type_id, RT.AssetType)
        except:
            raise NotFound('Object with specified id does not exist.')


    def force_delete_asset_type(self, asset_type_id=''):
        """Force delete an AssetType resource
        @param asset_type_id  str
        @throws NotFound    object with specified id does not exist
        @throws: BadRequest if object does not have _id or _rev attribute
        """
        if not asset_type_id:
            raise BadRequest("The asset_type_id parameter is empty")

        try:
            self.RR2.force_delete(asset_type_id, RT.AssetType)
        except:
            raise NotFound('Object with specified id does not exist.')


    def update_attribute_specifications(self, resource_id='', spec_dict=None):
        """ Update attribute_specifications of resource using spec_dict provided.

        @param resource_id   str        # id of RT.Asset or RT.EventDurationType
        @param spec_dict     []         # list of attribute specification name(s)
        @throws NotFound     object wth specified id does not exist
        @throws BadRequest   if object with specified id does not have_id or_rev attribute
        @throws Inconsistent unable to process resource of this type
        """
        # TODO NOTE: Must abide by state restriction model
        # Updating attribute_specification is dependent on state (i.e. if in integrated or deployment state,
        # updates are not permitted unless the operator has privileges to do so.
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')

        if not spec_dict:
            raise BadRequest('spec_dict parameter is empty')

        try:
            res = self.RR.read(resource_id)          # get resource, inspect type
        except:
            raise NotFound('object wth specified id does not exist')

        if res.type_ != RT.AssetType and res.type_ != RT.EventDurationType:
            raise Inconsistent('unable to process resource of this type (%s)' % res.type_)

        # todo revisit this
        update = False
        if res:
            for k, v in spec_dict.items():
                res.attribute_specifications[k] = v
                update = True

        if update:
            self.RR.update(res)

    def delete_attribute_specification(self, resource_id='', attr_spec_names=None):
        """Delete attribute_specifications in list of attr_spec_names and return the
        TypeResource attribute_specifications dictionary for resource_id.

        @param resource_id          str     # id of RT.Asset or RT.EventDurationType
        @param attr_spec_names      []      # list of attribute specification name(s)
        @retval r_obj               {}      # dictionary of attribute specification(s)
        @throws NotFound    object wth specified id does not exist
        @throws BadRequest  if object with specified id does not have_id or_rev attribute
        """
        # TODO NOTE: Must abide by state restriction model
        # Delete attribute_specifications in list of attr_spec_names and return the
        # TypeResource attribute_specifications dictionary for resource_id.

        if not resource_id:
            raise BadRequest('The resource_id parameter is empty')

        if not attr_spec_names:
            raise BadRequest('The attr_spec_names parameter is empty')

        as_obj = self.RR.read(resource_id)
        if as_obj:
            as_obj_type = as_obj.type_
            if as_obj_type == RT.AssetType:
                as_obj = self.read_asset_type(resource_id)
            elif as_obj_type == RT.EventDurationType:
                as_obj = self.read_event_duration_type(resource_id)
            else:
                raise BadRequest('Unable to process resource type with resource_id')
        else:
            raise NotFound('Unable to process resource with resource_id')

        update = False
        r_obj = None
        if as_obj:
            if as_obj.attribute_specifications:
                keys = as_obj.attribute_specifications.iterkeys()
                for attr_name in attr_spec_names:
                    if attr_name in keys:
                        del as_obj.attribute_specifications[attr_name]
                        update = True
            else:
                raise NotFound ('type resource with resource_id does not have attribute_specifications.')

            if update:
                self.RR.update(as_obj)
            r_obj = as_obj.attribute_specifications

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

        try:
            # Check (before creating new resource) that in fact the alt_ids value
            # provided is well formed and unique.
            if asset.alt_ids:
                name = asset.name
                altid_list = asset.alt_ids
                if len(altid_list) != 1:
                    raise Inconsistent('multiple alt_ids not permitted for Asset resources')
                chunk = altid_list[0]
                chunks = chunk.split(':')
                asset_ns = chunks[0]
                asset_altid_provided = chunks[1]
                if not asset_ns or not asset_altid_provided:
                    raise Inconsistent('malformed alt_ids provided for Asset; required format \'Asset:asset_name\'')
                if asset_ns != RT.Asset:
                    raise Inconsistent('invalid namespace (%s) provided for Asset resource' % asset_ns)
                if asset_altid_provided:
                    key = self._get_resource_key_by_name(asset_altid_provided, RT.Asset)
                    if key:
                        raise BadRequest('resource instance already exists (\'Asset\') with this altid: %s' % (asset_altid_provided))

            asset_id = self.RR2.create(asset, RT.Asset)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise Inconsistent('Invalid asset object')

        if asset_type_id:
            self.assign_asset_type_to_asset(asset_type_id, asset_id)

        return asset_id

    def read_asset(self, asset_id=''):
        """Read an Asset resource
        @param  asset_id    str
        @retval asset       RT.Asset
        @throws NotFound    object with specified id does not exist
        @throws BadRequest  the asset_id parameter is empty
        """
        if not asset_id:
            raise BadRequest('The asset_id parameter is empty')

        try:
            obj = self.RR2.read(asset_id, RT.Asset)
        except:
            raise NotFound('Object with specified id does not exist.')

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
        @throws BadRequest  'asset_update requires asset_attrs to be provided'
        @throws BadRequest  'update_asset: altid returned: %s; instance using current_altid_exists: %s'
        @throws BadRequest  (numerous error messages from get_unique_altid)
        @throws BadRequest  'update_asset failed'
        """
        # unique altid:
        # if altid not set, consider res.name value for alt_id name. Verify name to be used for alt_id
        # is unique within Asset namespace, if not append first 5 characters of id str.
        # If this provides a unique altid. ('Asset:molly-12345') then use it otherwise fail.
        # A fully formed unique altid is returned, else ''. Error conditions will throw.
        # todo finish attribute validation, scrub
        if not asset:
            raise BadRequest('asset object is empty')

        if not asset._id:
            raise NotFound('_id is empty')
        asset_id = asset._id

        asset_type_id = ''
        code_space_id = ''
        test_associations = []
        test_associations = self.RR.find_associations(subject=asset_id, predicate=PRED.implementsAssetType, id_only=False)
        if not test_associations:
            raise BadRequest('asset (id=%s) does not have association (PRED.implementsAssetType) defined' % asset_id)

        if len(test_associations) != 1:
            raise BadRequest('asset (id=%s) has more than one association (PRED.implementsAssetType) defined' % asset_id)

        asset_type_id = test_associations[0].o
        asset_type = self.read_asset_type(asset_type_id)

        if not asset_type.attribute_specifications:
            raise BadRequest('asset type (id: \'%s\') does not have attribute_specifications' % asset_type_id)

        attribute_specifications = asset_type.attribute_specifications
        if not asset.asset_attrs:
            raise BadRequest('asset_update requires asset_attrs to be provided')
        attributes = asset.asset_attrs
        value_type = ''
        for attribute_name, attribute in attributes.iteritems():
            if attribute_name in attribute_specifications:
                attribute_specification = attribute_specifications[attribute_name]
                attribute_specification = attribute_specifications[attribute_name]
                if 'value_type' not in attribute_specification:
                    raise BadRequest('attribute_specification requires value_type definition')
                value_type = attribute_specification['value_type']

            elif attribute_name not in attribute_specifications:
                value_type = attribute_specification['value_type']
                if value_type != 'CodeValue':
                    raise BadRequest('attribute (\'%s\') not in type resource attribute_specifications' % attribute_name)

            if value_type == 'CodeValue':
                code_space_id = self._get_code_space_id('MAM')      # this is terrible

            attribute_value = attribute['value']
            self._valid_attribute_value(attribute_specification, attribute_value, ['Asset'], code_space_id)

        try:
            altid = self.get_unique_altid(asset, RT.Asset)

            current_asset_obj = self.read_asset(asset._id)
            current_altids_exist = current_asset_obj.alt_ids
            if altid:
                if current_altids_exist:
                    raise BadRequest('update_asset: altid returned: %s; instance using current_altid_exists: %s' % (altid, current_altids_exist))
                asset.alt_ids.append(altid)
            self.RR2.update(asset, RT.Asset)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('update_asset failed')

        return

    def get_unique_altid(self,res=None, res_type=None):
        """ Using res and res_type, determine if res.alt_ids has a value, if not create unique and valid
        alt_id value to return. If res.alt_ids already has value(s), content may be malformed or invalid
        for marine tracking resources, therefore verify res.alt_ids consistent with marine tracking
        resources requirements.

        @param  res         resource        # RT.Asset or RT.EventDuration object
        @param  res_type    str             # value of Resource Type (RT)
        @throws BadRequest  res object is empty
        @throws BadRequest  res_type is empty
        @throws BadRequest  res_type (%s) invalid; must be one of system resource types' % res_type
        @throws BadRequest  object specified does not have type_ attribute
        @throws BadRequest  resource type (%s) does not match expected type (%s)' % (res.type_, res_type
        @throws BadRequest  res object does not have _id
        @throws BadRequest  unable to process resource of type %s', res_type
        @throws BadRequest  marine tracking resources require one and only one unique alt_id value (? check limitation)
        @throws BadRequest  alt_id provided has invalid namespace (%s); expected %s' % (ns, res_type)
        @throws BadRequest  alt_id provided for resource update is not unique (%s)' % name
        @throws BadRequest  Unable to create unique alt_id for this resource
        """
        if not res:
            raise BadRequest('res object is empty')
        if not res_type:
            raise BadRequest('res_type is empty')

        if res_type not in RT:
            raise BadRequest('res_type (%s) invalid; must be one of system resource types' % res_type)
        if not res.type_:
            raise BadRequest('object specified does not have type_ attribute')

        if res.type_ != res_type:
            raise BadRequest('resource type (%s) does not match expected type (%s)' % (res.type_, res_type))

        altid = ''
        if res._id:
            id5 = res._id[:5]
        else:
            raise BadRequest('res object does not have _id')

        if res.alt_ids:
            # verify one and only one properly formed unique alt_ids for res_type namespace...
            # verify attribute[attr_key_name] value available and matches alt_ids value provided! todo
            altids = res.alt_ids
            if len(altids) != 1:
                raise BadRequest('marine tracking resources require one and only one unique alt_id value')
            altid_to_check = altids[0][:]
            chunks = altid_to_check.split(':')
            if len(chunks) == 2:
                ns = chunks[0]
                name = chunks[1]
                if ns != res_type:
                    raise BadRequest('alt_id provided has invalid namespace (%s); expected %s' % (ns, res_type))
                if name:
                    # altid name is unique, verify integrity of update by ensuring name provided in altid is
                    # same resource by checking equality of ids
                    resource = self._get_resource_by_name(name, res_type)
                    if resource:
                        if resource._id != res._id:
                            raise BadRequest('alt_id provided for resource update is not unique (%s)' % name)

        else:
            if res.name:
                possible_alt_id = res.name
                if not possible_alt_id:
                    raise BadRequest('resource name value empty')

                resource = self._get_resource_by_name(possible_alt_id, res_type)
                if not resource:
                    altid = res_type + ":" + possible_alt_id
                else:
                    another_possible_alt_id = res.name + '-' + id5
                    resource = self._get_resource_by_name(another_possible_alt_id, res_type)
                    if not resource:
                        altid = res_type + ":" +  another_possible_alt_id
                    else:
                        raise BadRequest('Unable to create unique alt_id for this resource.')

            else:
                raise BadRequest('Unable to create unique alt_id for this resource')

        return altid


    def delete_asset(self, asset_id=''):
        """Delete an Asset resource

        @param asset_id     str
        @throws NotFound    object with specified id does not exist
        @throws BadRequest  'The asset_id parameter is empty'
        """
        if not asset_id:
            raise BadRequest('The asset_id parameter is empty')

        return self.RR2.retire(asset_id, RT.Asset)

    def force_delete_asset(self, asset_id=''):
        """ Force delete an Asset resource

        @param asset_id    str
        @throws NotFound   object with specified id does not exist
        @throws BadRequest  if object with specified id does not have_id or_rev attribute
        """
        if not asset_id:
            raise BadRequest('The asset_id parameter is empty')

        self.RR2.force_delete(asset_id, RT.Asset)

    def get_asset_extension(self, asset_id='', ext_associations=None, ext_exclude=None, user_id=''):
        if not asset_id:
            raise BadRequest("The asset_id parameter is empty")

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
        @throws NotFound      object with specified id does not exist
        @throws BadRequest  if object with specified id does not have_id or_rev attribute
        """
        if not asset_type_id:
            raise BadRequest('The asset_type_id parameter is empty')
        if not asset_id:
            raise BadRequest('The asset_id parameter is empty')

        self.RR2.assign_asset_type_to_asset_with_implements_asset_type(asset_type_id, asset_id)

    def unassign_asset_type_from_asset(self, asset_type_id='', asset_id=''):
        """Remove link of Asset from AssetType.

        @param asset_type_id  str
        @param asset_id       str
        @throws NotFound      object with specified id does not exist
        @throws BadRequest  if object with specified id does not have_id or_rev attribute
        """
        if not asset_type_id:
            raise BadRequest('The asset_type_id parameter is empty')
        if not asset_id:
            raise BadRequest('The asset_id parameter is empty')

        self.RR2.unassign_asset_type_from_asset_with_implements_asset_type(asset_type_id, asset_id)




    #
    #   EventDurationType
    #

    def create_event_duration_type(self, event_duration_type=None):
        """Create a EventDurationType resource.

        @param event_duration_type  RT.EventDurationType
        @retval event_duration_type_id str
        @throws NotFound    object with specified id does not exist
        @throws: BadRequest if object does not have _id or _rev attribute
        """
        # create the EventDurationType
        return self.RR2.create(event_duration_type, RT.EventDurationType)

    def read_event_duration_type(self, event_duration_type_id=''):
        """Read an EventDurationType resource.

        @param event_duration_type_id  str
        @retval event_duration_type    RT.EventDurationType
        @throws NotFound  object with specified id does not exists
        @throws: BadRequest if object does not have _id or _rev attribute
        """
        if not event_duration_type_id:
            raise BadRequest('The event_duration_type_id parameter is empty')

        return self.RR2.read(event_duration_type_id, RT.EventDurationType)

    def update_event_duration_type(self, event_duration_type=None):
        """Update an EventDurationType resource.

        @param event_duration_type  RT.EventDurationType
        """
        return self.RR2.update(event_duration_type, RT.EventDurationType)

    def delete_event_duration_type(self, event_duration_type_id=''):
        """Delete an EventDurationType resource.

        @param event_duration_type_id  str
        @throws: BadRequest if object does not have _id or _rev attribute
        """
        if not event_duration_type_id:
            raise BadRequest('The event_duration_type_id parameter is empty')

        return self.RR2.retire(event_duration_type_id, RT.EventDurationType)

    def force_delete_event_duration_type(self, event_duration_type_id=''):
        """Force delete an EventDurationType resource.

        @param event_duration__type_id  str
        @throws NotFound  object with specified id does not exist
        @throws: BadRequest if object does not have _id or _rev attribute
        """
        if not event_duration_type_id:
            raise BadRequest('The event_duration_type_id parameter is empty')

        self.RR2.force_delete(event_duration_type_id, RT.EventDurationType)

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

        try:
            # Check (before creating new resource) that in fact the alt_ids value
            # provided is well formed and unique.
            if event_duration.alt_ids:
                name = event_duration.name
                altid_list = event_duration.alt_ids
                if len(altid_list) != 1:
                    raise Inconsistent('multiple alt_ids not permitted for EventDuration resources')
                chunk = altid_list[0]
                chunks = chunk.split(':')
                res_ns = chunks[0]
                res_altid_provided = chunks[1]
                if not res_ns or not res_altid_provided:
                    raise Inconsistent('malformed EventDuration.alt_ids provided; required format \'EventDuration:event_name\'')
                if res_ns != RT.EventDuration:
                    raise Inconsistent('invalid namespace (%s) provided for EventDuration resource' % res_ns)
                if res_altid_provided:
                    key = self._get_resource_key_by_name(res_altid_provided, RT.EventDuration)
                    if key:
                        raise BadRequest('resource instance already exists (\'EventDuration\') with this altid: %s' % (res_altid_provided))

            # create the EventDuration
            event_duration_id = self.RR2.create(event_duration, RT.EventDuration)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise Inconsistent('Invalid event_duration object')



        if event_duration_type_id:
            self.assign_event_duration_type_to_event_duration(event_duration_type_id, event_duration_id)

        return event_duration_id

    def read_event_duration(self, event_duration_id=''):
        """Read an EventDuration resource.

        @param event_duration_id  str
        @retval event_duration    RT.EventDuration
        @throws BadRequest  The event_duration_id parameter is empty
        """
        if not event_duration_id:
            raise BadRequest('The event_duration_id parameter is empty')

        return self.RR2.read(event_duration_id, RT.EventDuration)

    def update_event_duration(self, event_duration=None):
        """Update an EventDuration resource and ensure alt_ids value (if provided) is well formed and
        unique in namespace. The event_duration object provided shall have event_duration_attrs
        defined and shall also have an association (PRED.implementsEventDurationType) defined or
        method shall fail. event_duration.event_duration_attrs and the association are required
        to perform validation and constraint checks prior to update.
        @param event_duration    RT.EventDuration
        @throws BadRequest      'update_event_duration failed'
        @throws BadRequest      'The event_duration parameter is empty'
        @throws BadRequest      'event_duration_update requires event_duration_attrs to be provided'
        @throws BadRequest      'event_duration_update: altid returned: %s and current_altid_exists: %s'
        """
        # todo finish attribute validation, scrub
        if not event_duration:
            raise BadRequest('The event_duration parameter is empty')

        if not event_duration._id:
            raise NotFound('_id is empty')
        event_duration_id = event_duration._id

        event_duration_type_id = ''
        test_associations = self.RR.find_associations(subject=event_duration_id, predicate=PRED.implementsEventDurationType, id_only=True)
        if not test_associations:
            raise BadRequest('event_duration (id=%s) does not have association (PRED.implementsEventDurationType) defined' % event_duration_id)
        if test_associations:
            if len(test_associations) != 1:
                raise BadRequest('event_duration (id=%s) has more than one association (PRED.implementsEventDurationType) defined' % event_duration_id)
            event_duration_type_id = test_associations[0]

        if not event_duration.event_duration_attrs:
            raise BadRequest('event_duration_update requires event_duration_attrs to be provided')

        try:
            altid = self.get_unique_altid(event_duration, RT.EventDuration)
            current_event_obj = self.read_event_duration(event_duration._id)
            current_altids_exist = current_event_obj.alt_ids
            if altid:
                if current_altids_exist:
                    raise BadRequest('event_duration_update: altid returned: %s and current_altid_exists: %s' % (altid, current_altids_exist))

                event_duration.alt_ids.append(altid)

            self.RR2.update(event_duration, RT.EventDuration)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except:
            raise BadRequest('update_event_duration failed')

        return

    def delete_event_duration(self, event_duration_id=''):
        """Delete an EventDuration resource.

        @param event_duration_id   str
        @throws BadRequest  The event_duration_id parameter is empty
        """
        if not event_duration_id:
            raise BadRequest('The event_duration_id parameter is empty')

        return self.RR2.retire(event_duration_id, RT.EventDuration)

    def force_delete_event_duration(self, event_duration_id=''):
        """ Force delete an EventDuration resource.

        @param event_duration_id    str
        @throws BadRequest  The event_duration_id parameter is empty
        """
        if not event_duration_id:
            raise BadRequest('The event_duration_id parameter is empty')

        self.RR2.force_delete(event_duration_id, RT.EventDuration)

    def assign_event_duration_type_to_event_duration(self, event_duration_type_id='', event_duration_id=''):
        """ Link an EventDuration to an EventDurationType.

        @param event_duration_type_id  str
        @param event_duration_id       str
        @throws BadRequest  The event_duration_type_id parameter is empty
        @throws BadRequest  The event_duration_id parameter is empty
        """
        if not event_duration_type_id:
            raise BadRequest('The event_duration_type_id parameter is empty')

        if not event_duration_id:
            raise BadRequest('The event_duration_id parameter is empty')

        self.RR2.assign_event_duration_type_to_event_duration_with_implements_event_duration_type(event_duration_type_id,event_duration_id)

    def unassign_event_duration_type_from_event_duration(self, event_duration_type_id='', event_duration_id=''):
        """Remove link of EventDuration from EventDurationType.

        @param event_duration_type_id  str
        @param event_duration_id       str
        @throws BadRequest      The event_duration_type_id parameter is empty
        @throws BadRequest      The event_duration_id parameter is empty
        """
        if not event_duration_type_id:
            raise BadRequest('The event_duration_type_id parameter is empty')

        if not event_duration_id:
            raise BadRequest('The event_duration_id parameter is empty')

        self.RR2.unassign_event_duration_type_from_event_duration_with_implements_event_duration_type(event_duration_type_id, event_duration_id)

    def get_event_duration_extension(self, event_duration_id='', ext_associations=None, ext_exclude=None, user_id=''):
        if not event_duration_id:
            raise BadRequest("The event_duration_id parameter is empty")

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
            try:
                event_obj = self.read_event_duration(event_duration_id)
            except:
                raise NotFound('event duration instance not found')
            try:
                asset_obj = self.read_asset(asset_id)
            except:
                raise NotFound('asset instance not found')

            # get event duration type that event duration implements; then retrieve event_category
            predicate = 'implementsEventDurationType'
            assoc_category = ''
            associations = self.RR.find_associations(subject=event_duration_id, predicate=predicate, id_only=False)
            if associations:
                if len(associations) == 1:
                    assoc_id = associations[0].o
                else:
                    raise Inconsistent('this event duration has multiple event duration types')
            else:
                raise BadRequest('this event duration does not have associated event duration type')

            event_type_obj = self.read_event_duration_type(assoc_id)

            # map event_duration_type.category to event_to_asset_association type
            assoc_category_id = event_type_obj.event_category
            if assoc_category_id == EventCategoryEnum.Location:
                assoc_category = PRED.hasLocationEvent
            elif assoc_category_id == EventCategoryEnum.Operability:
                assoc_category = PRED.hasOperabilityEvent
            elif assoc_category_id == EventCategoryEnum.Verification:
                assoc_category = PRED.hasVerificationEvent
            elif assoc_category_id == EventCategoryEnum.Assembly:
                assoc_category = PRED.hasAssemblyEvent
            else:
                raise BadRequest('unknown EventCategoryEnum value for association category')

            test_associations = self.RR.find_associations(subject=asset_id, predicate=assoc_category, id_only=False)
            if test_associations:
                if test_associations[0].o != event_duration_id:
                    raise BadRequest('an association (%s) already exists, cannot assign more than one association of the same type' % assoc_category)
            else:
                if assoc_category   == PRED.hasLocationEvent:
                    self.RR2.assign_event_duration_to_asset_with_has_location_event(event_duration_id, asset_id)
                elif assoc_category == PRED.hasOperabilityEvent:
                    self.RR2.assign_event_duration_to_asset_with_has_operability_event(event_duration_id, asset_id)
                elif assoc_category == PRED.hasVerificationEvent:
                    # singleton
                    self.RR2.assign_event_duration_to_asset_with_has_verification_event(event_duration_id, asset_id)
                elif assoc_category == PRED.hasAssembyEvent:
                    # singleton
                    self.RR2.assign_event_duration_to_asset_with_has_assembly_event(event_duration_id, asset_id)
                else:
                    raise BadRequest('unknown association category predicate (Event to Asset)')

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('failed to assign association (%s)' % assoc_category)


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
            # get event duration type that event duration implements
            predicate = 'implementsEventDurationType'
            assoc_category = ''
            associations = self.RR.find_associations(subject=event_duration_id, predicate=predicate, id_only=False)
            if associations:
                if len(associations) == 1:
                    assoc_id = associations[0].o
                else:
                    raise Inconsistent('this event duration implements multiple event duration types')
            else:
                raise BadRequest('this event duration does not have associated event duration type')

            event_type_obj = self.read_event_duration_type(assoc_id)

            # map event_duration_type.category to event_to_asset_association type
            assoc_category_id = event_type_obj.event_category
            if assoc_category_id == EventCategoryEnum.Location:
                assoc_category = PRED.hasLocationEvent
            elif assoc_category_id == EventCategoryEnum.Operability:
                assoc_category = PRED.hasOperabilityEvent
            elif assoc_category_id == EventCategoryEnum.Verification:
                assoc_category = PRED.hasVerificationEvent
            elif assoc_category_id == EventCategoryEnum.Assembly:
                assoc_category = PRED.hasAssemblyEvent

            # Now identify the association id of the event to asset relationship
            assoc = self.RR.find_associations(object=event_duration_id, predicate=assoc_category,subject=asset_id, id_only=True)
            if assoc:
                if len(assoc) == 1:
                    delete_id = assoc[0]
                    if delete_id:
                        self.RR.delete_association(delete_id)
                else:
                    raise Inconsistent('this event duration has multiple associations with asset')
            else:
                # raise or fail silently? recommend fail silently...
                raise BadRequest('this event duration is not associated with asset')

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())

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
            id = self.RR2.create(code_space, RT.CodeSpace)
        except:
            raise Inconsistent('invalid code_space object')

        return id

    def read_code_space(self, resource_id=''):
        """Read an CodeSpace resource.

        @param resource_id  str
        @retval code_space RT.CodeSpace
        @throws BadRequest 'resource_id parameter is empty'
        @throws NotFound   'object with specified id does not exist'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        try:
            obj = self.RR2.read(resource_id, RT.CodeSpace)
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

        return self.RR2.update(code_space, RT.CodeSpace)

    def delete_code_space(self, resource_id=''):
        """Delete a CodeSpace resource.

        @param resource_id  str
        @throws BadRequest 'resource_id parameter is empty'
        @throws NotFound   'object with specified id does not exist.'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        try:
            obj = self.RR2.retire(resource_id, RT.CodeSpace)
        except:
            raise NotFound('object with specified id does not exist.')

        return obj


    def force_delete_code_space(self, resource_id=''):
        """ Force delete a CodeSpace resource.

        @param resource_id      str
        @throws BadRequest      'resource_id parameter is empty'
        @throws NotFound        'object with specified id does not exist.'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')

        try:
            obj = self.RR2.force_delete(resource_id, RT.CodeSpace)
        except:
            raise NotFound('object with specified id does not exist.')

        return obj


    def read_codesets_by_name(self, resource_id='', names=None):
        """Read CodeSpace (id=resource_id) for list of codeset name(s); return list of CodeSets.

        @param resource_id  str
        @param names        []
        @throws: BadRequest 'resource_id parameter is empty'
        @throws: BadRequest 'names parameter is empty'
        @throws NotFound    'object with specified resource_id (type RT.CodeSpace) does not exist'
        """
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        if not names:
            raise BadRequest('names parameter is empty')

        codesets = []
        try:
            cs_obj = self.RR.read(resource_id, RT.CodeSpace)
        except:
            raise NotFound('object with specified resource_id (type RT.CodeSpace) does not exist')

        if cs_obj.codesets:
            keys = cs_obj.codesets.keys()
            for name in names:
                if name in keys:
                    rc = cs_obj.codesets[name]
                    if rc:
                        if rc not in codesets:      # added check
                            codesets.append(rc)
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

        codes = []
        try:
            cs_obj = self.RR.read(resource_id, RT.CodeSpace)
        except:
            raise NotFound('object with specified resource_id (type RT.CodeSpace) does not exist')

        if cs_obj.codes:
            keys = cs_obj.codes.keys()
            for name in names:
                if name in keys:
                    rc = cs_obj.codes[name]
                    if rc:
                        if id_only:
                            if id not in codes:     # added check
                                codes.append(rc.id)
                        else:
                            if rc not in codes:     # added check
                                codes.append(rc)
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

        try:
            cs_obj = self.RR.read(resource_id, RT.CodeSpace)
        except:
            raise NotFound('object with specified resource_id and type=RT.CodeSpace does not exist')

        update = False
        if cs_obj:
            if cs_obj.codes:
                keys = codes.keys()
                for key in keys:
                    if codes[key]:
                        try:
                            cs_obj.codes[codes[key].name] = codes[key]
                            update = True
                        except:
                            raise NotFound('code not found in CodeSpace (with id=resource_id).')
                    else:
                        raise NotFound('code provided for update with empty name.')
                if update:
                    self.RR.update(cs_obj)
            else:
                raise NotFound('codes not found in CodeSpace (with id=resource_id).')


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

        try:
            cs_obj = self.RR.read(resource_id, RT.CodeSpace)
        except:
            raise NotFound('object with specified resource_id and type=RT.CodeSpace does not exist')

        update = False
        if cs_obj.codesets:
            keys = codesets.keys()
            for key in keys:
                if codesets[key]:
                    try:
                        cs_obj.codesets[codesets[key].name] = codesets[key]
                        update = True
                    except:
                        raise NotFound('CodeSet not found in CodeSpace.')
                else:
                    raise NotFound('CodeSet provided for update with empty name.')
            if update:
                self.RR.update(cs_obj)
        else:
            raise NotFound('CodeSpace codesets is empty.')

    def delete_codes(self, resource_id='', names=None):
        """Delete Codes (identified in names list) from CodeSpace; return list of Codes in CodeSpace.
        Check if code is used by code_set; if so, remove code fom code_set, update code_set and then
        delete the code.

        @param  resource_id     str
        @param  names           []
        @throws BadRequest      'resource_id parameter is empty'
        @throws BadRequest      'names parameter is empty'
        @throws NotFound        'object with resource_id and type RT.CodeSpace does not exist
        """
        #todo scrub
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        if not names:
            raise BadRequest('names parameter is empty')

        codes_list = []
        try:
            cs_obj = self.RR.read(resource_id, RT.CodeSpace)
        except:
            raise NotFound('object with resource_id and type RT.CodeSpace does not exist')

        update = False
        if cs_obj.codes:

            for name in names:
                if cs_obj.codes:
                    keys = cs_obj.codes.keys()
                    if name in keys:
                        # code exists, is it used in a codeset?
                        code_id = cs_obj.codes[name].id
                        code_name = cs_obj.codes[name].name
                        code_in_use = False
                        for cset in cs_obj.codesets:

                            if cs_obj.codesets[cset].enumeration:
                                if code_name in cs_obj.codesets[cset].enumeration:
                                    # if code name in use remove from codeset; update codeset when code space updated
                                    if cs_obj.codesets[cset]:
                                        cs_obj.codesets[cset].enumeration.remove(code_name)
                                        update = True

                        if not code_in_use:   # not needed if now removing from all codesets and force deletion
                            del cs_obj.codes[name]
                            update = True
            if update:
                self.RR.update(cs_obj)
            if cs_obj.codes:
                codes_list = []
                code_names = cs_obj.codes.keys()
                for n in code_names:
                    rc = cs_obj.codes[n]
                    if rc:
                        if rc not in codes_list:
                            codes_list.append(rc)

        return codes_list

    def delete_codesets(self, resource_id='', names=None):
        """Delete CodeSets identified in list names; return list of CodeSets in CodeSpace.

        @param resource_id      str
        @param names            []
        @throws BadRequest      'resource_id parameter is empty'
        @throws BadRequest      'names parameter is empty'
        @throws NotFound        'object with resource_id and type RT.CodeSpace does not exist
        """
        #todo (* Return value scheduled to change.)
        if not resource_id:
            raise BadRequest('resource_id parameter is empty')
        if not names:
            raise BadRequest('names parameter is empty')

        try:
            cs_obj = self.RR.read(resource_id, RT.CodeSpace)
        except:
            raise NotFound('object with resource_id and type RT.CodeSpace does not exist')

        codeset_list = []

        update = False
        if cs_obj.codesets:
            keys = cs_obj.codesets.keys()
            for name in names:
                if name in keys:
                    del cs_obj.codesets[name]
                    update = True

            if update:
                self.RR.update(cs_obj)

            if cs_obj.codesets:
                codeset_names = cs_obj.codesets.keys()
                codeset_list = []
                for n in codeset_names:
                    rc = cs_obj.codesets[n]
                    if rc:
                        if rc not in codeset_list:
                            codeset_list.append(rc)

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
        @throws NotFound            if object with specified id does not exist
        @throws Inconsistent        if object with specified id does not exist
        """
        if not content:
            raise BadRequest('content parameter is empty')

        try:
            response = self._process_xls(content, content_type, content_encoding)

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
        @throws BadRequest      (from _download_xls)
        @throws NotFound        (from _download_xls)
        @throws Inconsistent    (from _download_xls)
        """
        try:
            content = self._download_xls()

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('asset tracking report failed to produce xls')

        return content

    # helper picklists for altids (Asset and Event[Duration]s)
    def get_assets_picklist(self, id_only=''):
        picklist = self.get_picklist(RT.Asset, id_only)
        return picklist

    def get_events_picklist(self, id_only=''):
        picklist = self.get_picklist(RT.EventDuration, id_only)
        return picklist


    def set_required_columns(self):
        """Set required columns for all [input xlsx] sheets.

        @param  all_sheets              []  # list of all sheet names (categories)
        @param  CodeSpaceCols           []  # valid columns for CodeSpaces sheet
        @param  CodeCols                []  # valid columns for Codes sheet
        @param  CodeSetCols             []  # valid columns for CodeSets sheet
        @param  TypeCols                []  # valid columns for AssetTypes sheet
        @param  EventTypeCols           []  # valid columns for EventDurationTypes sheet
        @param  ResourceCols            []  # valid columns for Assets and Events sheets
        @param  TypeAttributeSpecCols   []  # valid columns for <type>AttributeSpecs sheet
        @param  EventAssetMapCols       []  # valid columns for EventAssetMapCols sheet
        @retval required_columns        {}  # dictionary of valid columns by category
        """
        # helper function  for all available sheets, set required columns for headers of sheet;
        # returns dictionary of required columns for all sheets keyed by sheet name

        all_sheets = ['CodeSpaces', 'Codes', 'CodeSets', 'AssetTypes', 'EventTypes', 'Assets', 'Events',
                          'AssetAttributeSpecs', 'EventAttributeSpecs',
                          'AssetAttributes', 'EventAttributes', 'EventAssetMap']

        # Required columns for available sheets
        CodeSpaceCols = ['CodeSpace ID','Action','Description']
        CodeCols = ['CodeSpace ID','Action','Code ID', 'Description']
        CodeSetCols = ['CodeSpace ID','Action','CodeSet ID', 'Description', 'Enumeration']
        TypeCols = ['Resource ID',	'Action', 'Resource Type', 'Extends', 'Facility IDS', 'Concrete Type', 'Description']
        EventTypeCols = ['Resource ID',	'Action', 'Resource Type', 'Extends', 'Facility IDS', 'Concrete Type',
                         'Event Category', 'Description']
        EventAssetMapCols = ['Event Resource ID','Asset Resource ID', 'Action', 'Description']
        ResourceCols = ['Resource ID',	'Action', 'Resource Type', 'Implements', 'Facility IDS', 'Description']
        TypeAttributeSpecCols = ['Resource ID', 'Action',
                                 'Attribute ID', 'Value Type', 'Group Label',	'Attribute Label',	'Attribute Rank',
                                 'Visibility', 'Description', 'Cardinality',
                                 'Value Constraints', 'Value Pattern', 'Default Value', 'UOM',
                                 'Editable', 'Journal']

        required_columns = {}
        for category in all_sheets:
            if category == 'CodeSpaces':
                required_columns[category] = CodeSpaceCols
            elif category == 'Codes':
                required_columns[category] = CodeCols
            elif category == 'CodeSets':
                required_columns[category] = CodeSetCols
            elif category == 'AssetTypes':
                required_columns[category] = TypeCols
            elif category == 'EventTypes':
                required_columns[category] = EventTypeCols
            elif (category == 'Assets') or (category == 'Events'):
                required_columns[category] = ResourceCols
            elif (category == 'AssetAttributeSpecs') or (category == 'EventAttributeSpecs'):
                required_columns[category] = TypeAttributeSpecCols
            elif category == 'EventAssetMap':
                required_columns[category] = EventAssetMapCols

        return required_columns

    def valid_header(self, category, category_header, required_columns):
        if not category:
            raise BadRequest('category parameter is empty')
        if not category_header:
            raise BadRequest('category_header parameter is empty')
        if not required_columns:
            raise BadRequest('required_columns parameter is empty')

        valid = True
        for required_column in required_columns:
            if required_column not in category_header:
                valid = False
                break

        return valid

    def valid_value_type(self, value_type):
        if not value_type:
            raise BadRequest('value_type parameter is empty')

        valid_value_types = ['IntegerValue', 'RealValue', 'Boolean', 'StringValue',
                            'CodeValue', 'DateValue', 'TimeValue', 'DateTimeValue' ]
        # 'LocatedValue', 'ResourceReferenceValue'
        valid = False
        if value_type in valid_value_types:
            valid = True

        return valid

    def _get_working_objects(self, content, content_type, content_encoding):
        """Read all [spread sheet] content which defines asset tracking resources,
        instantiate resources then return dictionary of resource ids by category.

        @param  content              encoded blob              # binascii.b2a_hex(content)
        @param  content_type         file_descriptor.mimetype  # file descriptor type
        @param  content_encoding     'b2a_hex'                 # encoding (set to binascii.b2a_hex)
        @retval response             {}                        # dict of resource ids by category of resource type
        @throws BadRequest      if object with specified id does not have_id or_rev attribute
        @throws NotFound        if object with specified id does not have_id or_rev attribute
        @throws Inconsistent    if object with specified id does not have_id or_rev attribute
        """

        if not content:
            raise BadRequest('content parameter is empty')

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        #  Initialize variables for sheets (names, columns, etc.)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        AvailableSheets = ['CodeSpaces', 'Codes', 'CodeSets', 'AssetTypes', 'EventTypes', 'Assets', 'Events',
                          'AssetAttributeSpecs', 'EventAttributeSpecs',
                          'AssetAttributes', 'EventAttributes', 'EventAssetMap']

        # Required columns for available sheets
        CodeRelatedSheets = ['CodeSpaces', 'Codes', 'CodeSets']

        # Valid Action values, TypeResources, ValueTypes, cardinality
        valid_actions = ['add', 'remove']
        valid_type_resources = [RT.AssetType, RT.EventDurationType]
        valid_resources = [RT.Asset, RT.EventDuration]
        valid_value_types = ['LocatedValue', 'IntegerValue', 'RealValue', 'Boolean', 'StringValue',
                            'CodeValue', 'DateValue', 'TimeValue', 'DateTimeValue', 'ResourceReferenceValue']

        valid_cardinality = ['1..1', '0..1', '1..N', '0..N']
        const_code_space_name = "MAM"

        # Prepare response structure
        response = {}
        modified = {}
        modified['asset_types'] = []
        modified['assets'] = []
        modified['event_types'] = []
        modified['events'] = []
        modified['codespaces'] = []

        deleted = {}
        deleted['asset_types'] = []
        deleted['assets'] = []
        deleted['event_types'] = []
        deleted['events'] = []
        response['res_modified'] = modified
        response['res_removed'] = deleted
        status_value = 'ok'
        error_msg = ''

        response['status'] = status_value  # ok | error
        response['err_msg'] = error_msg
        response['res_modified'] = modified
        response['res_removed'] = deleted

        #-------------------------------------------------------------------------------------------
        # Step 1. Read and perform checks on input xlsx content. Load all data into object_definitions.
        #         Also get header row for sheets AssetAttributes and EventDurationAttributes
        #-------------------------------------------------------------------------------------------
        try:
            xcontent = binascii.a2b_hex(content)
            xls_files = XLSParser().extract_csvs(xcontent)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except:
            raise BadRequest('failed to process encoded content')

        object_definitions = {}
        hrCategories = {}
        required_columns = self.set_required_columns()

        try:
            for category in AvailableSheets:

                if category in xls_files:
                    reader = csv.DictReader(xls_files[category], delimiter=',')

                    if category in CodeRelatedSheets:
                        object_definitions[category], header = self._select_rows_for_codes(reader, category)
                        hrCategories[category] = header.keys()

                    elif category == 'EventAssetMap':
                        object_definitions[category], header = self._select_rows_for_event_asset_map(reader, category)
                        hrCategories[category] = header.keys()

                    elif category in AvailableSheets:
                        object_definitions[category], header = self._select_rows(reader, category)
                        hrCategories[category] = header.keys()


            if not object_definitions:
                raise BadRequest('no valid object definitions in content provided (check sheet names).')

            if 'AssetAttributes' in object_definitions:
                if 'AssetAttributes' not in hrCategories:
                    msg = 'failed to process header row for AssetAttributes.'
                    raise BadRequest(msg)
            if 'EventAttributes' in object_definitions:
                if 'EventAttributes' not in hrCategories:
                    msg = 'failed to process header row for EventAttributes.'
                    raise BadRequest(msg)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('failed to process decoded xlsx content.')

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Get object_definitions by category to process working objects such as AssetTypes, AssetResources, etc.
        # including hierarchical relations, validate references, (if an implements or extends
        # value is provided verify existence of type resource instance (e.g. prior definition.)
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        CodeSpaces = {}
        Codes = {}
        CodeSets = {}

        AssetTypes = {}                         # populated with AssetType working objects
        EventDurationTypes = {}                 # populated with EventDurationTypes working objects
        AssetResources = {}                     # populated with Asset working objects
        oAssetResources = []                    # populated with list of Asset working object names
        EventDurationResources = {}             # populate working objects for EventDuration resources
        oEventDurationResources = []            # populated with list of EventDuration working object names
        AssetTypeAttributeSpecs = {}            # AttributeSpecifications in dict key is AssetType altid
        EventDurationTypeAttributeSpecs = {}    # AttributeSpecifications in dict key is AssetType altid
        EventAssetMap = {}                      # association betweenAssets and Event Durations

        oAssetTypes = []                        # ordered list of AssetTypes
        oEventDurationTypes = []                # ordered list of EventTypes
        AssetTypeAttrSpecs = {}                 # AttributeSpecifications by AssetType altid
        EventDurationTypeAttrSpecs = {}         # AttributeSpecifications by EventDurationType altid
        xAssetTypeAttrSpecs = {}                # AttributeSpecifications by AssetType altid
        xEventDurationTypeAttrSpecs = {}        # AttributeSpecifications by EventDurationType altid

        code_space_id = self._get_code_space_id(const_code_space_name)
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Step 2. Process categories in RequiredSheets (e.g. tabs); populate working objects to
        # aggregate and manipulate data retrieved from each category. Attribute values (last two sheets
        # process later.
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        try:

            # No category 'CodeSpaces', then get CodeSpace id from system instance
            # of CodeSpace (const_code_space_name = "MAM")
            if ('CodeSpaces' not in object_definitions) and (not code_space_id):
                raise BadRequest('Unable to process input without CodeSpace instance or CodeSpaces sheet.')

            for category in AvailableSheets:

                if category == 'CodeSpaces':
                    if category in object_definitions:
                        if(not self.valid_header(category, hrCategories[category], required_columns[category])):
                            raise BadRequest('Sheet %s has invalid column headers' % category)

                        CodeSpaces = self._process_wo_codespaces(object_definitions[category], category, const_code_space_name)
                        if not CodeSpaces:
                            raise BadRequest('Failed to obtain CodeSpaces working objects.')

                        codespace_ids, deleted_codespace_ids = self._process_code_spaces(CodeSpaces, code_space_id)

                        if not code_space_id:
                            code_space_id = self._get_code_space_id(const_code_space_name)
                            if not code_space_id:
                                raise BadRequest('Unable to process input without CodeSpace instance or CodeSpaces sheet.')

                elif category == 'Codes':
                    if not code_space_id:
                        code_space_id = self._get_code_space_id(const_code_space_name)
                        if not code_space_id:
                            raise BadRequest('Unable to process input without CodeSpace instance or CodeSpaces sheet.')

                    if category in object_definitions:
                        if not self.valid_header(category, hrCategories[category], required_columns[category]):
                            raise BadRequest('Sheet %s has invalid column headers' % category)

                        Codes = self._process_wo_codes(object_definitions[category], category, code_space_id)

                        if Codes:
                            code_names, deleted_code_names = self._process_codes(Codes, code_space_id)

                elif category == 'CodeSets':
                    if not code_space_id:
                        code_space_id = self._get_code_space_id(const_code_space_name)
                        if not code_space_id:
                            raise BadRequest('Unable to process input without CodeSpace instance or CodeSpaces sheet.')

                    if category in object_definitions:
                        if not self.valid_header(category, hrCategories[category], required_columns[category]):
                            raise BadRequest('Sheet %s has invalid column headers' % category)

                        CodeSets = self._process_wo_codesets(object_definitions[category], category, code_space_id)
                        if CodeSets:
                            codeset_names, deleted_codeset_names = self._process_codesets(CodeSets, code_space_id)

                elif category == 'AssetTypes':

                    if category in object_definitions:
                        if not self.valid_header(category, hrCategories[category], required_columns[category]):
                            raise BadRequest('Sheet %s has invalid column headers' % category)
                        AssetTypes, oAssetTypes =  self._process_wo_type_resources(object_definitions[category],
                                                        category, valid_actions, valid_type_resources)

                elif category == 'EventTypes':

                    if category in object_definitions:
                        if not self.valid_header(category, hrCategories[category], required_columns[category]):
                            raise BadRequest('Sheet %s has invalid column headers' % category)
                        EventDurationTypes, oEventDurationTypes =  self._process_wo_type_resources(object_definitions[category],
                                                                        category, valid_actions, valid_type_resources)
                elif category == 'Assets':
                    if category in object_definitions:
                        if not self.valid_header(category, hrCategories[category], required_columns[category]):
                            raise BadRequest('Sheet %s has invalid column headers' % category)
                        AssetResources, oAssetResources, otype_resources_used = \
                            self._process_wo_resources(object_definitions[category],category, valid_actions, valid_resources)

                        # import any AssetTypes which are required by Asset resources and not available in
                        # dictionary of working objects
                        if otype_resources_used:
                            for type_resource_tuple in otype_resources_used:
                                name = type_resource_tuple[0]
                                type = type_resource_tuple[1]
                                if type == RT.AssetType:
                                    if name not in oAssetTypes:
                                        wo_type_resource =  self._import_wo_type_resource(name, type)
                                        if wo_type_resource:
                                            oAssetTypes.append(name)
                                            AssetTypes[name] = wo_type_resource
                                    else:
                                        if name not in oAssetTypes:
                                            wo_type_resource =  self._import_wo_type_resource(name, type)
                                            if wo_type_resource:
                                                oAssetTypes.append(name)
                                                AssetTypes[name] = wo_type_resource

                elif category == 'Events':
                    if category in object_definitions:
                        if not self.valid_header(category, hrCategories[category], required_columns[category]):
                            raise BadRequest('Sheet %s has invalid column headers' % category)

                        EventDurationResources, oEventDurationResources, otype_resources_used = \
                            self._process_wo_resources(object_definitions[category], category, valid_actions, valid_resources)


                        # import any EventDurationTypes which are required by EventDuration resources and not available in
                        # dictionary of working objects
                        if otype_resources_used:
                            for type_resource_tuple in otype_resources_used:
                                name = type_resource_tuple[0]
                                type = type_resource_tuple[1]
                                if type == RT.EventDuration:
                                    if name not in oEventDurationTypes:
                                        wo_type_resource =  self._import_wo_type_resource(name, type)
                                        if wo_type_resource:
                                            oEventDurationTypes.append(name)
                                            EventDurationTypes[name] = wo_type_resource
                                    else:
                                        if name not in oEventDurationTypes:
                                            wo_type_resource =  self._import_wo_type_resource(name, type)
                                            if wo_type_resource:
                                                oEventDurationTypes.append(name)
                                                EventDurationTypes[name] = wo_type_resource

                elif category == 'AssetAttributeSpecs':

                    if category in object_definitions:
                        if not self.valid_header(category, hrCategories[category], required_columns[category]):
                            raise BadRequest('Sheet %s has invalid column headers' % category)
                        if oAssetTypes and AssetTypes:
                            AssetTypeAttributeSpecs, AssetTypeAttrSpecs, xAssetTypeAttrSpecs = \
                                self._process_wo_attribute_spec(object_definitions[category], category, code_space_id,
                                                                oAssetTypes, AssetTypes, valid_type_resources)

                elif category == 'EventAttributeSpecs':
                    if category in object_definitions:
                        if not self.valid_header(category, hrCategories[category], required_columns[category]):
                            raise BadRequest('Sheet %s has invalid column headers' % category)

                        if oEventDurationTypes and EventDurationTypes:
                            EventDurationTypeAttributeSpecs, EventDurationTypeAttrSpecs, xEventDurationTypeAttrSpecs = \
                            self._process_wo_attribute_spec(object_definitions[category], category, code_space_id,
                                                            oEventDurationTypes, EventDurationTypes, valid_type_resources)

                elif category == 'EventAssetMap':
                    if category in object_definitions:
                        if not self.valid_header(category, hrCategories[category], required_columns[category]):
                            raise BadRequest('Sheet %s has invalid column headers' % category)
                        EventAssetMap =  self._process_wo_event_asset_map(object_definitions[category], EventDurationResources,
                                                                          AssetResources, category, valid_actions)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())

        except:
            raise BadRequest('Failed to annotate one or more working objects.')

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Process Sheet 5 - (AssetTypeAttributeSpec)
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        try:

            category = 'AssetAttributeSpecs'
            if AssetTypeAttrSpecs and xAssetTypeAttrSpecs and AssetTypes and oAssetTypes and AssetTypeAttributeSpecs:
                self._processProposedAssetAttributes(AssetTypeAttrSpecs, xAssetTypeAttrSpecs, AssetTypes, oAssetTypes,
                                                     AssetResources, AssetTypeAttributeSpecs)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())

        except:
            raise BadRequest('failed to process %s information.' % category)

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Process Sheet 6 - (EventAttributeSpecs)
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        try:
            category = 'EventAttributeSpecs'
            if EventDurationTypeAttrSpecs and xEventDurationTypeAttrSpecs and EventDurationTypes and \
                    oEventDurationTypes and EventDurationTypeAttributeSpecs:
                self._processProposedEventDurationAttributes(EventDurationTypeAttrSpecs,xEventDurationTypeAttrSpecs,
                                                      EventDurationTypes, oEventDurationTypes, EventDurationResources,
                                                      EventDurationTypeAttributeSpecs)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('failed to process %s information.' % category)

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Process Sheet 7 - (AssetAttributes) Process all attributes for Assets
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        try:
            category = 'AssetAttributes'

            if not code_space_id:
                code_space_id = self._get_code_space_id(const_code_space_name)
                if not code_space_id:
                    raise BadRequest('unable to process input without CodeSpace instance or CodeSpaces sheet.')
            if category in object_definitions and category in hrCategories:
                if AssetResources and AssetTypes:
                    self._processAttributes(RT.Asset, hrCategories[category], AssetResources, AssetTypes, category,
                                            valid_type_resources, valid_value_types, code_space_id, object_definitions[category])
                    self._processDefaultAttributes(RT.Asset, hrCategories[category], AssetResources, AssetTypes, category,
                                            valid_type_resources, valid_value_types, code_space_id)
                else:
                    # if only AssetAttributes sheet, no Assets and no AssetTypes sheets, you will receive this error.
                    # work around: for each Asset whose attributes are to be updated, present an Assets sheet.
                    #self._processAttributesOnly(RT.Asset, hrCategories[category], category, valid_type_resources, valid_value_types, code_space_id)
                    raise BadRequest('insufficient information to process sheet %s' % category)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())

        except:
            raise BadRequest('failed to process %s information.' % category)

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Process Sheet 8 - (EventAttributes) Process all attributes for EventDurations
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        try:
            category = 'EventAttributes'
            if not code_space_id:
                code_space_id = self._get_code_space_id(const_code_space_name)
                if not code_space_id:
                    raise BadRequest('unable to process input without CodeSpace instance or CodeSpaces sheet.')

            if category in object_definitions and category in hrCategories:
                if EventDurationResources and EventDurationTypes:
                    self._processAttributes(RT.EventDuration, hrCategories[category],
                                                 EventDurationResources, EventDurationTypes, category, valid_type_resources,
                                                 valid_value_types, code_space_id, object_definitions[category])
                else:
                    # see note for processAttributes for RT.Asset (above)
                    raise BadRequest('insufficient information to process %s' % category)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('failed to process %s information.' % category)

        return CodeSpaces, Codes, CodeSets, AssetTypes, oAssetTypes, EventDurationTypes, oEventDurationTypes, \
               AssetResources, oAssetResources, EventDurationResources, oEventDurationResources, EventAssetMap

    # -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Start - Output functions for download_xls
    # -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _writeEventDurationAttributes(self, active_sheet, oEventDurationResources, EventDurationResources ):

        if not oEventDurationResources:
            raise BadRequest('oEventDurationResources parameter is empty')
        if not EventDurationResources:
            raise BadRequest('EventDurationResources parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        result = []
        for ar in oEventDurationResources:
            res = EventDurationResources[ar]
            if res['_attributes']:
                tmp_names = res['_attributes']
                for nam in tmp_names:
                    if nam not in result:
                        result.append(nam)
            else:
                bresult = False
                raise BadRequest('EventDuration resource does not have attribute names defined as required.')

        attrAssetAttributes = result[:]

        # Write header row to sheet
        sheet_header_row = active_sheet.row(0)

        header_row = []
        header_row.append('Resource ID')
        header_row.append('Action')
        sheet_header_row.write(0,'Resource ID')
        sheet_header_row.write(1,'Action')

        cinx = 2
        for name in attrAssetAttributes:
            sheet_header_row.write(cinx,name)
            header_row.append(name)
            cinx += 1

        # Populate rows
        inx = 1
        cinx = 0
        for name in oEventDurationResources:
            res = EventDurationResources[name]
            if res:
                row = active_sheet.row(inx)
                row.write(0,res['altid'])
                row.write(1,res['action'])

                if res['_attributes']:
                    attributes = res['_attributes']
                    for attr_name, attribute in attributes.iteritems():
                        values = attribute['value']
                        # loop through attribute list, compile simple list of values
                        list_of_values = ''
                        for v in values:
                            value = v['value']
                            list_of_values += str(value) + ','
                        if list_of_values:
                            if list_of_values.endswith(","): list_of_values = list_of_values[:-1]
                        if attr_name in header_row:
                            cinx = header_row.index(attr_name)
                            row.write(cinx,list_of_values)

            inx += 1

    def _writeAssetAttributes(self, active_sheet, oAssetResources, AssetResources):

        if not oAssetResources:
            raise BadRequest('oAssetResources parameter is empty')
        if not AssetResources:
            raise BadRequest('AssetResources parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        result = []
        for ar in oAssetResources:
            res = AssetResources[ar]
            if res['_attributes']:
                tmp_names = res['_attributes']
                for nam in tmp_names:
                    if nam not in result:
                        result.append(nam)                      #tmp_names[::-1])
            else:
                bresult = False
                raise BadRequest('Asset resource does not have attribute names defined as required.')

        attrAssetAttributes = result[:]

        # Write header row to sheet
        sheet_header_row = active_sheet.row(0)
        header_row = []
        header_row.append('Resource ID')
        header_row.append('Action')
        sheet_header_row.write(0,'Resource ID')
        sheet_header_row.write(1,'Action')

        cinx = 2
        for name in attrAssetAttributes:
            sheet_header_row.write(cinx,name)
            header_row.append(name)
            cinx += 1

        # Populate rows
        inx = 1
        cinx = 0
        for name in oAssetResources:
            res = AssetResources[name]
            if res:
                row = active_sheet.row(inx)
                row.write(0,res['altid'])
                row.write(1,res['action'])

                if res['_attributes']:
                    attributes = res['_attributes']
                    for attr_name, attribute in attributes.iteritems():
                        values = attribute['value']
                        # loop through attribute list, compile simply value list
                        list_of_values = ''
                        for v in values:
                            value = v['value']
                            list_of_values += str(value) + ','
                        if list_of_values:
                            if list_of_values.endswith(","): list_of_values = list_of_values[:-1]
                        if attr_name in header_row:
                            cinx = header_row.index(attr_name)
                            row.write(cinx,list_of_values)

            inx += 1


    def _writeEventDurationTypeAttributeSpec(self, active_sheet, columns, oEventDurationTypes, EventDurationTypes):

        # consolidate into
        # _writeAttributeSpec(self, list_type_resources, type_resources, TypeAttributeSpecCols, active_sheet
        if not oEventDurationTypes:
            raise BadRequest('oEventDurationTypes parameter is empty')
        if not EventDurationTypes:
            raise BadRequest('EventDurationTypes parameter is empty')
        if not columns:
            raise BadRequest('columns parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        # Write header row
        header_row = active_sheet.row(0)
        cinx = 0
        for name in columns:
            header_row.write(cinx,name)
            cinx += 1

        # Populate rows
        inx = 1
        for name in oEventDurationTypes:
            res = EventDurationTypes[name]
            attr_specs = res['_attrs']
            for a_spec in attr_specs:
                row = active_sheet.row(inx)
                spec = res['_attribute_specifications'][a_spec]
                row.write(0, res['altid'])
                row.write(1, res['action'])

                row.write(2, spec['id'])
                row.write(3, spec['value_type'])
                row.write(4, spec['group_label'])
                row.write(5, spec['attr_label'])
                row.write(6, spec['rank'])
                row.write(7, spec['visibility'])
                row.write(8, spec['description'])
                row.write(9, spec['cardinality'])
                row.write(10, spec['value_constraints'])
                row.write(11, spec['value_pattern'])
                row.write(12, spec['default_value'])
                row.write(13, spec['uom'])
                row.write(14, spec['editable'])
                row.write(15, spec['journal'])
                inx += 1

        return

    def _writeAssetTypeAttributeSpec(self, active_sheet, columns, oAssetTypes, AssetTypes):

        if not oAssetTypes:
            raise BadRequest('oAssetTypes parameter is empty')
        if not AssetTypes:
            raise BadRequest('AssetTypes parameter is empty')
        if not columns:
            raise BadRequest('columns parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        # Write header row
        header_row = active_sheet.row(0)
        cinx = 0
        for name in columns:
            header_row.write(cinx,name)
            cinx += 1

        # Populate rows
        inx = 1
        for name in oAssetTypes:
            res = AssetTypes[name]
            attr_specs = res['_attrs']
            for a_spec in attr_specs:
                row = active_sheet.row(inx)
                spec = res['_attribute_specifications'][a_spec]
                row.write(0, res['altid'])
                row.write(1, res['action'])
                row.write(2, spec['id'])
                row.write(3, spec['value_type'])
                row.write(4, spec['group_label'])
                row.write(5, spec['attr_label'])
                row.write(6, spec['rank'])
                row.write(7, spec['visibility'])
                row.write(8, spec['description'])
                row.write(9, spec['cardinality'])
                row.write(10, spec['value_constraints'])
                row.write(11, spec['value_pattern'])
                row.write(12, spec['default_value'])
                row.write(13, spec['uom'])
                row.write(14, spec['editable'])
                row.write(15, spec['journal'])
                inx += 1

        return

    def _writeEventDurations(self, active_sheet, columns, oEventDurationResources, EventDurationResources):

        if not oEventDurationResources:
            raise BadRequest('oEventDurationResources parameter is empty')
        if not EventDurationResources:
            raise BadRequest('EventDurationResources parameter is empty')
        if not columns:
            raise BadRequest('columns parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        # Write header row
        header_row = active_sheet.row(0)
        cinx = 0
        for name in columns:
            header_row.write(cinx,name)
            cinx += 1

        # Populate rows
        inx = 1
        for name in oEventDurationResources:
            res = EventDurationResources[name]
            row = active_sheet.row(inx)
            row.write(0, res['altid'])
            row.write(1, res['action'])
            row.write(2, res['type'])
            row.write(3, res['implements'])
            row.write(4, res['org_altids'])
            row.write(5, res['description'])
            inx += 1

        return

    def _writeAssets(self, active_sheet, columns, oAssetResources, AssetResources):

        if not oAssetResources:
            raise BadRequest('oAssetResources parameter is empty')
        if not AssetResources:
            raise BadRequest('AssetResources parameter is empty')
        if not columns:
            raise BadRequest('columns parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        # Write header row
        header_row = active_sheet.row(0)
        cinx = 0
        for name in columns:
            header_row.write(cinx,name)
            cinx += 1

        # Populate rows
        inx = 1
        for name in oAssetResources:
            res = AssetResources[name]
            row = active_sheet.row(inx)
            row.write(0, res['altid'])
            row.write(1, res['action'])
            row.write(2, res['type'])
            row.write(3, res['implements'])
            row.write(4, res['org_altids'])
            row.write(5, res['description'])
            inx += 1

        return

    def _writeEventDurationTypes(self, active_sheet, columns, oEventDurationTypes, EventDurationTypes):

        if not oEventDurationTypes:
            raise BadRequest('oEventDurationTypes parameter is empty')
        if not EventDurationTypes:
            raise BadRequest('EventDurationTypes parameter is empty')
        if not columns:
            raise BadRequest('columns parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        # Write header row
        header_row = active_sheet.row(0)
        cinx = 0
        for name in columns:
            header_row.write(cinx,name)
            cinx += 1

        # Populate rows
        inx = 1
        for name in oEventDurationTypes:
            res = EventDurationTypes[name]
            event_category = ''
            if res['event_category']:
                event_category_id = res['event_category']
                event_category = EventCategoryEnum._str_map[event_category_id]

            row = active_sheet.row(inx)
            row.write(0, res['altid'])
            row.write(1, res['action'])
            row.write(2, res['type'])
            row.write(3, res['extends'])
            row.write(4, res['org_altids'])
            row.write(5, str(res['concrete']))
            row.write(6, event_category)
            row.write(7, res['description'])
            inx += 1

        return

    def _writeAssetTypes(self, active_sheet, columns, oAssetTypes, AssetTypes):

        if not oAssetTypes:
            raise BadRequest('oAssetTypes parameter is empty')
        if not AssetTypes:
            raise BadRequest('AssetTypes parameter is empty')
        if not columns:
            raise BadRequest('columns parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        # Write header row
        header_row = active_sheet.row(0)
        cinx = 0
        for name in columns:
            header_row.write(cinx,name)
            cinx += 1

        # Populate rows
        inx = 1
        for name in oAssetTypes:
            res = AssetTypes[name]
            row = active_sheet.row(inx)
            row.write(0, res['altid'])
            row.write(1, res['action'])
            row.write(2, res['type'])
            row.write(3, res['extends'])
            row.write(4, res['org_altids'])
            row.write(5, str(res['concrete']))
            row.write(6, res['description'])
            inx += 1

        return

    def _writeCodeSpaces(self, active_sheet, columns, CodeSpaces):

        if not CodeSpaces:
            raise BadRequest('CodeSpaces parameter is empty')
        if not columns:
            raise BadRequest('columns parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        # Write header row
        header_row = active_sheet.row(0)
        cinx = 0
        for name in columns:
            header_row.write(cinx,name)
            cinx += 1

        # Populate rows
        inx = 1
        for name in CodeSpaces:
            res = CodeSpaces[name]
            if res:
                row = active_sheet.row(inx)
                if res['altid']:
                    row.write(0, res['altid'])
                if res['action']:
                    row.write(1, res['action'])
                if res['description']:
                    row.write(2, res['description'])
                inx += 1

        return

    def _writeCodes(self, active_sheet, columns, Codes):

        if not Codes:
            raise BadRequest('Codes parameter is empty')
        if not columns:
            raise BadRequest('columns parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        # Write header row
        header_row = active_sheet.row(0)
        cinx = 0
        for name in columns:
            header_row.write(cinx,name)
            cinx += 1

        # Populate rows
        inx = 1
        for name in Codes:
            res = Codes[name]
            if res:
                row = active_sheet.row(inx)
                row.write(0, res['cs_altid'])
                row.write(1, res['action'])
                row.write(2, res['altid'])
                row.write(3, res['description'])
                inx += 1

        return

    def _writeCodeSets(self, active_sheet, columns,  CodeSets):
        """For each CodeSet instance, generate output sheet; default output action == 'add'
        @param  wo_resources    {}      # dictionary of instance resources
        @param  columns         []      # list of expected columns to produce in output (xls) sheet
        @param  active_sheet
        """
        if not CodeSets:
            raise BadRequest('CodeSets parameter is empty')
        if not columns:
            raise BadRequest('columns parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        # Write header row
        header_row = active_sheet.row(0)
        cinx = 0
        for name in columns:
            header_row.write(cinx,name)
            cinx += 1

        # Populate rows
        inx = 1
        for name in CodeSets:
            res = CodeSets[name]
            if res:
                row = active_sheet.row(inx)
                row.write(0, res['cs_altid'])
                row.write(1, res['action'])
                row.write(2, res['name'])
                row.write(3, res['description'])

                enumeration = ''
                if res['enumeration']:
                    for item in res['enumeration']:
                        tmp_item =  item + ","
                        enumeration += tmp_item
                if enumeration.endswith(","): enumeration = enumeration[:-1]
                row.write(4, enumeration)
                inx += 1

        return

    def _writeEventAssetMap(self, active_sheet, columns, wo_resources):
        """For each asset instance, if any event duration associations exist, specifically
        hasLocationEvent, hasOperabilityEvent, hasVerificationEvent, hasAssemblyEvent;
        retrieve asset and event endpoint ids (i.e. for each asset instance, fetch event duration with
        designated association. Check association predicate(s) and produce compiled
        list of asset-event id tuple(s). Finally, using compiled list of tuples, obtain altid resource ID
        (unique altid names) and generate output sheet using action == 'add'

        @param  wo_resources    {}      # dictionary of EventAsset resources
        @param  columns         []      # list of expected columns to produce in output (xls) sheet
        @param  active_sheet            # xlsx sheet for output
        @throws BadRequest      'active_sheet parameter is empty'
        @throws BadRequest      'columns parameter is empty'
        @throws BadRequest      'wo_resources is empty'
        """

        if not wo_resources:
            raise BadRequest('wo_resources is empty')
        if not columns:
            raise BadRequest('columns parameter is empty')
        if not active_sheet:
            raise BadRequest('active_sheet parameter is empty')

        # Known/defined predicates for asset event duration associations
        predicates = ['hasLocationEvent', 'hasOperabilityEvent', 'hasVerificationEvent', 'hasAssemblyEvent']

        # get all assets
        predicate = ''
        list_of_items = []
        for name, wo_resource in wo_resources.iteritems():
            for predicate in predicates:
                asset_id = wo_resource['_id']
                associations = self.RR.find_associations(subject=asset_id, predicate=predicate, id_only=False)
                if associations:
                    for assoc in associations:
                        tuple = []
                        if assoc.ot == RT.EventDuration:
                            event_id = assoc.o
                            tuple.append(asset_id)
                            tuple.append(event_id)
                            list_of_items.append(tuple)

        list_of_names = []
        for item in list_of_items:
            name_tuple = []
            asset_id = item[0]
            event_id = item[1]
            asset_obj = self.read_asset(asset_id)
            asset_altids = asset_obj.alt_ids
            if asset_altids:
                altid = asset_altids[0]
                tmp = altid.split(':')
                asset_altid = tmp[1][:]
                name_tuple.append(asset_altid)

            event_obj = self.read_event_duration(event_id)
            event_altids = event_obj.alt_ids
            if event_altids:
                altid = event_altids[0]
                tmp = altid.split(':')
                event_altid = tmp[1][:]
                name_tuple.append(event_altid)

            list_of_names.append(name_tuple)

        # Write header row (description not populated since instances pulled from system)
        header_row = active_sheet.row(0)
        cinx = 0
        for name in columns:
            header_row.write(cinx,name)
            cinx += 1

        # Populate rows
        inx = 1
        for tuple in list_of_names:
            row = active_sheet.row(inx)
            row.write(0, tuple[1])
            row.write(1, tuple[0])
            row.write(2, 'add')
            inx += 1

        return

    # -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # End - Output functions for download_xls
    # -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    # -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Start - _populate functions
    # -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _populate_wo_asset_resource(self, key):
        """ Populate marine resource working object from instance.
        Using key, working a working object for resource identified by key; resource must be of type
        RT.Asset or RT.EventDuration
        @param  key             {}          # key value
        @retval altid           str         # altid for resource
        @retval wo_resource     {}          # resource working object
        @throws BadRequest      'key parameter empty'
        @throws BadRequest      'unknown namespace (%s) provided; namespace must be one of Asset or EventDuration'
        @throws Inconsistent    'unknown or invalid instance type; should be one of Asset or EventDuration'
        @throws NotFound        'working object attribute _source_id is empty'
        """
        # Note - input key: : {'alt_id': 'Device', 'alt_id_ns': 'Asset', 'id': '2f4ba79a7c09460886c0bee0280aa966'}
        if not key:
            raise BadRequest('key parameter empty')

        try:
            id = key['id']
            altid = key['alt_id']
            altid_ns = key['alt_id_ns']

            # determine resource type from namespace designation; read resource instance based on type
            if altid_ns == RT.Asset:
                asset = self.read_asset(id)
            elif altid_ns == RT.EventDuration:
                asset = self.read_event_duration(id)
            else:
                raise BadRequest('unknown namespace (%s) provided; namespace must be one of Asset or EventDuration' % altid_ns)

            # populate working_object...
            resource  = \
                    {
                        'altid' : altid,                    # altid for resource
                        'action' : '',                      # action to perform for resource
                        'type' : '',                        # resource type (Asset|EventDuration) for collapse
                        'concrete' : 'True',                # concrete type {'True' | 'False'}
                        'name' : '',                        # name for resource
                        'description' : '',                 # Description provided for Resource
                        'implements' : '',                  # name of Type implemented
                        'org_altids' : '',                  # TypeResource shared across Facilities
                        '_id' : '',                         # system id for resource
                        '_implements_id' : '',              # system id for 'implements' TypeResource
                        '_scoped_names' : [],               # List of attribute names for this resource (no extends)
                        '_scoped_keys' : [],                # list of attributes keys for this resource (no extends)
                        '_pattribute_names' : [],           # complete ordered list of attribute names possible
                        '_pattribute_keys' : [],            # complete ordered list of attribute keys possible
                        '_attributes': {},                  # Dictionary of All Attributes for this resource
                        '_exists' : 'True'                  # instance exists?
                        #'_attribute_specifications' : {}   # Dictionary of All AttributeSpecifications
                    }

            resource['altid'] = altid              # alias or asset.name
            resource['action'] = 'add'             # ask about this; if dump for system load, then add is correct
            resource['type'] = asset.type_         # better be RT.Asset | RT.EventDuration think about this
            resource['concrete'] = 'True'          # all asset instances are concrete (deprecated) todo remove
            resource['name'] = asset.name          # name for resource
            resource['description'] = asset.description
            resource['_id'] = id                   # unique system id
            resource['_implements'] = ''           # name of Type implemented
            resource['_implements_id'] = ''        # system id for 'implements' TypeResource
            resource['_implements_type'] = ''      # resource type of TypeResource (RT.AssetType or RT.EventDurationType)
            resource['_scoped_names'] = []         # List of attribute names for this resource (no extends)
            resource['_scoped_keys'] = []          # list of attributes keys for this resource (no extends)
            resource['_pattribute_names'] = []     # complete ordered list of attribute names possible
            resource['_pattribute_keys'] = []      # complete ordered list of attribute keys possible

            resource['_exists'] = True             # instance exists? - bool

            org_altids = []
            orgs = ''

            # Determine org associations
            org_associations = self.RR.find_associations(object=id, predicate=PRED.hasResource, id_only=False)
            if org_associations:
                inx = 0
                max_inx = len(org_associations)
                while inx < max_inx:
                    item = org_associations[inx]
                    if item['st'] == RT.Org:
                        x_id = item['s']
                        x_type = item['st']
                        impl_obj = self.RR2.read(x_id, x_type)
                        if impl_obj:
                            # to use governance name or altid?
                            if impl_obj.org_governance_name not in org_altids:
                                org_altids.append(impl_obj.org_governance_name)
                                tmp = 'MF_' + impl_obj.org_governance_name
                                if orgs:
                                    orgs += ', ' + tmp
                                else:
                                    orgs = tmp


                    inx += 1

            resource['org_altids'] = orgs

            if altid_ns == RT.Asset:
                resource['_attributes'] = asset.asset_attrs  # Dictionary of All Attributes for this resource
                predicate = PRED.implementsAssetType

            elif altid_ns == RT.EventDuration:
                # Dictionary of All Attributes for this resource
                resource['_attributes'] = asset.event_duration_attrs
                predicate = PRED.implementsEventDurationType

            else:
                raise Inconsistent('unknown or invalid instance type; should be one of Asset or EventDuration')

            # get asset Associations where predicate {PRED.implementsAssetType | PRED.implementsEventDurationType},
            # returns id for marine tracking resource's TypeResource (for Asset returns id for AssetType)
            associations = self.RR.find_associations(subject=id, predicate=predicate, id_only=False)
            all_specifications = {}         # remove todo?
            all_names = []
            all_keys = []
            all_sources = []

            if associations:
                resource['_implements_id'] = associations[0].o
                resource['_implements_type'] = associations[0].ot

                x_id = associations[0].o
                x_type = associations[0].ot
                impl_obj = self.RR2.read(x_id, x_type)
                if impl_obj:
                    resource['implements'] = impl_obj.name                  # TypeResource name
                    for name in impl_obj.attribute_specifications.keys():
                        attr_obj = impl_obj.attribute_specifications[name]
                        if attr_obj:
                            if attr_obj['_source_id']:
                                if attr_obj['_source_id'] not in all_sources:
                                    all_sources.append(attr_obj['_source_id'])

                                if attr_obj['_source_id'] == impl_obj.name:
                                    resource['_scoped_names'].append(name)
                                    key = name + '|' + impl_obj.name
                                    resource['_scoped_keys'].append(key)
                            else:
                                raise NotFound('working object attribute _source_id is empty')
                        all_specifications[name] = impl_obj.attribute_specifications[name]
                        all_keys.append(key)
                        all_names.append(name)
                        # want all sources...

                    if not impl_obj.concrete:             # abstract
                        bDone = True

                # loop through remaining chain of implementsAssetTypes;
                # gathering names[, keys and attribute_specifications]
                bDone = False       # used with arbitrary ceiling, remove
                inx = 0             # used with arbitrary ceiling, remove
                max_inx = 5         # arbitrary ceiling on extends remove
                while not bDone:
                    pred = ''
                    # set predicate value based on type
                    if altid_ns == RT.Asset:
                        pred = PRED.extendsAssetType
                    elif altid_ns == RT.EventDuration:
                        pred = PRED.extendsEventDurationType

                    associations = self.RR.find_associations(subject=x_id,predicate=pred, id_only=False)
                    if associations:
                        x_id = associations[0].o
                        x_type = associations[0].ot
                        impl_obj = self.RR2.read(x_id, x_type)
                        if impl_obj:
                            if impl_obj.attribute_specifications:
                                for name in impl_obj.attribute_specifications.keys():
                                    all_keys.append(name + '|' + impl_obj.name)
                                    if name not in all_names:
                                        all_names.append(name)
                                    if name not in all_specifications:
                                        all_specifications[name] = impl_obj.attribute_specifications[name]

                            if not impl_obj.concrete:
                                bDone = True

                    if inx >= max_inx:
                        bDone = True

                    inx += 1

            resource['_pattribute_names'] = all_names
            resource['_pattribute_keys']  = all_keys

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('Failed to populate working object for marine tracking resource')

        return altid, resource

    def _populate_wo_marine_resources(self, keys):
        """Populate resource working objects from instance information in system.

        @param  keys        []     # list of resource instance ids
        @retval Resources   {}     # dictionary of working objects populated by reading instances in system.
        @retval oResources  []     # list of resource [instance] names
        @throws BadRequest  'keys parameter empty'
        @throws BadRequest  'failed to populate working object (altid: %s)'
        @throws BadRequest  'failed to populate working object (id: %s)'
        @throws BadRequest  'failed to populate marine resource working objects.'
        """

        if not keys:
            raise BadRequest('keys parameter empty')

        oResources = []
        Resources = {}

        try:

            if keys:
                for key in keys:
                    if key:
                        name, wo_asset = self._populate_wo_asset_resource(key)
                        if wo_asset:
                            if name not in oResources:
                                oResources.append(name)
                                Resources[name] = wo_asset
                        else:
                            if name:
                                raise BadRequest('failed to populate working object (altid: %s)' % name)

                            else:
                                raise BadRequest('failed to populate working object (id: %s)' % key)

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())

        except:
            raise BadRequest('failed to populate marine resource working objects.')

        return Resources, oResources

    def _populate_wo_type_resource(self,key):
        """Populate type resource working objects from instance information.

        @param  key         {}     # resource instance dictionary
        @retval altid       str    # dictionary of working objects populated by reading instances in system.
        @retval AssetType   {}     # type resource working object dictionary
        @throws BadRequest  'key parameter empty'
        @throws BadRequest  'invalid key provided to populate_type_resource id missing'
        @throws BadRequest  'invalid key provided to populate_type_resource alt_id missing'
        @throws BadRequest  'invalid key provided to populate_type_resource alt_id_ns missing'
        @throws BadRequest  'read_asset_type failed when populating working object for type resource (id=%s)'
        @throws BadRequest  'read_event_duration_type failed populating working object for type resource (id=%s)'
        @throws BadRequest  'unknown namespace (%s) when populating type resource (%s)'
        @throws BadRequest  'TypeResource %s does not have attribute_specifications defined'
        @throws BadRequest  'TypeResource %s does not have attribute_specifications'
        """
        # input key: : {'alt_id': 'DeviceType', 'alt_id_ns': 'AssetType', 'id': '2f4ba79a7c09460886c0bee0280aa966'}
        # populate working object
        if not key:
            raise BadRequest('key parameter empty')

        all_extends = []
        all_keys = []
        try:
            #exist_keys: [{'alt_id': 'Base', 'alt_id_ns': 'AssetType', 'id': '354df71507cf4e91a5516bea05975d73'}]
            if 'id' not in key:
                raise BadRequest('invalid key provided to populate_type_resource id missing')
            if 'alt_id' not in key:
                raise BadRequest('invalid key provided to populate_type_resource alt_id missing')
            if 'alt_id_ns' not in key:
                raise BadRequest('invalid key provided to populate_type_resource alt_id_ns missing')
            id = key['id']
            altid = key['alt_id']
            altid_ns = key['alt_id_ns']


            event_category = ''
            if altid_ns == RT.AssetType:
                try:
                    type_res = self.read_asset_type(id)         # read asset type instance
                except:
                    raise BadRequest('read_asset_type failed when populating working object for type resource (id=%s)' % id)

            elif altid_ns == RT.EventDurationType:
                try:
                    type_res = self.read_event_duration_type(id)    # read event duration type instance
                    event_category = type_res.event_category        # save event category
                except:
                    raise BadRequest('read_event_duration_type failed populating working object for type resource (id=%s)' % id)
            else:
                raise BadRequest('unknown namespace (%s) when populating type resource (%s)' % (altid_ns, altid))

            AssetType = {
                            'altid'         : altid,                # alt name provided for TypeResource
                            'action'        : 'add',                # action to perform for this TypeResource
                            'type'          : altid_ns,             # type specified for TypeResource
                            'concrete'      : '',                   # concrete type bool {True | False}
                            'id'            : altid,                # Name provided for TypeResource
                            'description'   : type_res.description, # Description provided for TypeResource
                            'extends'       : [],                   # TypeResource inherits from extends TypeResource
                            'org_altids'    : '',                   # TypeResource shared across Facilities/Orgs
                            '_id'           : id,                   # system assigned uuid4
                            '_extends_id'   : '',                   # system assigned uuid4 for extends
                            '_attrs'        : [],                   # (tmp) attribute spec names for this resource
                            '_xattrs'       : [],                   # X (tmp) attribute spec keys for this resource
                            '_attribute_specifications_names' : [], # comprehensive, ordered list of names
                            '_attribute_specifications_keys' : [],  # X comprehensive, ordered list of keys
                            '_attribute_specifications' : {},       # comprehensive attribute_specifications
                            '_extensions'   : [],                   # X ordered list of extends
                            '_exists'       : True,                 # instance exists?
                            '_import'       : False,                # used to flag wo created for extends processing
                            'event_category': event_category        # event type specific event category
                        }
            AssetType['concrete'] = 'FALSE'
            if type_res.concrete:
                AssetType['concrete'] = 'TRUE'

            # Dictionary of All Attributes for this resource
            if type_res.attribute_specifications:
                if type_res.attribute_specifications is not None:
                    AssetType['_attribute_specifications'] = type_res.attribute_specifications
                    AssetType['_attribute_specifications_names'] = type_res.attribute_specifications.keys()
                else:
                    raise BadRequest('TypeResource %s does not have attribute_specifications defined' % altid)
            else:
                raise BadRequest('TypeResource %s does not have attribute_specifications' % altid)

            org_altids = []
            orgs = ''

            # Determine org associations
            org_associations = self.RR.find_associations(object=id, predicate=PRED.hasResource, id_only=False)

            # [Association({'_id': '141ad975fa5245b395f214d7d54b16c7', 'retired': False, 'ts': '1400528406356',
            # '_rev': '1', 's': '5dfc09920416491b8c9e166f37870358', 'o': 'c580e5bf2d0f4252a3db49aa5e66a814',
            # 'st': 'Org', 'p': 'hasResource', 'attributes': {}, 'ot': 'Asset', 'order': ''})]
            if org_associations:
                inx = 0
                max_inx = len(org_associations)
                while inx < max_inx:
                    item = org_associations[inx]
                    if item['st'] == RT.Org:
                        x_id = item['s']
                        x_type = item['st']
                        impl_obj = self.RR2.read(x_id, x_type)
                        if impl_obj:
                            # to use governance name or altid? if only one altid then ok,
                            # used governance for now.  todo
                            if impl_obj.org_governance_name not in org_altids:
                                org_altids.append(impl_obj.org_governance_name)
                                tmp = 'MF_' + impl_obj.org_governance_name
                                if orgs:
                                    orgs += ', ' + tmp
                                else:
                                    orgs = tmp
                    inx += 1

            AssetType['org_altids'] = orgs

            # get asset Associations based on resource type (where PRED.extendsAssetType or PRED.extendsEventDurationType)
            if altid_ns == RT.AssetType:
                pred = PRED.extendsAssetType
            elif altid_ns == RT.EventDurationType:
                pred = PRED.extendsEventDurationType
            else:
                raise BadRequest('unable to process unknown type (\'%s\') of TypeResource instance' % altid_ns)

            x_associations = self.RR.find_associations(subject=id, predicate=pred, id_only=False)
            if x_associations:
                AssetType['_extends_id'] = x_associations[0].o
                x_id = x_associations[0].o
                x_type = x_associations[0].ot
                impl_obj = self.RR2.read(x_id, x_type)
                if impl_obj:
                    AssetType['extends'] = impl_obj.name                # AssetType
                    all_extends.append(impl_obj.name)                   # added 2014-07-02

            if AssetType['_attribute_specifications_names'] is None:
                raise BadRequest('no attribute_specifications values defined for TypeResource: %s' % type_res.altid)
            else:
                attr_spec_names = AssetType['_attribute_specifications_names']
                for name in attr_spec_names:
                    a_spec = type_res.attribute_specifications[name]
                    if a_spec is not None:
                        if a_spec['_source_id'] is not None:
                            if a_spec['_source_id'] not in all_extends:
                                all_extends.append(a_spec['_source_id'])
                                local_key = name + '|' + a_spec['_source_id']
                                all_keys.append(local_key)
                            if a_spec['_source_id'] == altid:
                                if a_spec['_source_id'] not in AssetType['_attrs']:
                                    AssetType['_attrs'].append(name)
                        else:
                            raise BadRequest('No AttributeSpecification _source_id for %s' % name)
                    else:
                        raise BadRequest('No AttributeSpecification for %s' % name)

            AssetType['_xattrs'] = all_keys
            if type_res.concrete:
                AssetType['_extensions'] = all_extends

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())

        except:
            raise BadRequest('Failed to populate working object for marine TypeResources')

        return altid, AssetType

    def _populate_wo_marine_type_resources(self, keys):
        """
        For each instance, use key provided and populate workings objects of types:
        AssetResources, oAssetResources, EventDurationResources, oEventDurationResources

        @params  keys       []      # list of instance keys
        @retval  Types      {}      # dictionary of working objects
        @retval  oTypes     []      # list of working object names
        @throws BadRequest
        """
        if not keys:
            raise BadRequest('keys parameter empty')

        oTypes = []
        Types = {}

        try:

            if keys:
                for key in keys:
                    if key:
                        name, wo_type = self._populate_wo_type_resource(key)
                        if wo_type:
                            if name not in oTypes:
                                oTypes.append(name)
                                Types[name] = wo_type
                        else:
                            if name:
                                raise BadRequest('failed to populate type resource working object (altid: %s)' % name)
                            else:
                                raise BadRequest('failed to populate type resource working object (key: %s)' % key)


        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())

        except:
            raise BadRequest('Failed to populate type resource working objects.')

        return Types, oTypes
    # -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # End - _populate functions
    # -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _populate_wo_codespace(self, key):

        # input key:  {'alt_id': 'MAM', 'alt_id_ns': 'CodeSpace', 'id': '34b3528f54744d3cb75263312d1e06bb'}
        # populate working object
        if not key:
            raise BadRequest('key parameter empty')

        try:
            id = key['id']
            altid = key['alt_id']
            altid_ns = key['alt_id_ns']
            code_space = ''
            CodeSpace = {}
            if altid_ns == RT.AssetType:
                try:
                    type_res = self.read_asset_type(id)         # read asset type instance
                except:
                    raise BadRequest('read_asset_type failed (id=%s' % id)
            elif altid_ns == RT.CodeSpace:
                code_space = self.read_code_space(id)   # read asset event type instance
            else:
                raise BadRequest('unknown name space: %s' % altid_ns)

            if code_space:
                description = ''
                if code_space['description']:
                    description = code_space['description']
                    description = description.strip()

                CodeSpace = {
                            'altid'         : altid,             # alt name provided for CodeSpace
                            'action'        : 'add',             # action to perform for this Code
                            'name'          : altid,             # Name provided for Code
                            'description'   : description,       # Description provided for Code
                            #'org_altids'    : [],               # Code shared across Facilities/Orgs
                            '_id'           : id,                # system assigned uuid4
                            '_exists'       : True,              # instance exists?
                            'codes'         : {},
                            'codesets'      : {}
                            }

                CodeSpace['codes'] = code_space.codes
                CodeSpace['codesets'] = code_space.codesets


        except BadRequest, Arguments:
            raise BadRequest('%s' % Arguments.get_error_message())

        except:
            raise BadRequest('Failed to populate working object for marine asset CodeSpace')

        return altid, CodeSpace

    def _populate_wo_code(self, code, codespace_id, codespace_altid):

        # input key: : {'alt_id': 'DeviceType', 'alt_id_ns': 'AssetType', 'id': '2f4ba79a7c09460886c0bee0280aa966'}
        # populate working object
        if not code:
            raise BadRequest('code parameter empty')
        if not codespace_id:
            raise BadRequest('codespace_id parameter empty')
        if not codespace_altid:
            raise BadRequest('codespace_altid parameter empty')

        try:
            Code = {}
            name = ''
            if code:
                id = code['id']
                name = code['name']
                description = code['description']
                cs_altid = codespace_altid
                cs_id    = codespace_id
                Code = {
                        'altid'         : name,                 # alt name provided for Code (not real)
                        'cs_altid'      : cs_altid,             # alt name for CodeSpace of Code
                        'cs_id'         : cs_id,                # id for CodeSpace of Code
                        'action'        : 'add',                # action to perform for this Code
                        'id'            : id,                   # Code's assigned uuid4
                        'name'          : name,                 # Name provided for Code
                        'description'   : description,          # Description provided for Code
                        'org_altids'    : [],                   # Code shared across Facilities/Orgs
                        '_exists'       : True                  # instance exists?
                        }

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())

        except:
            raise BadRequest('Failed to populate working object for marine asset Code')

        return name, Code

    def _populate_wo_codeset(self, codeset, codespace_id, codespace_altid):

        # input key: : {'alt_id': 'DeviceType', 'alt_id_ns': 'AssetType', 'id': '2f4ba79a7c09460886c0bee0280aa966'}
        # populate working object
        if not codeset:
            raise BadRequest('codeset parameter empty')
        if not codespace_id:
            raise BadRequest('codespace_id parameter empty')
        if not codespace_altid:
            raise BadRequest('codespace_altid parameter empty')

        try:
            CodeSet = {}
            name = ''
            if codeset:
                name = codeset['name']
                description = codeset['description']
                enumeration = codeset['enumeration']
                cs_altid = codespace_altid
                cs_id    = codespace_id
                CodeSet = {
                        'altid'         : name,                # alt name provided for CodeSet
                        'cs_altid'      : cs_altid,            # alt name for CodeSpace of CodeSet
                        'cs_id'         : cs_id,               # unique sys id of CodeSpace for CodeSet
                        'action'        : 'add',               # action to perform for this CodeSet
                        'name'          : name,                # Name provided for CodeSet
                        'description'   : description,         # Description provided for CodeSet
                        'enumeration'   : enumeration,         # Description provided for CodeSet
                        'org_altids'    : [],                  # Code shared across Facilities/Orgs
                        '_exists'       : False                # instance exists?
                        }

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())

        except:
            raise BadRequest('Failed to populate working object for marine asset CodeSet')

        return name, CodeSet

    def _populate_wo_marine_asset_codespaces(self, code_space_keys):
        """Populate working_objects associated with CodeSpaces, Codes and CodeSets

        @param  code_space_keys[]   list of asset resource ids
        @retval CodeSpaces {}       dictionary of working objects populated by reading CodeSpace instance(s) in system.
        @retval Codes {}            dictionary of working objects populated by reading Code working objects.
        @retval CodeSets {}         dictionary of working objects populated by reading CodeSet working objects.
        @throws BadRequest  if object with specified id does not have_id or_rev attribute
        @throws NotFound    if object with specified id does not exist
        """

        if not code_space_keys:
            raise BadRequest('code_space_keys parameter empty')

        CodeSpaces = {}
        Codes = {}
        CodeSets = {}
        code_names = []
        codeset_names = []
        codespace_ids = []
        already_processed = False

        try:

            # populate CodeSpaces working objects
            try:

                if code_space_keys:

                    for key in code_space_keys:
                        if key:
                            id = key['id']
                            if id:
                                if id not in codespace_ids:
                                    codespace_ids.append(id)
                                else:
                                    already_processed = True

                            if not already_processed:
                                name, wo_code_space = self._populate_wo_codespace(key)
                                if wo_code_space and name:
                                    CodeSpaces[name] = wo_code_space

            except BadRequest, Arguments:
                raise BadRequest(Arguments.get_error_message())
            except NotFound, Arguments:
                raise NotFound(Arguments.get_error_message())
            except:
                raise BadRequest('Failed to populate marine asset CodeSpace working objects.')

            # Discover Codes in CodeSpace instance, create wo_code
            try:

                if CodeSpaces:

                    for cs_name in CodeSpaces:
                        wo_code_space = CodeSpaces[cs_name]
                        if wo_code_space['codes']:
                            for name in wo_code_space['codes']:
                                wo_code = {}
                                if name:
                                    code = wo_code_space['codes'][name]
                                    name, wo_code = self._populate_wo_code(code, wo_code_space['_id'], wo_code_space['altid'])

                                if wo_code:
                                    if name not in code_names:
                                        code_names.append(name)
                                        Codes[name] = wo_code

            except BadRequest, Arguments:
                raise BadRequest(Arguments.get_error_message())
            except NotFound, Arguments:
                raise NotFound(Arguments.get_error_message())
            except:
                raise BadRequest('Failed to populate marine asset Codes working objects.')

            # Discover CodeSets in CodeSpace instance, create dictionary of CodeSet working objects
            try:

                if CodeSpaces:

                    for cs_name in CodeSpaces:
                        wo_code_space = CodeSpaces[cs_name]
                        if wo_code_space['codesets']:

                            for name in wo_code_space['codesets']:
                                wo_codeset = {}
                                if name:
                                    codeset = wo_code_space['codesets'][name]
                                    name, wo_codeset = self._populate_wo_codeset(codeset, wo_code_space['_id'], wo_code_space['altid'])

                                if wo_codeset:
                                    if name not in codeset_names:
                                        codeset_names.append(name)
                                        CodeSets[name] = wo_codeset

            except BadRequest, Arguments:
                raise BadRequest(Arguments.get_error_message())
            except NotFound, Arguments:
                raise NotFound(Arguments.get_error_message())
            except:
                raise BadRequest('Failed to populate marine asset CodeSet working objects.')

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except:
            raise BadRequest('Failed to populate marine asset CodeSpace related working objects.')

        return CodeSpaces, Codes, CodeSets

    def _download_xls(self):
        """Create xls content based on system instances. Get working objects from system instances, create export workbook,
        and return encoded workbook content. Query Marine Asset tracking resources in system by namespace
        (including Assets, AssetTypes, EventDurations and EventDurationTypes) retrieve all resources,
        populate working objects and produce xls output.

        @retval  content              encoded blob              # binascii.b2a_hex(content)
        @throws BadRequest      if object with specified id does not have_id or_rev attribute
        @throws NotFound        if object with specified id does not have_id or_rev attribute
        @throws Inconsistent    if object with specified id does not have_id or_rev attribute
        """
        content = ''
        namespaces = [RT.CodeSpace, RT.Asset, RT.AssetType, RT.EventDuration, RT.EventDurationType]

        all_keys = {}

        try:

            # Query all Marine Asset resources in system (memory issue)
            for namespace in namespaces:
                #res_keys, _ = self.container.resource_registry.find_resources_ext(alt_id_ns=namespace,id_only=True)
                _, res_keys = self.container.resource_registry.find_resources_ext(alt_id_ns=namespace,id_only=False)

                if res_keys:
                    all_keys[namespace] = res_keys

            CodeSpaces = {}                         # working objects for CodeSpaces
            CodeSets = {}                           # working objects for CodeSets
            Codes = {}                              # working objects for Codes

            AssetTypes = {}                         # working objects for AssetType
            EventDurationTypes = {}                 # working objects for EventDurationTypes
            AssetResources = {}                     # working objects for Asset resources
            EventDurationResources = {}             # working objects for EventDuration resources
            oAssetTypes = []                        # list of working objects for AssetType
            oEventDurationTypes = []                # list of working objects for EventDurationTypes
            oAssetResources = []                    # list of working objects for Asset resources
            oEventDurationResources = []            # list of working objects for EventDuration resources

            # populate working objects for marine asset tracking instance Resources (Asset and EventDuration)
            if RT.Asset in all_keys:
                AssetResources, oAssetResources = self._populate_wo_marine_resources(all_keys[RT.Asset])
            if RT.EventDuration in all_keys:
                EventDurationResources, oEventDurationResources = self._populate_wo_marine_resources(all_keys[RT.EventDuration])


            # populate working objects for marine asset tracking TypeResources (AssetType or EventDurationType), etc.
            if RT.AssetType in all_keys:
                AssetTypes, oAssetTypes = self._populate_wo_marine_type_resources(all_keys[RT.AssetType])
            if RT.EventDurationType in all_keys:
                EventDurationTypes, oEventDurationTypes = self._populate_wo_marine_type_resources(all_keys[RT.EventDurationType])
            if RT.CodeSpace in all_keys:
                CodeSpaces, Codes, CodeSets = self._populate_wo_marine_asset_codespaces(all_keys[RT.CodeSpace])

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())
        except:
            raise BadRequest('Failed to populate marine asset working objects.')

        try:

            AvailableSheets = ['CodeSpaces', 'Codes', 'CodeSets', 'AssetTypes', 'EventTypes', 'Assets', 'Events',
                                'AssetAttributeSpecs', 'EventAttributeSpecs',
                                'AssetAttributes', 'EventAttributes', 'EventAssetMap']

            required_columns = self.set_required_columns()

            # Process each category, creating a sheet for each in workbook (xls)
            mamBook = Workbook(encoding="utf-8")

            dirty = False
            for category in AvailableSheets:

                active_sheet = mamBook.add_sheet(category)

                if category == 'CodeSpaces':
                    if CodeSpaces:
                        self._writeCodeSpaces(active_sheet, required_columns[category], CodeSpaces)
                        dirty = True

                elif category == 'Codes':
                    if Codes:
                        self._writeCodes(active_sheet, required_columns[category], Codes)
                        dirty = True

                elif category == 'CodeSets':
                    if CodeSets:
                        self._writeCodeSets(active_sheet, required_columns[category], CodeSets)
                        dirty = True

                elif category == 'AssetTypes':
                    if oAssetTypes and AssetTypes:
                        self._writeAssetTypes(active_sheet, required_columns[category], oAssetTypes, AssetTypes)
                        dirty = True

                elif category == 'EventTypes':
                    if oEventDurationTypes and EventDurationTypes:
                        self._writeEventDurationTypes(active_sheet, required_columns[category], oEventDurationTypes, EventDurationTypes)
                        dirty = True

                elif category == 'Assets':
                    if oAssetResources and AssetResources:
                        self._writeAssets(active_sheet, required_columns[category], oAssetResources, AssetResources)
                        dirty = True

                elif category == 'Events':
                    if oEventDurationResources and EventDurationResources:
                        self._writeEventDurations(active_sheet, required_columns[category], oEventDurationResources, EventDurationResources)
                        dirty = True

                elif category == 'AssetAttributeSpecs':
                    if oAssetTypes and AssetTypes:
                        self._writeAssetTypeAttributeSpec(active_sheet, required_columns[category], oAssetTypes, AssetTypes)
                        dirty = True

                elif category == 'EventAttributeSpecs':
                    if oEventDurationTypes and EventDurationTypes:
                        self._writeEventDurationTypeAttributeSpec(active_sheet, required_columns[category], oEventDurationTypes, EventDurationTypes,)
                        dirty = True

                elif category == 'AssetAttributes':
                    if oAssetResources and AssetResources:
                        self._writeAssetAttributes(active_sheet, oAssetResources, AssetResources)
                        dirty = True

                elif category == 'EventAttributes':
                    if oEventDurationResources and EventDurationResources:
                        self._writeEventDurationAttributes(active_sheet, oEventDurationResources, EventDurationResources)
                        dirty = True

                elif category == 'EventAssetMap':
                    if AssetResources and EventDurationResources:
                        self._writeEventAssetMap(active_sheet, required_columns[category], AssetResources)
                        dirty = True

            # Put workbook into blob xcontent, encode and return as response
            try:
                if dirty:
                    import StringIO
                    f = StringIO.StringIO()                 # create a file-like object
                    mamBook.save(f)
                    xcontent = f.getvalue()
                    content = binascii.b2a_hex(xcontent)
                    f.close()
            except:
                raise BadRequest('failed to write marine asset tracking xls')

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())

        except:
            raise BadRequest('Failed to generate asset tracking report.')

        return content

    def _select_rows(self, reader, category):
        """Read all rows for category provided; check for required column values in 'Resource ID' and 'Action'.

        @param  reader                  # xlsx reader
        @param  category        ''      # category to be processed
        @retval rows            {}      # dictionary of rows from xlsx for specific category
        @retval header          {}      # header row for specific category
        @throws BadRequest      'reader parameter is empty.'
        @throws BadRequest      'category parameter is empty.'
        @throws BadRequest      'Sheet %s missing required value(s) for Action or Resource ID.'
        """
        if not reader:
            raise BadRequest('reader parameter is empty.')
        if not category:
            raise BadRequest('category parameter is empty.')

        rows = []
        inx = 0
        header = ''
        for row in reader:
            #print ', '.join(row)
            if inx == 0:
                header = row
            if not row['Action'] or not row['Resource ID']:
                raise BadRequest('Sheet %s missing required value(s) for Action or Resource ID.' % category)
            inx += 1
            rows.append(row)

        return rows, header

    def _select_rows_for_codes(self, reader, category):
        """Read all rows for Code-related category provided; check for required column values in 'CodeSpace ID' and 'Action'.

        @param  reader                  # xlsx reader
        @param  category        ''      # category to be processed
        @retval rows            {}      # dictionary of rows from xlsx for specific category
        @retval header          {}      # header row for specific category
        @throws BadRequest      'reader parameter is empty.'
        @throws BadRequest      'category parameter is empty.'
        @throws BadRequest      'Sheet %s missing required value(s) for Action or CodeSpace ID.'
        """
        if not reader:
            raise BadRequest('reader parameter is empty.')
        if not category:
            raise BadRequest('category parameter is empty.')

        rows = []
        inx = 0
        header = ''
        for row in reader:
            #print ', '.join(row)
            if inx == 0:
                header = row
            if not row['Action'] or not row['CodeSpace ID']:
                raise BadRequest('Sheet %s missing required value(s) for Action or CodeSpace ID.' % category)
            inx += 1
            rows.append(row)

        return rows, header

    def _select_rows_for_event_asset_map(self, reader, category):
        """Read all rows for EventAssetMap category; check for required column values in 'Event Resource ID',
         'Asset Resource ID' and 'Action'.

        @param  reader                  # xlsx reader
        @param  category        ''      # category to be processed
        @retval rows            {}      # dictionary of rows from xlsx for specific category
        @retval header          {}      # header row for specific category
        @throws BadRequest      'reader parameter is empty.'
        @throws BadRequest      'category parameter is empty.'
        @throws BadRequest      'Sheet %s missing required value(s) for Action, Event Resource ID, or Asset Resource ID.'
        """
        if not reader:
            raise BadRequest('reader parameter is empty.')
        if not category:
            raise BadRequest('category parameter is empty.')

        rows = []
        inx = 0
        header = ''
        for row in reader:
            #print ', '.join(row)
            if inx == 0:
                header = row
            # 'Event Resource ID','Asset Resource ID', 'Action'
            if not row['Action'] or not row['Event Resource ID'] or not row['Asset Resource ID']:
                raise BadRequest('Sheet %s missing required value(s) for Action, Event Resource ID, or Asset Resource ID.' % category)
            inx += 1
            rows.append(row)

        return rows, header

    def _import_wo_type_resource(self, name, type):
        """Process type resource instance to populate working object for type resource.

        @param  name                str     # dictionary of all rows fom sheet named 'category'
        @param  type                str     # category being processed (sheet name)
        @retval wo_type_resource    {}      # dictionary of type resource working object
        @throws BadRequest          'name parameter is empty'
        @throws BadRequest          'type parameter is empty'
        """
        # hook

        if not name:
            raise BadRequest('name parameter is empty')
        if not type:
            raise BadRequest('type parameter is empty')

        wo_type_resource = {}
        cnt = 2

        processing_type = type
        res_type = type
        action = 'add'
        altid = name

        # Existing instance with this altid and resource type should exist. Find resource of res_type by name
        res_exists = False
        if altid and res_type:
            key = self._get_type_resource_key_by_name(altid, res_type)
            if key:
                wo_name, wo_type_resource = self._populate_wo_type_resource(key)
                if wo_type_resource:
                    res_exists = True

        if res_exists:
            # rule cannot change 'type', cannot change 'altid', cannot change '_id' (system assigned uuid)
            wo_type_resource['action']          = action
            wo_type_resource['_exists']         = True
            wo_type_resource['_import']         = True

        return wo_type_resource

    def _process_wo_type_resources(self, items, category, valid_actions, valid_res_types):
        """Process object_definitions for TypeResources where categories 'AssetTypes', 'EventTypes'; create working objects.
        Resulting dictionary contains all AssetType or EventDurationType working objects with key = altid.
        _extensions is populated with the ordered list of extends used by the type resource.

        @param  items               {}      # dictionary of all rows fom sheet named 'category'
        @param  category            ''      # category being processed (sheet name)
        @param  valid_actions       []      # list of valid actions
        @param  valid_res_types     []      # list of valid rsource types
        @retval resources           {}      # dictionary of type resource working objects
        @retval oresources          []      # list of type resource names processed
        @throws BadRequest          'items parameter is empty'
        @throws BadRequest          'category parameter is empty'
        @throws BadRequest          'valid_actions parameter is empty'
        @throws BadRequest          'valid_res_types parameter is empty'
        @throws BadRequest          'unknown value (%s) for processing TypeResources'
        @throws BadRequest          'empty Resource ID value'
        @throws BadRequest          'invalid Action value for %s'
        @throws BadRequest          'unknown event category (\'%s\')'
        @throws BadRequest          'invalid Resource Type value for %s'
        @throws BadRequest          'processing type resource (\'%s\') different than requested (\'%s\')'
        @throws BadRequest          '%s Resource Type is empty, error.'
        @throws BadRequest          'type resource (%s) unknown type resource instance'
        """
        if not items:
            raise BadRequest('items parameter is empty')
        if not category:
            raise BadRequest('category parameter is empty')
        if not valid_actions:
            raise BadRequest('valid_actions parameter is empty')
        if not valid_res_types:
            raise BadRequest('valid_res_types parameter is empty')

        type_resources = {}
        otype_resources = []
        res_type = ''
        altid = ''
        cnt = 2

        try:
            if category == 'AssetTypes':
                processing_type = RT.AssetType
            elif category == 'EventTypes':
                processing_type = RT.EventDurationType
            else:
                raise BadRequest('unknown value (%s) for processing TypeResources' % category)

            for item in items:
                altid = self._get_item_value(item,'Resource ID')
                if not altid:
                    raise BadRequest('empty Resource ID value')

                action = self._get_item_value(item,'Action')
                if action:
                    action = action.lower()
                    if action not in valid_actions:
                        raise BadRequest('invalid Action value for %s' % altid)

                concrete    = self._get_item_value_boolean(item,'Concrete Type')
                extends     = self._get_item_value(item,'Extends')
                description = self._get_item_value(item,'Description')
                org_ids     = self._get_item_value(item,'Facility IDS')
                if org_ids:
                    org_ids = self.get_list_csv(item['Facility IDS'])

                event_category = ''
                if processing_type == RT.EventDurationType:
                    event_category = self._get_item_value(item,'Event Category')
                    try:
                        if event_category:
                            event_category_id = EventCategoryEnum._value_map[event_category]
                    except:
                        raise BadRequest('unknown event category (\'%s\')' % event_category)

                # verify resource type is a valid type in general and specifically, valid for category being processed
                res_type = self._get_item_value(item, 'Resource Type')
                if not res_type:
                    raise BadRequest('%s Resource Type is empty, error.' % altid)
                if res_type not in valid_res_types:
                    raise BadRequest('invalid Resource Type value for %s' % altid)
                if res_type != processing_type:
                    raise BadRequest('processing type resource (\'%s\') different than requested (\'%s\')' % (res_type, processing_type))

                # Existing instance with this altid and resource type? Find resource of res_type by name
                res_exists = False
                if altid and res_type:
                    key = self._get_type_resource_key_by_name(altid, res_type)
                    if key:
                        wo_name, wo_type_resource = self._populate_wo_type_resource(key)
                        if wo_type_resource:
                            res_exists = True

                if res_exists:
                    # rule cannot change 'type', cannot change 'altid', cannot change '_id' (system assigned uuid)
                    wo_type_resource['action']          = action
                    wo_type_resource['description']     = description
                    wo_type_resource['org_altids']      = org_ids
                    wo_type_resource['_exists']         = True
                    wo_type_resource['_import']         = False

                    # Deal with inbound attribute_specifications if provided in spread sheet
                    type_resource = wo_type_resource

                else:
                    type_resource = None
                    extensions = None
                    extensions = []
                    type_resource = {
                                'altid'         : altid,                  # alt name provided for TypeResource
                                'action'        : action,                 # action to perform for this TypeResource
                                'type'          : res_type,               # type specified for TypeResource
                                'concrete'      : concrete,               # concrete type {True | False}
                                'id'            : altid,                  # Name provided for TypeResource
                                'description'   : description,            # Description provided for TypeResource
                                'extends'       : extends,                # TypeResource inherits from extends TypeResource
                                'org_altids'    : [],                     # TypeResource shared across Facilities
                                '_id'           : '',                     # system assigned uuid4
                                '_extends_id'   : '',                     # system assigned uuid4 for extends
                                '_attrs'        : [],                     # (tmp) attribute spec names for this resource
                                '_xattrs'       : [],                     # (tmp) attribute spec keys for this resource
                                '_attribute_specifications_names' : [],   # comprehensive, ordered list of names
                                '_attribute_specifications_keys' : [],    # comprehensive, ordered list of keys
                                '_attribute_specifications' : {},         # comprehensive attribute_specifications
                                '_extensions'   : extensions,             # ordered list of extends
                                '_exists'       : False,                  # instance exists?
                                '_import'       : False,                  # used to flag wo created for extends processing
                                'event_category': event_category          # for event type processing
                                }

                    type_resource['org_altids'] = org_ids[:]

                if altid not in otype_resources:
                    otype_resources.append(altid)

                if altid not in type_resources:
                    type_resources[type_resource['altid']] = type_resource

                cnt += 1

            # Populate _extensions (do only if instance doesn't exist)
            if not altid:
                raise BadRequest('altid is empty')
            if not res_type:
                raise BadRequest('%s res_type is empty' % altid)
            add_otype_resources = []
            add_type_resources = {}
            count = 0
            for ax in otype_resources:
                count += 1
                resource = None
                resource = type_resources[ax]
                if resource:
                    if not resource['_exists']:     # if new instance to be created populate _extensions
                        if resource['extends']:
                            extends_name = ''
                            extend_name = resource['extends']
                            if extend_name not in resource['_extensions']:
                                resource['_extensions'].append(extend_name)
                            if extend_name in otype_resources:                  # else error; review use of altid
                                extend_resource = type_resources[extend_name]
                                if extend_resource['_extensions']:
                                    resource['_extensions'].extend(extend_resource['_extensions'])

                            else:
                                # the value which 'extends' is not provided in spread sheet
                                # find type resource (if it exists)
                                key = self._get_type_resource_key_by_name(extend_name, res_type)
                                if key:
                                    wo_nam, wo_res = self._populate_wo_type_resource(key)
                                    if wo_res:
                                        wo_res['_import'] = True
                                        wo_res['_exists'] = True
                                        if (wo_nam not in add_otype_resources) and (wo_nam not in otype_resources):
                                            add_otype_resources.append(wo_nam)
                                        if (wo_nam not in add_type_resources) and (wo_nam not in type_resources):
                                            add_type_resources[wo_nam] = wo_res

                                        # if there are extensions verify they are not already listed before adding...
                                        if wo_res['_extensions']:
                                            list_extensions = wo_res['_extensions']
                                            for extension in list_extensions:
                                                if (extension not in add_otype_resources) and (extension not in otype_resources):
                                                    add_otype_resources.append(extension)

                                else:
                                    raise BadRequest('type resource (%s) unknown type resource instance' % extend_name)

            # populate remaining add_type_resources
            tmp = add_otype_resources[:]
            new_add_list = tmp[::-1]

            inx = 0
            for item in new_add_list:
                inx += 1
                # determine if need to add working object...
                if (item not in add_type_resources) and (item not in type_resources):
                    key = self._get_type_resource_key_by_name(item, res_type)
                    if key:
                        wo_nam = None
                        wo_res = None
                        wo_nam, wo_res = self._populate_wo_type_resource(key)
                        if wo_res:
                            wo_res['_import'] = True
                            wo_res['_exists'] = True
                            if (wo_nam not in add_type_resources) and (wo_nam not in type_resources):
                                add_type_resources[wo_nam] = wo_res

                            # if there are extensions verify they are not already listed before adding...
                            if wo_res['_extensions']:
                                list_extensions = wo_res['_extensions']
                                for extension in list_extensions:
                                    if (extension not in add_otype_resources) or (extension not in otype_resources):
                                        add_otype_resources.append(extension)

            resources = {}
            oresources = []
            if new_add_list:

                for ores in otype_resources:
                    if ores not in new_add_list:
                        new_add_list.append(ores)

                for res in type_resources:
                    if res not in add_type_resources:
                        add_type_resources[res] = type_resources[res]

                resources = add_type_resources
                oresources = new_add_list

            else:
                resources = type_resources
                oresources = otype_resources

            # Display _extensions
            #for ax in otype_resources:
            #    resource = type_resources[ax]

        except BadRequest, Arguments:
            raise BadRequest('(sheet: %s) %s (row: %d)' % (category, Arguments.get_error_message(), cnt))
        except NotFound, Arguments:
            raise NotFound('(sheet: %s) %s (row: %d)' % (category, Arguments.get_error_message(), cnt))
        except Inconsistent, Arguments:
            raise Inconsistent('(sheet: %s) %s (row: %d)' % (category, Arguments.get_error_message(), cnt))

        except:
            raise BadRequest('Failed to process working object type resource')

        return resources, oresources

    def _process_wo_event_asset_map(self, items, wo_event_resources, wo_asset_resources, category, valid_actions):

        if not items:
            raise BadRequest('items parameter is empty')
        if not category:
            raise BadRequest('category parameter is empty')
        if not wo_event_resources:
            raise BadRequest('wo_event_resources parameter is empty')
        if not wo_asset_resources:
            raise BadRequest('wo_asset_resources parameter is empty')
        if not valid_actions:
            raise BadRequest('valid_actions parameter is empty')

        wo_resources = {}
        cnt = 2
        for item in items:

            event_altid = self._get_item_value(item,'Event Resource ID')
            if not event_altid:
                raise BadRequest('(sheet: %s) Empty Event Resource ID value (row: %d) ' % (category, cnt))

            asset_altid = self._get_item_value(item,'Asset Resource ID')
            if not asset_altid:
                raise BadRequest('(sheet: %s) Empty Asset Resource ID value (row: %d) ' % (category, cnt))

            action = self._get_item_value(item,'Action')
            if action not in valid_actions:
                raise BadRequest('(sheet: %s) Invalid Action value (row: %d)' % (category, cnt))

            description = self._get_item_value(item,'Description')

            # Verify Event resource ID instance exists?
            _id_event = ''
            _id_event_type = ''
            _exists_event = False
            event_org_altids = []
            event_category = ''
            event_category_id = ''
            event_res_exists = False
            if event_altid:
                key = self._get_resource_key_by_name(event_altid, RT.EventDuration)
                if key:
                    wo_event_name, wo_event_resource = self._populate_wo_asset_resource(key)
                    if wo_event_resource:
                        event_res_exists = True
                        _exists_event    = True
                        _id_event        = wo_event_resource['_id']
                        event_org_altids = wo_event_resource['org_altids']
                else:
                    if event_altid in wo_event_resources:
                        wo_event_resource = wo_event_resources[event_altid]
                        if wo_event_resource:
                            _exists_event    = False
                            event_org_altids = wo_event_resource['org_altids']
                        else:
                            raise BadRequest('wo_event_resource empty')
                    else:
                        # error - no instance and no working object provided for event
                        raise BadRequest('both event duration instance and working object are unavailable; cannot process.')

            # if event instance exists, get event duration type (id) that event duration implements
            if event_res_exists:
                predicate = 'implementsEventDurationType'
                assoc_category = ''
                associations = self.RR.find_associations(subject=_id_event, predicate=predicate, id_only=False)
                if associations:
                    if len(associations) == 1:
                        _id_event_type = associations[0].o
                    else:
                        raise Inconsistent('event duration has multiple event duration types')
                else:
                    raise BadRequest('event duration does not have associated event duration type')

                if _id_event_type:
                    # read type resource and map event_duration_type.category to event_to_asset_association type
                    event_type_obj = self.read_event_duration_type(_id_event_type)

                    # only concrete type resources will have an event category (rule)
                    if event_type_obj.concrete == True:
                        event_category_id = event_type_obj.event_category
                        event_category = EventCategoryEnum._str_map[event_category_id]

                else:
                    raise NotFound('failed to locate event duration type for event duration (%s)', event_altid)

            # Verify Asset resource ID instance exists? If so, gather information
            _id_asset = ''
            _exists_asset = False
            asset_org_altids = []
            if asset_altid:
                key = self._get_resource_key_by_name(asset_altid, RT.Asset)
                if key:
                    wo_asset_name, wo_asset_resource = self._populate_wo_asset_resource(key)
                    if wo_asset_resource:
                        _exists_asset    = True
                        _id_asset        = wo_asset_resource['_id']
                        asset_org_altids = wo_asset_resource['org_altids']

                else:
                    if asset_altid in wo_asset_resources:
                        wo_asset_resource = wo_asset_resources[asset_altid]
                        if wo_asset_resource:
                            _exists_asset    = False
                            asset_org_altids = wo_asset_resource['org_altids']
                        else:
                            raise BadRequest('wo_asset_resource empty')
                    else:
                        # error - no instance and no working object provided for event
                        raise BadRequest('both asset instance and working object are unavailable; cannot process.')

            _unique_id = ''
            if event_altid and asset_altid:
                _unique_id  = event_altid + '|' + asset_altid
            else:
                if not event_altid:
                    raise BadRequest('unable to form unique id for event to asset mapping; missing event duration altid')
                if not _id_asset:
                    raise BadRequest('unable to form unique id for event to asset mapping; missing asset altid')

            wo_resource = { '_unique_id'        : _unique_id,           # manufactured id (from event id and asset id)
                            'event_altid'       : event_altid,          # alt name provided for event resource
                            'event_category'    : event_category,       # event type resource category
                            'event_category_id' : event_category_id,    # event type resource category id
                            '_id_event'         : _id_event,            # sysid for event duration instance
                            '_id_event_type'    : _id_event_type,       # sysid for event duration type instance
                            'event_org_altids'  : event_org_altids,     # event duration shared across Facilities
                            'asset_altid'       : asset_altid,          # alt name provided for asset resource
                            '_id_asset'         : _id_asset,            # alt name provided for event resource
                            'asset_org_altids'  : asset_org_altids,     # TypeResource shared across Facilities
                            'action'            : action,               # action to perform for this TypeResource
                            'description'       : description,          # Description provided for event-asset mapping
                            '_exists_event'     : _exists_event,        # instance exists?
                            '_exists_asset'     : _exists_asset,        # instance exists?
                            '_import_event'     : False,                # used to flag wo created for further processing
                            '_import_asset'     : False                 # used to flag wo created for further processing
                          }

            if _unique_id not in wo_resources:
                wo_resources[_unique_id] = wo_resource

            cnt += 1

        return wo_resources


    def _process_wo_resources(self, items, category, valid_actions, valid_res_types):
        """Process object_definitions for TypeResources where categories 'Asset', 'EventDuration'; create working objects.
        Resulting dictionary contains all Asset or EventDuration working objects with key = altid.

        @param  items                   {}      # dictionary of all rows fom sheet named 'category'
        @param  category                ''      # category being processed (sheet name)
        @param  valid_actions           []      # list of valid actions
        @param  valid_res_types         []      # list of valid rsource types
        @retval resources               {}      # dictionary of type resource working objects
        @retval oresources              []      # list of type resource names processed
        @retval otype_resources_used    []      # list of all type resources used by Assets processed
        @throws BadRequest              if object with specified id does not have_id or_rev attribute
        """
        if not items:
            raise BadRequest('items parameter is empty')
        if not category:
            raise BadRequest('category parameter is empty')
        if not valid_actions:
            raise BadRequest('valid_actions parameter is empty')
        if not valid_res_types:
            raise BadRequest('valid_res_types parameter is empty')

        if category == 'Assets':
            processing_type = RT.Asset
        elif category == 'Events':
            processing_type = RT.EventDuration
        else:
            raise BadRequest('unknown category (%s) for processing marine tracking resources' % category)

        resources = {}
        oresources = []
        otype_resources_used = []
        cnt = 1
        for item in items:
            altid = self._get_item_value(item,'Resource ID')
            if not altid:
                raise BadRequest('(sheet: %s) Empty Resource ID value (row: %d) ' % (category, cnt))

            action = self._get_item_value(item,'Action')
            if action:
                action = action.lower()
            if action not in valid_actions:
                raise BadRequest('(sheet: %s) Invalid Action value for %s' % (category, altid))

            description = self._get_item_value(item,'Description')
            implements  = self._get_item_value(item,'Implements')
            org_ids     = self._get_item_value(item,'Facility IDS')
            if org_ids:
                org_ids = self.get_list_csv(item['Facility IDS'])

            # verify resource type is a valid type in general and specifically, valid for category being processed
            res_type = self._get_item_value(item, 'Resource Type')
            if res_type:
                if res_type not in valid_res_types:
                    raise BadRequest('(sheet: %s) Invalid Resource Type value for %s' % (category, altid))
                if res_type != processing_type:
                    raise BadRequest('Processing resource type (*%s*) different than type requested (*%s*)' % (res_type, processing_type))
            else:
                raise BadRequest('%s Resource Type is empty, error.' % altid)

            # Existing instance with this altid and resource type? Find resource of res_type by name
            res_exists = False
            if altid and res_type:
                key = self._get_resource_key_by_name(altid, res_type)
                if key:
                    wo_name, wo_resource = self._populate_wo_asset_resource(key)
                    if wo_resource:
                        res_exists = True

            if res_exists == True:
                wo_resource['action']          = action
                wo_resource['description']     = description
                wo_resource['org_altids']      = org_ids
                wo_resource['_exists']         = True         # Hook if this is false test duplicate namespace altids!
                wo_resource['_import']         = False

                # Deal with inbound attribute_specifications if provided in spread sheet
                resource = wo_resource

            else:
                # Define type implemented by resource, otherwise you will get burnt during _create_marine_resource!
                _implements_type = ''
                if res_type == RT.Asset:
                    _implements_type = RT.AssetType
                elif res_type == RT.EventDuration:
                    _implements_type = RT.EventDurationType
                else:
                    raise BadRequest('a valid resource type provided (%s) however type resource implemented is unknown.' % res_type)

                resource = {
                            'altid'             : altid,            # altid for resource
                            'action'            : action,           # action to perform for resource
                            'type'              : res_type,         # resource type (Asset|EventDuration) for collapse
                            'concrete'          : '',               # vestige - concrete type {True | False} todo remove
                            'name'              : altid,            # name for resource
                            'description'       : description,      # Description provided for resource
                            'implements'        : implements,       # name of TypeResource implemented
                            'org_altids'        : [],               # shared across Facilities
                            '_id'               : '',               # OOI system id for resource
                            '_implements_id'    : '',               # OOI system id for 'implements' TypeResource
                            '_implements_type'  : _implements_type, # OOI system type for 'implements' TypeResource
                            '_scoped_names'     : [],               # List of attribute names for this resource (no extends)
                            '_scoped_keys'      : [],               # list of attributes keys for this resource (no extends)
                            '_pattribute_names' : [],               # complete ordered list of attribute names possible
                            '_pattribute_keys'  : [],               # complete ordered list of attribute keys possible
                            '_attributes'       : {},               # Dictionary of All Attributes for this resource
                            '_exists'           : False,            # instance exists?
                            '_import'           : False
                            }

                if org_ids:
                    resource['org_altids'] = org_ids[:]
                if not implements:
                    raise BadRequest('(sheet: %s) marine tracking resources (%s) must implement a type resource (row: %d)' % (category, altid, cnt))

                type_tuple = []
                if implements not in otype_resources_used:
                    type_tuple.append(implements)
                    type_tuple.append(_implements_type)
                    otype_resources_used.append(type_tuple)

            if altid not in oresources:
                oresources.append(altid)

            resources[resource['altid']] = resource

            cnt += 1

        return resources, oresources, otype_resources_used


    def _process_wo_attribute_spec(self, items, category, code_space_id, list_wo_resources, wo_resources, valid_types):

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Process object_definitions for category AssetTypeAttributeSpec and create working
        # objects (AssetTypeAttributeSpec). AssetTypeAttributeSpecs dictionary holds all
        # holds all AssetTypeAttributeSpec working objects with key = name + '|' + altid
        #
        # Additionally, for each AttributeSpecification two other helper objects are created,
        # namely AssetTypeAttrSpecs (key='name') and xAssetTypeAttrSpecs (key='name | altid')
        #
        # throws:
        #  BadRequest items parameter is empty
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        if not items:
            raise BadRequest('items parameter is empty')
        if not category:
            raise BadRequest('category parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')
        if not list_wo_resources:
            raise BadRequest('list_wo_resources parameter is empty')
        if not wo_resources:
            raise BadRequest('wo_resources parameter is empty')
        if not valid_types:
            raise BadRequest('valid_types parameter is empty')

        # working_list and import_list are used to distinguish source of information
        # used to populate working objects. The import_list of working objects is populated
        # from instance information; working_list working objects have the spread sheet as the
        # source of their information.
        working_list = []
        import_list = []
        if list_wo_resources:
            for res in list_wo_resources:
                if not wo_resources[res]['_import']:
                    working_list.append(res)
                else:
                    import_list.append(res)

        all_names = []                      # used to prevent duplicate names on load
        wo_attribute_specifications = {}    # wo_attribute_specifications
        wo_attribute_specification = {}     # single wo attribute specification
        wo_attribute_specs_name = {}        # wo_attribute_specs_name   (dict by name)
        wo_attribute_specs_key = {}         # wo_attribute_specs_key    (dict by key)

        try:
            cnt = 2
            attrs_by_altid = []
            xattrs_by_altid = []
            for res in list_wo_resources:
                attrs_by_altid.append([])
                xattrs_by_altid.append([])
            type = ''
            if category == 'AssetAttributeSpecs':
                type = RT.AssetType
            elif category == 'EventAttributeSpecs':
                type = RT.EventDurationType
            else:
                raise BadRequest('Invalid or unknown sheet name; TypeResource value cannot be processed')

            # Read CodeSpace once, get codes and codesets (push back up into _get_working_objects) todo
            codes = {}
            codesets = {}
            code_space = self.read_code_space(code_space_id)
            if not code_space:
                raise NotFound('CodeSpace not found')
            if not code_space.codes:
                raise BadRequest('CodeSpace does not have Codes')
            if not code_space.codesets:
                raise BadRequest('CodeSpace does not have CodeSets')

            if code_space.codes:
                codes = code_space.codes
            if code_space.codesets:
                codesets = code_space.codesets

            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Process each AttributeSpecification - row by row from sheet AssetAttributeSpecs or EventAttributeSpecs
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            for item in items:

                altid = self._get_item_value(item,'Resource ID')
                if not altid:
                    raise BadRequest('Empty Resource ID value')

                if item['Attribute ID']:
                    name = item['Attribute ID']
                    # Produce unique key for each row; name itself must be unique for, and across, all resources.
                    if name:
                        key = name + '|' + altid
                        # Check: No duplicate names allowed (across all AssetType AttributeNames); raise BadRequest
                        if name in all_names:
                            # no duplicate names allowed across all AssetTypes, raise BadRequest (within this load)
                            # check across all type resource instances to prevent dup name (? yikes)  todo
                            raise BadRequest('Duplicate Name \'%s\' (sheet=%s, row=%d)' % (name, category, cnt))
                        else:
                            all_names.append(name)
                    else:
                        raise BadRequest('invalid Attribute ID value')
                else:
                    raise BadRequest('empty Attribute ID value')

                action = self._get_item_value(item,'Action')
                if action:
                    action = action.lower()

                default_value   = self._get_item_value(item,'Default Value')
                uom             = self._get_item_value(item,'UOM')
                visibility      = self._get_item_value_boolean(item, 'Visibility')
                editable        = self._get_item_value_boolean(item, 'Editable')
                journal         = self._get_item_value_boolean(item, 'Journal')
                description     = self._get_item_value(item,'Description')
                group_label     = self._get_item_value(item,'Group Label')
                attribute_label = self._get_item_value(item,'Attribute Label')
                attribute_rank  = self._get_item_value(item,'Attribute Rank')

                # validate value_type
                value_type      = self._get_item_value(item,'Value Type')
                if not value_type:
                    raise BadRequest('value_type empty')
                if self.valid_value_type(value_type) == False:
                    raise BadRequest('invalid value_type (%s)' % value_type)

                # validate_cardinality
                cardinality     = self._get_item_value(item,'Cardinality')
                if not cardinality:
                    raise BadRequest('Cardinality is empty')
                if not self.valid_cardinality(cardinality):
                    raise BadRequest('Invalid cardinality (\'%s\')' % cardinality)

                # validate value_constraints
                value_constraints = ''
                value_constraints = self._get_item_value(item,'Value Constraints')

                # Only value constraints required are for CodeValues; otherwise option value constraints are
                # supported for RealValues ('min' and 'max')
                if not value_constraints:
                    if value_type == 'CodeValue':
                        raise BadRequest('value_constraints empty; value_constraints required for CodeValue')

                if value_type == 'CodeValue' or (value_type == 'RealValue') or (value_type == 'IntegerValue'):
                    # CodeValues must have a 'set' command which identifies the codespace:codeset_name to be used for
                    # validation; additionally the default value provided for the CodeValue must be one of the
                    # CodeSet.enumeration values or equal to 'NONE'
                    if value_type == 'CodeValue':
                        commands = []
                        commands = self._get_command_value(value_type, value_constraints)   # validate value_constraints
                        if not commands:
                            raise BadRequest('value constraints provided but did not result in processing commands')

                        if len(commands) != 1:
                            raise BadRequest('CodeValue requires one and only one \'set\' command')
                        command_value = commands[0]
                        if command_value[0] != 'set':
                            raise BadRequest('CodeValue requires \'set\' command')
                        codeset_name = command_value[1]
                        if codeset_name:
                            if codeset_name not in codesets:
                                raise BadRequest('CodeSet name (\'%s\') not in CodeSpace' % codeset_name)
                        if not default_value:
                            raise BadRequest('CodeSet default value empty; must be one of CodeSet enumeration values')

                        # verify default value is one of CodeSet enumeration values
                        the_code_set = codesets[codeset_name]
                        if not the_code_set:
                            raise BadRequest('CodeSet (\'%s\') is empty' % codeset_name)
                        codeset_enum = the_code_set.enumeration
                        if not codeset_enum:
                            raise BadRequest('CodeSet (\'%s\') enumeration is empty' % codeset_name)
                        if default_value not in codeset_enum:
                            raise BadRequest('CodeSet (\'%s\') enumeration does not include default value \'%s\'' % (codeset_name, default_value))

                    # RealValue have optional 'min' and 'max' commands; if provided ensure one of 'min or 'max'
                    elif value_type == 'RealValue':
                        rvalue = None
                        if default_value:
                            if default_value != 'NONE':
                                if self.isfloat(default_value):
                                    rvalue = float(default_value)
                                else:
                                    raise Inconsistent('invalid default value \'%s\'; expected %s' % (default_value, value_type))
                        else:
                            raise Inconsistent('default value empty; expected %s' % value_type)

                        if value_constraints:
                            commands = []
                            commands = self._get_command_value(value_type, value_constraints)   # validate value_constraints
                            if not commands:
                                raise BadRequest('value constraints provided but did not result in processing commands')

                            valid_real_value_commands = ['min', 'max']
                            if commands:
                                if len(commands) > 2:
                                    raise BadRequest('too many commands; %s supports commands \'min\' and \'max\'' % value_type)

                                for command in commands:
                                    cmd = command[0]
                                    if cmd not in valid_real_value_commands:
                                        raise BadRequest('invalid command value \'%s\'; valid command include \'min\' and \'max\'' % cmd)

                                    if rvalue:
                                        # get range values if provided, compare attribute range values
                                        min_value = ''
                                        max_value = ''
                                        cmd_value = command[1]
                                        # get min and max values (if provided)
                                        if cmd_value:
                                            if cmd == 'min':
                                                min_value = cmd_value
                                            elif cmd == 'max':
                                                max_value = cmd_value
                                        else:
                                            raise BadRequest('attribute (%s) has invalid command value (%s)'% (name, cmd_value))

                                        # validate default_value is within constraints
                                        rmin = None
                                        rmax = None
                                        if min_value:
                                            rmin = float(min_value)
                                            if rvalue < rmin:
                                                raise BadRequest('attribute (%s) default value (%s) less than minimum Value Constraints (%s)' % (name, str(rvalue), str(rmin)))
                                        if max_value:
                                            rmax = float(max_value)
                                            if rvalue > rmax:
                                                raise BadRequest('attribute (%s) default value (%s) greater than maximum Value Constraints (%s)' % (name, str(rvalue), str(rmax)))


                    # IntegerValue have optional 'min' and 'max' commands; if provided ensure one of 'min or 'max'
                    elif value_type == 'IntegerValue':
                        rvalue = None
                        if default_value:
                            if default_value != 'NONE':
                                if self.isint(default_value):
                                    rvalue = int(default_value)
                                else:
                                    raise Inconsistent('invalid default value \'%s\'; expected %s' % (default_value, value_type))
                        else:
                            raise Inconsistent('default value empty; expected %s' % value_type)

                        if value_constraints:
                            commands = []
                            commands = self._get_command_value(value_type, value_constraints)   # validate value_constraints
                            if not commands:
                                raise BadRequest('value constraints provided, but did not result in processing commands')

                            valid_integer_value_commands = ['min', 'max']
                            if commands:
                                if len(commands) > 2:
                                    raise BadRequest('too many commands; %s supports commands \'min\' and \'max\'' % value_type)
                                for command in commands:
                                    cmd = command[0]
                                    if cmd not in valid_integer_value_commands:
                                        raise BadRequest('invalid command value \'%s\'; valid command include \'min\' and \'max\'' % cmd)

                                    if rvalue:
                                        # get range values if provided, compare attribute range values
                                        min_value = ''
                                        max_value = ''
                                        cmd_value = command[1]

                                        # get min and max values (if provided)
                                        if cmd_value:
                                            if cmd == 'min':
                                                min_value = cmd_value
                                            elif cmd == 'max':
                                                max_value = cmd_value
                                        else:
                                            raise BadRequest('attribute (%s) has invalid command value (%s)'% (name, cmd_value))

                                        # validate default_value is within constraints
                                        rmin = None
                                        rmax = None
                                        if min_value:
                                            rmin = int(min_value)
                                            if rvalue < rmin:
                                                raise BadRequest('attribute (%s) default value (%s) less than minimum Value Constraints (%s)' % (name, str(rvalue), str(rmin)))
                                        if max_value:
                                            rmax = int(max_value)
                                            if rvalue > rmax:
                                                raise BadRequest('attribute (%s) default value (%s) greater than maximum Value Constraints (%s)' % (name, str(rvalue), str(rmax)))


                # validate value_pattern
                value_pattern  = self._get_item_value(item,'Value Pattern')
                if not value_pattern:
                    raise BadRequest('value_pattern empty')

                if default_value != 'NONE':
                    regular_expression = ''.join(r'%s' % (value_pattern))
                    if not regular_expression:
                        raise BadRequest('evaluation of value_pattern (%s) failed to produce regular expression' % value_pattern)

                    m = re.match(regular_expression, default_value)
                    if not m:
                        raise BadRequest('validation of default value (\'%s\') failed pattern value regex (\'%s\')' % (default_value, value_pattern))

                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Declare and populate working object
                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                wo_attribute_specification = { '_key' : key,                # key = name + "|" +  altid
                               '_target'       : type,                      # AttributeSpecification parent TypeResource
                               'altid'         : altid,                     # altid for resource
                               'action'        : action,                    # action to perform
                               'id'            : name,                      # name for resource
                               'value_type'    : value_type,                # ValueType of attribute
                               'group_label'   : group_label,               # group display label
                               'attr_label'    : attribute_label,           # attribute label
                               'rank'          : attribute_rank,            # rank order for attribute display
                               'visibility'    : visibility,                # 'TRUE' or 'FALSE'
                               'description'   : description,               # description
                               'cardinality'   : cardinality,               # cardinality
                               'value_constraints': value_constraints,      # constraints on value range
                               'value_pattern' : value_pattern,             # validate values regex
                               'default_value' : default_value,             # default value
                               'uom'           : uom,                       # unit of measure
                               'editable'      : editable,                  # 'TRUE' or 'FALSE'
                               'journal'       : journal,                   # 'TRUE' or 'FALSE'
                               '_source_id'    : altid                      # name of source TypeResource
                               }

                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Add to helper objects AssetTypeAttrSpecs (key='name') and xAssetTypeAttrSpecs (key='name | altid')
                # Also, populate the list of names and keys for this AssetType in working
                # object AssetTypeAttributeSpec, in fields attrs and xattrs respectively.
                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                for res in working_list:            # list_wo_resources:
                    if altid == res:
                        # Check to see if duplicate AttributeSpecification name (for resource res)
                        #ix = list_wo_resources.index(res)
                        ix = list_wo_resources.index(res)
                        if key not in xattrs_by_altid[ix]:          # if not in list, add - otherwise duplicate
                            attrs_by_altid[ix].append(name)         # accumulate names by altid
                            xattrs_by_altid[ix].append(key)         # accumulate keys  by altid
                            wo_attribute_specifications[key] = wo_attribute_specification.copy()
                            asset_type = wo_resources[altid]        # rethink this! todo
                            asset_type['_attrs'].append(name)       # ordered list of names (no extends)
                            asset_type['_xattrs'].append(key)       # ordered list of keys (no extends)
                        else:
                            # Duplicate found within the resource scope (leave in for now); currently covered by
                            # the check above for name in all_names.
                            raise BadRequest('Duplicate AttributeSpecification Name (%s); must be unique.' % name)

                cnt += 1


            # working import_list
            for res in import_list:
                if res in wo_resources:
                    tmp = wo_resources[res]
                    altid = tmp['altid']

                    if tmp['_attribute_specifications']:
                        specs = tmp['_attribute_specifications']
                        for sname, s in specs.iteritems():

                            name = s['id']
                            key = name + "|" + s['_source_id']

                            wo_attribute_specification = { '_key' : key,        # key = name + "|" +  altid
                                       '_target'       : type,                  # AttributeSpecification parent TypeResource
                                       'altid'         : altid,                 # altid for resource
                                       'action'        : 'add',                 # action to perform
                                       'id'            : name,                  # name for resource
                                       'value_type'    : s['value_type'],       # ValueType of attribue
                                       'group_label'   : s['group_label'],      # group display label
                                       'attr_label'    : s['attr_label'],       # attribute label
                                       'rank'          : s['rank'],             # rank order for attribute display
                                       'visibility'    : s['visibility'],       # 'TRUE' or 'FALSE'
                                       'description'   : s['description'],      # description
                                       'cardinality'   : s['cardinality'],      # cardinality
                                       'value_constraints': s['value_constraints'], # constraints on value range
                                       'value_pattern': s['value_pattern'],   # validate values
                                       'default_value' : s['default_value'],    # default value
                                       'uom'           : s['uom'],              # unit of measure
                                       'editable'      : s['editable'],         # 'TRUE' or 'FALSE'
                                       'journal'       : s['journal'],          # 'TRUE' or 'FALSE'
                                       '_source_id'    : s['_source_id']        # name of source TypeResource
                                       }

                            ix = list_wo_resources.index(res)
                            if key not in xattrs_by_altid[ix]:          # if not in list, add - otherwise duplicate
                                attrs_by_altid[ix].append(name)         # accumulate names by altid
                                xattrs_by_altid[ix].append(key)         # accumulate keys by altid
                                wo_attribute_specifications[key] = wo_attribute_specification.copy()

                            else:
                                # Duplicate found within the resource scope (leave in for now); currently covered by
                                # the check above for name in all_names.
                                raise BadRequest('Duplicate AttributeSpecification Name (%s); must be unique.' % category)


            inx = 0
            for res in list_wo_resources:
                wo_attribute_specs_name[res] = attrs_by_altid[inx]         # by name
                wo_attribute_specs_key[res] = xattrs_by_altid[inx]         # by key
                inx += 1


            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # If a TypeResource does not have AttributeSpecifications - then error.
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            for res in working_list:
                if not wo_attribute_specs_name[res]:
                    raise BadRequest('TypeResource (%s) does not have AttributeSpecifications.' % res)

            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # If number of AttributeSpecification names and keys does not match, error
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            #if len(wo_attribute_specs_name[res]) != len(wo_attribute_specs_key[res]):
            #    raise BadRequest('Number of attribute specification names does not match number of keys.')

        except BadRequest, Arguments:
            raise BadRequest('(sheet: %s) %s (row: %d)' % (category, Arguments.get_error_message(), cnt))
        except NotFound, Arguments:
            raise NotFound('(sheet: %s) %s (row: %d)' % (category, Arguments.get_error_message(), cnt))
        except Inconsistent, Arguments:
            raise Inconsistent('(sheet: %s) %s (row: %d)' % (category, Arguments.get_error_message(), cnt))

        except:
            raise BadRequest('Failed to annotate one or more working objects.')


        return wo_attribute_specifications, wo_attribute_specs_name, wo_attribute_specs_key


    def _processProposedAssetAttributes(self, AssetTypeAttrSpecs, xAssetTypeAttrSpecs, AssetTypes, oAssetTypes,
                                            AssetResources, AssetTypeAttributeSpecs):

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Using information provided in sheet AssetAttributeSpecs (sheet 5):
        # Create lists by AssetType, for: (1) Attribute names, (2) AttributeSpecification names and
        # (3) similar comprehensive list of name-altid keys; (4) populate AssetType _attribute_specifications.
        # For each Asset, identify comprehensive lists of Asset Attribute names and corresponding comprehensive list
        # of name-altid keys.
        #
        # throws:
        #  BadRequest items parameter is empty
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        if not AssetTypeAttrSpecs:
            raise BadRequest('AssetTypeAttrSpecs parameter is empty')
        if not xAssetTypeAttrSpecs:
            raise BadRequest('xAssetTypeAttrSpecs parameter is empty')
        if not AssetTypes:
            raise BadRequest('AssetTypes parameter is empty')
        if not oAssetTypes:
            raise BadRequest('oAssetTypes parameter is empty')
        if not AssetTypeAttributeSpecs:
            raise BadRequest('AssetTypeAttributeSpecs parameter is empty')

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Process through list of AssetTypes identified in Sheet 5, entitled AssetTypeAttributeSpec
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        for res in oAssetTypes:         # use oAssetTypes to process ordered list of working objects

            # Get AttributeSpecifications names for this res from TypeResource AttributeSpecs
            attrs = []
            if res in AssetTypeAttrSpecs:
                if AssetTypeAttrSpecs[res]:
                    attrs = AssetTypeAttrSpecs[res]
            else:
                raise BadRequest('TypeResource (%s) does not have AttributeSpecifications.' % res)

            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # (NAMES) Build ordered list of all AttributeSpecification names to identify content of
            # AssetType attribute_specifications; same names used by implementing TypeResource
            # (for Attribute names).
            # Start with ordered list of names for this resource (in '_attrs'), then
            # process the _extensions also associated with this TypeResource (e.g. AssetType or EventDuration).
            # Capitalize on resulting list as representing the corresponding (and required) Attribute names
            # for the Asset or EventDuration which will have an Association with this TypeResource. To do this,
            # the hierarchy of relationships between TypeResources (Sheets 1 and 2) is used; this hierarchy
            # is stored in TypeResource['_extensions'] as an ordered list of TypeResources which 'extends'
            # one another. Each TypeResource has attribute names/attribute specification names stored in a
            # working element called 'attrs'
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            total_attrs = 0
            ext_attrs = []
            if attrs:
                AssetTypes[res]['_attrs'] = attrs[:]
                ext_attrs = attrs[::-1]
                total_attrs += len(attrs)

                if AssetTypes[res]['_extensions']:
                    extensions = AssetTypes[res]['_extensions']
                    current = res
                    for extends in extensions:
                        if extends in oAssetTypes:
                            if AssetTypes[extends]['_attrs']:
                                temp = AssetTypes[extends]['_attrs']
                                ext_attrs.extend(temp[::-1])
                                total_attrs += len(temp)

                        current = extends

                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Populate AssetType['_attribute_specifications_names']
                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                AssetTypes[res]['_attribute_specifications_names'] = ext_attrs[::-1]

                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Populate Asset['_pattribute_names'] (resource which implementsAssetType)
                # Known/proposed attributes are assigned to {Asset | EventDuration}Resources in _pattribute_names.
                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                if AssetResources:
                    for arx in AssetResources:
                        ar = AssetResources[arx]
                        if ar['implements'] == res:
                            ar['_pattribute_names'] = ext_attrs[::-1]    # complete ordered list of all attribute names
                            ar['_scoped_names'] = AssetTypeAttrSpecs[res][::-1]# ordered list of attribute names(no extends)
                            break

            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # (KEYS) Build complete and ordered list of keys to identify AttributeSpecification(s) to be
            # constructed and used by TypeResource. Working element xattrs used to store name|altid key
            # value for single TypeResource (no extends)
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            xattrs = []
            if xAssetTypeAttrSpecs[res]:
                xattrs = xAssetTypeAttrSpecs[res]
            else:
                raise BadRequest('AssetType (%s) resource does not have Attributes.' % res)
            xtotal_attrs = 0
            xext_attrs = []
            if xattrs:
                AssetTypes[res]['_xattrs'] = xattrs[:]
                xext_attrs = xattrs[::-1]
                xtotal_attrs += len(xattrs)

                if AssetTypes[res]['_extensions']:
                    extensions = AssetTypes[res]['_extensions']
                    current = res
                    for extends in extensions:
                        if extends in oAssetTypes:
                            if AssetTypes[extends]['_xattrs']:
                                temp = AssetTypes[extends]['_xattrs']
                                xext_attrs.extend(temp[::-1])
                                xtotal_attrs += len(temp)

                        current = extends

                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Populate AssetType['_attribute_specifications_keys']
                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                AssetTypes[res]['_attribute_specifications_keys'] = xext_attrs[::-1]

                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Populate Asset['_pattribute_keys'] (in resource which implementsAssetType);
                # the _pattribute_keys can be used to look up AttributeSpecification in
                # AssetType['_attribute_specifications']
                # Comprehensive, ordered list of known/proposed attribute name(s) is assigned
                # to {Asset | EventDuration}Resources in _pattribute_names.  _pattribute_keys is
                # the same but rather than using 'name' as its' key, it has a key == 'name | altid'
                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                if AssetResources:
                    for arx in AssetResources:
                        ar = AssetResources[arx]
                        if ar['implements'] == res:
                            ar['_pattribute_keys'] = xext_attrs[::-1]       # all keys (ordered), includes extends
                            ar['_scoped_keys'] = xAssetTypeAttrSpecs[res]   # keys for this resource only (no extends)
                            break


        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # (AttributeSpecification(s)) Populate all AssetType _attribute_specifications {}
        # In addition, a complete ordered list of known/proposed attribute name(s) is assigned
        # to {Asset | EventDuration}Resources in _pattribute_names.  _pattribute_keys is
        # the same, but rather than using 'name' as the key, it has key == 'attribute name | altid'
        if AssetResources:
            for ar in AssetResources:
                res = AssetResources[ar]
                if res['implements']:
                    implements_name = res['implements']
                    if implements_name:
                        if implements_name in AssetTypes:
                            asset_type = AssetTypes[implements_name]
                            attribute_specifications_keys = asset_type['_attribute_specifications_keys']

                        else:
                            # Bad/unknown 'implements' value for EventDuration (not in EventDurationTypes)
                            raise BadRequest('Asset implements an unknown AssetType (%s).' % implements_name)
                    else:
                        # Empty 'implements' value for EventDuration
                        raise BadRequest('Asset (%s) implements value is empty.' % res['altid'])

                    attribute_specifications = {}
                    for key in attribute_specifications_keys:
                        p = key.split('|')
                        name = p[0][:]
                        if name:
                            if key in AssetTypeAttributeSpecs.keys():
                                attribute_specification = AssetTypeAttributeSpecs[key]
                                attribute_specifications[name] = attribute_specification
                            else:
                                raise BadRequest('AttributeSpecification identified but not found in AssetTypeAttributeSpecs.')

                    if attribute_specifications:
                        if implements_name in oAssetTypes:
                            asset_type = AssetTypes[implements_name]
                            asset_type['_attribute_specifications'] = attribute_specifications
                    #else:
                    #    log.debug('\n\n ---- No attribute specifications for %s', ar)

        for ar in oAssetTypes:
            res = AssetTypes[ar]
            if not res['_attribute_specifications']:
                if res['_attribute_specifications_keys']:
                    as_keys = res['_attribute_specifications_keys']
                    some_attr_specs = {}
                    for key in as_keys:
                        tmp = key.split('|')
                        name = tmp[0]
                        if key in AssetTypeAttributeSpecs:
                            attribute_specification = AssetTypeAttributeSpecs[key]
                            some_attr_specs[name] = attribute_specification
                        else:
                            raise BadRequest('key (%s) not in AssetTypeAttributeSpecs' % key)

                    res['_attribute_specifications'] = some_attr_specs

        return

    def _processProposedEventDurationAttributes(self, EventDurationTypeAttrSpecs,xEventDurationTypeAttrSpecs, EventDurationTypes,
                                             oEventDurationTypes, EventDurationResources, EventDurationTypeAttributeSpecs):

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Using information provided in sheet EventDurationTypeAttributeSpec (sheet 6):
        # Create lists by EventDurationType, for: (1) Attribute names, (2) AttributeSpecification names and
        # (3) similar comprehensive list of name-altid keys; (4) populate EventDurationType _attribute_specifications.
        # For each EventDuration, identify comprehensive lists of *available* EventDuration Attribute names and similiar
        # comprehensive list of *available* name-altid keys.
        # The list of actual attributes used by an instance of a tracking resource (asset or asset event)
        # can be equal to or less than the number of attribute specifications defined in the TypeResource.
        # Rules:
        # 1. An attribute shall be required if the attribute specification Cardinality
        #       is 1..1 or 1..N - if not, then error.
        #
        # throws:
        #  BadRequest items parameter is empty
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        if not EventDurationTypeAttrSpecs:
            raise BadRequest('EventDurationTypeAttrSpecs parameter is empty')
        if not xEventDurationTypeAttrSpecs:
            raise BadRequest('xEventDurationTypeAttrSpecs parameter is empty')
        if not EventDurationTypes:
            raise BadRequest('EventDurationTypes parameter is empty')
        if not oEventDurationTypes:
            raise BadRequest('oEventDurationTypes parameter is empty')
        if not EventDurationTypeAttributeSpecs:
            raise BadRequest('EventDurationTypeAttributeSpecs parameter is empty')

        for res in oEventDurationTypes:
            # error: the EventDurationTypeAttributeSpecifications attribute refers to an EventDurationType
            # which wasn't defined in sheet 'EventDurationType'
            if res not in oEventDurationTypes:
                raise BadRequest('EventDurationTypeAttributeSpec specifies an unknown EventDurationType(%s).' % res)

            # Get AttributeSpecifications names for this res from AssetType AttributeSpecs
            attrs = []
            if EventDurationTypeAttrSpecs[res]:
                attrs = EventDurationTypeAttrSpecs[res]
            else:
                raise BadRequest('TypeResource (%s) does not have AttributeSpecifications.' % res)

            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # (NAMES) Build ordered list of all AttributeSpecification names to identify content of
            # AssetType attribute_specifications; same names used by implementing TypeResource
            # (for Attribute names).
            # Start with ordered list of names for this resource (in '_attrs'), then
            # process the _extensions also associated with this TypeResource (e.g. AssetType or EventDuration).
            # Capitalize on resulting list as representing the corresponding (and required) Attribute names
            # for the Asset or EventDuration which will have an Association with this TypeResource. To do this,
            # the hierarchy of relationships between TypeResources (Sheets 1 and 2) is used; this hierarchy
            # is stored in TypeResource['_extensions'] as an ordered list of TypeResources which 'extends'
            # one another. Each TypeResource has attribute names/attribute specification names stored in a
            # working element called 'attrs'
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            total_attrs = 0
            ext_attrs = []
            if attrs:
                EventDurationTypes[res]['_attrs'] = attrs[:]
                ext_attrs = attrs[::-1]
                total_attrs += len(attrs)

                if EventDurationTypes[res]['_extensions']:
                    extensions = EventDurationTypes[res]['_extensions']
                    current = res
                    for extends in extensions:
                        if extends in oEventDurationTypes:
                            if EventDurationTypes[extends]['_attrs']:
                                temp = EventDurationTypes[extends]['_attrs']
                                ext_attrs.extend(temp[::-1])
                                total_attrs += len(temp)
                        current = extends


                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Populate EventDurationType['_attribute_specifications_names']
                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                EventDurationTypes[res]['_attribute_specifications_names'] = ext_attrs[::-1]

                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Populate EventDuration['_pattribute_names'] (resource which implementsEventDurationType)
                # Known/proposed attributes are assigned to {Asset | EventDuration}Resources in _attrspecs
                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                if EventDurationResources:
                    for arx in EventDurationResources:
                        ar = EventDurationResources[arx]
                        if ar['implements'] == res:
                            ar['_pattribute_names'] = ext_attrs[::-1]    # comprehensive ordered list of attribute names
                            #ar['_attrs'] = attrs[::-1]                  # ordered list of attribute names (no extends)
                            ar['_scoped_names'] = EventDurationTypeAttrSpecs[res][::-1]
                            break

            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # (KEYS) Build complete and ordered list of keys to identify AttributeSpecification(s) to be
            # constructed and used by TypeResource. Working element xattrs used to store 'name|altid' key
            # value for single TypeResource (no extends)
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            xattrs = []
            if xEventDurationTypeAttrSpecs[res]:
                xattrs = xEventDurationTypeAttrSpecs[res]
            else:
                raise BadRequest('TypeResource does not have AttributeSpecifications.')

            #verbose = False
            xtotal_attrs = 0
            xext_attrs = []
            if xattrs:
                EventDurationTypes[res]['_xattrs'] = xattrs[:]
                xext_attrs = xattrs[::-1]
                xtotal_attrs += len(xattrs)

                if EventDurationTypes[res]['_extensions']:
                    extensions = EventDurationTypes[res]['_extensions']
                    for extends in extensions:
                        if extends in oEventDurationTypes:
                            if EventDurationTypes[extends]['_xattrs']:
                                temp = EventDurationTypes[extends]['_xattrs']
                                xext_attrs.extend(temp[::-1])
                                xtotal_attrs += len(temp)

                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Populate AssetType['_attribute_specifications_keys']
                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                EventDurationTypes[res]['_attribute_specifications_keys'] = xext_attrs[::-1]

                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Populate EventDuration['_pattribute_keys'] (in the resource which implementsEventDurationType).
                # the _pattribute_keys can be used to look up AttributeSpecification in
                # EventDurationType['_attribute_specifications']  todo TEST THIS
                # Comprehensive, ordered list of known/proposed attribute name(s) is assigned
                # to {Asset | EventDuration}Resources in _pattribute_names.  _pattribute_keys (_xattrspecs)
                # is same, but rather than using 'name' as its' key, it has a key == 'attribute name | altid'
                #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                if EventDurationResources:
                    for arx in EventDurationResources:
                        ar = EventDurationResources[arx]
                        if ar['implements'] == res:
                            ar['_pattribute_keys'] = xext_attrs[::-1]    # all keys (ordered) includes extends
                            ar['_scoped_keys'] = xEventDurationTypeAttrSpecs[res][::-1] # keys for res w/o extend
                            break
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Populate all EventDurationType _attribute_specifications {}
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if EventDurationResources:
            for arx in EventDurationResources:
                ar = EventDurationResources[arx]
                if ar['implements']:
                    implements_name = ar['implements']
                    if implements_name:
                        if implements_name in oEventDurationTypes:
                            asset_type = EventDurationTypes[implements_name]
                            attribute_specifications_keys = asset_type['_attribute_specifications_keys']

                        else:
                            # Bad/unknown 'implements' value for EventDuration (not in EventDurationTypes)
                            raise BadRequest('EventDuration implements an unknown EventDurationType (\'%s\')' % implements_name)
                    else:
                        # Empty 'implements' value for EventDuration
                        raise BadRequest('EventDuration implements value is empty.')

                    attribute_specifications = {}
                    for key in attribute_specifications_keys:
                        p = key.split('|')
                        name = p[0][:]
                        if name:
                            if key in EventDurationTypeAttributeSpecs.keys():
                                attribute_specification = EventDurationTypeAttributeSpecs[key]
                                attribute_specifications[name] = attribute_specification
                            else:
                                raise BadRequest('AttributeSpecification identified but not found in AssetTypeAttributeSpecs.')

                    if attribute_specifications:
                        if implements_name in oEventDurationTypes:
                            asset_type = EventDurationTypes[implements_name]
                            asset_type['_attribute_specifications'] = attribute_specifications

        for ar in oEventDurationTypes:
            res = EventDurationTypes[ar]
            if not res['_attribute_specifications']:
                if res['_attribute_specifications_keys']:
                    as_keys = res['_attribute_specifications_keys']
                    some_attr_specs = {}
                    for key in as_keys:
                        tmp = key.split('|')
                        name = tmp[0]
                        attribute_specification = EventDurationTypeAttributeSpecs[key]
                        some_attr_specs[name] = attribute_specification

                    res['_attribute_specifications'] = some_attr_specs

        return

    def _processAttributes(self, res_type, hrAttributes, wo_resources, wo_types, category,
                           valid_resource_type, valid_value_types, code_space_id, items):

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Process sheet Attribute sheets  (sheet 7, 8) 'AssetAttributes', 'EventDurationAttributes'
        # For each row (get resource), get type[_attribute_specification_names]
        # loop through, populate whats available. If an AttributeSpecification 'cardinality' indicates
        # it is required (1..1 or 1..N) and value is not provided, then raise exception.
        # Utilize the AttributeSpecification for error processing.
        # Checks for ValueType (string, integer, real, CodeValue, etc.),  Value Constraints,
        # Value Pattern etc.
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if not items:
            raise BadRequest('items parameter is empty')
        if not res_type:
            raise BadRequest('res_type parameter is empty')
        if not hrAttributes:
            raise BadRequest('hrAttributes parameter is empty')
        if not wo_resources:
            raise BadRequest('wo_resources parameter is empty')
        if not wo_types:
            raise BadRequest('wp_types parameter is empty')
        if not category:
            raise BadRequest('category parameter is empty')
        if not valid_resource_type:
            raise BadRequest('valid_resource_type parameter is empty')
        if not valid_value_types:
            raise BadRequest('valid_value_types parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')


        # Read CodeSpace once, get codes and codesets (push back up into _get_working_objects) todo
        cnt = 1
        codes = {}
        codesets = {}
        if code_space_id:
            code_space = self.read_code_space(code_space_id)
            if code_space:
                if code_space.codes:
                    codes = code_space.codes
                if code_space.codesets:
                    codesets = code_space.codesets

        for item in items:

            cnt += 1
            altid = self._get_item_value(item,'Resource ID')
            if altid:
                altid = altid.strip()

            action = self._get_item_value(item,'Action')
            if not action:
                raise BadRequest('(sheet: %s) Action is empty (row: %d)' % (category, cnt))

            action = (action.lower()).strip()
            if altid not in wo_resources:
                raise BadRequest('No %s resource with name \'%s\'  (row: %d)' % (res_type, altid, cnt))

            res = wo_resources[altid]

            # Check to ensure this category of TypeResource is (1) known/exists (value in implements),
            # (2) the value in implements is available as a working object
            implements = res['implements']
            if not implements:
                raise BadRequest('(sheet: %s) No TypeResource association defined (row: %d)' % (category, cnt))
            else:
                type_resource = wo_types[implements]          # actual type of TypeResource
                if type_resource:
                    if type_resource['type'] not in valid_resource_type:
                        raise BadRequest('(sheet: %s) Invalid resource type value with name \'%s\'  (row: %d)' % (category, type_resource, cnt))
                else:
                    raise BadRequest('(sheet: %s) No %sType resource with name \'%s\'  (row: %d)' % (category, res_type, altid, cnt))


            if not type_resource['_attribute_specifications_names']:
                raise BadRequest('(sheet: %s) No AttributeSpecifications defined for implemented TypeResource (row: %d)' % (category, cnt))
            else:
                attribute_specifications_names = type_resource['_attribute_specifications_names'][:]

            _attributes = {}                            # gather attribute value(s) for each resource

            for a in attribute_specifications_names:

                a_attribute_spec = type_resource['_attribute_specifications'][a]
                cardinality      = self._get_item_value(a_attribute_spec,'Cardinality')
                is_required = False
                if cardinality:
                    if not self.valid_cardinality(cardinality):
                        raise BadRequest('(sheet: %s) Invalid cardinality (%s) value  (row: %d)' % (category, cardinality, cnt))
                    if '1.' in cardinality:
                        is_required = True

                value_type = a_attribute_spec['value_type']
                if not value_type:
                    raise BadRequest('(sheet: %s) Empty value_type value (row: %d)' % (category, cnt))

                value_type = value_type.strip()
                if self.valid_value_type(value_type) == False:
                    #if value_type not in valid_value_types:
                    raise BadRequest('(sheet: %s) Invalid value_type value  (row: %d)' % (category, cnt))

                Attribute = {
                            'name'     : a,                 # key dictionary with attribute name
                            'value'    : [],                # revisit when types available
                            'source'   : implements,        # name of categoryType (which is source of AttributeSpecification)
                            'action' : action
                            }

                if a not in hrAttributes:

                    if res['_exists'] == False:
                        Attribute['value'] = [a_attribute_spec['default_value']]
                        _attributes[a] = Attribute

                else:
                    # Validate value content against the meta-data in AttributeSpecification (the TypeResource)
                    if not item[a]:
                        Attribute['value'] = [a_attribute_spec['default_value']]
                        _attributes[a] = Attribute
                    elif item[a]:

                        tvalue = item[a][:]

                        if tvalue:
                            # [hook]
                            try:
                                # return true or throw error; send only codeset of interest todo
                                self._valid_attribute_value_xls(a_attribute_spec, tvalue, valid_resource_type, code_space_id) #codes, codesets)
                                Attribute['value'] = self.get_list_csv(item[a])

                            except Inconsistent, Arguments:
                                raise BadRequest('(sheet: %s) %s (row: %d)' % (category,  Arguments.get_error_message(), cnt))

                            except BadRequest, Arguments:
                                raise BadRequest('(sheet: %s) %s (row: %d)' % (category,  Arguments.get_error_message(), cnt))

                            except NotFound, Arguments:
                                raise BadRequest('(sheet: %s) %s (row: %d)' % (category,  Arguments.get_error_message(), cnt))

                            # Gather attributes
                            _attributes[a] = Attribute     # Either (default value) or user supplied value - have attribute

            if not _attributes:
                raise BadRequest('%s (%s) does not have Attribute(s).' % (category, altid))

            # Right here - end of processing row
            res['_attributes'] = _attributes

        return

    def _processDefaultAttributes(self, res_type, hrAttributes, wo_resources, wo_types, category,
                           valid_resource_type, valid_value_types, code_space_id):

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # for each wo_resource which does not have _attributes defined (no entries in Attribute sheets
        # (sheet 7, 8) 'AssetAttributes', 'EventDurationAttributes') the default attributes are processed here.
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        if not res_type:
            raise BadRequest('res_type parameter is empty')
        if not hrAttributes:
            raise BadRequest('hrAttributes parameter is empty')
        if not wo_resources:
            raise BadRequest('wo_resources parameter is empty')
        if not wo_types:
            raise BadRequest('wp_types parameter is empty')
        if not category:
            raise BadRequest('category parameter is empty')
        if not valid_resource_type:
            raise BadRequest('valid_resource_type parameter is empty')
        if not valid_value_types:
            raise BadRequest('valid_value_types parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')

        verbose = False

        for wo_name, res in wo_resources.iteritems():
            # Get the resource and populate the _attributes (altid and action not empty), checked in _select_rows)
            altid = res['altid']
            action = res['action']
            category = res['type']
            if res['_attributes']:
                continue

            # Check to ensure this category of TypeResource is (1) known/exists (value in implements),
            # (2) the value in implements is available as a working object
            implements = res['implements']
            if not implements:
                raise BadRequest('(%s: %s) does not implement a TypeResource; no association defined' % (category, altid))
            else:
                type_resource = wo_types[implements]          # actual type of TypeResource
                if type_resource:
                    if type_resource['type'] not in valid_resource_type:
                        raise BadRequest('(%s) Invalid resource type value with name %s  ' % (category, type_resource))
                else:
                    raise BadRequest('(%s) No %sType resource with name %s' % (category, res_type, altid))


            if not type_resource['_attribute_specifications_names']:
                raise BadRequest('(%s) No AttributeSpecifications defined for TypeResource implemented (%s)' % (category, implements))

            attribute_specifications_names = type_resource['_attribute_specifications_names'][:]
            _attributes = {}

            for a in attribute_specifications_names:

                a_attribute_spec = type_resource['_attribute_specifications'][a]
                cardinality      = self._get_item_value(a_attribute_spec,'Cardinality')
                if cardinality:
                    if not self.valid_cardinality(cardinality):
                        raise BadRequest('(%s) Invalid cardinality (%s) value' % (category, cardinality))

                value_type = a_attribute_spec['value_type']
                if not value_type:
                    raise BadRequest('(%s) Empty value_type value' % category)

                value_type = value_type.strip()
                if value_type not in valid_value_types:
                    raise BadRequest('(%s) Invalid value_type value' % category)

                Attribute = {
                            'name'     : a,                 # key dictionary with attribute name
                            'value'    : [],                # revisit when types available
                            'source'   : implements,        # name of categoryType (which is source of AttributeSpecification)
                            'action' : action
                            }
                Attribute['value'] = [a_attribute_spec['default_value']]
                _attributes[a] = Attribute     # default value

            if not _attributes:
                raise BadRequest('%s (%s) does not have Attribute(s).' % (category, altid))

            res['_attributes'] = _attributes

        return

    def _get_command_value(self, value_type, value_constraints):

        # helper
        # process Value Constraints provided based on value_type

        commands = []

        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # Processing CodeValue - only valid command for CodeValue is 'set'
        # - - - - - - - - - - - - - - - - - - - - - - - - -
        if value_type == 'CodeValue':
            cmd_tuple = []
            command = ''
            command_value   = ''

            if 'set' not in value_constraints:
                raise BadRequest('\'set\' command required to process CodeSet value (set=codespace name:codeset name)')
            if ':' not in value_constraints:
                raise BadRequest('malformed \'set\' command; use colon (:) to separate codespace name and codeset name')
            if '=' not in value_constraints:
                raise BadRequest('malformed \'set\' command; use \'=\' to separate command from value')

            tmpwork = value_constraints[:]
            tmp = tmpwork.split('=')
            if len(tmp) == 2:
                tmpcommand = tmp[0]
                if tmpcommand:
                    tmpcommand = tmpcommand.lower()
                    tmpcommand = tmpcommand.strip()
                    if tmpcommand != 'set':
                        BadRequest('Invalid command (%s)to left of colon' % tmpcommand)
                    command = tmpcommand

                tmpvalue = tmp[1]     #codespace_name:codeset_name
                if tmpvalue:
                    tmpvalue = tmpvalue.strip()
                    if ':' not in tmpvalue:
                        raise BadRequest('use a colon to separate the codespace name and codeset name')
                    tmpnames = tmpvalue.split(':')
                    if len(tmpnames) != 2:
                        raise BadRequest('malformed value; there can only be one colon to the right of the equal sign')

                    cs_name = tmpnames[0]
                    codeset_name = tmpnames[1]
                    if not cs_name:
                        raise BadRequest('codespace name cannot be empty')
                    if not codeset_name:
                        raise BadRequest('codeset name cannot be empty')

                    command_value = codeset_name

                cmd_tuple.append(command)
                cmd_tuple.append(command_value)
                commands.append(cmd_tuple)

        # - - - - - - - - - - - - - - - - - - - - - - - - -
        # Processing RealValue or IntegerValue commands
        # valid commands for RealValue processing: example: 'min=0.00, max=10923.00'
        # valid commands for IntegerValue processing: example: 'min=0, max=10923'
        # all commands optional; commands available include 'min' and 'max'
        # if more than one command is used , separate by comma (as shown in example)
        # - - - - - - - - - - - - - - - - - - - - - - - - -
        elif value_type == 'RealValue' or value_type == 'IntegerValue':

            valid_commands = ['min', 'max']
            command_strings = []
            cmds_list = []
            cmds_value_list = []
            if value_constraints:
                if ',' in value_constraints:
                    command_strings = value_constraints.split(',')
                else:
                    command_strings.append(value_constraints)

            if len(command_strings) > 2:
                raise BadRequest('only two supported commands for %s constraints: %s' % (value_type, valid_commands))

            # commands is a list of command-value tuples
            commands = []
            for command_string in command_strings:
                cmd_tuple = []
                if '=' not in command_string:
                    raise BadRequest('malformed command string; valid format is: command=value')

                whole_command = command_string[:]
                command_parts = whole_command.split('=')
                if len(command_parts) > 2:
                    raise BadRequest('malformed command string; only one equal sign permitted, valid format is: command=value')
                elif len(command_parts) != 2:
                    raise BadRequest('malformed command string; valid format is: command=value')

                # Process command parts - command and command value
                tmp_command = command_parts[0]
                tmp_value = command_parts[1]
                cmds_list.append(tmp_command)           # local/temporary list
                cmds_value_list.append(tmp_value)       # local/temporary list
                # get command
                cmd = ''
                if tmp_command:
                    tmp_command = tmp_command.strip()
                    if tmp_command:
                        tmp_command = tmp_command.lower()
                        if tmp_command not in valid_commands:
                            raise BadRequest('invalid command (%s); valid commands are: %s' % (tmp_command, valid_commands))
                        cmd = tmp_command[:]
                    else:
                        raise BadRequest('type value (%s) command is empty' % value_type)

                # get command value
                cmd_value = ''
                if tmp_value:
                    cmd_value = cmd_value.strip()
                    if tmp_value:
                        cmd_value = tmp_value[:]
                    else:
                        raise BadRequest('type value (%s) command value is empty' % value_type)
                cmd_tuple.append(cmd)
                cmd_tuple.append(cmd_value)
                commands.append(cmd_tuple)

            # Two additional checks: (1) not same command, (2) min value <= max value
            if len(commands) == 2:
                if cmds_list and cmds_value_list:
                    if len(cmds_list) == 2 and len(cmds_value_list) == 2:
                        # same command?
                        if cmds_list[0] == cmds_list[1]:
                            raise BadRequest('same command twice; two commands with same name (%s)' % cmds_list[0])

                        # min less than or equal to max
                        rmin = 0.0
                        rmax = 0.0
                        if cmds_list[0] == 'min':
                            min_value = cmds_value_list[0]
                            max_value = cmds_value_list[1]
                        else:
                            min_value = cmds_value_list[1]
                            max_value = cmds_value_list[0]

                        if value_type == 'RealValue':
                            if self.isfloat(max_value):
                                rmax = float(max_value)
                            else:
                                raise Inconsistent('invalid max value \'%s\'; expected RealValue' % (max_value))
                            if self.isfloat(min_value):
                                rmin = float(min_value)
                            else:
                                raise Inconsistent('invalid min value \'%s\'; expected RealValue' % (min_value))

                            if rmin > rmax:
                                raise BadRequest('value of min command (%s) greater than value of max command (%s)', rmin, rmax)
                        else:
                            if self.isint(max_value):
                                rmax = int(max_value)
                            else:
                                raise Inconsistent('invalid max value \'%s\'; expected IntegerValue' % (max_value))
                            if self.isint(min_value):
                                rmin = int(min_value)
                            else:
                                raise Inconsistent('invalid min value \'%s\'; expected IntegerValue' % (min_value))
                            if rmin > rmax:
                                raise BadRequest('value of min command (%s) greater than value of max command (%s)', rmin, rmax)

        else:
            raise Inconsistent('unknown value_type (%s) for command value processing' % value_type)

        return commands

    def _valid_attribute_value_xls(self, attr_spec, attr_value, valid_resource_type, code_space_id):

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # given an attribute value determine if it is valid based on AttributeSpecification provided;
        # if not valid throw
        #
        #   valid_value_types = [   'IntegerValue', 'RealValue', 'Boolean', 'StringValue',
        #                           'CodeValue', 'DateValue', 'TimeValue', 'DateTimeValue', 'ResourceReferenceValue'],
        #                       ]
        #
        # For a given attr_value:
        #   Process according to value_type
        #   1. Verify valid value_type
        #   2. Verify valid cardinality; determine if required value
        #   3. For each kind of value type:
        #       a.    Process attr_value against value_pattern (parse)
        #       b.    Process attr_value against value_constraints (commands: set, min, max)
        #       c.    Process attr_value against uom (todo)
        #       d.    is_required? (todo)
        #       e.    default value (todo)
        #       f.    cardinality (todo)
        #
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        verbose = False

        if not attr_spec:
            raise BadRequest('attr_spec parameter is empty')
        if not attr_value:
            raise BadRequest('attr_value parameter is empty')
        if not valid_resource_type:
            raise BadRequest('valid_resource_type parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')

        is_required = False
        cardinality     = self._get_item_value(attr_spec,'Cardinality')
        if cardinality:
            if not self.valid_cardinality(cardinality):
                raise BadRequest('Invalid cardinality (%s)' % (cardinality))
            if '1.' in cardinality:
                is_required = True

        name = self._get_item_value(attr_spec,'id')
        value_constraints_value = self._get_item_value(attr_spec,'value_constraints')
        value_pattern           = self._get_item_value(attr_spec,'value_pattern')
        if not value_pattern:
            raise BadRequest('value pattern empty')
        default_value = self._get_item_value(attr_spec,'default_value')
        uom           = self._get_item_value(attr_spec,'uom')
        attr_label    = self._get_item_value(attr_spec,'attr_label')
        value_type    = self._get_item_value(attr_spec,'value_type')
        if value_type:
            if self.valid_value_type(value_type) == False:
                raise BadRequest('invalid value_type (%s)' % value_type)
        else:
            raise BadRequest('unknown value_type (%s)' % value_type)

        # CodeValue processing
        if value_type == 'CodeValue':
            # value_constraints == set=MAM:asset type, where syntax command=codespace_name:codeset_name
            # parse value_constraints to get codeset name; get codeset and check if attr_value in codeset.enumeration

            if not value_constraints_value:
                raise BadRequest('CodeValue requires Value Constraints value identifying an associated CodeSet')

            # process command parts - command and command value (i.e. codeset name)
            commands = self._get_command_value(value_type, value_constraints_value)
            if commands:
                if len(commands) != 1:
                    raise BadRequest('one and only one command expected for CodeValue constraints')
            else:
                raise BadRequest('set command required to defined codespace and codeset for this CodeValue')

            cmds = commands[0]
            cmd = cmds[0][:]
            codeset_name = cmds[1][:]
            if not codeset_name:
                raise BadRequest('codeset name required; check set command for codeset name')

            if value_pattern:
                regular_expression = ''.join(r'%s' % (value_pattern))
                #p = re.compile(regular_expression)
                m = re.match(regular_expression, codeset_name )
                #m = p.match(codeset_name )
                if not m:
                    raise BadRequest('attribute (%s) has invalid codeset name (%s)' % (name, codeset_name))
            # Read CodeSpace once, get codes and codesets
            codes = {}
            codesets = {}
            code_space = self.read_code_space(code_space_id)
            if not code_space:
                raise NotFound('code_space (id=%s) not found' % code_space_id)
            if code_space.codes:
                codes = code_space.codes
            if code_space.codesets:
                codesets = code_space.codesets
            if not codes:
                raise BadRequest('code space does not have Codes defined')
            if not codesets:
                raise BadRequest('code space does not have CodeSets defined')

            # verify codeset available in codesets and attr_value in codeset enumeration
            if codeset_name in codesets:
                some_codeset = codesets[codeset_name]
                enumeration = some_codeset['enumeration']
                if attr_value not in enumeration:
                    raise BadRequest('Attribute value (%s) not in valid Value Constraints for CodeValue \'%s\'' % (attr_value, attr_label))
            else:
                raise BadRequest('value_constraints codeset value (%s) not in codesets' % codeset_name)


        # StringValue processing
        elif value_type == 'StringValue':
            values = self.get_list_csv(attr_value)
            for value in values:
                # parse according to value_pattern
                if value_pattern:
                    regular_expression = ''.join(r'%s' % (value_pattern))
                    m = re.match(regular_expression, value )
                    if not m:
                        raise BadRequest('attribute (%s) has invalid %s' % (name, value_type))

                # constraints applied (?)


        # DateValue processing
        elif value_type == 'DateValue':
            values = self.get_list_csv(attr_value)
            for value in values:
                # parse
                if value_pattern:
                    regular_expression = ''.join(r'%s' % (value_pattern))
                    m = re.match(regular_expression, value )
                    if not m:
                        raise BadRequest('attribute (%s) has invalid %s' % (name, value_type))

                # range check month, day, year (todo)

        # TimeValue processing (todo implement)
        elif value_type == 'TimeValue':
            values = self.get_list_csv(attr_value)
            for value in values:
                # parse
                if value_pattern:
                    regular_expression = ''.join(r'%s' % (value_pattern))
                    m = re.match(regular_expression, value )
                    if not m:
                        raise BadRequest('attribute %s has invalid %s' % (name, value_type))
                # range check hours and minutes (24 hour clock)

        # RealValue processing
        elif value_type == 'RealValue':
            values = self.get_list_csv(attr_value)
            for value in values:

                # Type check, parse with pattern_value
                rvalue = None
                if self.isfloat(value):
                    rvalue = float(value)
                else:
                    raise Inconsistent('attribute (%s) invalid value \'%s\'; expected %s ' % (name, value, value_type))

                if value_pattern:
                    regular_expression = ''.join(r'%s' % (value_pattern))
                    m = re.match(regular_expression, value)
                    if not m:
                        raise BadRequest('attribute (%s) has invalid %s' % (name, value_type))

                if value_constraints_value:
                    # Parse commands (if provided, commands 'min' and optional 'max')
                    commands = self._get_command_value(value_type, value_constraints_value)

                    # get range values if provided, compare attribute range values
                    min_value = ''
                    max_value = ''
                    if commands:
                        for command in commands:
                            cmd = command[0]
                            cmd_value = command[1]
                            if cmd == 'min':
                                min_value = cmd_value
                            elif cmd == 'max':
                                max_value = cmd_value
                        rmin = None
                        rmax = None
                        if min_value:
                            rmin = float(min_value)
                            if rvalue < rmin:
                                raise BadRequest('attribute (%s) value less than minimum Value Constraints (%s)' % (name, str(rmin)))
                        if max_value:
                            rmax = float(max_value)
                            if rvalue > rmax:
                                raise BadRequest('attribute (%s) value greater than maximum Value Constraints (%s)' % (name, str(rmax)))

        # IntegerValue processing
        elif value_type == 'IntegerValue':
            values = self.get_list_csv(attr_value)
            for value in values:
                # parse
                if self.isint(value):
                    rvalue = int(value)
                    val = self.create_value(rvalue)
                else:
                    raise Inconsistent('attribute (%s) invalid value \'%s\'; expected %s ' % (name, value, value_type))

                if value_pattern:
                    regular_expression = ''.join(r'%s' % (value_pattern))
                    m = re.match(regular_expression, value)
                    if not m:
                        raise BadRequest('attribute (%s) has invalid %s' % (name, value_type))

                if value_constraints_value:
                    # Parse commands (if provided, commands 'min' and optional 'max')
                    commands = self._get_command_value(value_type, value_constraints_value)
                    # get range values if provided, compare attribute range values
                    min_value = ''
                    max_value = ''
                    if commands:
                        for command in commands:
                            cmd = command[0]
                            cmd_value = command[1]
                            if cmd == 'min':
                                min_value = cmd_value
                            elif cmd == 'max':
                                max_value = cmd_value
                        rmin = None
                        rmax = None
                        if min_value:
                            rmin = float(min_value)
                            if rvalue < rmin:
                                raise BadRequest('attribute (%s) value less than minimum Value Constraints (%s)' % (name, str(rmin)))
                        if max_value:
                            rmax = float(max_value)
                            if rvalue > rmax:
                                raise BadRequest('attribute (%s) value greater than maximum Value Constraints (%s)' % (name, str(rmax)))


        # Boolean processing (todo unit test; not actually used in spread sheets)
        elif value_type == 'Boolean':
            values = self.get_list_csv(attr_value)
            for value in values:
                # parse
                if self.isbool(value):                  # TODO Test boolean values
                    bvalue = True
                else:
                    raise Inconsistent('invalid value \'%s\'; expected Boolean ' % value)

        else:
            raise BadRequest('Unknown Value Type value (%s)' % value_type)

        """
        # todo ResourceReferenceValue
        # ResourceReferenceValue processing
        elif value_type == 'ResourceReferenceValue':
            #log.debug('\n\n[service] validate ResourceReferenceValue')
        """

        return


    def _get_attribute_value_as_list(self, attribute):
        if not attribute:
            raise BadRequest('attribute parameter is empty')

        return_value = []
        for item in attribute:
            attr_value = []
            if 'value' not in item:
                raise BadRequest('value list item does not contain value attribute')

            attr_value = item['value']
            if attr_value:
                return_value.append(attr_value)

        return return_value


    def _valid_attribute_value(self, attr_spec, attr_value, valid_resource_type, code_space_id):

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # given an attribute value determine if it is valid based on AttributeSpecification provided;
        # if not valid throw
        #
        #   valid_value_types = [   'IntegerValue', 'RealValue', 'Boolean', 'StringValue',
        #                           'CodeValue', 'DateValue', 'TimeValue', 'DateTimeValue', 'ResourceReferenceValue'],
        #                       ]
        #
        # For a given attr_value:
        #   Process according to value_type
        #   1. Verify valid value_type
        #   2. Verify valid cardinality; determine if required value
        #   3. For each kind of value type:
        #       a.    Process attr_value against value_pattern (parse)
        #       b.    Process attr_value against value_constraints (commands: set, min, max)
        #       c.    Process attr_value against uom (todo)
        #       d.    is_required? (todo)
        #       e.    default value (todo)
        #       f.    cardinality (todo)
        #
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if not attr_spec:
            raise BadRequest('attr_spec parameter is empty')
        if not attr_value:
            raise BadRequest('attr_value parameter is empty')
        if not valid_resource_type:
            raise BadRequest('valid_resource_type parameter is empty')

        try:
            is_required = False
            cardinality     = self._get_item_value(attr_spec,'Cardinality')
            if cardinality:
                if not self.valid_cardinality(cardinality):
                    raise BadRequest('Invalid cardinality (%s)' % (cardinality))
                if '1.' in cardinality:
                    is_required = True

            name = self._get_item_value(attr_spec,'id')
            value_constraints_value = self._get_item_value(attr_spec,'value_constraints')
            value_pattern           = self._get_item_value(attr_spec,'value_pattern')
            if not value_pattern:
                raise BadRequest('value pattern empty')
            default_value = self._get_item_value(attr_spec,'default_value')
            uom           = self._get_item_value(attr_spec,'uom')
            attr_label    = self._get_item_value(attr_spec,'attr_label')
            value_type    = self._get_item_value(attr_spec,'value_type')
            if value_type:
                if self.valid_value_type(value_type) == False:
                    raise BadRequest('invalid value_type (%s)' % value_type)
            else:
                raise BadRequest('unknown value_type (%s)' % value_type)

            # CodeValue processing
            if value_type == 'CodeValue':
                # value_constraints == set=MAM:asset type, where syntax command=codespace_name:codeset_name
                # parse value_constraints to get codeset name; get codeset and check if attr_value in codeset.enumeration
                if not code_space_id:
                    code_space_id = self._get_code_space_id('MAM')      #arg
                    if not code_space_id:
                        raise BadRequest('unable to determine code space id for code_space')

                values = self._get_attribute_value_as_list(attr_value)
                if len(values) != 1:
                    raise BadRequest('one and only one code value from enumeration is supported')
                enum_value = values[0]

                if not value_constraints_value:
                    raise BadRequest('CodeValue requires Value Constraints value identifying an associated CodeSet')

                # process command parts - command and command value (i.e. codeset name)
                commands = self._get_command_value(value_type, value_constraints_value)
                if commands:
                    if len(commands) != 1:
                        raise BadRequest('one and only one command expected for CodeValue constraints')
                else:
                    raise BadRequest('set command required to defined codespace and codeset for this CodeValue')

                cmds = commands[0]
                cmd = cmds[0][:]
                codeset_name = cmds[1][:]
                if not codeset_name:
                    raise BadRequest('codeset name required; check set command for codeset name')

                if value_pattern:
                    regular_expression = ''.join(r'%s' % (value_pattern))
                    m = re.match(regular_expression, codeset_name )
                    if not m:
                        raise BadRequest('attribute (%s) has invalid codeset name (%s)' % (name, codeset_name))

                # Read CodeSpace once, get codes and codesets
                codes = {}
                codesets = {}
                code_space = self.read_code_space(code_space_id)
                if not code_space:
                    raise NotFound('code_space (id=%s) not found' % code_space_id)
                if code_space.codes:
                    codes = code_space.codes
                if code_space.codesets:
                    codesets = code_space.codesets
                if not codes:
                    raise BadRequest('code space does not have Codes defined')
                if not codesets:
                    raise BadRequest('code space does not have CodeSets defined')

                # verify codeset available in codesets and attr_value in codeset enumeration
                if codeset_name in codesets:
                    some_codeset = codesets[codeset_name]
                    enumeration = some_codeset['enumeration']
                    if enum_value not in enumeration:
                        raise BadRequest('Attribute value (%s) not in valid Value Constraints for CodeValue \'%s\'' % (attr_value, attr_label))
                else:
                    raise BadRequest('value_constraints codeset value (%s) not in codesets' % codeset_name)


            # StringValue processing
            elif value_type == 'StringValue':
                values = self._get_attribute_value_as_list(attr_value)
                for value in values:
                    # parse according to value_pattern
                    if value_pattern:
                        regular_expression = ''.join(r'%s' % (value_pattern))
                        m = re.match(regular_expression, value )
                        if not m:
                            raise BadRequest('attribute (%s) has invalid %s' % (name, value_type))

                    # constraints applied (?)

            # DateValue processing
            elif value_type == 'DateValue':
                values = self._get_attribute_value_as_list(attr_value)
                for value in values:
                    if value_pattern:
                        regular_expression = ''.join(r'%s' % (value_pattern))
                        m = re.match(regular_expression, value )
                        if not m:
                            raise BadRequest('attribute (%s) has invalid %s' % (name, value_type))

                    # range check month, day, year (todo)

            # TimeValue processing (todo implement)
            elif value_type == 'TimeValue':
                values = self._get_attribute_value_as_list(attr_value)
                for value in values:
                    if value_pattern:
                        regular_expression = ''.join(r'%s' % (value_pattern))
                        m = re.match(regular_expression, value )
                        if not m:
                            raise BadRequest('attribute %s has invalid %s' % (name, value_type))
                    # range hour and minutes (todo)

            # RealValue processing
            elif value_type == 'RealValue':
                values = self._get_attribute_value_as_list(attr_value)
                for value in values:
                    # Type check, parse with pattern_value
                    rvalue = None
                    if self.isfloat(value):
                        rvalue = float(value)
                    else:
                        raise Inconsistent('attribute (%s) invalid value \'%s\'; expected %s ' % (name, value, value_type))

                    if value_pattern:
                        # todo re adress this
                        """
                        regular_expression = ''.join(r'%s' % (value_pattern))

                        if regular_expression:
                            m = re.match(regular_expression, value)
                            if not m:
                                raise BadRequest('attribute (%s) has invalid %s' % (name, value_type))
                        """

                    if value_constraints_value:
                        # Parse commands (if provided, commands 'min' and optional 'max')
                        commands = self._get_command_value(value_type, value_constraints_value)

                        # get range values if provided, compare attribute range values
                        min_value = ''
                        max_value = ''
                        if commands:
                            for command in commands:
                                cmd = command[0]
                                cmd_value = command[1]
                                if cmd == 'min':
                                    min_value = cmd_value
                                elif cmd == 'max':
                                    max_value = cmd_value
                            rmin = None
                            rmax = None
                            if min_value:
                                rmin = float(min_value)
                                if rvalue < rmin:
                                    raise BadRequest('attribute (%s) value less than minimum Value Constraints (%s)' % (name, str(rmin)))
                            if max_value:
                                rmax = float(max_value)
                                if rvalue > rmax:
                                    raise BadRequest('attribute (%s) value greater than maximum Value Constraints (%s)' % (name, str(rmax)))


            # IntegerValue processing
            elif value_type == 'IntegerValue':
                values = self._get_attribute_value_as_list(attr_value)
                for value in values:
                    # parse
                    if self.isint(value):
                        rvalue = int(value)
                        val = self.create_value(rvalue)
                    else:
                        raise Inconsistent('attribute (%s) invalid value \'%s\'; expected %s ' % (name, value, value_type))

                    if value_pattern:
                        regular_expression = ''.join(r'%s' % (value_pattern))
                        m = re.match(regular_expression, value)
                        if not m:
                            raise BadRequest('attribute (%s) has invalid %s' % (name, value_type))

                    if value_constraints_value:
                        # Parse commands (if provided, commands 'min' and optional 'max')
                        commands = self._get_command_value(value_type, value_constraints_value)
                        # get range values if provided, compare attribute range values
                        min_value = ''
                        max_value = ''
                        if commands:
                            for command in commands:
                                cmd = command[0]
                                cmd_value = command[1]
                                if cmd == 'min':
                                    min_value = cmd_value
                                elif cmd == 'max':
                                    max_value = cmd_value
                            rmin = None
                            rmax = None
                            if min_value:
                                rmin = float(min_value)
                                if rvalue < rmin:
                                    raise BadRequest('attribute (%s) value less than minimum Value Constraints (%s)' % (name, str(rmin)))
                            if max_value:
                                rmax = float(max_value)
                                if rvalue > rmax:
                                    raise BadRequest('attribute (%s) value greater than maximum Value Constraints (%s)' % (name, str(rmax)))


            # Boolean processing (todo unit test; not actually used in spread sheets)
            elif value_type == 'Boolean':
                values = self.get_list_csv(attr_value)
                for value in values:
                    # parse
                    if self.isbool(value):                  # TODO Test boolean values
                        bvalue = True
                    else:
                        raise Inconsistent('invalid value \'%s\'; expected Boolean ' % value)

            else:
                raise BadRequest('Unknown Value Type value (%s)' % value_type)

            """
            # todo ResourceReferenceValue
            # ResourceReferenceValue processing
            elif value_type == 'ResourceReferenceValue':
            """

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except NotFound, Arguments:
            raise NotFound(Arguments.get_error_message())
        except Inconsistent, Arguments:
            raise Inconsistent(Arguments.get_error_message())

        except:
            raise BadRequest('Failed to annotate one or more working objects.')

        return


    def _process_attribute_value(self, attr_spec, attr_value):

        #, valid_resource_type, valid_cardinality, valid_value_types,
        # only used for code value: codes, codesets):

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # given an attribute value determine if it is valid based on AttributeSpecification provided
        #
        # Removed 'ObjectValue', 'ObjectReferenceValue',
        valid_value_types = [ 'IntegerValue', 'RealValue', 'Boolean', 'StringValue',
                            'CodeValue', 'DateValue', 'TimeValue', 'DateTimeValue']
        # todo 'ResourceReferenceValue'

        #valid_type_resources = [RT.AssetType, RT.EventDurationType]
        #valid_resources = [RT.Asset, RT.EventDuration]
        #valid_cardinality = ['1..1', '0..1', '1..N', '0..N']
        #const_code_space_name = "MAM"
        # todo constraints for all
        # Boolean todo/unit test

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        if not attr_spec:
            raise BadRequest('attr_spec parameter is empty')
        if not attr_value:
            raise BadRequest('attr_value parameter is empty')

        is_required = False
        if attr_spec['cardinality']:
            cardinality = attr_spec['cardinality']
            cardinality = cardinality.strip()
            if '1.' in cardinality: is_required = True

        value_constraints_value = ''
        if attr_spec['value_constraints']:
            value_constraints_value = attr_spec['value_constraints']
            value_constraints_value = value_constraints_value.strip()

        value_pattern = ''
        if attr_spec['value_pattern']:
            value_pattern = attr_spec['value_pattern']
            value_pattern = value_pattern.strip()

        default_value = ''
        if attr_spec['default_value']:
            default_value = attr_spec['default_value']
            default_value = default_value.strip()

        uom = ''
        if attr_spec['uom']:
            uom = attr_spec['uom']
            uom = uom.strip()

        attr_label = ''
        if attr_spec['attr_label']:
            attr_label = attr_spec['attr_label']
            attr_label = attr_label.strip()

        value_type = ''
        if attr_spec['value_type']:
            valid = True
            value_type = attr_spec['value_type']
            value_type = value_type.strip()
            if value_type not in valid_value_types:
                raise BadRequest('value_type (%s) not a valid value type' % value_type)

            else:  # value_type in valid_value_types:

                vals = []
                # CodeValue processing
                if value_type == 'CodeValue':
                    # value_constraints == codeset name; get codeset and check if attr_value in CodeSet.enumeration
                    val = self.create_complex_value('CodeValue', attr_value[0],)
                    vals.append(val)

                # IntegerValue processing
                elif value_type == 'IntegerValue':
                    for value in attr_value:
                        if self.isint(value):
                            ivalue = int(value)
                            val = self.create_value(ivalue)
                            vals.append(val)
                        else:
                            raise Inconsistent('invalid value \'%s\'; expected IntegerValue ' % value)

                # Boolean processing
                elif value_type == 'Boolean':
                    for value in attr_value:
                        # parse
                        if self.isbool(value):
                            bvalue = bool(value)
                            val = self.create_value(bvalue)
                            vals.append(val)
                        else:
                            raise Inconsistent('invalid value \'%s\'; expected Boolean ' % value)

                # StringValue processing
                elif value_type == 'StringValue':
                    # Sample StringValue: values: [StringValue({, 'value': 'need a bigger boat'}), StringValue({, 'value': 'hello world'})]
                    for value in attr_value:
                        val = self.create_value(value)
                        vals.append(val)

                # DateValue processing
                elif value_type == 'DateValue':
                    for value in attr_value:
                        val = self.create_complex_value('DateValue', value)
                        vals.append(val)

                # DateValue processing
                elif value_type == 'TimeValue':
                    for value in attr_value:
                        val = self.create_complex_value('TimeValue', value)
                        vals.append(val)

                # RealValue processing
                elif value_type == 'RealValue':
                    for value in attr_value:
                        if self.isfloat(value):
                            rvalue = float(value)
                            val = self.create_value(rvalue)
                            vals.append(val)
                        else:
                            raise Inconsistent('invalid value \'%s\'; expected RealValue ' % value)

                else:
                    raise BadRequest('Unknown Value Type value (%s)' % value_type)

                """
                # ResourceReferenceValue processing
                elif value_type == 'ResourceReferenceValue':
                """

        return  vals




    #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # Create resources (types resources and marine resources) from working objects.
    #
    # TypeResource(s):      (CRUD)
    #   _create_type_resource(wo_resource, valid_types)
    #   _update_type_resource(wo_resource, valid_types)
    #   _remove_type_resource(wo_resource, valid_types)
    #
    # Marine Resource(s):   (CRUD)
    #   _create_marine_resource(wo_resource, valid_types)
    #   _update_marine_resource(wo_resource, valid_types)
    #   _remove_marine resource(wo_resource, valid_types)
    #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # TypeResources: _create_type_resource, _update_type_resource, _remove_type_resource
    #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _create_type_resource(self, wo_resource, valid_types):
        """ Create TypeResource instance (one of AssetType or EventDurationType).

        @param  wo_resource     {}      # working object
        @param  valid_types     []      # list of valid type resources
        @retval resource_id     str     # sys uuid (if successful; '' is not
        @throws BadRequest      wo_resource parameter is empty
        @throws BadRequest      valid_types parameter is empty
        @throws BadRequest      Unknown (%s) and an invalid TypeResource (not one of %s)
        """
        # todo enum processing, handle not _attribute_specifications
        if not wo_resource:
            raise BadRequest('wo_resource parameter is empty')
        if not valid_types:
            raise BadRequest('valid_types parameter is empty')

        resource_id = ''
        namespace_prefix = ''
        name = self._get_item_value(wo_resource, 'id')
        description = self._get_item_value(wo_resource, 'description')
        specific_type = self._get_item_value(wo_resource, 'type')
        if specific_type in valid_types:
            namespace_prefix = specific_type + ":"
        else:
            raise BadRequest('Unknown (%s) and an invalid TypeResource (not one of %s)' % (specific_type, valid_types))

        nickname = ''
        altid = self._get_item_value(wo_resource, 'altid')
        if altid and namespace_prefix:
            nickname = namespace_prefix +  altid

        ion_asset_type = IonObject(specific_type, name=name, description=description)

        # Create attribute specification(s)
        some_attribute_specifications = {}
        if wo_resource['_attribute_specifications']:
            attr_specs = wo_resource['_attribute_specifications']
            for s in attr_specs:
                sobj = attr_specs[s]
                attr_type_obj = IonObject(OT.AttributeSpecification,
                          id=sobj['id'],
                          description   =sobj['description'],
                          value_type    =sobj['value_type'],
                          group_label   =sobj['group_label'],
                          attr_label    =sobj['attr_label'],
                          rank          =sobj['rank'],
                          visibility    =sobj['visibility'],
                          value_constraints   =sobj['value_constraints'],
                          default_value =sobj['default_value'],
                          uom           =sobj['uom'],
                          value_pattern=sobj['value_pattern'],
                          cardinality   =sobj['cardinality'],
                          editable      =sobj['editable'],
                          journal       =sobj['journal'],
                          _source_id    =sobj['_source_id'])

                some_attribute_specifications[attr_type_obj['id']] = attr_type_obj

            ion_asset_type.alt_ids.append(nickname)
            bconcrete = False
            if wo_resource['concrete']:
                if wo_resource['concrete'].upper() == 'TRUE':
                    bconcrete = True
            ion_asset_type.concrete = bconcrete
            ion_asset_type.attribute_specifications = some_attribute_specifications

            if specific_type == RT.AssetType:
                asset_type_id = self.create_asset_type(ion_asset_type)
                wo_resource['_id'] = asset_type_id
                resource_id = asset_type_id

            # hook enum
            elif specific_type == RT.EventDurationType:
                if bconcrete == True:
                    event_category = self._get_item_value(wo_resource, 'event_category')
                    if not event_category:
                        raise BadRequest('all concrete type resources shall define an event category.')
                    if event_category   == EventCategoryEnum._str_map[EventCategoryEnum.Location]:
                        event_enum = EventCategoryEnum.Location
                    elif event_category == EventCategoryEnum._str_map[EventCategoryEnum.Operability]:
                        event_enum = EventCategoryEnum.Operability
                    elif event_category == EventCategoryEnum._str_map[EventCategoryEnum.Verification]:
                        event_enum = EventCategoryEnum.Verification
                    elif event_category == EventCategoryEnum._str_map[EventCategoryEnum.Assembly]:
                        event_enum = EventCategoryEnum.Assembly
                    else:
                        raise BadRequest('unknown event category(%s) for concrete type resource' % event_category)

                    ion_asset_type.event_category = event_enum

                asset_type_id = self.create_event_duration_type(ion_asset_type)
                wo_resource['_id'] = asset_type_id
                resource_id = asset_type_id

        #else:
            #log.debug('\n\n TypeResource %s does not have _attribute_specifications!', altid)
            # todo this should be a raise?

        return resource_id


    def _update_type_resource(self, wo_resource, valid_types):
        """ Update TypeResource instance (one of AssetType or EventDurationType).

        @param  wo_resource     {}      # working object
        @param  valid_types     []      # list of valid type resources
        @retval resource_id     str     # sys uuid (if successful; '' is not
        @throws BadRequest      wo_resource parameter is empty
        @throws BadRequest      valid_types parameter is empty
        @throws BadRequest      Unknown (%s) and an invalid TypeResource (not one of %s)
        @throws BadRequest      working object (of TypeResource) type value is empty
        @throws BadRequest      Unknown id for type resource %s'
        """
        # todo try/except
        if not wo_resource:
            raise BadRequest('wo_resource parameter is empty')
        if not valid_types:
            raise BadRequest('valid_types parameter is empty')

        namespace_prefix = ''
        name = self._get_item_value(wo_resource, 'id')
        description = self._get_item_value(wo_resource, 'description')
        specific_type = self._get_item_value(wo_resource, 'type')
        if specific_type in valid_types:
            namespace_prefix = specific_type + ":"
        else:
            raise BadRequest('Unknown (%s) and an invalid TypeResource (not one of %s)' % (specific_type, valid_types))

        nickname = ''
        altid = self._get_item_value(wo_resource, 'altid')
        if altid and namespace_prefix:
            nickname = namespace_prefix +  altid

        resource_id = self._get_item_value(wo_resource, '_id')
        if not resource_id:
            raise BadRequest('Unknown id for type resource %s' % name)

        # handle/discuss Org updates todo
        #org_ids = ''
        #if wo_resource['org_altids']:
        #    org_ids = wo_resource['org_altids']

        nickname = ''
        altid = self._get_item_value(wo_resource, 'altid')
        if altid and namespace_prefix:
            nickname = namespace_prefix +  altid

        # UPDATE -- instance exists, update instance
        # Compare existing values to wo_resource, if different set to new value and dirty = True
        # todo consider wrapper read_type_resource(res_id, res_type);
        # todo same with update_type_resource(res_id, res_type)
        # todo try/throw for read which fails
        attribute_specifications = {}
        asset_type = None
        if specific_type == RT.AssetType:
            asset_type = self.read_asset_type(resource_id)
        elif specific_type == RT.EventDurationType:
            asset_type = self.read_event_duration_type(resource_id)

        if not asset_type:
            raise BadRequest('failed to read type resource (id=\'%s\')' % resource_id)
        if asset_type.attribute_specifications:
            attribute_specifications = asset_type.attribute_specifications
        else:
            raise BadRequest('current instance of type resource does not have AttributeSpecifications')
        # Review every field of a TypeResource and address here. . . . . . . TODO
        # If delta from current rev, update and set dirty flag
        dirty = False

        # Modify alt_ids? TODO - Review this; if already has altid...
        if nickname not in asset_type.alt_ids:
            asset_type.alt_ids.append(nickname)
            dirty = True

        # Modify description?
        if asset_type.description:
            if asset_type.description != description:
                asset_type.description = description
                dirty = True

        # TODO here we are dealing with AttributeSpecifications
        # TODO Compare existing instance of AttributeSpecification and compare each specification
        # TODO item to determine if modification; authorize/validate modification and set dirty flag
        # AttributeSpecification updates - one by one and field by field
        # (list below for attributes (not here)! move this list)
        # Update of attribute to new value requires new value for attribute to be
        # validated against AttributeSpecification
        # test case - receive all attributes with only some changes (partial update)
        # test case - receive all attributes no changes
        # test case - some attributes and some changes (partial update)
        # test case - some attributes and no changes (state of attrs after change)

        ref_AttributeSpecification = IonObject(OT.AttributeSpecification)
        if wo_resource['_attribute_specifications']:
            attr_specs = wo_resource['_attribute_specifications']
            if not attr_specs:
                raise BadRequest('working object provided does not have attribute_specifications')

        # Compare existing instance attribute_specifications to proposed working objects
        # are all items in current attribute_specifications in new attribute_specifications?
        # are there new items? are there less items?
        same_attribute_specification_names  = True
        equal_attribute_specification_names = False     # len(attribute_specifications)==len(attr_specs)
        more_attribute_specification_names = False      # len(attribute_specifications)> len(attr_specs) ***
        less_attribute_specification_names = False      # len(attribute_specifications)< len(attr_specs) ***
        if len(attribute_specifications) == len(attr_specs):
            equal_attribute_specification_names = True
            # verify same AttributeSpecification names in both
            for current_name in attribute_specifications:
                if current_name not in attr_specs:
                    same_attribute_specification_names = False
                    break
        else:
            # if the number of AttributeSpecification objects in working object is LESS THAN the number of
            # AttributeSpecification objects in existing instance . . .
            # first, determine if all of the working object AttributeSpecification objects have counterpart
            # with same name in existing instance (i.e. is this update for subset of existing specs)
            # if yes, well - do what needs done: update AttributeSpecification objects and reflect in instance. ***
            # *** what do we do about other tracking resource instances which use the current type resource instance?
            # if no, this means we have a new AttributeSpecification not currently avaiable in existing instance;
            # and if we add this new AttributeSpecification on update any existing instances using this type resource
            # will fail validation unless they also add a corresponding attribute for this new AttributeSpecification.
            # *** (i.e. a problem here...)
            if len(attribute_specifications) > len(attr_specs):
                more_attribute_specification_names = True
                # verify same AttributeSpecification names in both
                for name in attr_specs:
                    if name not in attribute_specifications:
                        same_attribute_specification_names = False
                        break
            else:
                less_attribute_specification_names = True
                # verify all names in existing type resource instance are available in proposed working object attribute_specifications
                for current_name in attribute_specifications:
                    if current_name not in attr_specs:
                        same_attribute_specification_names = False
                        break

        # Compile dictionary of AttributeSpecification objects for the update
        some_attribute_specifications = {}

        # if any AttributeSpecification(s) have changed (when compared to current instance), set to true
        count = 0
        attribute_specifications_is_dirty = False
        for s in attr_specs:
            count += 1
            # get current instance specification
            add_new_attribute_specification = False
            if s in attribute_specifications:
                current_spec = attribute_specifications[s]
                if not current_spec:
                    raise BadRequest('unable to access AttributeSpecification item %s in current type instance' % s)
            else:
                add_new_attribute_specification = True

            # get wo specification
            obj = attr_specs[s]

            # initialize AttributeSpecification object for storing values during update
            new_spec_obj = IonObject(OT.AttributeSpecification)
            spec_dirty = False
            if add_new_attribute_specification:
                # new AttributeSpecification in working object which does not exist in current type resource instance
                # (i.e. new row added to attribute specification spread sheet for an existing type resource)
                for item in ref_AttributeSpecification.__dict__:
                    if item != 'type_':
                        new_spec_obj[item] = obj[item]
                    spec_dirty = True
            else:
                # check each element for difference, if different take update and set dirty
                # 'id' represents the name of the AttributeSpecification - review changing it on update (TODO)
                for item in ref_AttributeSpecification.__dict__:
                    if item != 'type_':
                        if obj[item] != current_spec[item]:
                            new_spec_obj[item] = obj[item]
                            spec_dirty = True
                        else:
                            new_spec_obj[item] = current_spec[item]

            if spec_dirty:
                some_attribute_specifications[new_spec_obj['id']] = new_spec_obj
                attribute_specifications_is_dirty = True
            else:
                some_attribute_specifications[current_spec['id']] = current_spec

        # completed processing of each AttributeSpecification item for this attr_spec
        if attribute_specifications_is_dirty:
            asset_type.attribute_specifications = some_attribute_specifications
            dirty = True

        # Update if required (dirty)
        if dirty:
            if specific_type == RT.AssetType:
                self.update_asset_type(asset_type)

            elif specific_type == RT.EventDurationType:
                self.update_event_duration_type(asset_type)

        return resource_id

    def _remove_type_resource(self, wo_resource, valid_types):

        # for valid TypeResources remove resource.
        # TODO
        # discuss governance, rules for deletion of TypeResource given system state, user permissions, etc.
        # discuss deletion when TypeResource not used by other system resource
        # discuss deletion when TypeResource is used in an extend hierarchy
        # discuss deletion when TypeResource is used by marine resource
        # add try/except for NotFound todo
        verbose = False
        if not wo_resource:
            raise BadRequest('wo_resource parameter is empty')
        if not valid_types:
            raise BadRequest('valid_types parameter is empty')

        resource_id = ''
        if wo_resource['_id']:
            resource_id = wo_resource['_id']
            if not resource_id:
                raise BadRequest('_remove_type_resource could not produce id from working object')

        if wo_resource['type']:
            specific_resource_type = wo_resource['type']
            if specific_resource_type:
                if specific_resource_type not in valid_types:
                    raise BadRequest('Unknown (%s) and an invalid TypeResource (not one of %s)' % (specific_resource_type, valid_types))
        else:
            raise BadRequest('working object (of TypeResource) type is empty')

        if specific_resource_type == RT.AssetType:
            self.delete_asset_type(resource_id)                     # add try/throw NotFound
        elif specific_resource_type == RT.EventDurationType:
            self.delete_event_duration_type(resource_id)            # add try/throw NotFound


        return resource_id

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Marine Resource(s):   (CRUD)
    #   _create_marine_resource(wo_resource, valid_types)
    #   _update_marine_resource(wo_resource, valid_types)
    #   _remove_marine resource(wo_resource, valid_types)
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _create_marine_resource(self, wo_resource, valid_types):
        """ Create resource instance (one of Asset or EventDuration).

        @param  wo_resource     {}      # working object
        @param  valid_types     []      # list of valid type resources
        @retval resource_id     str     # sys uuid (if successful; '' is not
        @throws BadRequest      wo_resource parameter is empty
        @throws BadRequest      valid_types parameter is empty
        @throws BadRequest      Unknown (%s) and an invalid TypeResource (not one of %s)
        @throws BadRequest      working object type value is empty
        @throws BadRequest      marine resource (%s) altid is empty
        @throws BadRequest      type resource (%s) not found; unable to process update
        @throws BadRequest      this attribute (%s) not defined in type resource (%s) attribute_specifications
        @throws BadRequest      Request to process marine tracking resource of unknown or invalid type (%s)
        """
        # todo general work/cleanup including try/except
        if not wo_resource:
            raise BadRequest('wo_resource parameter is empty')
        if not valid_types:
            raise BadRequest('valid_types parameter is empty')

        resource_id = ''
        name = ''
        if wo_resource['name']:
            name = wo_resource['name']

        description = ''
        if wo_resource['description']:
            description = wo_resource['description']

        nicknameResource = ''
        if wo_resource['type']:
            specific_resource_type = wo_resource['type']
            if specific_resource_type:
                if specific_resource_type in valid_types:
                    nicknameResource = specific_resource_type + ":"
                else:
                    raise BadRequest('Unknown (%s) and an invalid Resource (not one of %s)' % (specific_resource_type, valid_types))
        else:
            raise BadRequest('working object type value is empty')

        altid = ''
        nickname = ''
        if wo_resource['altid']:
            altid = wo_resource['altid']
            nickname = nicknameResource +  altid
        else:
            raise BadRequest('marine resource (%s) altid is empty' % specific_resource_type )

        # Check (before creating new resource) that in fact this is a unique altid...
        if altid and specific_resource_type:
            key = self._get_resource_key_by_name(altid, specific_resource_type)
            if key:
                raise BadRequest('a resource instance exists (type %s) with this altid: %s' % (specific_resource_type, altid))

        # Prepare to validate - must have attribute specification for this resource, read type (wo or instance)
        # wo_resource keys on CREATE:
        # ['implements', '_implements_type', '_scoped_names', 'name', 'altid', 'org_altids',
        # '_pattribute_keys', '_attributes', 'concrete', '_exists', '_pattribute_names', 'action',
        # '_implements_id', '_scoped_keys', '_id', 'type', 'description']
        #
        # wo_resource keys on UPDATE:
        # ['implements', '_exists', '_scoped_names', 'name',  'altid', 'org_altids',
        # '_pattribute_keys', '_attributes', 'concrete', '_implements',
        # '_pattribute_names', 'action', '_implements_id', '_scoped_keys', '_id', 'type', 'description']

        # On create this isn't available (_implements_id) do look up here;
        # note - is available during an update
        implements = ''
        if wo_resource['implements']:
            implements = wo_resource['implements']

        _implements = ''
        if wo_resource['implements']:
            _implements = wo_resource['implements']

        type = ''
        _implements_type = ''
        namespace = ''
        if wo_resource['type']:
            type = wo_resource['type']
            if type == RT.EventDuration:
                _implements_type = RT.EventDurationType
            elif type == RT.Asset:
                _implements_type = RT.AssetType

        type_resource = ''
        if implements and _implements_type:
            type_resource = self._get_type_resource_by_name(implements, _implements_type)
        if not type_resource:
            raise NotFound('type resource (%s) not found; unable to process update' % implements)

        # Obtain type resource AttributeSpecifications for processing (parse, validate and constrain) attributes
        # wo type resource
        wo_attr_specs = {}
        if type_resource.attribute_specifications:
            wo_attr_specs = type_resource.attribute_specifications
        asset_obj = IonObject(specific_resource_type, name=name, description=description)
        asset_obj.alt_ids.append(nickname)

        # ----- Read, create attributes and update resource object, [if verbose read again]
        some_attributes = {}
        if not wo_resource['_attributes']:
            raise BadRequest('working object resource(%s) does not have attributes' % altid)

        if wo_resource['_attributes']:
            attr_specs = wo_resource['_attributes']
            for s in attr_specs:                        # todo iteritems()
                sobj = attr_specs[s]
                name = ''
                if sobj['name']:
                    name = sobj['name']
                    if name not in wo_attr_specs:
                        # for now raise, but this must be satisfied back when wo_resource is populated.
                        # in the case where an instance of resource is being created using an existing asset type instance,
                        # there (currently) will not be a wo associated with the type (to be addressed)
                        raise BadRequest('this attribute (%s) not defined in type resource (%s) attribute_specifications' % (name, implements))

                attr_spec = {}
                if name:
                    attr_spec = wo_attr_specs[name]

                attribute = IonObject(OT.Attribute)
                attribute['name'] = sobj['name']
                something = []                          # todo better name hook hook
                if attr_spec and sobj['value']:
                    something = self._process_attribute_value(attr_spec=attr_spec, attr_value=sobj['value'])
                attribute['value'] = something
                some_attributes[sobj['name']] = attribute

            if specific_resource_type == RT.Asset:
                asset_obj.asset_attrs = some_attributes
            elif specific_resource_type == RT.EventDuration:
                asset_obj.event_duration_attrs = some_attributes

        if specific_resource_type == RT.Asset:
            asset_id = self.create_asset(asset_obj)
            wo_resource['_id'] = asset_id
            resource_id = asset_id
        elif specific_resource_type == RT.EventDuration:
            asset_id = self.create_event_duration(asset_obj)
            wo_resource['_id'] = asset_id
            resource_id = asset_id
        else:
            raise BadRequest('Request to process marine tracking resource of unknown or invalid type (%s)' % specific_resource_type)

        return resource_id



    def _update_marine_resource(self, wo_resource, valid_types):

        # todo general cleanup and review

        if not wo_resource:
            raise BadRequest('wo_resource parameter is empty')
        if not valid_types:
            raise BadRequest('valid_types parameter is empty')

        verbose = False

        # ['implements', '_exists', '_scoped_names', 'name', '_implements_type', 'altid', 'org_altids',
        # '_pattribute_keys', '_attributes', 'concrete', '_implements',
        # '_pattribute_names', 'action', '_implements_id', '_scoped_keys', '_id', 'type', 'description']

        resource_id = ''
        if wo_resource['_id']:
            resource_id = wo_resource['_id']
        if not resource_id:
            raise BadRequest('resource working object does not have _id value, unable to process update')

        name = ''
        if wo_resource['name']:
            name = wo_resource['name']
        description = ''
        if wo_resource['description']:
            description = wo_resource['description']

        nicknameResource = ''
        if wo_resource['type']:
            specific_TypeResource = wo_resource['type']
            if specific_TypeResource:
                if specific_TypeResource in valid_types:
                    nicknameResource = specific_TypeResource + ":"
                else:
                    raise BadRequest('Unknown (%s) and an invalid Resource (not one of %s)' % (specific_TypeResource, valid_types))
        else:
            raise BadRequest('working object (of TypeResource) type is empty')

        altid = ''
        nickname = ''
        if wo_resource['altid']:
            altid = wo_resource['altid']
            nickname = nicknameResource +  altid
        else:
            raise BadRequest('marine resource (%s) altid is empty' % specific_TypeResource )

        # UPDATE - resource instance exists, update instance
        asset_id = resource_id
        current_attributes = {}
        if specific_TypeResource == RT.Asset:
            # todo try/except here; rename to resource
            asset = self.read_asset(asset_id)
            if asset:
                current_attributes = asset.asset_attrs
        elif specific_TypeResource == RT.EventDuration:
            # todo try/except here; rename to resource
            asset = self.read_event_duration(asset_id)
            if asset:
                current_attributes = asset.event_duration_attrs

        dirty = False
        if nickname not in asset.alt_ids:
            asset.alt_ids.append(nickname)
            dirty = True

        #--------------------------------------------------------
        # update:  description, attributes
        #--------------------------------------------------------
        if asset.description:
            if asset.description != description:
                asset.description = description
                dirty = True

        implements = ''
        if wo_resource['implements']:
            implements = wo_resource['implements']
        _implements = ''
        if wo_resource['_implements']:
            _implements = wo_resource['_implements']
        _implements_id = ''
        if wo_resource['_implements_id']:
            _implements_id = wo_resource['_implements_id']
        type = ''
        _implements_type = ''
        if wo_resource['type']:
            type = wo_resource['type']
            if type == RT.EventDuration:
                _implements_type = RT.EventDurationType
            elif type == RT.Asset:
                _implements_type = RT.AssetType

        if wo_resource['_attributes']:
            working_attributes = wo_resource['_attributes']
        else:
            raise BadRequest('working object resource (%s) does not have attributes', altid)

        # Obtain type resource AttributeSpecifications for processing (parse, validate and constrain) attributes
        wo_attr_specs = {}
        type_resource = ''
        if _implements_id and _implements_type:
            try:
                type_resource = self.RR2.read(resource_id=_implements_id,specific_type=_implements_type)
            except:
                raise NotFound('Unable to locate type resource (%s) associated with resource %s (type: %s)' % (implements, name, type))

        # wo type resource attribute_specifications
        if type_resource:
            wo_attr_specs = type_resource.attribute_specifications

        if working_attributes:
            if len(working_attributes) < len(current_attributes): # < len(wo_attr_specs):
                # less attributes than current instance attributes
                for attrib_name, attrib in current_attributes.iteritems():
                    if attrib_name not in working_attributes:
                        #working_attributes[attrib_name] = attrib
                        Attribute = {
                            'name'     : attrib_name,       # key dictionary with attribute name
                            'value'    : [],                # revisit when types available
                            'source'   : implements,        # name of categoryType (which is source of AttributeSpecification)
                            'action' : 'add'
                        }
                        value_list = attrib.value
                        values = []
                        for value in value_list:
                            values.append( value['value'])
                        Attribute['value'] = values
                        working_attributes[attrib_name] = Attribute
            """
            if verbose:
                if len(working_attributes) != len(current_attributes):
                    log.debug('\n\n[service] [2A] number of attributes NE current instance attributes! ***** *****')
                else:
                    log.debug('\n\n[service] [2B] number of attributes EQ current instance attributes! ***** *****')
            """
        attributes = {}
        wo_attributes = {}
        if wo_resource['_attributes']:
            wo_attributes = wo_resource['_attributes']
            if wo_attributes:
                # processing each of the working object (wo) attributes; compare (if possible) with attribute
                # value in current resource instance - if different then update. If new update. (think about)
                outbound_values = []
                for name, sobj in wo_attributes.iteritems():
                    attribute = IonObject(OT.Attribute, name=name)
                    if name in wo_attr_specs:
                        some_attr_spec = wo_attr_specs[name]
                    else:
                        raise BadRequest('unable to determine AttributeSpecification for %s (type resource:)' % (name))
                    vals = []
                    vals = sobj['value']
                    if current_attributes:
                        # if name of attribute in working object exist in current instance attributes...
                        if name in current_attributes:
                            current_value = []
                            current_values = current_attributes[name]
                            list_of_current_values = []
                            if 'value' in current_values:
                                list_of_current_values = current_values['value']
                            vinx = 0
                            list_of_values = []

                            # Take list of current annotated attribute values and create simple un-annotated list of values
                            if not list_of_current_values:
                                raise BadRequest('list_of_current_values is empty (attribute \'%s\')' % name)
                            for item in list_of_current_values:
                                if item['value']:
                                    list_of_values.append(item['value'])
                                vinx += 1

                            # Compare current resource attributes with those provided on update
                            # todo major section to be completed; requires several unit tests
                            resulting_list_of_values = []
                            if len(vals) != len(list_of_values):    # something has changed
                                if len(vals) < len(list_of_values): # something has been removed on update
                                    if verbose: log.debug('\n\n[service]new list of values has less items than current values')
                                else:
                                    if verbose: log.debug('\n\n[service]new list of values has more items than current values')
                            else:
                                inx = 0
                                resulting_list_of_values = []
                                for current_value in list_of_values:
                                    if vals[inx] != current_value:
                                        dirty = True
                                        the_value = vals[inx]
                                    else:
                                        the_value = current_value
                                    resulting_list_of_values.append(the_value)
                                    inx += 1

                            # prepare outbound attributes
                            outbound_values = []
                            if not resulting_list_of_values:
                                raise BadRequest('resulting_list_of_values is empty (attribute \'%s\')' % name)

                            for el in resulting_list_of_values:
                                try:
                                    tmp = []
                                    tmp.append(el)
                                    attr = self._process_attribute_value(some_attr_spec, tmp)
                                    outbound_values.append(attr[0])

                                except Inconsistent, Arguments:
                                    raise Inconsistent(Arguments.get_error_message())
                                except BadRequest, Arguments:
                                    raise BadRequest(Arguments.get_error_message())
                                except NotFound, Arguments:
                                    raise NotFound(Arguments.get_error_message())

                    attribute['value'] = outbound_values        #todo review [] outbound_values
                    attributes[attribute['name']] = attribute

                if specific_TypeResource == RT.Asset:
                    asset.asset_attrs = attributes
                if specific_TypeResource == RT.EventDuration:
                    asset.event_duration_attrs = attributes
                dirty = True

        # todo raise/except
        if dirty:
            if specific_TypeResource == RT.Asset:
                try:
                    self.update_asset(asset)
                    #asset = self.read_asset(asset_id)
                except BadRequest, Arguments:
                    raise BadRequest(Arguments.get_error_message())
                except NotFound, Arguments:
                    raise NotFound(Arguments.get_error_message())
                except Inconsistent, Arguments:
                    raise Inconsistent(Arguments.get_error_message())
                except:
                    raise BadRequest('failed to update update Asset instance (%s)' % altid)

            elif specific_TypeResource == RT.EventDuration:
                try:
                    self.update_event_duration(asset)
                    #asset = self.read_event_duration(asset_id)
                except BadRequest, Arguments:
                    raise BadRequest(Arguments.get_error_message())
                except NotFound, Arguments:
                    raise NotFound(Arguments.get_error_message())
                except Inconsistent, Arguments:
                    raise Inconsistent(Arguments.get_error_message())
                except:
                    raise BadRequest('failed to update update EventDuration instance (%s)' % altid)

        return resource_id

    def _remove_marine_resource(self, wo_resource, valid_types):

        if not wo_resource:
            raise BadRequest('wo_resource parameter is empty')
        if not valid_types:
            raise BadRequest('valid_types parameter is empty')
        # review governance and discuss impact across Orgs, etc.
        resource_id = ''
        if wo_resource['_id']:
            resource_id = wo_resource['_id']
        if wo_resource['type']:
            specific_TypeResource = wo_resource['type']
            if specific_TypeResource:
                if specific_TypeResource not in valid_types:
                    raise BadRequest('Unknown (%s) and an invalid instance Resource (not one of %s)' % (specific_TypeResource, valid_types))
        else:
            raise BadRequest('working object (of instance Resource) is empty')

        if specific_TypeResource == RT.Asset:
            if resource_id:
                self.delete_asset(resource_id)
        elif specific_TypeResource == RT.EventDuration:
            if resource_id:
                self.delete_event_duration(resource_id)

        return resource_id

    def _process_marine_type_resources(self, wo_type_resources, valid_types):
        """Process all marine type resource working objects and create, update or delete instances.

        @param  wo_resources        {}      # dictionary of AssetType or EventDurationType working objects
        @param  valid_types         {}      # list of supported types [RT.AssetType, RT.EventDurationType]
        #@throws BadRequest     if object does not have _id or _rev attribute
        """
        if not wo_type_resources:
            raise BadRequest('wo_type_resources parameter is empty')
        if not valid_types:
            raise BadRequest('valid_types parameter is empty')

        type_resource_ids = []
        del_type_resource_ids = []
        for name, wo_type_resource in wo_type_resources.iteritems():

            if wo_type_resource:

                if wo_type_resource['action']:

                    # Add - create and update
                    if wo_type_resource['action'] == 'add':

                        if wo_type_resource['_exists']:

                            if wo_type_resource['_import'] == False:
                                id = self._update_type_resource(wo_type_resource, valid_types)
                                if id:
                                    if id not in type_resource_ids:
                                        type_resource_ids.append(id)

                        else:
                            id = self._create_type_resource(wo_type_resource, valid_types)
                            if id:
                                if id not in type_resource_ids:
                                    type_resource_ids.append(id)

                    # Remove - delete
                    elif wo_type_resource['action'] == 'remove':

                        if wo_type_resource['_exists']:
                            id = self._remove_type_resource(wo_type_resource, valid_types)
                            if id:
                                if id not in del_type_resource_ids:
                                    del_type_resource_ids.append(id)

        return type_resource_ids, del_type_resource_ids

    def _process_marine_resources(self, wo_resources, valid_types):
        """Process all resource working objects and create, update or delete instances.

        @param  wo_resources        {}      # dictionary of Asset or EventDuration working objects
        @param  valid_types         {}      # list of supported types [RT.Asset, RT.EventDuration]
        #@throws BadRequest     if object does not have _id or _rev attribute
        """
        if not wo_resources:
            raise BadRequest('wo_resources parameter is empty')
        if not valid_types:
            raise BadRequest('valid_types parameter is empty')

        resource_ids = []
        del_resource_ids = []
        if wo_resources:

            for name, wo_resource in wo_resources.iteritems():

                if wo_resource:

                    if wo_resource['action'] == 'add':

                        if wo_resource['_exists']:
                            if wo_resource['_import'] == False:
                                id = self._update_marine_resource(wo_resource, valid_types)
                                if id:
                                    if id not in resource_ids:
                                        resource_ids.append(id)
                        else:
                            id = self._create_marine_resource(wo_resource, valid_types)
                            if id:
                                if id not in resource_ids:
                                    resource_ids.append(id)

                    elif wo_resource['action'] == 'remove':

                        if wo_resource['_exists']:
                            id = self._remove_marine_resource(wo_resource, valid_types)
                            if id:
                                if id not in del_resource_ids:
                                    del_resource_ids.append(id)

        return resource_ids, del_resource_ids

    def _processTypeResourceExtends(self, res_type, list_wo_resources, wo_resources):
        """Add 'extends' association for each resource; association is PRED.extendsAssetType or
        PRED.extendsEventDurationType depending on resource type being processed.

        @param  res_type            str     # value of RT.AssetType or RT.EventDurationType
        @param  wo_resources        {}      # dictionary of AssetType or EventDurationType working objects
        #@throws BadRequest     if object does not have _id or _rev attribute
        """
        # todo rename this
        # note: here we are instantiating the extends hierarchy of type resource(s) by adding association
        # (PRED.extendsTypeResource) in the situation where one type resource 'extends' another.
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Add 'extends' association for each TypeResource
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if not res_type:
            raise BadRequest('res_type parameter is empty')
        if not list_wo_resources:
            raise BadRequest('list_wo_resources parameter is empty')
        if not wo_resources:
            raise BadRequest('wo_resources parameter is empty')

        if res_type == RT.AssetType:
            predicate = PRED.extendsAssetType
        elif res_type == RT.EventDurationType:
            predicate = PRED.extendsEventDurationType
        else:
            raise BadRequest('Failed to extend unknown TypeResource value: %s' % res_type)

        # return and review, cleanup - todo
        for name in list_wo_resources:
            res = None
            res = wo_resources[name]

            if res['type']:
                if res['type'] != res_type:
                    bname = res['id']
                    # TODO - type mismatch - raise

            if res['extends']:
                res_id = ''
                res_id = res['_id']
                target_res_id = ''
                target_type_name = res['extends']
                if target_type_name in list_wo_resources:
                    # we have it handy, use it
                    target_res = wo_resources[target_type_name]
                    if target_res:
                        if target_res['_id']:
                            target_res_id = target_res['_id']
                            associations = None
                            associations = self.RR.find_associations(subject=res_id, predicate=predicate,
                                                                     object=target_res_id, id_only=False)
                            if associations:
                                # does current type resource (res) already have an extendsXXX association with the target_res?
                                # if so, continue; otherwise add association
                                # put a loop for assoc in associations: if assoc.o == target_res_id -> found assoc (also check type)
                                found_assoc_to_extends_type = False
                                x_id = ''
                                for assoc in associations:
                                    x_id = assoc.o
                                    x_type = assoc.ot
                                    if target_res_id == x_id:
                                        # extends association exists, set flag
                                        found_assoc_to_extends_type = True
                                if not found_assoc_to_extends_type:
                                    # make the association using predicate
                                    self.RR.create_association(res_id, predicate, target_res_id)
                                else:
                                    # in the instance where an association has been determined to exist, check type values
                                    # for consistency check
                                    if target_res_id != x_id:
                                        # was res['altid']
                                        raise BadRequest('association %s to different object type (\'%s\'); expected type \'%s\'' % (predicate, res['altid'], res_type) )
                            else:
                                # make the association using predicate
                                self.RR.create_association(res_id, predicate, target_res_id)

                else:
                    # not handy must retrieve from system, if not available then error
                    raise BadRequest('do not have %s to process (type \'%s\')' % (target_type_name, res_type))

        return

    def _processInstanceResourceImplements(self, res_type, wo_resources, wo_type_resources):
        """Add association for each Resource; association is PRED.implementsAssetType or
        PRED.implementsEventDurationType depending on resource type being processed.

        @param  res_type            str     # value of RT.Asset or RT.EventDuration
        @param  wo_resources        {}      # dictionary of Asset or EventDuration working objects
        @param  wo_type_resources   {}      # dictionary of Asset or EventDuration working objects
        #@throws BadRequest     if object does not have _id or _rev attribute
        """
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Add implements association for each Resource
        # todo:
        #  determine if association already exists before doing assign
        #  rename local variables appropriately
        #  [added] when an instance resource is to be associated with a TypeResource instance not in
        #    working object collection of TypeResources, must retrieve from system instances
        #    and not simply fail. (todo unit test - update resource without providing <resource>Types sheet
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if not res_type:
            raise BadRequest('res_type parameter is empty')
        if not wo_resources:
            raise BadRequest('wo_resources parameter is empty')
        if not wo_type_resources:
            raise BadRequest('wo_type_resources parameter is empty')

        if res_type == RT.Asset:
            predicate = PRED.implementsAssetType
            nickname_prefix = RT.Asset + '+'
            namespace = RT.Asset
        elif res_type == RT.EventDuration:
            predicate = PRED.implementsEventDurationType
            nickname_prefix = RT.EventDuration + '+'
            namespace = RT.EventDuration
        else:
            raise BadRequest('Failed to extend unknown TypeResource: %s' % res_type)

        implements_name = ''
        for name, res in wo_resources.iteritems():
            if res['action']:
                action = res['action']
                #if action == 'remove':          # has already been deleted!
                if action != 'add':          # has already been deleted!
                    continue

            altid = res['altid']
            nickname = nickname_prefix +  res['altid']
            exist_ids, _ = self.container.resource_registry.find_resources_ext(alt_id_ns=namespace,
                                                            alt_id=altid, id_only=True)
            if exist_ids:
                if len(exist_ids) != 1:
                    raise BadRequest('more than one id returned indicating multiple resources using \'%s\'' % altid)
                asset_id = exist_ids[0]
            else:
                raise BadRequest('this %s instance (%s) does not exist' % (res_type, nickname))

            # ----- assign TypeResource to InstanceResource (first check if already Associated....)
            if res['implements']:
                implements_name = res['implements']
                if implements_name in wo_type_resources:
                    asset_type = wo_type_resources[implements_name]
                else:
                    asset_type = self._get_type_resource_by_name(implements_name, res_type)
                    if not asset_type:
                        raise BadRequest('TypeResource %s not available' % implements_name)

                if asset_type:
                    if asset_type['_id']:
                        asset_type_id = asset_type['_id']
                    else:
                        raise BadRequest('TypeResource instance %s does not exist' % implements_name)

                    # get resource Associations where predicate {PRED.implementsAssetType | PRED.implementsEventDurationType},
                    # returns id for marine tracking resource's TypeResource (e.g. for Asset returns id for AssetType)
                    associations = self.RR.find_associations(subject=asset_id, predicate=predicate, id_only=False)
                    if associations:
                        res['_implements_id'] = associations[0].o           # todo scrub
                        res['_implements_type'] = associations[0].ot        # todo scrub
                    else:
                        if asset_id and asset_type_id:
                            if res_type == RT.Asset:
                                self.assign_asset_type_to_asset(asset_type_id, asset_id)
                            elif res_type == RT.EventDuration:
                                self.assign_event_duration_type_to_event_duration(asset_type_id, asset_id)

                        else:
                            raise BadRequest('unable to create association %s' % predicate)

                else:
                    raise BadRequest('Unknown %s (name requested %s)' % (res_type, implements_name))
            else:
                raise BadRequest('There is no %s instance %s' % (res_type, implements_name))

        return

    def _shareTypeResources(self, res_type, wo_type_resources):
        """Share TypeResource with designated Orgs; TypeResource (either AssetType or EventDurationType).

        @param  res_type            str     # value of RT.Asset or RT.EventDuration
        @param  wo_type_resources   {}      # dictionary of Asset or EventDuration working objects
        #@throws BadRequest     if object does not have _id or _rev attribute
        """
        if not res_type:
            raise BadRequest('res_type parameter is empty')
        if not wo_type_resources:
            raise BadRequest('wo_type_resources parameter is empty')

        predicate = PRED.hasResource
        if res_type != RT.AssetType and res_type != RT.EventDurationType :
            raise BadRequest('Failed to share unknown marine asset TypeResource: %s' % res_type)

        for name,res in wo_type_resources.iteritems():

            if res['action']:
                action = res['action']
                if action != 'add':      # if remove, do not share
                    continue

            if res['_id']:
                res_id = res['_id']
            if res_id:
                org_ids = []
                if res['org_altids']:
                    org_altids = res['org_altids'][:]
                    if org_altids:
                        org_ids = self._get_org_ids(org_altids)
                        if org_ids:
                            for org_id in org_ids:
                                # does this resource already have hasResource association? check before proceeding.
                                assoc = self.container.resource_registry.find_associations(subject=org_id,
                                                        predicate=predicate, object=res_id, id_only=True)
                                if not assoc:
                                    self.assign_resource_to_observatory_org(res_id, org_id)

        return

    def _shareInstanceResources(self, res_type, wo_resources):
        """Share resource instances with designated facility ids (Orgs).

        @param  res_type        str     # value of RT.Asset or RT.EventDuration
        @param  wo_resources    {}      # dictionary of Asset or EventDuration working objects
        #@throws BadRequest     if object does not have _id or _rev attribute
        """
        if not res_type:
            raise BadRequest('res_type parameter is empty')
        if not wo_resources:
            raise BadRequest('wo_type_resources parameter is empty')

        predicate = PRED.hasResource
        if res_type != RT.Asset and res_type != RT.EventDuration:
            raise BadRequest('Failed to share unknown marine asset instance resource: %s' % res_type)

        for name,res in wo_resources.iteritems():

            if res['action']:
                action = res['action']
                if action != 'add':      # do not share
                    continue

            if res['name']:
                altid = res['name']

            if res['_id']:
                res_id = res['_id']
            if res_id:
                if res['org_altids']:
                    org_altids = res['org_altids'][:]
                    if org_altids:
                        org_ids = self._get_org_ids(org_altids)
                        if org_ids:
                            for org_id in org_ids:
                                # does this resource already have hasResource association? check before proceeding.
                                assoc = self.container.resource_registry.find_associations(subject=org_id,
                                                predicate=predicate, object=res_id, id_only=True)
                                if not assoc:
                                    self.assign_resource_to_observatory_org(res_id, org_id)
        return

    def _process_event_asset_map_associations(self, wo_map):

        if not wo_map:
            raise BadRequest('wo_map is empty')

        for name, item in wo_map.iteritems():
            asset_res_exists = False
            event_res_exists = False
            event_altid = item['event_altid']
            asset_altid = item['asset_altid']
            action   = item['action']
            if item['_exists_event']:
                # instance exists, use event id and event type category to process
                _id_event = item['_id_event']
                event_res_exists = True

            else:
                # instance id not available - get event instance id, get asset instance - continue
                # Verify Event resource ID instance exists?
                event_res_exists = False
                _id_event = ''
                if event_altid:
                    event_obj = self._get_resource_by_name(event_altid, RT.EventDuration)
                    if event_obj:
                        _id_event = event_obj._id
                        event_res_exists = True
                    else:
                        raise BadRequest('unable to process association to asset without event instance')
                else:
                    raise BadRequest('event id cannot be identified with altid provided; cannot process event')

            if item['_exists_asset']:
                if item['_id_asset']:
                    _id_asset = item['_id_asset']
                    asset_res_exists = True
            else:
                _id_asset = ''
                if asset_altid:
                    asset_obj = self._get_resource_by_name(asset_altid, RT.Asset)
                    if asset_obj:
                        _id_asset = asset_obj._id
                        asset_res_exists = True
                    else:
                        raise BadRequest('unable to process association to event duration without asset instance')

            if event_res_exists and asset_res_exists:
                # determine if this association between event and asset already exists...
                if action == 'add':
                    self.assign_event_duration_to_asset(_id_event, _id_asset)
                elif action == 'remove':
                    self.unassign_event_duration_to_asset(_id_event, _id_asset)
            else:
                raise BadRequest('either event duration instance or asset instance does not exist; cannot process association')


    def _process_xls(self, content, content_type, content_encoding):
        """Declares asset management resources based on [xlsx] content provided.

        Then, based on working objects and action value, either modifies ('add' action) or
        deletes ('remove' action) instances. Returns dictionary resource types and list of ids
        created of that type that have been modified or deleted.
        #@param content             encoded blob               # xlsx encoded blob
        #@param content_type        file_descriptor.mimetype   # file descriptor type
        #@param content_encoding    'b2a_hex'                  # encoding (set to binascii.b2a_hex)
        #@retval response           {}                         # dictionary of modfied or removed working objects
        #@throws NotFound           object with specified id does not exist
        #@throws BadRequest         if object does not have _id or _rev attribute
        """
        if not content:
            raise BadRequest('content parameter is empty')
        if not content_type:
            raise BadRequest('content_type parameter is empty')
        if not content_encoding:
            raise BadRequest('content_encoding parameter is empty')

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Used by service declare_asset_management_resources (to upload xlsx info and declare resources)
        # response dictionary format:
        #           { 'status' : 'ok' | 'error',
        #             'err_msg': 'error msg text',
        #             'res_modified' : { 'asset_types'  : [],   # list of asset_type ids
        #                                'assets'       : [],   # list of asset ids
        #                                'event_types'  : [],   # list of event_type ids
        #                                'events'       : []    # list of event ids
        #                                'codespaces'   : []    # list of codespace ids
        #                              }
        #             'res_removed'  : { 'asset_types'  : [],   # list of asset_type ids
        #                                'assets'       : [],   # list of asset ids
        #                                'event_types'  : [],   # list of event_type ids
        #                                'events'       : []    # list of event ids
        #                                'codespaces'   : []    # list of codespace ids
        #                              }
        #           }
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if not content:
            raise BadRequest('content parameter is empty')

        verbose = False              # [debug] general debug statements
        valid_type_resource_types = [RT.AssetType, RT.EventDurationType]
        valid_resource_types = [RT.Asset, RT.EventDuration]

        # Prepare response dictionary structure
        response = {}
        modified = {}
        modified['asset_types'] = []
        modified['assets']      = []
        modified['event_types'] = []
        modified['events']      = []
        modified['codespaces']  = []

        deleted = {}
        deleted['asset_types'] = []
        deleted['assets']      = []
        deleted['event_types'] = []
        deleted['events']      = []
        deleted['codespaces']  = []

        response['res_modified'] = modified
        response['res_removed'] = deleted
        status_value = 'ok'
        error_msg = ''

        response['status'] = status_value  # ok | error
        response['err_msg'] = error_msg
        response['res_modified'] = modified
        response['res_removed'] = deleted

        try:
            # Read xlsx content, process into working objects
            CodeSpaces, Codes, CodeSets, AssetTypes, oAssetTypes, EventDurationTypes, oEventDurationTypes, \
               AssetResources, oAssetResources, EventDurationResources, oEventDurationResources, EventAssetMap = \
                self._get_working_objects(content, content_type, content_encoding)

        except BadRequest, Arguments:
            status_value = 'error'
            error_msg = Arguments.get_error_message()
            response['status'] = status_value  # ok | error
            response['err_msg'] = error_msg
            return response
        except NotFound, Arguments:
            status_value = 'error'
            error_msg = Arguments.get_error_message()
            response['status'] = status_value  # ok | error
            response['err_msg'] = error_msg
            return response
        except Inconsistent, Arguments:
            status_value = 'error'
            error_msg = Arguments.get_error_message()
            response['status'] = status_value  # ok | error
            response['err_msg'] = error_msg
            return response
        except:
            status_value = 'error'
            error_msg = 'Failed to annotate working objects.'
            response['status'] = status_value  # ok | error
            response['err_msg'] = error_msg
            return response

        """
        # For add/create, do following:
        #
        # Create instances of TypesResource(s) (AssetType, EventDurationType), instance resources (Asset, EventDuration);
        #
        # Perform assignment of implements associations between subject, predicate and object:
        #    Asset          PRED.implementsAssetType        AssetType
        #    EventDuration  PRED.implementsEventDurationType   EventDurationType
        #
        # Perform assignment of extends associations between subject, predicate and object:
        #    AssetType          PRED.extendsAssetType               AssetType
        #    EventDurationType  PRED.extendsEventDurationType       EventDurationType
        #
        # Share above type resource and instance resources.
        #    Perform assignment of associations with Org ids (PRED.hasResource) using self.assign_resource_to_observatory_org
        #    (for following: Asset, EventDuration, AssetType, AssetventType)
        #
        # Prepare response and return ids
        #
        """
        try:

            codespace_ids = []
            deleted_codespace_ids = []
            if CodeSpaces:
                for name, cs in CodeSpaces.iteritems():
                    if cs['_id']:
                        if cs['action'] == 'add':
                            if cs['_id'] not in codespace_ids:
                                codespace_ids.append(cs['_id'])
                        else:
                            if cs['_id'] not in deleted_codespace_ids:
                                deleted_codespace_ids.append(cs['_id'])

            # Codes and CodeSet, when modified reflect change back to the CodeSpace which they are
            # members. derived_codespace_ids is a list of codespace_ids representing the CodeSpaces
            # which have been 'touched' due to Code or CodeSet changes.
            # Codes (names & ids)
            code_names = []
            del_code_names = []
            code_ids = []
            del_code_ids = []

            derived_codespace_ids = []
            if Codes:
                for name, cs in Codes.iteritems():
                    if cs:
                        if cs['action']:
                            if cs['cs_id']:
                                if cs['cs_id'] not in derived_codespace_ids:
                                    derived_codespace_ids.append(cs['cs_id'])
                            if cs['action'] == 'add':
                                if name not in code_names:
                                    code_names.append(name)
                                if cs['id'] not in code_ids:
                                    code_ids.append(cs['id'])
                            else:
                                if name not in del_code_names:
                                    del_code_names.append(name)
                                if cs['id'] not in del_code_ids:
                                    del_code_ids.append(cs['id'])

            # CodeSets
            codeset_names = []
            del_codeset_names = []
            if CodeSets:
                for name, cs in CodeSets.iteritems():
                    if cs:
                        if cs['action']:
                            if cs['cs_id']:
                                if cs['cs_id'] not in derived_codespace_ids:
                                    derived_codespace_ids.append(cs['cs_id'])
                            if cs['action'] == 'add':
                                if name not in codeset_names:
                                    codeset_names.append(name)
                            else:
                                if name not in del_codeset_names:
                                    del_codeset_names.append(name)

            # When a Code or CodeSet has been modified and CodeSpace updated (more or less indirectly through
            # Code or CodeSet changes - derived_codespace_ids will have content. If codespace_ids is empty but
            # changes have been made to one or more Code(s) or CodeSet(s) then the derived_codespace_ids can be
            # utilized to flag change on response.
            if derived_codespace_ids:
                if not codespace_ids:
                    codespace_ids = derived_codespace_ids

            if verbose: log.debug('\n\n [service] _process_marine_type_resources...')

            # Process AssetTypes (create TypeResource instances from working objects)
            asset_type_ids = []
            deleted_asset_type_ids = []
            if AssetTypes:
                asset_type_ids, deleted_asset_type_ids = self._process_marine_type_resources(AssetTypes, valid_type_resource_types)

            # Process EventDurationTypes (create TypeResource instances from working objects)
            event_duration_type_ids = []
            deleted_event_duration_type_ids = []
            if EventDurationTypes:
                event_duration_type_ids, deleted_event_duration_type_ids = self._process_marine_type_resources(EventDurationTypes, valid_type_resource_types)

            if verbose: log.debug('\n\n _process_marine_tracking_resources...')

            # Process AssetResources (create marine resource instances from working objects)
            asset_ids = []
            deleted_asset_ids = []
            if AssetResources:
                asset_ids, deleted_asset_ids = self._process_marine_resources(AssetResources, valid_resource_types)

            # Process EventDurationResources (create marine resource instances from working objects)
            event_duration_ids = []
            deleted_event_duration_ids = []
            if EventDurationResources:
                event_duration_ids, deleted_event_duration_ids = self._process_marine_resources(EventDurationResources, valid_resource_types)

            if verbose: log.debug('\n\n _processTypeResourceExtends...')
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Add extends association for each TypeResource {AssetType | EventDurationType }
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            if (oAssetTypes and AssetTypes):
                self._processTypeResourceExtends(RT.AssetType, oAssetTypes, AssetTypes)

            if (oEventDurationTypes and EventDurationTypes):
                self._processTypeResourceExtends(RT.EventDurationType, oEventDurationTypes, EventDurationTypes)

            if verbose: log.debug('\n\n _processInstanceResourceImplements...')
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Add implements association for each instance resource {Asset | EventDuration }
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            if (AssetResources and AssetTypes):
                self._processInstanceResourceImplements(RT.Asset, AssetResources, AssetTypes)
            if (EventDurationResources and EventDurationTypes):
                self._processInstanceResourceImplements(RT.EventDuration, EventDurationResources, EventDurationTypes)

            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Share all types and instances across Org IDS
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            if verbose: log.debug('\n\n[service] Share TypeResources...')
            if AssetTypes:
                self._shareTypeResources(RT.AssetType, AssetTypes)
            if EventDurationTypes:
                self._shareTypeResources(RT.EventDurationType, EventDurationTypes)

            if verbose: log.debug('\n\n[service] Share InstanceResources...')
            if AssetResources:
                self._shareInstanceResources(RT.Asset, AssetResources)
            if EventDurationResources:
                self._shareInstanceResources(RT.EventDuration, EventDurationResources)

            if verbose: log.debug('\n\n[service] EventAssetMap...')

            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Process EventDuration to Asset associations
            #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            if EventAssetMap:
                self._process_event_asset_map_associations(EventAssetMap)

        except BadRequest, Arguments:
            status_value = 'error'
            error_msg = Arguments.get_error_message()
            response['status'] = status_value  # ok | error
            response['err_msg'] = error_msg
            return response
        except NotFound, Arguments:
            status_value = 'error'
            error_msg = Arguments.get_error_message()
            response['status'] = status_value  # ok | error
            response['err_msg'] = error_msg
            return response
        except Inconsistent, Arguments:
            status_value = 'error'
            error_msg = Arguments.get_error_message()
            response['status'] = status_value  # ok | error
            response['err_msg'] = error_msg
            return response
        except:
            status_value = 'error'
            error_msg = 'Failed to annotate working objects.'
            response['status'] = status_value  # ok | error
            response['err_msg'] = error_msg
            return response

        if verbose: log.debug('\n\n[service] prepare response.........')

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Prepare and return response
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        response = {}
        status_value = 'ok'
        error_msg = ''

        modified = {}
        modified['asset_types'] = asset_type_ids
        modified['assets']      = asset_ids
        modified['event_types'] = event_duration_type_ids
        modified['events']      = event_duration_ids
        modified['codespaces']  = codespace_ids

        deleted = {}
        deleted['asset_types']  = deleted_asset_type_ids
        deleted['assets']       = deleted_asset_ids
        deleted['event_types']  = deleted_event_duration_type_ids
        deleted['events']       = deleted_event_duration_ids
        deleted['codespaces']   = deleted_codespace_ids

        response['status']      = status_value       # ok | error
        response['err_msg']     = error_msg
        response['res_modified']= modified
        response['res_removed'] = deleted

        return response

    """
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    #   CodeSpace, Codes and CodeSet service helpers
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    """

    def _process_wo_codesets(self, items, category, code_space_id):
        """Create CodeSet working objects from object_definitions for category 'CodeSets'.
        Resulting dictionary holds all CodeSet working objects with key = altid.

        @params  items          {}              # xlsx data each row stored as item in object_definitions
        @params  category       str             # category ('Codes' == sheet name) used for error msgs
        @params  code_space_id  str             # code_space_id for Codes being processed
        @retval  CodeSet        {}              # dictionary of CodeSet working objects
        @throws BadRequest if object does not have _id or _rev attribute
        """
        if not items:
            raise BadRequest('items parameter is empty')
        if not category:
            raise BadRequest('category parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')

        CodeSets = {}
        cnt = 2
        code_space_codesets = {}
        # Check to see if this CodeSpace is available in system instances; if so get it and then the codes
        if code_space_id:
            code_space = self.RR2.read(resource_id=code_space_id, specific_type=RT.CodeSpace)
            if code_space:
                if code_space['codesets']:
                    code_space_codesets = code_space['codesets']
            else:
                raise BadRequest('Failed to read CodeSpace (id: %s); unable to continue processing codes' % code_space_id)

        for item in items:

            cs_altid    = item['CodeSpace ID']      # codespace altid - CodeSpace of code
            action      = item['Action']            # action to perform { 'add' | 'remove'}
            altid       = item['CodeSet ID']        # altid for code
            enumeration = item['Enumeration']       # comma separated list of strings

            if cs_altid:
                cs_altid = cs_altid.strip()
                if not cs_altid:
                    raise BadRequest('(sheet: %s) CodeSpace ID value empty (row: %d) ' % (category, cnt))

            if action:
                action = (action.lower()).strip()
                if not action:
                    raise BadRequest('(sheet: %s) Action value empty (row: %d) ' % (category, cnt))

            if altid:
                altid = altid.strip()
                if not altid:
                    raise BadRequest('(sheet: %s) CodeSet ID value empty (row: %d) ' % (category, cnt))

            description = item['Description']
            if description:
                description = description.strip()

            if enumeration:
                enumeration = self.get_list_csv(enumeration)
                if not enumeration:
                    raise BadRequest('(sheet: %s) Enumeration value empty (row: %d) ' % (category, cnt))

            # Determine if codeset existing in codespace
            res_exists = False
            if code_space_codesets:
                if altid in code_space_codesets:
                    res_exists = True
                    some_codeset = code_space_codesets[altid]
                    if some_codeset:
                        wo_name, wo_codeset = self._populate_wo_codeset(some_codeset, code_space_id, cs_altid)

            if res_exists:
                # set working object values (those which can be modified to values of input xlsx)
                # rule: cannot change of name codeset once created
                wo_codeset['action'] = action
                wo_codeset['enumeration'] = enumeration
                wo_codeset['description'] = description
                wo_codeset['_exists'] = True
                CodeSet = wo_codeset

            else:
                CodeSet = {
                        'altid'         : altid,                # alt name provided for CodeSet
                        'cs_altid'      : cs_altid,             # alt name for CodeSpace of CodeSet
                        'cs_id'         : code_space_id,        # unique sys uuid for CodeSpace of CodeSet
                        'action'        : action,               # action to perform for this CodeSet
                        'name'          : altid,                # Name provided for CodeSet
                        'description'   : description,          # Description provided for CodeSet
                        'enumeration'   : enumeration,          # Description provided for CodeSet
                        'org_altids'    : [],                   # Code shared across Facilities/Orgs
                        '_exists'       : False                 # instance exists?
                        }

                # revisit orgs and codespace issue todo
                ## Not currently used for Codesets (possible)
                #if item['Facility IDS']:
                #    org_ids = self.get_list_csv(item['Facility IDS'])
                #    Code['org_altids'] = org_ids[:]


            CodeSets[CodeSet['altid']] = CodeSet
            cnt += 1

        return CodeSets

    def _process_wo_codespaces(self, items, category, const_code_space_name):
        """Create CodeSpace working objects from object_definitions for category 'CodeSpaces'.
        Resulting dictionary holds all CodeSpace working objects with key = altid.

        @params  items                  {}      # xlsx data each row stored as item in object_definitions
        @params  category               str     # category ('CodeSpaces' == sheet name) used for error msgs
        @params  const_code_space_name  str     # code_space_id for Codes being processed
        @retval  CodeSpaces             {}      # dictionary of CodeSpace working objects
        @throws BadRequest if object does not have _id or _rev attribute
        """
        if not items:
            raise BadRequest('items parameter is empty')
        if not category:
            raise BadRequest('category parameter is empty')

        CodeSpaces = {}
        code_space_names = []
        cnt = 2
        for item in items:
            # Check to see if this CodeSpace is available in system instances
            if item['CodeSpace ID']:
                code_space_name = item['CodeSpace ID']
                code_space_name = code_space_name.strip()
                if not code_space_name:
                    raise BadRequest('(sheet: %s) CodeSpace ID value is empty(row: %d) ' % (category, cnt))
                else:
                    if code_space_name not in code_space_names:
                        code_space_names.append(code_space_name)
            else:
                raise BadRequest('(sheet: %s) CodeSpace ID value is empty(row: %d) ' % (category, cnt))

            if item['Action']:
                action = item['Action']
                action = (action.lower()).strip()
            else:
                raise BadRequest('(sheet: %s) %s Action value is empty. (row: %d) ' % (category, code_space_name, cnt))

            code_space_exists = True
            code_space_id = self._get_code_space_id(code_space_name)
            if not code_space_id:
                code_space_exists = False

            if code_space_exists:
                #create key:
                #{'alt_id': 'MAM', 'alt_id_ns': 'CodeSpace', 'id': '34b3528f54744d3cb75263312d1e06bb'}
                key = {}
                key['id'] = code_space_id
                key['alt_id_ns'] = RT.CodeSpace
                key['alt_id'] = const_code_space_name
                cs_name, CodeSpace = self._populate_wo_codespace(key)
                if cs_name and CodeSpace:
                    CodeSpaces[CodeSpace['altid']] = CodeSpace

                if not CodeSpace:
                    raise BadRequest('failed to create CodeSpace working object from CodeSpace instance')

            else:
                description = ''
                if item['Description']:
                    description = item['Description']
                    description = description.strip()

                CodeSpace = {
                    'altid'         : code_space_name,   # alt name provided for CodeSpace
                    'action'        : 'add',             # action to perform for this CodeSpace
                    'name'          : code_space_name,   # Name provided for CodeSpace
                    'description'   : description,       # Description provided for CodeSpace
                    'org_altids'    : [],                # Code shared across Facilities/Orgs
                    '_id'           : '',                # system assigned uuid4
                    '_exists'       : False,             # instance exists?
                    'codes'         : {},
                    'codesets'      : {}
                    }

                CodeSpaces[CodeSpace['altid']] = CodeSpace
                cnt += 1

        return CodeSpaces

    def _process_wo_codes(self, items, category, code_space_id):
        """Create Code working objects from object_definitions for category 'Codes'.
        Resulting dictionary holds all Code working objects with key = altid.

        @params  items          {}              # xlsx data each row stored as item in object_definitions
        @params  category       str             # category ('Codes' == sheet name) used for error msgs
        @params  code_space_id  str             # code_space_id for Codes being processed
        @retval  Codes          {}              # dictionary of Code working objects
        @throws BadRequest if object does not have _id or _rev attribute
        """
        if not items:
            raise BadRequest('items parameter is empty')
        if not category:
            raise BadRequest('category parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')

        CodeSpaces = {}
        Codes = {}
        code_space_names = []                   # tmp working variable
        cnt = 2
        code_space_codes = {}

        # Check to see if this CodeSpace is available in system instances; if so get it and then the codes
        if code_space_id:
            code_space = self.RR2.read(resource_id=code_space_id, specific_type=RT.CodeSpace)
            if code_space:
                if code_space['codes']:
                    code_space_codes = code_space['codes']
            else:
                raise BadRequest('Failed to read CodeSpace (id: %s); unable to process codes' % code_space_id)

        for item in items:

            cs_altid = self._get_item_value(item, 'CodeSpace ID')
            if not cs_altid:
                raise BadRequest('(sheet: %s) CodeSpace ID value is empty(row: %d) ' % (category, cnt))
            else:
                if cs_altid not in code_space_names:
                    code_space_names.append(cs_altid)

            altid = self._get_item_value(item,'Code ID')
            if not altid:
                raise BadRequest('(sheet: %s) Code ID value is empty (row: %d) ' % (category, cnt))

            action = self._get_item_value(item, 'Action')
            if action:
                action = action.lower()
                if not action:
                    raise BadRequest('(sheet: %s) Action value is empty (row: %d) ' % (category, cnt))
            else:
                raise BadRequest('(sheet: %s) Action value is empty (row: %d) ' % (category, cnt))

            description = self._get_item_value(item, 'Description')

            res_exists = False
            if code_space_codes:
                if altid in code_space_codes:
                    res_exists = True
                    some_code = code_space_codes[altid]
                    if some_code:
                        wo_name, wo_code = self._populate_wo_code(some_code, code_space_id, cs_altid)
                        # todo review, seems like should raise
                        #if wo_code:
                        #    if verbose: log.debug('\n\n[service] wo_code: %s', wo_code)

            if res_exists:
                wo_code['action'] = action
                wo_code['description'] = description
                wo_code['_exists'] = True
                Code = wo_code

            else:

                Code = {
                        'altid'         : altid,                # alt name provided for Code
                        'cs_altid'      : cs_altid,             # alt name for CodeSpace of Code
                        'cs_id'         : code_space_id,        # unique sys id of CodeSpace of Code
                        'action'        : action,               # action to perform for this Code
                        'name'          : altid,                # Name provided for Code
                        'description'   : description,          # Description provided for Code
                        'org_altids'    : [],                   # Code shared across Facilities/Orgs [future?]
                        'id'            : '',                   # marine asset tracking assigned uuid4
                        '_exists'       : False                 # instance exists?
                        }

                """
                # todo discuss orgs and codes
                if item['Facility IDS']:
                    org_ids = self.get_list_csv(item['Facility IDS'])
                    Code['org_altids'] = org_ids[:]
                """

            Codes[Code['altid']] = Code

            cnt += 1

        return Codes

    def _create_code_space(self, wo_code_space):
        """Create CodeSpace instance based on information in working object.

        @param  wo_code_space   {}      # CodeSpace working object
        @retval code_space_id   str     # id of CodeSpace created
        @throws BadRequest      'CodeSpace ID is empty; it is required for CodeSpace creation.'
        @throws BadRequest      'failed to create requested code_space_id (code_space_name: %s)'
        """
        if not wo_code_space:
            raise BadRequest('wo_code_space parameter is empty')

        code_space_id = ''
        nicknameCodeSpace = RT.CodeSpace + ':'

        # using wo_code_space, create code_space object and return code_space_id
        if not wo_code_space['altid']:
            raise BadRequest('CodeSpace ID is empty; it is required for CodeSpace creation.')

        if wo_code_space['altid']:
            code_space_name = wo_code_space['altid']
        if wo_code_space['description']:
            description = wo_code_space['description']
            description = description.strip()

        exists = wo_code_space['_exists']

        if (not exists) and code_space_name:
            # create code_space
            ion_obj = IonObject(RT.CodeSpace, name=code_space_name, description=description)
            tmp_alt_id = nicknameCodeSpace + code_space_name
            ion_obj.alt_ids.append(tmp_alt_id)
            ion_obj.codes = {}
            ion_obj.codesets = {}
            code_space_id = self.create_code_space(ion_obj)
            if not code_space_id:
                raise BadRequest('failed to create requested code_space_id (code_space_name: %s)' % code_space_name)
            #cs_obj = self.read_code_space(code_space_id)

        return code_space_id

    def _remove_code_space(self, wo_code_space):
        """Remove CodeSpace instance based on information in working object.
        @param  wo_code_space   {}      # CodeSpace working object
        @retval code_space_id   str     # id of CodeSpace removed
        """
        # todo review governance and impact of codespace deletion
        if not wo_code_space:
            raise BadRequest('wo_code_space parameter is empty')

        if not wo_code_space:
            raise BadRequest('wo_code_space parameter is empty')

        code_space_id = ''
        if wo_code_space['_id']:
            code_space_id = wo_code_space['_id']

        if wo_code_space['_exists']:
            exists = wo_code_space['_exists']

        if code_space_id and exists:
            self.delete_code_space(code_space_id)                # delete code_space

        return code_space_id

    def _update_code_space(self, wo_code_space):
        """Update CodeSpace instance based on information in working object.

        @param  wo_code_space   {}      # CodeSpace working object
        @retval code_space_id   str     # id of CodeSpace updated
        @throws BadRequest      'wo_code_space parameter is empty'
        @throws BadRequest      'unable to read codespace instance (%s)'
        """
        # Note:  wo_code_space keys: ['_exists', 'codesets', 'codes', 'name', 'altid', 'action', '_id', 'description']
        if not wo_code_space:
            raise BadRequest('wo_code_space parameter is empty')

        verbose = False
        code_space_id = ''
        code_space_name = ''
        description = ''
        exists = False
        if wo_code_space['_id']:
            code_space_id = wo_code_space['_id']
        if wo_code_space['name']:
            code_space_name = wo_code_space['name']

        if wo_code_space['_exists'] == True:
            exists = True #wo_code_space['_exists']

        if wo_code_space['description']:
            description = wo_code_space['description']

        if code_space_id and exists:
            try:
                is_dirty = False
                current_codespace = self.read_code_space(code_space_id)
                if not current_codespace:
                    raise BadRequest('unable to read codespace instance (%s)' % code_space_name)
                if description != current_codespace['description']:
                    current_codespace['description'] = description
                    is_dirty = True
                if is_dirty == True:
                    self.update_code_space(current_codespace)
                    #code_space_obj = self.read_code_space(code_space_id)

            except BadRequest, Arguments:
                raise BadRequest(Arguments.get_error_message())

        return code_space_id


    def _process_code_spaces(self, wo_code_spaces, code_space_id):
        """Process code space working objects and create, update and delete CodeSpace instance(s).

        @param  wo_code_spaces      {}      # dict of CodeSpace instances (in namespace RT.CodeSpace)
        @param  code_space_id       str     # unique OOI system assigned uuid4
        @retval code_space_ids      []      # code_space_ids modified (create or update; action 'add')
        @retval del_code_space_ids  []      # list of code_space_ids deleted; using action 'remove'
        @throws BadRequest          'wo_code_spaces parameter is empty'
        @throws BadRequest          'Error processing code space working object; should reflect CodeSpace exists.'
        @throws BadRequest
        """
        if not wo_code_spaces:
            raise BadRequest('wo_code_spaces parameter is empty')
        # NOTE: ALLOW code_space_id to enter as empty value

        verbose = False

        # Note: in future multiple CodeSpaces may exist; using different names
        # Note: in future CodeSpaces may be Org specific or utilize Org information
        code_space_ids = []
        del_code_space_ids = []
        code_space_exists = False
        if code_space_id:
            code_space_exists = True

        if wo_code_spaces:

            for code_space_name, wo_code_space in wo_code_spaces.iteritems():

                if wo_code_space:

                    if wo_code_space['action'] == 'add':

                        if wo_code_space['_exists']:
                            id = self._update_code_space(wo_code_space)
                            if id:
                                wo_code_space['_id'] = id
                                if id not in code_space_ids:
                                    code_space_ids.append(id)
                        else:
                            if code_space_exists:
                                raise BadRequest('Error processing code space working object; should reflect CodeSpace exists.')

                            id = self._create_code_space(wo_code_space)
                            if id:
                                wo_code_space['_id'] = id
                                if id not in code_space_ids:
                                    code_space_ids.append(id)

                    elif wo_code_space['action'] == 'remove':

                        id = self._remove_code_space(wo_code_space)
                        if id:
                            if id not in del_code_space_ids:
                                del_code_space_ids.append(id)

        return code_space_ids, del_code_space_ids

    def _create_code(self, wo_code, code_space_id):
        """Create Code in the CodeSpace identified by code_space_id.

        @param wo_code          {}          # Code working object
        @param code_space_id    str         # CodeSpace id
        @retval code_name       str         # name of Code (name, not id)
        @throws NotFound   object with specified id does not exist
        @throws BadRequest if object does not have _id or _rev attribute
        """
        # create code - generation of id using uuid.uuid4() should change to ooi uui4 hex TODO
        if not wo_code:
            raise BadRequest('wo_code parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')

        code_name = ''
        if wo_code['altid']:
            code_name = wo_code['altid']

        code_description = ''
        if wo_code['description']:
            code_description = wo_code['description']

        try:
            code_space_obj = self.read_code_space(code_space_id)
        except:
            raise BadRequest('Unable to read CodeSpace using id provided; cannot create Code')


        # Check to see if code already exists in CodeSpace.codes; if not create
        if code_name not in code_space_obj.codes.keys():
            code_space_obj.codes[code_name] = IonObject(OT.Code,id=str(uuid.uuid4()), name=code_name, description=code_description)
            self.update_code_space(code_space_obj)

        return code_name


    def _update_code(self, wo_code, code_space_id, code):
        """Update Code in the CodeSpace identified by code_space_id.
        @param wo_code          {}          # Code working object
        @param code_space_id    str         # CodeSpace id
        @param code             OT.Code     # existing Code to be updated
        @retval code_name       str         # name of Code (name not id)
        @throws NotFound   object with specified id does not exist
        @throws BadRequest if object does not have _id or _rev attribute
        """
        if not wo_code:
            raise BadRequest('wo_code parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')
        if not code:
            raise BadRequest('code parameter is empty')

        codes = {}
        dirty = False
        code_name = ''
        if wo_code['altid']:
            code_name = wo_code['altid']
        code_description = ''
        if wo_code['description']:
            code_description = wo_code['description']
        if wo_code['cs_altid']:
            code_space_name = wo_code['cs_altid']

        # Determine field changes
        if code.name != code_name:
            code.name = code_name
            dirty = True
        if code.description != code_description:
            code.description = code_description
            dirty = True

        if dirty:
            codes[code.name] = code
            self.update_codes(code_space_id, codes)

        return code_name

    def _remove_code(self, wo_code, code_space_id):
        """Remove Code in a specific CodeSpace.
        @param wo_code          {}          # Code working object
        @param code_space_id    str         # CodeSpace id
        @retval code_name       str         # name of Code (name not id)
        @throws NotFound   object with specified id does not exist
        @throws BadRequest if object does not have _id or _rev attribute
        """
        if not wo_code:
            raise BadRequest('wo_code parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')

        code_name = ''
        codes = []
        if wo_code['altid']:
            code_name = wo_code['altid']

        codes.append(code_name)
        self.delete_codes(code_space_id, codes)

        return code_name

    def _create_codeset(self, wo_codeset, code_space_id):
        """Create CodeSet in the CodeSpace identified by code_space_id.

        @param wo_codeset       {}          # CodeSet working object
        @param code_space_id    str         # CodeSpace id
        @retval codeset_name    str         # name of CodeSet (OT so no uuid)
        @throws NotFound   object with specified id does not exist
        @throws BadRequest if object does not have _id or _rev attribute
        """
        if not wo_codeset:
            raise BadRequest('wo_codeset parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')

        codeset_name = ''
        if wo_codeset['altid']:
            codeset_name = wo_codeset['altid']

        codeset_description = ''
        if wo_codeset['description']:
            codeset_description = wo_codeset['description']

        exists = wo_codeset['_exists']

        codeset_enumeration = ''
        if wo_codeset['enumeration']:
            codeset_enumeration = wo_codeset['enumeration']

        dirty = False
        # verify all codeset enumeration items are codes in CodeSpace.codes
        # if not, raise NotFound
        if not exists:
            code_space_obj = self.read_code_space(code_space_id)
            good_stuff = True
            if code_space_obj.codes and codeset_enumeration:
                for item in codeset_enumeration:
                    if item not in code_space_obj.codes:
                        raise NotFound('enumeration item (%s) in codeset (%s) not a valid Code' % (item, codeset_name))
                        #good_stuff = False
                        #break
            #else:
            #    log.debug('\n\n[service] not code_space_obj.codes and codeset_enumeration')
            if good_stuff:
                # create code - generation of id using uuid.uuid4() should change todo
                codeset = IonObject(OT.CodeSet,name=codeset_name,description=codeset_description)
                codeset.enumeration = codeset_enumeration
                code_space_obj.codesets[codeset.name] = codeset
                dirty = True

        if dirty:
            self.update_code_space(code_space_obj)

        return codeset_name

    def _update_codeset(self, wo_codeset, code_space_id, codeset):
        """Update CodeSet in a specific CodeSpace identified by code_space_id.

        @param  wo_codeset      {}          # CodeSet working object
        @param  code_space_id   str         # CodeSpace id
        @param  codeset         str         # Current CodeSet in CodeSpace
        @retval codeset_name    str         # name of CodeSet (OT so no uuid, use name)
        @throws BadRequest      'wo_codeset parameter is empty'
        @throws BadRequest      'code_space_id parameter is empty'
        @throws BadRequest      'codeset parameter is empty'
        """
        if not wo_codeset:
            raise BadRequest('wo_codeset parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')
        if not codeset:
            raise BadRequest('codeset parameter is empty')

        # Read CodeSpace; get the codesets
        codesets = {}
        dirty = False
        code_space_codesets = {}

        codeset_name = ''
        if wo_codeset['name']:
            codeset_name = wo_codeset['name']
            codeset_name = codeset_name.strip()

        codeset_description = ''
        if wo_codeset['description']:
            codeset_description = wo_codeset['description']
            codeset_description = codeset_description.strip()

        codeset_enumeration = []
        if wo_codeset['enumeration']:
            codeset_enumeration = wo_codeset['enumeration']

        if codeset.description != codeset_description:
            codeset.description = codeset_description
            dirty = True

        if not codeset.enumeration:
            codeset.enumeration = codeset_enumeration
            dirty = True

        elif codeset.enumeration:
            if (len(codeset_enumeration) != len(codeset.enumeration)):
                codeset.enumeration = codeset_enumeration
                dirty = True
            else:
                for value in codeset.enumeration:
                    if value not in codeset_enumeration:
                        codeset.enumeration = codeset_enumeration
                        dirty = True
                        break

        if dirty:
            codesets[codeset.name] = codeset
            self.update_codesets(code_space_id, codesets)

        return codeset_name

    def _remove_codeset(self, wo_codeset, code_space_id):
        """Remove CodeSet in CodeSpace identified by code_space_id.
        @param wo_codeset       {}          # CodeSet working object
        @param code_space_id    str         # CodeSpace id
        @retval codeset_name    str         # name of CodeSet (OT so no uuid, use name)
        """
        if not wo_codeset:
            raise BadRequest('wo_codeset parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')

        codeset_name = ''
        names = []
        if wo_codeset['altid']:
            codeset_name = wo_codeset['altid']
        if codeset_name:
            names.append(codeset_name)
            self.delete_codesets(code_space_id, names)

        return codeset_name

    def _remove_code_space(self, wo_code_space):
        """Remove CodeSpace.

        @param wo_code_space    {}          # CodeSet working object
        @retval code_space_id   str         # CodeSpace id
        @throws BadRequest      'wo_code_space parameter is empty'
        """
        # todo review code space policy and usage
        # - testing required
        # - deleting CodeSpace has many ramifications which need to be
        # both discussed and documented for and with system engineers.

        if not wo_code_space:
            raise BadRequest('wo_code_space parameter is empty')

        code_space_id = ''
        if wo_code_space['altid']:
            code_space_id = wo_code_space['_id']
            exists = wo_code_space['_exists']
            if code_space_id and exists:
                self.delete_code_space(code_space_id)

        return code_space_id


    def _process_codes(self, wo_codes, code_space_id):
        """Process Code working objects based on action value ('add or 'remove')

        @param wo_codes         {}          # Code working objects
        @param code_space_id    str         # CodeSpace id
        @retval code_ids        []          # list of code names of modified codes
        @retval del_code_ids    []          # list of code names of codes removed
        @throws NotFound   'Unable to update Code %s, does not exist in CodeSpace'
        @throws BadRequest 'wo_codes parameter is empty'
        @throws BadRequest 'code_space_id parameter is empty'
        @throws BadRequest 'failed to read CodeSpace (id: %s); unable to continue processing codes'
        @throws BadRequest 'Failed to update Code \'%s\''
        """
        if not wo_codes:
            raise BadRequest('wo_codes parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')

        code_ids = []
        del_code_ids = []
        code_space_codes = {}
        # read CodeSpace and get codes
        try:
            code_space = self.RR2.read(resource_id=code_space_id, specific_type=RT.CodeSpace)
        except:
            raise BadRequest('failed to read CodeSpace (id: %s); unable to continue processing codes' % code_space_id)

        if code_space['codes']:
            code_space_codes = code_space['codes']

        if wo_codes:

            for code_name, wo_code in wo_codes.iteritems():

                if wo_code:

                    if wo_code['action'] == 'add':

                        if wo_code['_exists']:
                            if code_name:
                                if not code_space_codes:
                                    raise NotFound('Unable to update Code %s, does not exist in CodeSpace' % code_name)
                                else:
                                    if code_name in code_space_codes:
                                        code = code_space_codes[code_name]
                                        id = self._update_code(wo_code, code_space_id, code)
                                        if id:
                                            if id not in code_ids:
                                                code_ids.append(id)
                                        else:
                                            raise BadRequest('Failed to update Code \'%s\'' % code_name)

                        else:
                            id = self._create_code(wo_code, code_space_id)
                            if id:
                                if id not in code_ids:
                                    code_ids.append(id)

                    elif wo_code['action'] == 'remove':

                        if wo_code['_exists']:
                            id = self._remove_code(wo_code, code_space_id)
                            if id:
                                if id not in del_code_ids:
                                    del_code_ids.append(id)

        return code_ids, del_code_ids

    def _process_codesets(self, wo_codesets, code_space_id):
        """Process CodeSet working objects based on action value ('add or 'remove')

        @param wo_codesets      {}          # Code working objects
        @param code_space_id    str         # CodeSpace id
        @retval codeset_ids     []          # list of code names of modified codes
        @retval del_codeset_ids []          # list of code names of codes removed
        @throws BadRequest 'wo_codesets parameter is empty'
        @throws BadRequest 'code_space_id parameter is empty'
        @throws BadRequest 'failed to read CodeSpace (id: %s); unable to continue processing CodeSets'
        @throws NotFound   'unable to update CodeSet %s; does not exist in CodeSpace'
        @throws NotFound   'unable to update CodeSet %s; does not exist in CodeSpace'
        """
        verbose = False

        if not wo_codesets:
            raise BadRequest('wo_codesets parameter is empty')
        if not code_space_id:
            raise BadRequest('code_space_id parameter is empty')

        codeset_names = []
        del_codeset_names = []
        try:
            code_space = self.RR2.read(resource_id=code_space_id, specific_type=RT.CodeSpace)
        except:
            raise BadRequest('failed to read CodeSpace (id: %s); unable to continue processing CodeSets' % code_space_id)

        if code_space['codesets']:
            code_space_codesets = code_space['codesets']

        if wo_codesets:
            for codeset_name, wo_codeset in wo_codesets.iteritems():
                if wo_codeset:
                    if wo_codeset['action']:

                        if wo_codeset['action'] == 'add':
                            if wo_codeset['_exists']:
                                if codeset_name:
                                    if not code_space_codesets:
                                        raise NotFound('unable to update CodeSet %s; does not exist in CodeSpace' % codeset_name)
                                    else:
                                        if codeset_name in code_space_codesets:
                                            codeset = code_space_codesets[codeset_name]
                                            cs_name = self._update_codeset(wo_codeset, code_space_id, codeset)
                                            if cs_name:
                                                if cs_name not in codeset_names:
                                                    codeset_names.append(cs_name)
                                        else:
                                            raise NotFound('unable to update CodeSet %s; does not exist in CodeSpace' % codeset_name)

                            else:

                                cs_name = self._create_codeset(wo_codeset, code_space_id)
                                if cs_name:
                                    if cs_name not in codeset_names:
                                        codeset_names.append(cs_name)

                        elif wo_codeset['action'] == 'remove':

                            if wo_codeset['_exists']:
                                cs_name = self._remove_codeset(wo_codeset, code_space_id)
                                if cs_name:
                                    if cs_name not in del_codeset_names:
                                        del_codeset_names.append(cs_name)

        return codeset_names, del_codeset_names

    def _get_code_space_id(self, code_space_name):
        """If CodeSpace exists, return id.

        @param  code_space_name       str         # marine asset management code space name ('MAM')
        @retval code_space_id         str         # unique sys uuid4 of code space, else ''
        @throws BadRequest  'code_space_name parameter is empty'
        @throws BadRequest  'unable to locate CodeSpace instance (named \'%s\')'
        """
        if not code_space_name:
            raise BadRequest('code_space_name parameter is empty')

        code_space_id = ''
        try:
            res_objs, res_keys = self.container.resource_registry.find_resources_ext(alt_id_ns=RT.CodeSpace,
                                        alt_id=code_space_name, id_only=False)
            if res_keys:
                if len(res_keys) == 1:
                    code_space_id = res_keys[0]['id']
                    key = res_keys[0]

        except BadRequest, Arguments:
            raise BadRequest(Arguments.get_error_message())
        except:
            raise BadRequest('unable to locate CodeSpace instance (named \'%s\')' % code_space_name)

        return code_space_id

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # (END) private [supporting] source for public services
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    ############################
    #
    #  END - MAM
    #
    ############################

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

        @param org_id    str
        @param ext_associations    dict
        @param ext_exclude    list
        @retval observatory    ObservatoryExtension
        @throws BadRequest    A parameter is missing
        @throws NotFound    An object with the specified observatory_id does not exist
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
