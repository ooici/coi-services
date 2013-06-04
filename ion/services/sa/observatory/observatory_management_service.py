#!/usr/bin/env python

"""Observatory Management Service to keep track of observatories sites, logical platform sites, instrument sites,
and the relationships between them"""
import string

import time
from ion.services.sa.instrument.rollx_builder import RollXBuilder
from ion.services.sa.instrument.status_builder import AgentStatusBuilder
from ion.services.sa.observatory.deployment_activator import DeploymentActivatorFactory, DeploymentResourceCollectorFactory
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from pyon.core.exception import NotFound, BadRequest
from pyon.public import CFG, IonObject, RT, PRED, LCS, LCE, OT
from pyon.ion.resource import ExtendedResourceContainer
from pyon.agent.agent import ResourceAgentState

from ooi.logging import log


from ion.services.sa.observatory.observatory_util import ObservatoryUtil
from ion.util.geo_utils import GeoUtils

from interface.services.sa.iobservatory_management_service import BaseObservatoryManagementService
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.objects import OrgTypeEnum, ComputedValueAvailability, ComputedIntValue, ComputedListValue, ComputedDictValue, AggregateStatusType, DeviceStatusType
from interface.objects import MarineFacilityOrgExtension, NegotiationStatusEnum, NegotiationTypeEnum, ProposalOriginatorEnum
from collections import defaultdict

from ion.util.related_resources_crawler import RelatedResourcesCrawler

from ion.services.sa.observatory.deployment_util import describe_deployments

INSTRUMENT_OPERATOR_ROLE  = 'INSTRUMENT_OPERATOR'
OBSERVATORY_OPERATOR_ROLE = 'OBSERVATORY_OPERATOR'
DATA_OPERATOR_ROLE        = 'DATA_OPERATOR'
AGENT_STATUS_EVENT_DELTA_DAYS = 5

class ObservatoryManagementService(BaseObservatoryManagementService):


    def on_init(self):
        IonObject("Resource")  # suppress pyflakes error
        CFG, log, RT, PRED, LCS, LCE, NotFound, BadRequest, log  #suppress pyflakes errors about "unused import"

        self.override_clients(self.clients)
        self.outil = ObservatoryUtil(self)
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
            self.RR    = new_clients.resource_registry
            
        if hasattr(new_clients, "instrument_management"):
            self.IMS   = new_clients.instrument_management

        if hasattr(new_clients, "data_process_management"):
            self.PRMS  = new_clients.data_process_management

        #farm everything out to the impls


        self.dataproductclient = DataProductManagementServiceClient()
        self.dataprocessclient = DataProcessManagementServiceClient()

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
        return self.RR2.retire(observatory_id, RT.Observatory)

    def force_delete_observatory(self, observatory_id=''):
        return self.RR2.pluck_delete(observatory_id, RT.Observatory)



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
        self.RR2.retire(subsite_id, RT.Subsite)

    def force_delete_subsite(self, subsite_id=''):
        self.RR2.pluck_delete(subsite_id, RT.Subsite)



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
        self.RR2.retire(platform_site_id, RT.PlatformSite)

    def force_delete_platform_site(self, platform_site_id=''):
        self.RR2.pluck_delete(platform_site_id, RT.PlatformSite)


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
        # todo: give InstrumentSite a lifecycle in COI so that we can remove the "True" argument here
        self.RR2.retire(instrument_site_id, RT.InstrumentSite)

    def force_delete_instrument_site(self, instrument_site_id=''):
        self.RR2.pluck_delete(instrument_site_id, RT.InstrumentSite)



    #todo: convert to resource_impl

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
                self.RR2.assign_deployment_to_site_with_has_deployment(deployment_id, site_id)

        if device_id:

            device_obj = self.RR2.read(device_id)
            if device_obj:
                self.RR2.assign_deployment_to_device_with_has_deployment(deployment_id, device_id)

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

        self.RR2.retire(deployment_id, RT.Deployment)

    def force_delete_deployment(self, deployment_id=''):
        self.RR2.pluck_delete(deployment_id, RT.Deployment)


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
        self.RR2.unassign_instrument_model_from_instrument_site_with_has_model(self, instrument_model_id, instrument_site_id)

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



    def deploy_instrument_site(self, instrument_site_id='', deployment_id=''):
        self.RR2.assign_deployment_to_instrument_site_with_has_deployment(deployment_id, instrument_site_id)

    def undeploy_instrument_site(self, instrument_site_id='', deployment_id=''):
        self.RR2.unassign_deployment_from_instrument_site_with_has_deployment(deployment_id, instrument_site_id)

    def deploy_platform_site(self, platform_site_id='', deployment_id=''):
        self.RR2.assign_deployment_to_platform_site_with_has_deployment(deployment_id, platform_site_id)

    def undeploy_platform_site(self, platform_site_id='', deployment_id=''):
        self.RR2.unassign_deployment_from_platform_site_with_has_deployment(deployment_id, platform_site_id)



    def activate_deployment(self, deployment_id='', activate_subscriptions=False):
        """
        Make the devices on this deployment the primary devices for the sites
        """
        #Verify that the deployment exists
        depl_obj = self.RR2.read(deployment_id)
        log.debug("Activating deployment '%s' (%s)", depl_obj.name, deployment_id)

        deployment_activator_factory = DeploymentActivatorFactory(self.clients)
        deployment_activator = deployment_activator_factory.create(depl_obj)
        deployment_activator.prepare()

        # process any removals
        for site_id, device_id in deployment_activator.hasdevice_associations_to_delete():
            log.info("Unassigning hasDevice; device '%s' from site '%s'", device_id, site_id)
            self.unassign_device_from_site(device_id, site_id)

        # process the additions
        for site_id, device_id in deployment_activator.hasdevice_associations_to_create():
            log.info("Setting primary device '%s' for site '%s'", device_id, site_id)
            self.assign_device_to_site(device_id, site_id)


        #        self.RR.execute_lifecycle_transition(deployment_id, LCE.DEPLOY)


    def deactivate_deployment(self, deployment_id=''):
        """Remove the primary device designation for the deployed devices at the sites

        @param deployment_id    str
        @throws NotFound    object with specified id does not exist
        @throws BadRequest    if devices can not be undeployed
        """

        #Verify that the deployment exists
        deployment_obj = self.RR2.read(deployment_id)

#        if LCS.DEPLOYED != deployment_obj.lcstate:
#            raise BadRequest("This deploment is not active")

        # get all associated components
        collector_factory = DeploymentResourceCollectorFactory(self.clients)
        resource_collector = collector_factory.create(deployment_obj)
        resource_collector.collect()

        # must only remove from sites that are not deployed under a different active deployment
        # must only remove    devices that are not deployed under a different active deployment
        def filter_alternate_deployments(resource_list):
            # return the list of ids for devices or sites not connected to an alternate lcs.deployed deployment
            ret = []
            for r in resource_list:
                depls, _ = self.RR.find_objects(r, PRED.hasDeployment, RT.Deployment)
                keep = True
                for d in depls:
                    if d._id != deployment_id and LCS.DEPLOYED == d.lcstate:
                        keep = False
                if keep:
                    ret.append(r)
            return ret

        device_ids = filter_alternate_deployments(resource_collector.collected_device_ids())
        site_ids   = filter_alternate_deployments(resource_collector.collected_site_ids())

        # delete only associations where both site and device have passed the filter
        for s in site_ids:
            ds, _ = self.RR.find_objects(s, PRED.hasDevice, id_only=True)
            for d in ds:
                if d in device_ids:
                    a = self.RR.get_association(s, PRED.hasDevice, d)
                    self.RR.delete_association(a)
#
#        # mark deployment as not deployed (developed seems appropriate)
#        self.RR.execute_lifecycle_transition(deployment_id, LCE.DEVELOPED)







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


    def find_related_sites(self, parent_resource_id='', exclude_site_types=None, include_parents=False, id_only=False):
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

        site_resources, site_children = self.outil.get_child_sites(site_id, org_id,
                                   exclude_types=exclude_site_types, include_parents=include_parents, id_only=id_only)

        return site_resources, site_children


    def get_sites_devices_status(self, parent_resource_id='', include_devices=False, include_status=False):
        if not parent_resource_id:
            raise BadRequest("Must provide a parent parent_resource_id")

        parent_resource = self.RR.read(parent_resource_id)

        org_id, site_id = None, None
        if parent_resource.type_ == RT.Org:
            org_id = parent_resource_id
        elif RT.Site in parent_resource._get_extends():
            site_id = parent_resource_id

        result_dict = {}

        site_resources, site_children = self.outil.get_child_sites(site_id, org_id, include_parents=True, id_only=False)
        result_dict["site_resources"] = site_resources
        result_dict["site_children"] = site_children
        if include_status:
            RR2 = EnhancedResourceRegistryClient(self.RR)
            RR2.cache_predicate(PRED.hasSite)
            RR2.cache_predicate(PRED.hasDevice)
            #add code to grab the master status table to pass in to the get_status_roll_ups calc
            all_device_statuses = self._get_master_status_table( RR2, site_children.keys())
            log.debug('get_sites_devices_status site master_status_table:   %s ', all_device_statuses)
            result_dict["site_status"] = all_device_statuses

            #create the aggreagate_status for each device and site

            log.debug("calculate site aggregate status")
            site_status = self._get_site_rollup_list(RR2, all_device_statuses, [s for s in site_children.keys()])
            site_status_dict = dict(zip(site_children.keys(), site_status))
            log.debug('get_sites_devices_status  site_status_dict:   %s ', site_status_dict)
            result_dict["site_aggregate_status"] = site_status_dict

            if include_devices:
                log.debug("calculate device aggregate status")
                inst_status = [self.agent_status_builder._crush_status_dict(all_device_statuses.get(k, {}))
                               for k in all_device_statuses.keys()]
                device_agg_status_dict = dict(zip(all_device_statuses.keys(), inst_status))
                log.debug('get_sites_devices_status  device_agg_status_dict:   %s ', device_agg_status_dict)
                result_dict["device_aggregate_status"] = device_agg_status_dict


        return result_dict

    def find_site_data_products(self, parent_resource_id='', include_sites=False, include_devices=False,
                                include_data_products=False):
        if not parent_resource_id:
            raise BadRequest("Must provide a parent parent_resource_id")

        res_dict = self.outil.get_site_data_products(parent_resource_id, include_sites=include_sites,
                                                     include_devices=include_devices,
                                                     include_data_products=include_data_products)

        return res_dict


    ############################
    #
    #  EXTENDED RESOURCES
    #
    ############################


    def _get_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Returns a site extension object containing common information, plus some helper objects

        @param site_id    str
        @param ext_associations    dict
        @param ext_exclude    list
        @retval observatory    ObservatoryExtension
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


            RR2 = EnhancedResourceRegistryClient(self.RR)
            RR2.cache_predicate(PRED.hasModel)

            log.debug("Getting status of Site instruments.")
            a, b =  self._get_instrument_states(extended_site.instrument_devices)
            extended_site.instruments_operational, extended_site.instruments_not_operational = a, b

            log.debug("Building list of model objs")
            # lookup all hasModel predicates
            # lookup is a 2d associative array of [subject type][subject id] -> object id
            lookup = dict([(rt, {}) for rt in [RT.InstrumentDevice, RT.PlatformDevice]])
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

            this_device_id = None
            try:
                this_device_id = RR2.find_object(site_id, predicate=PRED.hasDevice, id_only=True)
            except NotFound:
                pass

            return extended_site, RR2, this_device_id
        except:
            log.error('failed', exc_info=True)
            raise

    def _get_hierarchy_devices_sites(self, a_site_id, RR2):
        log.debug("beginning related resources crawl for subsites")
        finder = RelatedResourcesCrawler()
        get_assns = finder.generate_related_resources_partial(RR2, [PRED.hasSite])
        full_crawllist = [RT.InstrumentSite, RT.PlatformSite, RT.Subsite]
        search_down = get_assns({PRED.hasSite: (True, False)}, full_crawllist)

        # the searches return a list of association objects, so compile all the ids by extracting them
        subsite_ids = set([])

        # we want only those IDs that are not the input resource id
        for a in search_down(a_site_id, -1):
            if a.o != a_site_id:
                subsite_ids.add(a.o)

        subsite_ids = list(subsite_ids)
        log.debug("converting retrieved ids to objects = %s" % subsite_ids)
        subsite_objs = RR2.read_mult(subsite_ids)

        log.debug("building tree of child devices from site")
        all_child_inst_devices = []
        all_child_plat_devices = []
        device_of_site = {}
        for s_id in subsite_ids:
            try:
                device_of_site[s_id] = RR2.find_instrument_device_id_of_instrument_site_using_has_device(s_id)
                all_child_inst_devices.append(device_of_site[s_id])
            except NotFound:
                pass

            try:
                device_of_site[s_id] = RR2.find_platform_device_id_of_platform_site_using_has_device(s_id)
                all_child_plat_devices.append(device_of_site[s_id])
            except NotFound:
                pass

        return subsite_objs, all_child_inst_devices, all_child_plat_devices, device_of_site


    def _get_root_platforms(self, RR2, platform_device_list):
        # get all relevant assocation objects
        filter_fn = lambda a: a.o in platform_device_list

        # get child -> parent dict
        lookup = dict([(a.o, a.s) for a in RR2.filter_cached_associations(PRED.hasDevice, filter_fn)])

        # root platforms have no parent, or a parent that's not in our list
        return [r for r in platform_device_list if (r not in lookup or (lookup[r] not in platform_device_list))]


    def _augment_platformsite_extension(self, extended_site, RR2, platform_device_id):
        log.debug("_augment_platformsite_extension")
        if not RR2.has_cached_predicate(PRED.hasDevice):
            RR2.cache_predicate(PRED.hasDevice)

        site_id = extended_site._id

        # have portals but need to reduce to appropriate subset...
        objects,associations = RR2.find_objects_mult(subjects=[p._id for p in extended_site.sites], id_only=False)
        log.debug('subsites of %s have %d hasDevice associations', site_id, len(associations))
        extended_site.portal_instruments = [None]*len(extended_site.sites)
        for i in xrange(len(extended_site.sites)):
            for o,a in zip(objects,associations):
                if a.p==PRED.hasDevice:
                    if a.ot not in (RT.InstrumentDevice, RT.PlatformDevice):
                        log.warn('unexpected association Site %s hasDevice %s %s (was not InstrumentDevice or PlatformDevice)', a.s, a.ot, a.o)
                    elif a.s==extended_site.sites[i]._id:
                        extended_site.portal_instruments[i] = o

        # prepare to make a lot of rollups
        site_object_dict, site_children = self.outil.get_child_sites(parent_site_id=site_id, id_only=False)
        log.debug("Found these site children: %s", site_children.keys())
        devices_for_status = set(site_children.keys())
        for i in extended_site.portal_instruments:
            if i:
                devices_for_status.add(i._id)
        all_device_statuses = self._get_master_status_table(RR2, devices_for_status)
        log.debug("Found all device statuses: %s", all_device_statuses)

        # portal status rollup
        portal_status = [self.agent_status_builder._crush_status_dict(all_device_statuses.get(k._id, {})) if k else DeviceStatusType.STATUS_UNKNOWN for k in extended_site.portal_instruments]
        extended_site.computed.portal_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=portal_status)

        log.debug("generating site status rollup")
        site_status = self._get_site_rollup_list(RR2, all_device_statuses, [s._id for s in extended_site.sites])
        extended_site.computed.site_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED,
                                                               value=site_status)

        log.debug("generating instrument status rollup") # (is easy)
        inst_status = [self.agent_status_builder._crush_status_dict(all_device_statuses.get(k._id, {}))
                       for k in extended_site.instrument_devices]
        extended_site.computed.instrument_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED,
                                                                     value=inst_status)

        log.debug("generating platform status rollup")
        plat_status = self._get_platform_rollup_list(RR2, all_device_statuses, [s._id for s in extended_site.platform_devices])
        extended_site.computed.platform_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED,
                                                                   value=plat_status)

        log.debug("generating rollup of this site")
        site_aggregate = self._get_site_rollup_dict(RR2, all_device_statuses, site_id)
        self.agent_status_builder.set_status_computed_attributes(extended_site.computed,
                                                                 site_aggregate,
                                                                 ComputedValueAvailability.PROVIDED)

        # filtered subsites
        def fs(resource_type, filter_fn):
            both = lambda s: ((resource_type == s._get_type()) and filter_fn(s))
            return filter(both, site_object_dict.values())

        def pfs(filter_fn):
            return fs(RT.PlatformSite, filter_fn)

        def ifs(filter_fn):
            return fs(RT.InstrumentSite, filter_fn)

        def clv(value):
            return ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=value)

        extended_site.computed.platform_station_sites   = clv(pfs(lambda s: "StationSite" == s.alt_resource_type))
        extended_site.computed.platform_component_sites = clv(pfs(lambda s: "PlatformComponentSite" == s.alt_resource_type))
        extended_site.computed.platform_assembly_sites  = clv(pfs(lambda s: "PlatformAssemblySite" == s.alt_resource_type))
        extended_site.computed.instrument_sites         = clv(ifs(lambda _: True))

        extended_site.deployment_info = describe_deployments(extended_site.deployments, self.clients, instruments=extended_site.instrument_devices, instrument_status=extended_site.computed.instrument_status.value)

    # TODO: will remove this one
    def get_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        # make a very basic determination of what to do
        site_type = self.RR2.read(site_id)._get_type()

        if RT.InstrumentSite == site_type:
            return self.get_instrument_site_extension(site_id, ext_associations, ext_exclude, user_id)

        extended_site, RR2, device_id = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)
        self._augment_platformsite_extension(extended_site, RR2, device_id)
        return extended_site


    def get_observatory_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        extended_site, RR2, device_id = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)
        self._augment_platformsite_extension(extended_site, RR2, device_id)

        return extended_site


    def get_platform_station_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        extended_site, RR2, device_id = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)
        self._augment_platformsite_extension(extended_site, RR2, device_id)
        return extended_site


    def get_platform_assembly_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        extended_site, RR2, device_id = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)
        self._augment_platformsite_extension(extended_site, RR2, device_id)
        return extended_site

    def get_platform_component_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        extended_site, RR2, device_id = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)
        self._augment_platformsite_extension(extended_site, RR2, device_id)
        return extended_site


    def get_instrument_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):

        extended_site, RR2, inst_device_id = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)

        log.debug("Reading status for device '%s'", inst_device_id)
        self.agent_status_builder.add_device_rollup_statuses_to_computed_attributes(inst_device_id,
                                                                                    extended_site.computed,
                                                                                    None)

        instrument_status_list = [self.agent_status_builder.get_aggregate_status_of_device(d._id)
                                  for d in extended_site.instrument_devices]

        def clv(value=None):
            if value is None: value = []
            return ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=value)


        # there are no child sites, and therefore no child statuses
        extended_site.computed.platform_station_sites   = clv()
        extended_site.computed.platform_component_sites = clv()
        extended_site.computed.platform_assembly_sites  = clv()
        extended_site.computed.instrument_sites         = clv()
        extended_site.computed.platform_status          = clv()
        extended_site.computed.site_status              = clv()
        extended_site.computed.instrument_status        = clv(instrument_status_list)

        extended_site.deployment_info = describe_deployments(extended_site.deployments, self.clients, instruments=extended_site.instrument_devices, instrument_status=extended_site.computed.instrument_status.value)

        # have portals but need to reduce to appropriate subset...
        extended_site.portal_instruments = []#*len(extended_site.sites)
        extended_site.computed.portal_status = clv() #ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=portal_status)
        return extended_site


    def _get_instrument_states(self, instrument_device_obj_list=None):

        op = []
        non_op = []
        if instrument_device_obj_list is None:
            instrument_device_list = []

        #call eventsdb to check  data-related events from this device. Use UNix vs NTP tiem for now, as
        # resource timestaps are in Unix, data is in NTP

        now = str(int(time.time() * 1000))
        query_interval = str(int(time.time() - (AGENT_STATUS_EVENT_DELTA_DAYS * 86400) )  *1000)

        for device_obj in instrument_device_obj_list:
            # first check the instrument lifecycle state
#            if not ( device_obj.lcstate in [LCS.DEPLOYED_AVAILABLE, LCS.INTEGRATED_DISCOVERABLE] ):
            # TODO: check that this is the intended lcs behavior and maybe check availability
            if not ( device_obj.lcstate in [LCS.DEPLOYED, LCS.INTEGRATED] ):
                non_op.append(device_obj)

            else:
                # we dont have a find_events that takes a list yet so loop thru the instruments and get
                # recent events for each.
                events = self.clients.user_notification.find_events(origin=device_obj._id,
                                                                    type= 'ResourceAgentStateEvent',
                                                                    max_datetime = now,
                                                                    min_datetime = query_interval,
                                                                    limit=1)
                # the most recent event is first so assume that is the current state
                if not events:
                    non_op.append(device_obj)
                else:
                    current_instrument_state = events[0].state
                    if current_instrument_state in [ResourceAgentState.STREAMING,
                                                    ResourceAgentState.CALIBRATE,
                                                    ResourceAgentState.BUSY,
                                                    ResourceAgentState.DIRECT_ACCESS]:
                        op.append(device_obj)
                    else:
                        op.append(device_obj)

        return op, non_op

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

        devices = set()
        instrument_device_ids = []
        iplatform_device_ids = []
        subjs, _ = self.RR.find_subjects( predicate=PRED.hasDeployment, object=deployment_id, id_only=False)
        for subj in subjs:

            if subj.type_ == "InstrumentDevice":
                extended_deployment.instrument_devices.append(subj)
                devices.add((subj._id, PRED.hasModel))
            elif subj.type_ == "InstrumentSite":
                extended_deployment.instrument_sites.append(subj)
            elif subj.type_ == "PlatformDevice":
                extended_deployment.platform_devices.append(subj)
                devices.add((subj._id, PRED.hasModel))
            elif subj.type_ == "PlatformSite":
                extended_deployment.platform_sites.append(subj)
            else:
                log.warning("get_deployment_extension found invalid type connected to deployment %s. Object details: %s ", deployment_id, subj)

        all_models = set()
        device_to_model_map = {}
        model_map = {}
        assocs = self.RR.find_associations(anyside=list(devices), id_only=False)
        for assoc in assocs:

            all_models.add(assoc.o)
            device_to_model_map[assoc.s] = assoc.o

        model_objs = self.RR.read_mult( list(all_models) )
        for model_obj in model_objs:
            model_map[model_obj._id] = model_obj

        for instrument in extended_deployment.instrument_devices:
            model_id = device_to_model_map[instrument._id]
            extended_deployment.instrument_models.append( model_map[model_id] )

        for platform in extended_deployment.platform_devices:
            model_id = device_to_model_map[platform._id]
            extended_deployment.platform_models.append( model_map[model_id] )

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


        # clients.resource_registry may return us the container's resource_registry instance
        self._rr = self.clients.resource_registry

        # extended object contains list of member actors, so need to change to user info
        actors_list = extended_org.members
        user_list = []
        for actor in actors_list:
            log.debug("get_marine_facility_extension: actor:  %s ", actor)
            user_info_objs, _ = self._rr.find_objects(subject=actor._id, predicate=PRED.hasInfo, object_type=RT.UserInfo, id_only=False)
            if user_info_objs:
                log.debug("get_marine_facility_extension: user_info_obj  %s ", user_info_objs[0])
                user_list.append( user_info_objs[0] )

        extended_org.members = user_list

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

        log.debug("time to make the rollups")
        _, site_children = self.outil.get_child_sites(org_id=org_id, id_only=False)
        all_device_statuses = self._get_master_status_table(RR2, site_children.keys())

        log.debug("site status rollup")
        site_status = self._get_site_rollup_list(RR2, all_device_statuses, [s._id for s in extended_org.sites])
        extended_org.computed.site_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED,
                                                              value=site_status)

        log.debug("instrument status rollup") # (is easy)
        inst_status = [self.agent_status_builder._crush_status_dict(all_device_statuses.get(k._id, {}))
                       for k in extended_org.instruments]
        extended_org.computed.instrument_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED,
                                                                    value=inst_status)

        log.debug("platform status rollup")
        plat_status = self._get_platform_rollup_list(RR2, all_device_statuses, [s._id for s in extended_org.platforms])
        extended_org.computed.platform_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED,
                                                                  value=plat_status)

        log.debug("rollup of this org includes all found devices")
        org_aggregate = {}
        for k, v in AggregateStatusType._str_map.iteritems():
            aggtype_list = [a.get(k, DeviceStatusType.STATUS_UNKNOWN) for a in all_device_statuses.values()]
            org_aggregate[k] = self.agent_status_builder._crush_status_list(aggtype_list)

        self.agent_status_builder.set_status_computed_attributes(extended_org.computed,
                                                                 org_aggregate,
                                                                 ComputedValueAvailability.PROVIDED)

        # station_site is currently all PlatformSites, need to limit to those with alt_resource_type
        subset = []
        for site in extended_org.station_sites:
            if site.alt_resource_type=='StationSite':
                subset.append(site)
        extended_org.station_sites = subset
        station_status = self._get_site_rollup_list(RR2, all_device_statuses, [s._id for s in extended_org.station_sites])
        extended_org.computed.station_status = ComputedListValue(status=ComputedValueAvailability.PROVIDED, value=station_status)

        extended_org.deployment_info = describe_deployments(extended_org.deployments, self.clients, instruments=extended_org.instruments, instrument_status=extended_org.computed.instrument_status.value)
        return extended_org


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
            _, underlings = self.outil.get_child_sites(parent_site_id=s, id_only=True)
            master_status_rollup_list.append(self.agent_status_builder._crush_status_dict(
                self._get_site_rollup_dict(RR2, master_status_table, s)))

        return master_status_rollup_list

    # based on return a site rollup dict corresponding to a site in the site_id_list
    def _get_site_rollup_dict(self, RR2, master_status_table, site_id):

        log.debug("_get_site_rollup_dict for site %s", site_id)
        _, underlings = self.outil.get_child_sites(parent_site_id=site_id, id_only=True)
        site_aggregate = {}
        all_site_ids = underlings.keys()
        all_device_ids = []
        for s in all_site_ids:
            all_device_ids += RR2.find_objects(s, PRED.hasDevice, RT.PlatformDevice, True)
            all_device_ids += RR2.find_objects(s, PRED.hasDevice, RT.InstrumentDevice, True)

        log.debug("Calculating cumulative rollup values for all_device_ids = %s", all_device_ids)
        for k, v in AggregateStatusType._str_map.iteritems():
            aggtype_list = [master_status_table.get(d, {}).get(k, DeviceStatusType.STATUS_UNKNOWN) for d in all_device_ids]
            log.trace("aggtype_list for %s is %s", v, zip(all_device_ids, aggtype_list))
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
