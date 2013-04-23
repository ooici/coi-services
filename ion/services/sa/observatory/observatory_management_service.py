#!/usr/bin/env python

"""Observatory Management Service to keep track of observatories sites, logical platform sites, instrument sites,
and the relationships between them"""

import time
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
from interface.objects import OrgTypeEnum, ComputedValueAvailability, ComputedIntValue, StatusType

from ion.util.related_resources_crawler import RelatedResourcesCrawler



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
                                             name='Observatory Operator',   #previously Instrument Operator
                                             description='Operate and post events related to Observatory Platforms and Instruments')
        self.clients.org_management.add_user_role(org_id, instrument_operator_role)
        observatory_operator_role = IonObject(RT.UserRole,
                                             governance_name=OBSERVATORY_OPERATOR_ROLE,
                                             name='Observatory Manager',   # previously Observatory Operator
                                             description='Change Observatory configuration, post Site-related events')
        self.clients.org_management.add_user_role(org_id, observatory_operator_role)
        data_operator_role = IonObject(RT.UserRole,
                                       governance_name=DATA_OPERATOR_ROLE,
                                       name='Observatory Data Operator',  # previously Data Operator
                                       description='Manipulate and post events related to Observatory Data products')
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
        log.debug("Activing deployment '%s' (%s)", depl_obj.name, deployment_id)

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
        if include_status:
            status_rollups = self.outil.get_status_roll_ups(parent_resource_id, parent_resource.type_, include_structure=True)
            struct_dict = status_rollups.pop("_system") if "_system" in status_rollups else {}

            result_dict["site_resources"] = struct_dict.get("sites", {})
            result_dict["site_children"] = struct_dict.get("ancestors", {})
            if include_devices:
                site_devices = struct_dict.get("devices", {})
                result_dict["site_devices"] = site_devices
                device_ids = [tuple_list[0][1] for tuple_list in site_devices.values() if tuple_list]
                device_objs = self.RR.read_mult(device_ids)
                result_dict["device_resources"] = dict(zip(device_ids, device_objs))
            result_dict["site_status"] = status_rollups

        else:
            site_resources, site_children = self.outil.get_child_sites(site_id, org_id, include_parents=True, id_only=False)
            result_dict["site_resources"] = site_resources
            result_dict["site_children"] = site_children

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
        """Returns an InstrumentDeviceExtension object containing additional related information

        @param site_id    str
        @param ext_associations    dict
        @param ext_exclude    list
        @retval observatory    ObservatoryExtension
        @throws BadRequest    A parameter is missing
        @throws NotFound    An object with the specified observatory_id does not exist
        """

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

        # Get status of Site instruments.
        a, b =  self._get_instrument_states(extended_site.instrument_devices)
        extended_site.instruments_operational, extended_site.instruments_not_operational = a, b

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


        # Status computation
        extended_site.computed.instrument_status = [self.agent_status_builder.get_aggregate_status_of_device(idev._id, "aggstatus")
                                                    for idev in extended_site.instrument_devices]
        extended_site.computed.platform_status   = [self.agent_status_builder.get_aggregate_status_of_device(pdev._id, "aggstatus")
                                                    for pdev in extended_site.platform_devices]

#            self.agent_status_builder.add_device_aggregate_status_to_resource_extension(device_id,
#                                                                                    'aggstatus',
#                                                                                    extended_site)
        def status_unknown():
            return ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=StatusType.STATUS_UNKNOWN)
        extended_site.computed.communications_status_roll_up = status_unknown()
        extended_site.computed.power_status_roll_up          = status_unknown()
        extended_site.computed.data_status_roll_up           = status_unknown()
        extended_site.computed.location_status_roll_up       = status_unknown()
        extended_site.computed.aggregated_status             = status_unknown()

        extended_site.computed.site_status = [StatusType.STATUS_UNKNOWN] * len(extended_site.sites)



        return extended_site, RR2


    def _get_site_extension_plus(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        # the "plus" means "plus all sub-site objects"

        extended_site, RR2 = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)

        # use the related resources crawler
        finder = RelatedResourcesCrawler()
        get_assns = finder.generate_related_resources_partial(RR2, [PRED.hasSite])
        full_crawllist = [RT.InstrumentSite, RT.PlatformSite, RT.Subsite]
        search_down = get_assns({PRED.hasSite: (True, False)}, full_crawllist)

        # the searches return a list of association objects, so compile all the ids by extracting them
        subsite_ids = set([])

        # we want only those IDs that are not the input resource id
        for a in search_down(site_id, -1):
            if a.o != site_id:
                subsite_ids.add(a.o)

        log.trace("converting retrieved ids to objects = %s" % subsite_ids)
        subsite_objs = RR2.read_mult(list(subsite_ids))

        # filtered subsites
        def fs(resource_type, filter_fn):
            both = lambda s: ((resource_type == s._get_type()) and filter_fn(s))
            return filter(both, subsite_objs)

        def pfs(filter_fn):
            return fs(RT.PlatformSite, filter_fn)

        def ifs(filter_fn):
            return fs(RT.InstrumentSite, filter_fn)

        extended_site.computed.platform_station_sites = pfs(lambda s: "StationSite" == s.alt_resource_type)
        extended_site.computed.platform_component_sites = pfs(lambda s: "PlatformComponentSite" == s.alt_resource_type)
        extended_site.computed.platform_assembly_sites = pfs(lambda s: "PlatformAssemblySite" == s.alt_resource_type)
        extended_site.computed.instrument_sites = ifs(lambda _: True)

        return extended_site, RR2, subsite_objs

    # TODO: will remove this one
    def get_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        extended_site, _ = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)
        return extended_site

    def get_observatory_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        extended_site, RR2, subsite_objs = self._get_site_extension_plus(site_id, ext_associations, ext_exclude, user_id)
        return extended_site


    def get_platform_station_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        extended_site, RR2, subsite_objs = self._get_site_extension_plus(site_id, ext_associations, ext_exclude, user_id)
        return extended_site


    def get_platform_assembly_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        extended_site, RR2, subsite_objs = self._get_site_extension_plus(site_id, ext_associations, ext_exclude, user_id)
        return extended_site

    def get_platform_component_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        extended_site, RR2, subsite_objs = self._get_site_extension_plus(site_id, ext_associations, ext_exclude, user_id)
        return extended_site


    def get_instrument_site_extension(self, site_id='', ext_associations=None, ext_exclude=None, user_id=''):
        extended_site, _ = self._get_site_extension(site_id, ext_associations, ext_exclude, user_id)

        # no subsites of instrument, so shortcut
        extended_site.computed.platform_station_sites = []
        extended_site.computed.platform_component_sites = []
        extended_site.computed.platform_assembly_sites = []
        extended_site.computed.instrument_sites = []

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
            log.debug('get_deployment_extension  obj:   %s', subj)
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
            log.debug('get_deployment_extension  assoc subj:   %s  pred: %s    obj:   %s', assoc.s, assoc.p, assoc.o)
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