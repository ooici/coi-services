#!/usr/bin/env python

"""Observatory Management Service to keep track of observatories sites, logical platform sites, instrument sites,
and the relationships between them"""

import time


from pyon.core.exception import NotFound, BadRequest, Inconsistent
from pyon.public import CFG, IonObject, RT, PRED, LCS, LCE, OT
from pyon.ion.resource import ExtendedResourceContainer
from pyon.util.containers import create_unique_identifier
from pyon.agent.agent import ResourceAgentState

from ooi.logging import log

from ion.services.sa.observatory.observatory_impl import ObservatoryImpl
from ion.services.sa.observatory.subsite_impl import SubsiteImpl
from ion.services.sa.observatory.platform_site_impl import PlatformSiteImpl
from ion.services.sa.observatory.instrument_site_impl import InstrumentSiteImpl
from ion.services.sa.observatory.observatory_util import ObservatoryUtil

#for logical/physical associations, it makes sense to search from MFMS
from ion.services.sa.instrument.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.instrument.platform_device_impl import PlatformDeviceImpl

from interface.services.sa.iobservatory_management_service import BaseObservatoryManagementService
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.objects import OrgTypeEnum, ComputedValueAvailability, ComputedIntValue

from ion.util.related_resources_crawler import RelatedResourcesCrawler

import constraint

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
#                             self.observatory.policy_fn_lcs_precondition("observatory_id"))
#            reg_precondition(self, 'execute_subsite_lifecycle',
#                             self.subsite.policy_fn_lcs_precondition("subsite_id"))
#            reg_precondition(self, 'execute_platform_site_lifecycle',
#                             self.platform_site.policy_fn_lcs_precondition("platform_site_id"))
#            reg_precondition(self, 'execute_instrument_site_lifecycle',
#                             self.instrument_site.policy_fn_lcs_precondition("instrument_site_id"))


    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        #shortcut names for the import sub-services
        if hasattr(new_clients, "resource_registry"):
            self.RR    = new_clients.resource_registry
            
        if hasattr(new_clients, "instrument_management"):
            self.IMS   = new_clients.instrument_management

        if hasattr(new_clients, "data_process_management"):
            self.PRMS  = new_clients.data_process_management

        #farm everything out to the impls

        self.observatory      = ObservatoryImpl(new_clients)
        self.subsite          = SubsiteImpl(new_clients)
        self.platform_site    = PlatformSiteImpl(new_clients)
        self.instrument_site  = InstrumentSiteImpl(new_clients)

        self.instrument_device   = InstrumentDeviceImpl(new_clients)
        self.platform_device     = PlatformDeviceImpl(new_clients)
        self.dataproductclient = DataProductManagementServiceClient()
        self.dataprocessclient = DataProcessManagementServiceClient()



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
                                             name=INSTRUMENT_OPERATOR_ROLE,
                                             label='Observatory Operator',   #previously Instrument Operator
                                             description='Operate and post events related to Observatory Platforms and Instruments')
        self.clients.org_management.add_user_role(org_id, instrument_operator_role)
        observatory_operator_role = IonObject(RT.UserRole,
                                              name=OBSERVATORY_OPERATOR_ROLE,
                                             label='Observatory Manager',   # previously Observatory Operator
                                             description='Change Observatory configuration, post Site-related events')
        self.clients.org_management.add_user_role(org_id, observatory_operator_role)
        data_operator_role = IonObject(RT.UserRole,
                                       name=DATA_OPERATOR_ROLE,
                                       label='Observatory Data Operator',  # previously Data Operator
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

        # create the marine facility
        observatory_id = self.observatory.create_one(observatory)

        if org_id:
            self.assign_resource_to_observatory_org(observatory_id, org_id)

        return observatory_id

    def read_observatory(self, observatory_id=''):
        """Read a Observatory resource

        @param observatory_id    str
        @retval observatory    Observatory
        @throws NotFound    object with specified id does not exist
        """
        return self.observatory.read_one(observatory_id)

    def update_observatory(self, observatory=None):
        """Update a Observatory resource

        @param observatory    Observatory
        @throws NotFound    object with specified id does not exist
        """
        return self.observatory.update_one(observatory)

    def delete_observatory(self, observatory_id=''):
        """Delete a Observatory resource

        @param observatory_id    str
        @throws NotFound    object with specified id does not exist
        """
        return self.observatory.delete_one(observatory_id)

    def force_delete_observatory(self, observatory_id=''):
        return self.observatory.force_delete_one(observatory_id)



    def create_subsite(self, subsite=None, parent_id=''):
        """Create a Subsite resource. A subsite is a frame of reference within an observatory. Its parent is
        either the observatory or another subsite.

        @param subsite    Subsite
        @param parent_id    str
        @retval subsite_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        subsite_id = self.subsite.create_one(subsite)

        if parent_id:
            self.subsite.link_parent(subsite_id, parent_id)

        return subsite_id

    def read_subsite(self, subsite_id=''):
        """Read a Subsite resource

        @param subsite_id    str
        @retval subsite    Subsite
        @throws NotFound    object with specified id does not exist
        """
        return self.subsite.read_one(subsite_id)

    def update_subsite(self, subsite=None):
        """Update a Subsite resource

        @param subsite    Subsite
        @throws NotFound    object with specified id does not exist
        """
        return self.subsite.update_one(subsite)

    def delete_subsite(self, subsite_id=''):
        """Delete a subsite resource, removes assocations to parents

        @param subsite_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.subsite.delete_one(subsite_id)

    def force_delete_subsite(self, subsite_id=''):
        self.subsite.force_delete_one(subsite_id)



    def create_platform_site(self, platform_site=None, parent_id=''):
        """Create a PlatformSite resource. A platform_site is a frame of reference within an observatory. Its parent is
        either the observatory or another platform_site.

        @param platform_site    PlatformSite
        @param parent_id    str
        @retval platform_site_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        platform_site_id = self.platform_site.create_one(platform_site)

        if parent_id:
            self.platform_site.link_parent(platform_site_id, parent_id)

        return platform_site_id

    def read_platform_site(self, platform_site_id=''):
        """Read a PlatformSite resource

        @param platform_site_id    str
        @retval platform_site    PlatformSite
        @throws NotFound    object with specified id does not exist
        """
        return self.platform_site.read_one(platform_site_id)

    def update_platform_site(self, platform_site=None):
        """Update a PlatformSite resource

        @param platform_site    PlatformSite
        @throws NotFound    object with specified id does not exist
        """
        return self.platform_site.update_one(platform_site)

    def delete_platform_site(self, platform_site_id=''):
        """Delete a PlatformSite resource, removes assocations to parents

        @param platform_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.platform_site.delete_one(platform_site_id)

    def force_delete_platform_site(self, platform_site_id=''):
        self.platform_site.force_delete_one(platform_site_id)


    def create_instrument_site(self, instrument_site=None, parent_id=''):
        """Create a InstrumentSite resource. A instrument_site is a frame of reference within an observatory. Its parent is
        either the observatory or another instrument_site.

        @param instrument_site    InstrumentSite
        @param parent_id    str
        @retval instrument_site_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        instrument_site_id = self.instrument_site.create_one(instrument_site)

        if parent_id:
            self.instrument_site.link_parent(instrument_site_id, parent_id)

        return instrument_site_id

    def read_instrument_site(self, instrument_site_id=''):
        """Read a InstrumentSite resource

        @param instrument_site_id    str
        @retval instrument_site    InstrumentSite
        @throws NotFound    object with specified id does not exist
        """
        return self.instrument_site.read_one(instrument_site_id)

    def update_instrument_site(self, instrument_site=None):
        """Update a InstrumentSite resource

        @param instrument_site    InstrumentSite
        @throws NotFound    object with specified id does not exist
        """
        return self.instrument_site.update_one(instrument_site)

    def delete_instrument_site(self, instrument_site_id=''):
        """Delete a InstrumentSite resource, removes assocations to parents

        @param instrument_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        # todo: give InstrumentSite a lifecycle in COI so that we can remove the "True" argument here
        self.instrument_site.delete_one(instrument_site_id)

    def force_delete_instrument_site(self, instrument_site_id=''):
        self.instrument_site.force_delete_one(instrument_site_id)



    #todo: convert to resource_impl

    def create_deployment(self, deployment=None, site_id="", device_id=""):
        """
        Create a Deployment resource. Represents a (possibly open-ended) time interval
        grouping one or more resources within a given context, such as an instrument
        deployment on a platform at an observatory site.
        """

        deployment_id, version = self.clients.resource_registry.create(deployment)

        #Verify that site and device exist, add links if they do
        if site_id:
            site_obj = self.clients.resource_registry.read(site_id)
            if site_obj:
                self.clients.resource_registry.create_association(site_id, PRED.hasDeployment, deployment_id)

        if device_id:
            device_obj = self.clients.resource_registry.read(device_id)
            if device_obj:
                self.clients.resource_registry.create_association(device_id, PRED.hasDeployment, deployment_id)

        return deployment_id

    def update_deployment(self, deployment=None):
        # Overwrite Deployment object
        self.clients.resource_registry.update(deployment)

    def read_deployment(self, deployment_id=''):
        # Read Deployment object with _id matching id
        log.debug("Reading Deployment object id: %s", deployment_id)
        deployment_obj = self.clients.resource_registry.read(deployment_id)

        return deployment_obj

    def delete_deployment(self, deployment_id=''):
        """
        Delete a Deployment resource
        """
        #Verify that the deployment exist
        deployment_obj = self.clients.resource_registry.read(deployment_id)
        if not deployment_obj:
            raise NotFound("Deployment %s does not exist" % deployment_id)

        # Remove the link between the Stream Definition resource and the Data Process Definition resource
        associations = self.clients.resource_registry.find_associations(None, PRED.hasDeployment, deployment_id, id_only=True)
        if not associations:
            raise NotFound("No Sites or Devices associated with this Deployment identifier " + str(deployment_id))
        for association in associations:
            self.clients.resource_registry.delete_association(association)

        # Delete the deployment
        self.clients.resource_registry.retire(deployment_id)

    def force_delete_deployment(self, deployment_id=''):
        self.clients.resource_registry.delete(deployment_id)


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
        parent_site_obj = self.subsite.read_one(parent_site_id)
        parent_site_type = parent_site_obj._get_type()

        if RT.Observatory == parent_site_type:
            self.observatory.link_site(parent_site_id, child_site_id)
        elif RT.Subsite == parent_site_type:
           self.subsite.link_site(parent_site_id, child_site_id)
        elif RT.PlatformSite == parent_site_type:
           self.platform_site.link_site(parent_site_id, child_site_id)
        else:
           raise BadRequest("Tried to assign a child site to a %s resource" % parent_site_type)

    def unassign_site_from_site(self, child_site_id='', parent_site_id=''):
        """Disconnects a child site (any subtype) from a parent site (any subtype)

        @param child_site_id    str
        @param parent_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        parent_site_obj = self.subsite.read_one(parent_site_id)
        parent_site_type = parent_site_obj._get_type()

        if RT.Observatory == parent_site_type:
            self.observatory.unlink_site(parent_site_id, child_site_id)
        elif RT.Subsite == parent_site_type:
            self.subsite.unlink_site(parent_site_id, child_site_id)
        elif RT.PlatformSite == parent_site_type:
            self.platform_site.unlink_site(parent_site_id, child_site_id)
        else:
            raise BadRequest("Tried to unassign a child site from a %s resource" % parent_site_type)

    def assign_device_to_site(self, device_id='', site_id=''):
        """Connects a device (any type) to a site (any subtype)

        @param device_id    str
        @param site_id    str
        @throws NotFound    object with specified id does not exist
        """
        site_obj = self.subsite.read_one(site_id)
        site_type = site_obj._get_type()

        if RT.PlatformSite == site_type:
            self.platform_site.link_device(site_id, device_id)
        elif RT.InstrumentSite == site_type:
            self.instrument_site.link_device(site_id, device_id)
        else:
            raise BadRequest("Tried to assign a device to a %s resource" % site_type)

    def unassign_device_from_site(self, device_id='', site_id=''):
        """Disconnects a device (any type) from a site (any subtype)

        @param device_id    str
        @param site_id    str
        @throws NotFound    object with specified id does not exist
        """
        site_obj = self.subsite.read_one(site_id)
        site_type = site_obj._get_type()

        if RT.PlatformSite == site_type:
           self.platform_site.unlink_device(site_id, device_id)
        elif RT.InstrumentSite == site_type:
           self.instrument_site.unlink_device(site_id, device_id)
        else:
           raise BadRequest("Tried to unassign a device from a %s resource" % site_type)

    def assign_site_to_observatory(self, site_id='', observatory_id=''):
        self.observatory.link_site(observatory_id, site_id)

    def unassign_site_from_observatory(self, site_id="", observatory_id=''):
        self.observatory.unlink_site(observatory_id, site_id)

    def assign_instrument_model_to_instrument_site(self, instrument_model_id='', instrument_site_id=''):
        self.instrument_site.link_model(instrument_site_id, instrument_model_id)

    def unassign_instrument_model_from_instrument_site(self, instrument_model_id='', instrument_site_id=''):
        self.instrument_site.unlink_model(instrument_site_id, instrument_model_id)

    def assign_platform_model_to_platform_site(self, platform_model_id='', platform_site_id=''):
        self.platform_site.link_model(platform_site_id, platform_model_id)

    def unassign_platform_model_from_platform_site(self, platform_model_id='', platform_site_id=''):
        self.platform_site.unlink_model(platform_site_id, platform_model_id)

    def assign_resource_to_observatory_org(self, resource_id='', org_id=''):
        if not org_id:
            raise BadRequest("Org id not given")
        if not resource_id:
            raise BadRequest("Resource id not given")

        log.debug("assign_resource_to_observatory_org: org_id=%s, resource_id=%s ", org_id, resource_id)
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
        self.instrument_site.link_deployment(instrument_site_id, deployment_id)

    def undeploy_instrument_site(self, instrument_site_id='', deployment_id=''):
        self.instrument_site.unlink_deployment(instrument_site_id, deployment_id)

    def deploy_platform_site(self, platform_site_id='', deployment_id=''):
        self.platform_site.link_deployment(platform_site_id, deployment_id)

    def undeploy_platform_site(self, platform_site_id='', deployment_id=''):
        self.platform_site.unlink_deployment(platform_site_id, deployment_id)


    def create_site_data_product(self, site_id="", data_product_id=""):
        # verify that both exist
        site_obj = self.RR.read(site_id)
        self.RR.read(data_product_id)

        sitetype = type(site_obj).__name__

        if not (RT.InstrumentSite == sitetype or RT.PlatformSite == sitetype):
            raise BadRequest("Can't associate a data product to a %s" % sitetype)

        # validation
        prods, _ = self.RR.find_objects(site_id, PRED.hasOutputProduct, RT.DataProduct)
        if 0 < len(prods):
            raise BadRequest("%s '%s' already has an output data product" % (sitetype, site_id))

        sites, _ = self.RR.find_subjects(sitetype, PRED.hasOutputProduct, data_product_id)
        if 0 < len(sites):
            raise BadRequest("DataProduct '%s' is already an output product of a %s" % (data_product_id, sitetype))

        #todo: re-use existing defintion?  how?

        #-------------------------------
        # Process Definition
        #-------------------------------
#        process_definition = ProcessDefinition()
#        process_definition.name = 'SiteDataProduct'
#        process_definition.description = site_id
#
#        process_definition.executable = {'module':'ion.processes.data.transforms.logical_transform', 'class':'logical_transform'}
#
#        process_dispatcher = ProcessDispatcherServiceClient()
#        process_definition_id = process_dispatcher.create_process_definition(process_definition=process_definition)

#        subscription = self.clients.pubsub_management.read_subscription(subscription_id = in_subscription_id)
#        queue_name = subscription.exchange_name
#
#
#        configuration = DotDict()
#
#        configuration['process'] = dict({
#            'output_streams' : [stream_ids[0]],
#            'publish_streams': {data_product_id: }
#        })
#
#        # ------------------------------------------------------------------------------------
#        # Process Spawning
#        # ------------------------------------------------------------------------------------
#        # Spawn the process
#        process_dispatcher.schedule_process(
#            process_definition_id=process_definition_id,
#            configuration=configuration
#        )
#
#        ###########

        #----------------------------------------------------------------------------------------------------
        # Create a data process definition
        #----------------------------------------------------------------------------------------------------

        dpd_obj = IonObject(RT.DataProcessDefinition,
            name= create_unique_identifier(prefix='SiteDataProduct'), #as per Maurice.  todo: constant?
            description=site_id,    #as per Maurice.
            module='ion.processes.data.transforms.logical_transform',
            class_name='logical_transform')


        data_process_def_id = self.dataprocessclient.create_data_process_definition(dpd_obj)

        #----------------------------------------------------------------------------------------------------
        # Create a data process
        #----------------------------------------------------------------------------------------------------
        data_process_id = self.dataprocessclient.create_data_process(data_process_def_id,
                                                                     None,
                                                                     {"logical":data_product_id})

        self.dataprocessclient.activate_data_process(data_process_id)

        #make it all happen
        if RT.InstrumentSite == sitetype:
            self.instrument_site.link_output_product(site_id, data_product_id)
        elif RT.PlatformSite == sitetype:
            self.platform_site.link_output_product(site_id, data_product_id)


    def streamdef_of_site(self, site_id):
        """
        return the streamdef associated with the output product of a site
        """

        #assume we've previously validated that the site has 1 product
        p, _ = self.RR.find_objects(site_id, PRED.hasOutputProduct, RT.DataProduct, True)
        streams, _ = self.RR.find_objects(p[0], PRED.hasStream, RT.Stream, True)
        if 1 != len(streams):
            raise BadRequest("Expected 1 stream on DataProduct '%s', got %d" % (p[0], len(streams)))
        sdefs, _ = self.RR.find_objects(streams[0], PRED.hasStreamDefinition, RT.StreamDefinition, True)
        if 1 != len(sdefs):
            raise BadRequest("Expected 1 streamdef on StreamDefinition '%s', got %d" % (streams[0], len(sdefs)))

        return sdefs[0]


    def streamdefs_of_device(self, device_id):
        """
        return a dict of streamdef_id => stream_id for a given device
        """

        assert(type("") == type(device_id))

        #recursive function to get all data producers
        def child_data_producers(dpdc_ids):
            def cdp_helper(acc2, dpdc_id2):
                children, _ = self.RR.find_subjects(RT.DataProducer, PRED.hasParent, dpdc_id2, True)
                for child in children:
                    acc2.append(child)
                    acc2 = cdp_helper(acc2, child)
                return acc

            #call helper using input list of data products
            acc = []
            for d in dpdc_ids:
                acc = cdp_helper(acc, d)
            return acc

        #initial list of data producers
        pdcs, _ = self.RR.find_objects(device_id, PRED.hasDataProducer, RT.DataProducer, True)
        if 0 == len(pdcs):
            raise BadRequest("Expected data producer(s) on device '%s', got none" % device_id)

        #now the full list of data producers, with children
        pdcs = child_data_producers(pdcs)
        log.debug("Got %s data producers", len(pdcs))

        streamdefs = {}
        for pdc in pdcs:
            log.debug("Checking data producer %s", pdc)
            prods, _ = self.RR.find_subjects(RT.DataProduct, PRED.hasDataProducer, pdc, True)
            for p in prods:
                log.debug("Checking product %s", p)
                streams, _ = self.RR.find_objects(p, PRED.hasStream, RT.Stream, True)
                for s in streams:
                    log.debug("Checking stream %s", s)
                    sdefs, _ = self.RR.find_objects(s, PRED.hasStreamDefinition, RT.StreamDefinition, True)
                    for sd in sdefs:
                        log.debug("Checking streamdef %s", sd)
                        if sd in streamdefs:
                            raise BadRequest("Got a duplicate stream definition stemming from device %s" % device_id)
                        streamdefs[sd] = s

        return streamdefs


    def check_site_for_deployment(self, site_id, site_type, model_type, check_data_products=True):
        assert(type("") == type(site_id))
        assert(type(RT.Resource) == type(site_type) == type(model_type))

        log.debug("checking %s for deployment, will return %s", site_type, model_type)
        # validate and return supported models
        models, _ = self.RR.find_objects(site_id, PRED.hasModel, model_type, True)
        if 1 > len(models):
            raise BadRequest("Expected at least 1 model for %s '%s', got %s" % (site_type, site_id, len(models)))

        if check_data_products:
            log.trace("checking site data products")
            #todo: remove this when platform data products start working
            if site_type != RT.PlatformSite:
                prods, _ = self.RR.find_objects(site_id, PRED.hasOutputProduct, RT.DataProduct, True)
                if 1 != len(prods):
                    raise BadRequest("Expected 1 output data product on %s '%s', got %s" % (site_type,
                                                                                            site_id,
                                                                                            len(prods)))
        log.trace("check_site_for_deployment returning %s models", len(models))

        return models



    def check_device_for_deployment(self, device_id, device_type, model_type):
        assert(type("") == type(device_id))
        assert(type(RT.Resource) == type(device_type) == type(model_type))

        log.trace("checking %s for deployment, will return %s", device_type, model_type)
        # validate and return model
        models, _ = self.RR.find_objects(device_id, PRED.hasModel, model_type, True)
        if 1 != len(models):
            raise BadRequest("Expected 1 model for %s '%s', got %d" % (device_type, device_id, len(models)))
        log.trace("check_device_for_deployment returning 1 model")
        return models[0]

    def check_site_device_pair_for_deployment(self, site_id, device_id, site_type=None, device_type=None):
        assert(type("") == type(site_id) == type(device_id))

        log.debug("checking %s/%s pair for deployment", site_type, device_type)
        #return a pair that should be REMOVED, or None

        if site_type is None:
            site_type = type(self.RR.read(site_id)).__name__

        if device_type is None:
            device_type = type(self.RR.read(device_id)).__name__

        ret = None

        log.trace("checking existing hasDevice links from site")
        devices, _ = self.RR.find_objects(site_id, PRED.hasDevice, device_type, True)
        if 1 < len(devices):
            raise Inconsistent("Found more than 1 hasDevice relationship from %s '%s'" % (site_type, site_id))
        elif 0 < len(devices):
            if devices[0] != device_id:
                ret = (site_id, devices[0])
                log.info("%s '%s' is already hasDevice with a %s", site_type, site_id, device_type)

        return ret

#    def has_matching_streamdef(self, site_id, device_id):
#        if not self.streamdef_of_site(site_id) in self.streamdefs_of_device(device_id):
#            raise BadRequest("No matching streamdefs between %s '%s' and %s '%s'" %
#                             (site_type, site_id, device_type, device_id))

    def collect_deployment_components(self, deployment_id):
        """
        get all devices and sites associated with this deployment and use their ID as a key to list of models
        """
        assert(type("") == type(deployment_id))

        device_models = {}
        site_models = {}

        # significant change-up in how this works.
        #
        # collect all devices in this deployment

        def add_sites(site_ids, site_type, model_type):
            for s in site_ids:
                models = self.check_site_for_deployment(s, site_type, model_type, False)
                if s in site_models:
                    log.warn("Site '%s' was already collected in deployment '%s'", s, deployment_id)
                site_models[s] = models

        def add_devices(device_ids, device_type, model_type):
            for d in device_ids:
                model = self.check_device_for_deployment(d, device_type, model_type)
                if d in device_models:
                    log.warn("Device '%s' was already collected in deployment '%s'", d, deployment_id)
                device_models[d] = model


        def collect_specific_resources(site_type, device_type, model_type):
            # check this deployment -- specific device types -- for validity
            # return a list of pairs (site, device) to be associated
            log.trace("Collecting resources: site=%s device=%s model=%s", site_type, device_type, model_type)
            new_site_ids, _ = self.RR.find_subjects(site_type,
                                                    PRED.hasDeployment,
                                                    deployment_id,
                                                    True)

            new_device_ids, _ = self.RR.find_subjects(device_type,
                                                      PRED.hasDeployment,
                                                      deployment_id,
                                                      True)

            add_sites(new_site_ids, site_type, model_type)
            add_devices(new_device_ids, device_type, model_type)

        # collect platforms, verify that only one platform device exists in the deployment
        collect_specific_resources(RT.PlatformSite, RT.PlatformDevice, RT.PlatformModel)
        if 1 < len(device_models):
            raise BadRequest("Multiple platforms in the same deployment are not allowed")
        elif 0 < len(device_models):
            log.trace("adding devices and sites that are children of platform device / site")
            child_device_objs = self.platform_device.find_stemming_platform_device(device_models.keys()[0])
            child_site_objs = self.find_related_frames_of_reference(site_models.keys()[0],
                [RT.PlatformSite, RT.InstrumentSite])

            child_device_ids = [x._id for x in child_device_objs]
            child_site_ids   = [x._id for x in child_site_objs[RT.InstrumentSite]]

            # IGNORE child platforms
            #  verify that platform site has no sub-platform-sites
            #if 0 < len(child_site_ids[RT.PlatformSite]):
            #    raise BadRequest("Deploying a platform with its own child platform is not allowed")

            #  gather a list of all instrument sites on platform site
            #  gather a list of all instrument devices on platform device
            add_devices(child_device_ids, RT.InstrumentDevice, RT.InstrumentModel)
            add_sites(child_site_ids, RT.InstrumentSite, RT.InstrumentModel)
        else:
            log.warn("0 platforms in deployment being activated")

        collect_specific_resources(RT.InstrumentSite, RT.InstrumentDevice, RT.InstrumentModel)

        return device_models, site_models


    def activate_deployment(self, deployment_id='', activate_subscriptions=False):
        """
        Make the devices on this deployment the primary devices for the sites
        """
        #Verify that the deployment exists
        self.clients.resource_registry.read(deployment_id)

#        if LCS.DEPLOYED == deployment_obj.lcstate:
#            raise BadRequest("This deploment is already active")

        log.trace("activate_deployment about to collect components")
        device_models, site_models = self.collect_deployment_components(deployment_id)
        log.trace("Collected %s device models, %s site models", len(device_models), len(site_models))

        # create a CSP so we can solve it
        problem = constraint.Problem()

        # add variables - the devices to be assigned, and their range (possible sites)
        for device_id in device_models.keys():
            device_model = device_models[device_id]
            possible_sites = [s for s in site_models.keys()
                              if device_model in site_models[s]]
                                    #and self.streamdef_of_site(s) in self.streamdefs_of_device(device_id)]
            problem.addVariable("device_%s" % device_id, possible_sites)

        # add the constraint that all the variables have to pick their own site
        problem.addConstraint(constraint.AllDifferentConstraint(),
            ["device_%s" % device_id for device_id in device_models.keys()])

        # perform CSP solve; this will be a list of solutions, each a dict of var -> value
        solutions = problem.getSolutions()

        def solution_to_string(soln):
            ret = "%s" % type(soln).__name__
            for k, v in soln.iteritems():
                dev_obj = self.RR.read(k)
                site_obj = self.RR.read(v)
                ret = "%s, %s '%s' -> %s '%s'" % (ret, dev_obj._get_type(), k, site_obj._get_type(), v)
            return ret

        if 1 > len(solutions):
            raise BadRequest("The set of devices could not be mapped to the set of sites, based on matching " +
                             "model and streamdefs")
        elif 1 < len(solutions):
            log.warn("Found %d possible ways to map device and site, but just picking the first one", len(solutions))
            log.warn("Here is the %s of all of them:", type(solutions).__name__)
            for i, s in enumerate(solutions):
                log.warn("Option %d: %s" , i+1, solution_to_string(s))
        else:
            log.info("Found one possible way to map devices and sites.  Best case scenario!")

        pairs_add = []
        pairs_rem = []

        #figure out if any of the devices in the new mapping are already mapped and need to be removed
        #then add the new mapping to the output array
        for device_id in device_models.keys():
            site_id = solutions[0]["device_%s" % device_id]
            old_pair = self.check_site_device_pair_for_deployment(site_id, device_id)
            if old_pair:
                pairs_rem.append(old_pair)
            new_pair = (site_id, device_id)
            pairs_add.append(new_pair)

        # process any removals
        for site_id, device_id in pairs_rem:
            log.info("Unassigning hasDevice; device '%s' from site '%s'", device_id, site_id)
            if not activate_subscriptions:
                log.warn("The input to the data product for site '%s' will no longer come from its primary device",
                         site_id)
            self.unassign_device_from_site(device_id, site_id)

        # process the additions
        for site_id, device_id in pairs_add:
            log.info("Setting primary device '%s' for site '%s'", device_id, site_id)
            self.assign_device_to_site(device_id, site_id)
            if activate_subscriptions:
                log.info("Activating subscription as requested")
                self.transfer_site_subscription(site_id)
#
#        self.RR.execute_lifecycle_transition(deployment_id, LCE.DEPLOY)


    def deactivate_deployment(self, deployment_id=''):
        """Remove the primary device designation for the deployed devices at the sites

        @param deployment_id    str
        @throws NotFound    object with specified id does not exist
        @throws BadRequest    if devices can not be undeployed
        """

        #Verify that the deployment exists
        self.clients.resource_registry.read(deployment_id)

#        if LCS.DEPLOYED != deployment_obj.lcstate:
#            raise BadRequest("This deploment is not active")

        # get all associated components
        device_models, site_models = self.collect_deployment_components(deployment_id)

        #must only remove from sites that are not deployed under a different active deployment
        # must only remove devices that are not deployed under a different active deployment
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

        device_ids = filter_alternate_deployments(device_models.keys())
        site_ids   = filter_alternate_deployments(site_models.keys())

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



    def transfer_site_subscription(self, site_id=""):
        """
        Transfer the site subscription to the current hasDevice link
        """

        # get site obj
        log.info('Getting site object: %s', site_id)
        site_obj = self.RR.read(site_id)

        # error if no hasDevice
        devices, _ = self.RR.find_objects(site_id, PRED.hasDevice, None, True)
        if 1 != len(devices):
            raise BadRequest("Expected 1 hasDevice association, got %d" % len(devices))
        device_id = devices[0]

        # get device obj
        device_obj = self.RR.read(device_id)

        # error if models don't match
        site_type = type(site_obj).__name__
        device_type = type(device_obj).__name__
        if RT.InstrumentDevice == device_type:
            model_type = RT.InstrumentModel
        elif RT.PlatformDevice == device_type:
            #model_type = RT.PlatformModel
            #todo: actually transfer the subsription.  for now we abort because there are no platform data products
            return
        else:
            raise BadRequest("Expected a device type, got '%s'" % device_type)

        device_model = self.check_device_for_deployment(device_id, device_type, model_type)
        site_models = self.check_site_for_deployment(site_id, site_type, model_type)
        # commented out as per Maurice, 8/7/12
        device_model, site_models #suppress pyflakes warnings
#        if device_model not in site_models:
#            raise BadRequest("The site and device model types are incompatible")

        # check site/device pair.
        # this function re-checks the association as a side effect, so this error should NEVER happen
        if self.check_site_device_pair_for_deployment(site_id, device_id, site_type, device_type):
            raise BadRequest("Magically and unfortunately, the site and device are no longer associated")

        # get deployments
        depl_site, _ = self.RR.find_objects(site_id, PRED.hasDeployment, RT.Deployment, True)
        depl_dev, _  = self.RR.find_objects(device_id, PRED.hasDeployment, RT.Deployment, True)

        # error if no matching deployments
        found = False
        for ds in depl_site:
            if ds in depl_dev:
                found = True
                break
        if not found:
            raise BadRequest("Site and device do not share a deployment!")

        # check product and process from site
        pduct_ids, _ = self.RR.find_objects(site_id, PRED.hasOutputProduct, RT.DataProduct, True)
        if 1 != len(pduct_ids):
            raise BadRequest("Expected 1 DataProduct associated to site '%s' but found %d" % (site_id, len(pduct_ids)))
        process_ids, _ = self.RR.find_subjects(RT.DataProcess, PRED.hasOutputProduct, pduct_ids[0], True)
        if not process_ids:
            log.info('No DataProcess associated to the data product of this site')
#        if 1 != len(process_ids):
#            raise BadRequest("Expected 1 DataProcess feeding DataProduct '%s', but found %d" %
#                             (pduct_ids[0], len(process_ids)))

        #look up stream defs
        ss = self.streamdef_of_site(site_id)
        ds = self.streamdefs_of_device(device_id)

        if not ss in ds:
            raise BadRequest("Data product(s) of site does not have any matching streamdef for data product of device")
        if process_ids:
            data_process_id = process_ids[0]
            log.info("Changing subscription: %s", data_process_id)
            log.info('ds of ss: %s', ds[ss])
            self.PRMS.update_data_process_inputs(data_process_id, [ds[ss]])

        log.info("Successfully changed subscriptions")





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


        log.debug("converting retrieved ids to objects = %s" % retval_ids)

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





    ############################
    #
    #  EXTENDED RESOURCES
    #
    ############################


    def get_site_extension(self, site_id='', ext_associations=None, ext_exclude=None):
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
            ext_exclude=ext_exclude)

        # Get status of Site instruments.
        extended_site.instruments_operational, extended_site.instruments_not_operational = self._get_instrument_states(extended_site.instrument_devices)

        # Status computation
        extended_site.computed.instrument_status = [4]*len(extended_site.instrument_devices)
        extended_site.computed.platform_status = [4]*len(extended_site.platform_devices)

        extended_site.computed.communications_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=4)
        extended_site.computed.power_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=4)
        extended_site.computed.data_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=4)
        extended_site.computed.location_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=4)
        extended_site.computed.aggregated_status = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=4)

        try:
            status_rollups = self.outil.get_status_roll_ups(site_id, extended_site.resource._get_type())

            extended_site.computed.instrument_status = [status_rollups.get(idev._id,{}).get("agg",4) for idev in extended_site.instrument_devices]
            extended_site.computed.platform_status = [status_rollups.get(pdev._id,{}).get("agg",4) for pdev in extended_site.platform_devices]

            extended_site.computed.communications_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[site_id]["comms"])
            extended_site.computed.power_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[site_id]["power"])
            extended_site.computed.data_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[site_id]["data"])
            extended_site.computed.location_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[site_id]["loc"])
            extended_site.computed.aggregated_status = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[site_id]["agg"])
        except Exception as ex:
            log.exception("Computed attribute failed for %s" % site_id)

        return extended_site


        #Bogus functions for computed attributes
    def get_number_data_sets(self, observatory_id):
        return "0"

    def get_number_instruments_deployed(self, observatory_id):
        return "0"


    def get_number_instruments_operational(self, observatory_id):
        return "0"


    def get_number_instruments_inoperational(self, observatory_id):
        return "0"


    def get_number_instruments(self, observatory_id):
        return "0"


    def get_number_platforms(self, observatory_id):
        return "0"

    def get_number_platforms_deployed(self, observatory_id):
        return "0"

    def _get_instrument_states(self, instrument_device_obj_list=None):

        op = []
        non_op = []
        if instrument_device_obj_list is None:
            instrument_device_list = []

        #call eventsdb to check  data-related events from this device. Use UNix vs NTP tiem for now, as resource timestaps are in Unix, data is in NTP

        now = str(int(time.time() * 1000))
        query_interval = str(int(time.time() - (AGENT_STATUS_EVENT_DELTA_DAYS * 86400) )  *1000)

        for device_obj in instrument_device_obj_list:
            # first check the instrument lifecycle state
            if not ( device_obj.lcstate in [LCS.DEPLOYED_AVAILABLE, LCS.INTEGRATED_DISCOVERABLE] ):
                non_op.append(device_obj)

            else:
                # we dont have a find_events that takes a list yet so loop thru the instruments and get recent events for each.
                events = self.clients.user_notification.find_events(origin=device_obj._id, type= 'ResourceAgentStateEvent', max_datetime = now, min_datetime = query_interval, limit=1)
                # the most recent event is first so assume that is the current state
                if not events:
                    non_op.append(device_obj)
                else:
                    current_instrument_state = events[0].state
                    if current_instrument_state in [ResourceAgentState.STREAMING, ResourceAgentState.CALIBRATE, ResourceAgentState.BUSY, ResourceAgentState.DIRECT_ACCESS]:
                        op.append(device_obj)
                    else:
                        op.append(device_obj)

        return op, non_op

