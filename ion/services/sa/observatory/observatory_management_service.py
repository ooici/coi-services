#!/usr/bin/env python

'''
@package ion.services.sa.observatory Implementation of IObservatoryManagementService interface
@file ion/services/sa/observatory/observatory_management_service.py
@author
@brief Observatory Management Service to keep track of observatories sites, logical platform sites, instrument sites,
and the relationships between them
'''




from pyon.core.exception import NotFound, BadRequest, Inconsistent
from pyon.public import CFG, IonObject, log, RT, PRED, LCS, LCE

#from pyon.util.log import log
from ion.services.sa.observatory.observatory_impl import ObservatoryImpl
from ion.services.sa.observatory.subsite_impl import SubsiteImpl
from ion.services.sa.observatory.platform_site_impl import PlatformSiteImpl
from ion.services.sa.observatory.instrument_site_impl import InstrumentSiteImpl

#for logical/physical associations, it makes sense to search from MFMS
from ion.services.sa.instrument.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.instrument.platform_device_impl import PlatformDeviceImpl

from interface.services.sa.iobservatory_management_service import BaseObservatoryManagementService
from interface.objects import OrgTypeEnum

import constraint

INSTRUMENT_OPERATOR_ROLE  = 'INSTRUMENT_OPERATOR'
OBSERVATORY_OPERATOR_ROLE = 'OBSERVATORY_OPERATOR'
DATA_OPERATOR_ROLE        = 'DATA_OPERATOR'



class ObservatoryManagementService(BaseObservatoryManagementService):


    def on_init(self):
        IonObject("Resource")  # suppress pyflakes error
        CFG, log, RT, PRED, LCS, LCE, NotFound, BadRequest, log  #suppress pyflakes errors about "unused import"

        self.override_clients(self.clients)

        self.HIERARCHY_DEPTH = {RT.InstrumentSite: 3,
                                RT.PlatformSite: 2,
                                RT.Subsite: 1,
                                RT.Observatory: 0,
                                }
        
        self.HIERARCHY_LOOKUP = [RT.Observatory, 
                                 RT.Subsite, 
                                 RT.PlatformSite, 
                                 RT.InstrumentSite]



    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        #shortcut names for the import sub-services
        if hasattr(self.clients, "resource_registry"):
            self.RR    = self.clients.resource_registry
            
        if hasattr(self.clients, "instrument_management"):
            self.IMS   = self.clients.instrument_management

        if hasattr(self.clients, "data_process_management"):
            self.PRMS  = self.clients.data_process_management

        #farm everything out to the impls

        self.observatory      = ObservatoryImpl(self.clients)
        self.subsite          = SubsiteImpl(self.clients)
        self.platform_site    = PlatformSiteImpl(self.clients)
        self.instrument_site  = InstrumentSiteImpl(self.clients)

        self.instrument_device   = InstrumentDeviceImpl(self.clients)
        self.platform_device     = PlatformDeviceImpl(self.clients)



    
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
        log.debug("ObservatoryManagementService.create_marine_facility(): %s" % org)
        
        # create the org
        org.org_type = OrgTypeEnum.MARINE_FACILITY
        org_id = self.clients.org_management.create_org(org)

        #Instantiate initial set of User Roles for this marine facility
        instrument_operator_role = IonObject(RT.UserRole, name=INSTRUMENT_OPERATOR_ROLE, 
                                             label='Instrument Operator', description='Marine Facility Instrument Operator')
        self.clients.org_management.add_user_role(org_id, instrument_operator_role)
        observatory_operator_role = IonObject(RT.UserRole, name=OBSERVATORY_OPERATOR_ROLE, 
                                             label='Observatory Operator', description='Marine Facility Observatory Operator')
        self.clients.org_management.add_user_role(org_id, observatory_operator_role)
        data_operator_role = IonObject(RT.UserRole, name=DATA_OPERATOR_ROLE, 
                                             label='Data Operator', description='Marine Facility Data Operator')
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
        log.debug("ObservatoryManagementService.create_virtual_observatory(): %s" % org)

        # create the org
        org.org_type = OrgTypeEnum.VIRTUAL_OBSERVATORY
        org_id = self.clients.org_management.create_org(org)

        return org_id


    def create_observatory(self, observatory=None):
        """Create a Observatory resource. An observatory  is coupled
        with one Org. The Org is created and associated as part of this call.

        @param observatory    Observatory
        @retval observatory_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """

        # create the marine facility
        observatory_id = self.observatory.create_one(observatory)

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



    def create_deployment(self, deployment=None):
        """
        Create a Deployment resource. Represents a (possibly open-ended) time interval
        grouping one or more resources within a given context, such as an instrument
        deployment on a platform at an observatory site.
        """
#
#        #Verify that site and device exist
#        site_obj = self.clients.resource_registry.read(site_id)
#        if not site_obj:
#            raise NotFound("Deployment site %s does not exist" % site_id)
#        device_obj = self.clients.resource_registry.read(device_id)
#        if not device_obj:
#            raise NotFound("Deployment device %s does not exist" % device_id)

        deployment_id, version = self.clients.resource_registry.create(deployment)

#        # Create the links
#        self.clients.resource_registry.create_association(site_id, PRED.hasDeployment, deployment_id)
#        self.clients.resource_registry.create_association(device_id, PRED.hasDeployment, deployment_id)

        return deployment_id

    def update_deployment(self, deployment=None):
        # Overwrite Deployment object
        self.clients.resource_registry.update(deployment)

    def read_deployment(self, deployment_id=''):
        # Read Deployment object with _id matching id
        log.debug("Reading Deployment object id: %s" % deployment_id)
        deployment_obj = self.clients.resource_registry.read(deployment_id)

        return deployment_obj

    def delete_deployment(self, deployment_id=''):
        """
        Delete a Deployment resource
        """
        #Verify that the deployment exist
        deployment_obj = self.clients.resource_registry.read(deployment_id)
        if not deployment_obj:
            raise NotFound("Deployment  %s does not exist" % deployment_id)

        # Remove the link between the Stream Definition resource and the Data Process Definition resource
        associations = self.clients.resource_registry.find_associations(None, PRED.hasDeployment, deployment_id, id_only=True)
        if not associations:
            raise NotFound("No Sites or Devices associated with this Deployment identifier " + str(deployment_id))
        for association in associations:
            self.clients.resource_registry.delete_association(association)

        # Delete the deployment
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

        log.debug("assign_resource_to_observatory_org: org_id=%s, resource_id=%s " % (org_id, resource_id))
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
        data_product_obj = self.RR.read(data_product_id)
        sitetype = type(site_obj).__name__

        if not (RT.InstrumentSite == sitetype or RT.PlatformSite == sitetype):
            raise BadRequest("Can't associate a data product to a %s" % sitetype)

        # validation
        prods, _ = self.RR.find_objects(site_id, PRED.hasOutputProduct, RT.DataProduct)
        if 0 < len(prods):
            raise BadRequest("%s '%s' already has an ouptut data product" % (sitetype, site_id))

        sites, _ = self.RR.find_subjects(sitetype, PRED.hasOutputProduct, data_product_id)
        if 0 < len(sites):
            raise BadRequest("DataProduct '%s' is already an output product of a %s" % (data_product_id, sitetype))

        #todo: re-use existing defintion?  how?
        log.info("Creating data process definition")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='logical_transform',
                            description='send the packet from the in stream to the out stream unchanged',
                            module='ion.processes.data.transforms.logical_transform',
                            class_name='logical_transform',
                            process_source="For %s '%s'" % (sitetype, site_id))

        logical_transform_dprocdef_id = self.PRMS.create_data_process_definition(dpd_obj)

        log.info("Creating data process")
        dproc_id = self.PRMS.create_data_process(logical_transform_dprocdef_id, [], {"output":data_product_id})
        log.info("Created data process")

        log.info("associating site hasOutputProduct")
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
        log.debug("Got %d data producers" % len(pdcs))

        streamdefs = {}
        for pdc in pdcs:
            log.debug("Checking data prodcer %s" % pdc)
            prods, _ = self.RR.find_subjects(RT.DataProduct, PRED.hasDataProducer, pdc, True)
            for p in prods:
                log.debug("Checking product %s" % p)
                streams, _ = self.RR.find_objects(p, PRED.hasStream, RT.Stream, True)
                for s in streams:
                    log.debug("Checking stream %s" % s)
                    sdefs, _ = self.RR.find_objects(s, PRED.hasStreamDefinition, RT.StreamDefinition, True)
                    for sd in sdefs:
                        log.debug("Checking streamdef %s" % sd)
                        if sd in streamdefs:
                            raise BadRequest("Got a duplicate stream definition stemming from device %s" % device_id)
                        streamdefs[sd] = s

        return streamdefs

    def check_site_for_deployment(self, site_id, site_type, model_type):
        log.debug("checking %s for deployment, will return %s" % (site_type, model_type))
        # validate and return supported models
        models, _ = self.RR.find_objects(site_id, PRED.hasModel, model_type, True)
        if 1 > len(models):
            raise BadRequest("Expected at least 1 model for %s '%s', got %d" % (site_type, site_id, len(models)))

        log.debug("checking site data products")
        prods, _ = self.RR.find_objects(site_id, PRED.hasOutputProduct, RT.DataProduct, True)
        if 1 != len(prods):
            raise BadRequest("Expected 1 output data product on %s '%s', got %d" % (site_type, site_id, len(prods)))
        return models



    def check_device_for_deployment(self, device_id, device_type, model_type):
        log.debug("checking %s for deployment, will return %s" % (device_type, model_type))
        # validate and return model
        models, _ = self.RR.find_objects(device_id, PRED.hasModel, model_type, True)
        if 1 != len(models):
            raise BadRequest("Expected 1 model for %s '%s', got %d" % (device_type, device_id, len(models)))
        return models[0]

    def check_site_device_pair_for_deployment(self, site_id, device_id, site_type=None, device_type=None):
        log.debug("checking %s/%s pair for deployment" % (site_type, device_type))
        #return a pair that should be REMOVED, or None

        if site_type is None:
            site_type = type(self.RR.read(site_id)).__name__

        if device_type is None:
            device_type = type(self.RR.read(device_id)).__name__

        ret = None

        log.debug("checking existing hasDevice links from site")
        devices, _ = self.RR.find_objects(site_id, PRED.hasDevice, device_type, True)
        if 1 < len(devices):
            raise Inconsistent("Found more than 1 hasDevice relationship from %s '%s'" % (site_type, site_id))
        elif 0 < len(devices):
            if devices[0] != device_id:
                ret = (site_id, devices[0])
                log.info("%s '%s' is already hasDevice with a %s" % (site_type, site_id, device_type))

        return ret

    def has_matching_streamdef(self, site_id, device_id):
        if not self.streamdef_of_site(site_id) in self.streamdefs_of_device(device_id):
            raise BadRequest("No matching streamdefs between %s '%s' and %s '%s'" %
                             (site_type, site_id, device_type, device_id))

    def activate_deployment(self, deployment_id='', activate_subscriptions=False):
        """
        Make the devices on this deployment the primary devices for the sites
        """
        #Verify that the deployment exists
        deployment_obj = self.clients.resource_registry.read(deployment_id)

        if LCS.DEPLOYED == deployment_obj.lcstate:
            raise BadRequest("This deploment is already active")

        device_models = {}
        site_models = {}

        # significant change-up in how this works.
        #
        # collect all devices in this deployment

        def add_sites(site_ids, site_type, model_type):
            for s in site_ids:
                models = self.check_site_for_deployment(s, site_type, model_type)
                if s in site_models:
                    log.warn("Site '%s' was already collected in deployment '%s'" % (s, deployment_id))
                site_models[s] = models

        def add_devices(device_ids, device_type, model_type):
            for d in device_ids:
                model = self.check_device_for_deployment(d, device_type, model_type)
                if d in device_models:
                    log.warn("Site '%s' was already collected in deployment '%s'" % (s, deployment_id))
                device_models[d] = model


        def collect_specific_resources(site_type, device_type, model_type):
            # check this deployment -- specific device types -- for validity
            # return a list of pairs (site, device) to be associated

            new_site_ids, _ = self.RR.find_subjects(site_type,
                                                    PRED.hasDeployment,
                                                    deployment_id,
                                                    True)

            new_device_ids, _ = self.RR.find_subjects(device_type,
                                                      PRED.hasDeployment,
                                                      deployment_id,
                                                      True)

            add_sites(new_site_ids, site_type, model_type)
            add_devices(new_device_ids, site_type, model_type)

        # collect platforms, verify that only one platform device exists in the deployment
        collect_specific_resources(RT.PlatformSite, RT.PlatformDevice, RT.PlatformModel)
        if 1 < len(device_models):
            raise BadRequest("Multiple platforms in the same deployment are not allowed")
        elif 0 < len(device_models):
            # add devices and sites that are children of platform device / site
            child_device_ids = self.platform_device.find_stemming_device(device_models.keys()[0])
            child_site_ids = self.find_related_frames_of_reference(site_models.keys()[0],
                                                                   [RT.PlatformSite, RT.InstrumentSite])
            #  verify that platform site has no sub-platform-sites
            if 0 < len(child_site_ids[RT.PlatformSite]):
                raise BadRequest("Deploying a platform with its own child platform is not allowed")

            #  gather a list of all instrument sites on platform site
            #  gather a list of all instrument devices on platform device
            add_devices(child_device_ids, RT.InstrumentDevice, RT.InstrumentModel)
            add_sites(child_site_ids[RT.InstrumentSite], RT.InstrumentSite, RT.InstrumentModel)

        collect_specific_resources(RT.InstrumentSite, RT.InstrumentDevice, RT.InstrumentModel)

        # create a CSP so we can solve it
        problem = constraint.Problem()

        # add variables - the devices to be assigned, and their range (possible sites)
        for device_id in device_models.keys():
            device_model = device_models[device_id]
            possible_sites = [s for s in site_models.keys()
                              if device_model in site_models[s]
                                    and self.streamdef_of_site(s) in self.streamdefs_of_device(device_id)]
            problem.addVariable("device_%s" % device_id, possible_sites)

        # add the constraint that all the variables have to pick their own site
        problem.addConstraint(constraint.AllDifferentConstraint(),
            ["device_%s" % device_id for device_id in device_models.keys()])

        # perform CSP solve; this will be a list of solutions, each a dict of var -> value
        solutions = problem.getSolutions()

        if 1 > len(solutions):
            raise BadRequest("The set of devices could not be mapped to the set of sites, based on matching " +
                             "model and streamdefs")
        elif 1 < len(solutions):
            log.warn("Found %d possible ways to map device and site, but just picking the first one" % len(solutions))

        pairs_add = []
        pairs_rem = []

        for device_id in device_models.keys():
            site_id = solutions[0]["device_%s" % device_id]
            old_pair = self.check_site_device_pair_for_deployment(site_id, device_id)
            if old_pair:
                pairs_rem.append(old_pair)
            new_pair = (site_id, device_id)
            pairs_add.append(new_pair)

        # process any removals
        for site_id, device_id in pairs_rem:
            log.info("Unassigning hasDevice; device '%s' from site '%s'" % (device_id, site_id))
            if not activate_subscriptions:
                log.warn("The input to the data product for site '%s' will no longer come from its primary device" %
                         site_id)
            self.unassign_device_from_site(device_id, site_id)

        # process the additions
        for site_id, device_id in pairs_add:
            log.info("Setting primary device '%s' for site '%s'" % (device_id, site_id))
            self.assign_device_to_site(device_id, site_id)
            if activate_subscriptions:
                log.info("Activating subscription too")
                self.transfer_site_subscription(site_id)

        self.RR.execute_lifecycle_transition(deployment_id, LCE.DEPLOY)


    def deactivate_deployment(self, deployment_id=''):
        """Remove the primary device designation for the deployed devices at the sites

        @param deployment_id    str
        @throws NotFound    object with specified id does not exist
        @throws BadRequest    if devices can not be undeployed
        """

        #Verify that the deployment exists
        deployment_obj = self.clients.resource_registry.read(deployment_id)

        if LCS.DEPLOYED != deployment_obj.lcstate:
            raise BadRequest("This deploment is not active")

        self.RR.execute_lifecycle_transition(deployment_id, LCE.DEVELOPED)




    def transfer_site_subscription(self, site_id=""):
        """
        Transfer the site subscription to the current hasDevice link
        """

        # get site obj
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
            model_type = RT.PlatformModel
        else:
            raise BadRequest("Expected a device type, got '%s'" % device_type)

        device_model = self.check_device_for_deployment(device_id, device_type, model_type)
        site_models = self.check_site_for_deployment(site_id, site_type, model_type)
        if device_model not in site_models:
            raise BadRequest("The site and device model types are incompatible")

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
        if 1 != len(process_ids):
            raise BadRequest("Expected 1 DataProcess feeding DataProduct '%s', but found %d" %
                             (pduct_ids[0], len(process_ids)))

        #look up stream defs
        ss = self.streamdef_of_site(site_id)
        ds = self.streamdefs_of_device(device_id)

        data_process_id = process_ids[0]
        log.info("Changing subscription")
        self.PRMS.update_data_process_inputs(data_process_id, [ds[ss]])





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

        # the relative depth of each resource type in our tree
        depth = self.HIERARCHY_DEPTH

        input_obj  = self.RR.read(input_resource_id)
        input_type = input_obj._get_type()

        #input type checking
        if not input_type in depth:
            raise BadRequest("Input resource type (got %s) must be one of %s" % 
                             (input_type, self.HIERARCHY_LOOKUP))
        for t in output_resource_type_list:
            if not t in depth:
                raise BadRequest("Output resource types (got %s) must be one of %s" %
                                 (str(output_resource_type_list), self.HIERARCHY_LOOKUP))

                             

        subordinates = [x for x in output_resource_type_list if depth[x] > depth[input_type]]
        superiors    = [x for x in output_resource_type_list if depth[x] < depth[input_type]]

        acc = {}
        acc[input_type] = [input_obj]


        if subordinates:
            # figure out the actual depth we need to go
            deepest_type = input_type #initial value
            for output_type in output_resource_type_list:
                if depth[deepest_type] < depth[output_type]:
                    deepest_type = output_type

            log.debug("Deepest level for search will be '%s'" % deepest_type)

            acc = self._traverse_entity_tree(acc, input_type, deepest_type, True)


        if superiors:
            highest_type = input_type #initial value

            for output_type in output_resource_type_list:
                if depth[highest_type] > depth[output_type]:
                    highest_type = output_type

            log.debug("Highest level for search will be '%s'" % highest_type)

            acc = self._traverse_entity_tree(acc, highest_type, input_type, False)

        # Don't include input type in response            
        #TODO: maybe just remove the input resource id 
        if input_type in acc:
            stripped = []
            for r_obj in acc[input_type]:
                if r_obj._id == input_resource_id:
                    log.debug("Stripped input from return value")
                else:
                    stripped.append(r_obj)

            acc[input_type] = stripped
                

        return acc
                    

    def _traverse_entity_tree(self, acc, top_type, bottom_type, downward):

        call_list = self._build_call_list(top_type, bottom_type, downward)

        # start calling functions
        if downward:
            for (p, c) in call_list:
                acc = self._find_subordinate(acc, p, c)
        else:
            for (p, c) in call_list:
                acc = self._find_superior(acc, p, c)

        return acc


    def _build_call_list(self, top_type, bottom_type, downward):

        call_list = []

        if downward:
            step = 1
            top_ord = self.HIERARCHY_DEPTH[top_type]
            bot_ord = self.HIERARCHY_DEPTH[bottom_type]
        else:
            step = -1
            top_ord = self.HIERARCHY_DEPTH[bottom_type] - 1
            bot_ord = self.HIERARCHY_DEPTH[top_type] - 1


        for i in range(top_ord, bot_ord, step):
            child  = self.HIERARCHY_LOOKUP[i + 1]
            parent = self.HIERARCHY_LOOKUP[i]
            if downward:
                tmp = [(parent, parent), (parent, child), (child, child)]
            else:
                tmp = [(child, child), (parent, child), (parent, parent)]


            for pair in tmp:
                if not pair in call_list:
                    #log.debug("adding %s" % str(pair))
                    call_list.append(pair)

        return call_list



    def _find_subordinate(self, acc, parent_type, child_type):
        #acc is an accumulated dictionary

        find_fns = {
            (RT.Observatory, RT.Subsite):         self.observatory.find_stemming_site,
            (RT.Subsite, RT.Subsite):             self.subsite.find_stemming_subsite,
            (RT.Subsite, RT.PlatformSite):        self.subsite.find_stemming_platform_site,
            (RT.PlatformSite, RT.PlatformSite):   self.platform_site.find_stemming_platform_site,
            (RT.PlatformSite, RT.InstrumentSite): self.platform_site.find_stemming_instrument_site,
            }

        if (parent_type, child_type) in find_fns:
            find_fn = find_fns[(parent_type, child_type)]
        else:
            find_fn = (lambda x : [])
        
        if not parent_type in acc: acc[parent_type] = []

        log.debug("Subordinates: '%s'x%d->'%s'" % (parent_type, len(acc[parent_type]), child_type))

        #for all parents in the acc, add all their children
        for parent_obj in acc[parent_type]:
            parent_id = parent_obj._id
            for child_obj in find_fn(parent_id):
                actual_child_type = child_obj._get_type()
                if not actual_child_type in acc:
                    acc[actual_child_type] = []
                acc[actual_child_type].append(child_obj)

        return acc



    def _find_superior(self, acc, parent_type, child_type):
        # acc is an accumualted dictionary

        #log.debug("Superiors: '%s'->'%s'" % (parent_type, child_type))
        #if True:
        #    return acc
            
        find_fns = {
            (RT.Observatory, RT.Subsite):         self.observatory.find_having_site,
            (RT.Subsite, RT.Subsite):             self.subsite.find_having_site,
            (RT.Subsite, RT.PlatformSite):        self.subsite.find_having_site,
            (RT.PlatformSite, RT.PlatformSite):   self.platform_site.find_having_site,
            (RT.PlatformSite, RT.InstrumentSite): self.platform_site.find_having_site,
            }

        if (parent_type, child_type) in find_fns:
            find_fn = find_fns[(parent_type, child_type)]
        else:
            find_fn = (lambda x : [])
        
        if not child_type in acc: acc[child_type] = []

        log.debug("Superiors: '%s'->'%s'x%d" % (parent_type, child_type, len(acc[child_type])))

        #for all children in the acc, add all their parents
        for child_obj in acc[child_type]:
            child_id = child_obj._id
            for parent_obj in find_fn(child_id):
                actual_parent_type = parent_obj._get_type()
                if not actual_parent_type in acc:
                    acc[actual_parent_type] = []
                acc[actual_parent_type].append(parent_obj)

        return acc



