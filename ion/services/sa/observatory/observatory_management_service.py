#!/usr/bin/env python

'''
@package ion.services.sa.observatory Implementation of IObservatoryManagementService interface
@file ion/services/sa/observatory/observatory_management_service.py
@author
@brief Observatory Management Service to keep track of observatories sites, logical platform sites, instrument sites,
and the relationships between them
'''




from pyon.core.exception import NotFound, BadRequest
from pyon.public import CFG, IonObject, log, RT, PRED, LCS, LCE

#from pyon.util.log import log
from ion.services.sa.resource_impl.org_impl import OrgImpl
from ion.services.sa.resource_impl.observatory_impl import ObservatoryImpl
from ion.services.sa.resource_impl.subsite_impl import SubsiteImpl
from ion.services.sa.resource_impl.platform_site_impl import PlatformSiteImpl
from ion.services.sa.resource_impl.instrument_site_impl import InstrumentSiteImpl

#for logical/physical associations, it makes sense to search from MFMS
from ion.services.sa.resource_impl.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.resource_impl.platform_device_impl import PlatformDeviceImpl

from interface.services.sa.iobservatory_management_service import BaseObservatoryManagementService


INSTRUMENT_OPERATOR_ROLE = 'INSTRUMENT_OPERATOR'
OBSERVATORY_OPERATOR_ROLE = 'OBSERVATORY_OPERATOR'
DATA_OPERATOR_ROLE = 'DATA_OPERATOR'


class ObservatoryManagementService(BaseObservatoryManagementService):


    def on_init(self):
        IonObject("Resource")  # suppress pyflakes error
        CFG, log, RT, PRED, LCS, LCE, NotFound, BadRequest, log  #suppress pyflakes errors about "unused import"

        self.override_clients(self.clients)


    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        #shortcut names for the import sub-services
        if hasattr(self.clients, "resource_registry"):
            self.RR    = self.clients.resource_registry
            
        if hasattr(self.clients, "instrument_management"):
            self.IMS   = self.clients.instrument_management
            

        #farm everything out to the impls

        self.org              = OrgImpl(self.clients)
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



    def create_marine_facility(self, observatory=None):
        """Create a SPECIAL marine facility Observatory resource. An observatory  is coupled
        with one Org. The Org is created and associated as part of this call.

        @param observatory    Observatory
        @retval observatory_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        log.debug("ObservatoryManagementService.create_observatory(): %s" %str(observatory))
        
        observatory_id = self.create_observatory(observatory)
        
        # create the org 
        org_obj = IonObject(RT.Org, name=observatory.name+'_org')
        org_id = self.clients.org_management.create_org(org_obj)
        
        # Associate the facility with the org
        asso_id, _ = self.clients.resource_registry.create_association(org_id,  PRED.hasObservatory, observatory_id)
        
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
        
        return observatory_id


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
        
        # find the org for this MF
        org_ids, _ = self.clients.resource_registry.find_subjects(RT.Org, PRED.hasObservatory, observatory_id, id_only=True)
        if len(org_ids) == 0:
            log.warn("ObservatoryManagementService.delete_observatory(): no org for MF " + observatory_id)
        else:
            if len(org_ids) > 1:
                log.warn("ObservatoryManagementService.delete_observatory(): more than 1 org for MF " + observatory_id)
                # TODO: delete the others and/or raise exception???
            # delete the set of User Roles for this marine facility that this service created
            self.clients.org_management.remove_user_role(org_ids[0], INSTRUMENT_OPERATOR_ROLE)
            self.clients.org_management.remove_user_role(org_ids[0], OBSERVATORY_OPERATOR_ROLE)
            self.clients.org_management.remove_user_role(org_ids[0], DATA_OPERATOR_ROLE)
            # delete the org
            self.clients.org_management.delete_org(org_ids[0])
        
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
        self.instrument_site.delete_one(instrument_site_id)



    


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
        pass

    def unassign_site_from_site(self, child_site_id='', parent_site_id=''):
        """Disconnects a child site (any subtype) from a parent site (any subtype)

        @param child_site_id    str
        @param parent_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass


    def assign_site_to_observatory(self, site_id='', observatory_id=''):
        self.observatory.link_site(observatory_id, site_id)


    def unassign_site_from_observatory(self, site_id="", observatory_id=''):
        self.observatory.unlink_site(observatory_id, site_id)


    #these don't seem to be doing any associations... should they be renamed???

    def assign_resource_to_observatory(self, resource_id='', observatory_id=''):
        #resource_obj = self.clients.resource_registry.read(resource_id)
        #observatory_obj = self.clients.resource_registry.read(observatory_id)

        org_objs = self.org.find_having_observatory(observatory_id)

        if not org_objs:
            raise NotFound ("Observatory is not associated with an Org: %s ", observatory_id)

        org_id = org_objs[0]._id

        log.debug("ObservatoryManagementService:assign_resource_to_observatory org id: %s     resource id:  %s ", str(org_id), str(resource_id))
        self.clients.org_management.share_resource(org_id, resource_id)

        return

    def unassign_resource_from_observatory(self, resource_id='', observatory_id=''):
        #resource_obj = self.clients.resource_registry.read(resource_id)
        #observatory_obj = self.clients.resource_registry.read(observatory_id)


        org_objs = self.org.find_having_observatory(observatory_id)

        if not org_objs:
            raise NotFound ("Observatory is not associated with an Org: %s ", observatory_id)

        org_id = org_objs[0]._id

        self.clients.org_management.unshare_resource(org_id, resource_id)
        return




    ##########################################################################
    #
    # FIND OPS
    #
    ##########################################################################



    def find_observatories(self, filters=None):
        """

        """
        return self.observatory.find_some(filters)


    def find_subsites(self, filters=None):
        """

        """
        return self.site.find_some(filters)


    def find_instrument_sites(self, filters=None):
        """

        """
        return self.instrument_site.find_some(filters)


    def find_platform_sites(self, filters=None):
        """

        """
        return self.platform_site.find_some(filters)


    def find_org_by_observatory(self, observatory_id=''):
        """

        """
        return self.org.find_having_observatory(observatory_id)




