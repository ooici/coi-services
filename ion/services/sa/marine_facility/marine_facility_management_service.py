#!/usr/bin/env python

'''
@package ion.services.sa.marine_facility_management.marine_facility Implementation of IMarineFacilityManagementService interface
@file ion/services/sa/marine_facility/marine_facility.py
@author M Manning
@brief Marine Facility Management service to keep track of Marine Facilities, sites, logical platforms, etc
and the relationships between them
'''

from pyon.core.exception import NotFound
from pyon.public import CFG, IonObject, log, RT, AT, LCS



from ion.services.sa.marine_facility.logical_instrument_impl import LogicalInstrumentImpl
from ion.services.sa.marine_facility.logical_platform_impl import LogicalPlatformImpl
from ion.services.sa.marine_facility.marine_facility_impl import MarineFacilityImpl
from ion.services.sa.marine_facility.site_impl import SiteImpl


from interface.services.sa.imarine_facility_management_service import BaseMarineFacilityManagementService


class MarineFacilityManagementService(BaseMarineFacilityManagementService):


    def on_init(self):
        IonObject("Resource")  # suppress pyflakes error

        self.override_clients(self.clients)


    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        #shortcut names for the import sub-services
        if hasattr(self.clients, "resource_registry"):
            self.RR    = self.clients.resource_registry
            

        #farm everything out to the impls

        self.logical_instrument  = LogicalInstrumentImpl(self.clients)
        self.logical_platform    = LogicalPlatformImpl(self.clients)
        self.marine_facility     = MarineFacilityImpl(self.clients)
        self.site                = SiteImpl(self.clients)





    ##########################################################################
    #
    # MARINE FACILITY
    #
    ##########################################################################

    def create_marine_facility(self, marine_facility=None):
        """
        create a new instance
        @param marine_facility the object to be created as a resource
        @retval marine_facility_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.marine_facility.create_one(marine_facility)

    def update_marine_facility(self, marine_facility=None):
        """
        update an existing instance
        @param marine_facility the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.marine_facility.update_one(marine_facility)


    def read_marine_facility(self, marine_facility_id=''):
        """
        fetch a resource by ID
        @param marine_facility_id the id of the object to be fetched
        @retval LogicalInstrument resource

        """
        return self.marine_facility.read_one(marine_facility_id)

    def delete_marine_facility(self, marine_facility_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param marine_facility_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.marine_facility.delete_one(marine_facility_id)

    def find_marine_facilities(self, filters=None):
        """

        """
        return self.marine_facility.find_some(filters)


    ##########################################################################
    #
    # SITE
    #
    ##########################################################################

    def create_site(self, site=None):
        """
        create a new instance
        @param site the object to be created as a resource
        @retval site_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.site.create_one(site)

    def update_site(self, site=None):
        """
        update an existing instance
        @param site the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.site.update_one(site)


    def read_site(self, site_id=''):
        """
        fetch a resource by ID
        @param site_id the id of the object to be fetched
        @retval LogicalInstrument resource

        """
        return self.site.read_one(site_id)

    def delete_site(self, site_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param site_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.site.delete_one(site_id)

    def find_sites(self, filters=None):
        """

        """
        return self.site.find_some(filters)







    ##########################################################################
    #
    # LOGICAL INSTRUMENT
    #
    ##########################################################################

    def create_logical_instrument(self, logical_instrument=None):
        """
        create a new instance
        @param logical_instrument the object to be created as a resource
        @retval logical_instrument_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.logical_instrument.create_one(logical_instrument)

    def update_logical_instrument(self, logical_instrument=None):
        """
        update an existing instance
        @param logical_instrument the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.logical_instrument.update_one(logical_instrument)


    def read_logical_instrument(self, logical_instrument_id=''):
        """
        fetch a resource by ID
        @param logical_instrument_id the id of the object to be fetched
        @retval LogicalInstrument resource

        """
        return self.logical_instrument.read_one(logical_instrument_id)

    def delete_logical_instrument(self, logical_instrument_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param logical_instrument_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.logical_instrument.delete_one(logical_instrument_id)

    def find_logical_instruments(self, filters=None):
        """

        """
        return self.logical_instrument.find_some(filters)



    ##########################################################################
    #
    # LOGICAL PLATFORM
    #
    ##########################################################################

    def create_logical_platform(self, logical_platform=None):
        """
        create a new instance
        @param logical_platform the object to be created as a resource
        @retval logical_platform_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.logical_platform.create_one(logical_platform)

    def update_logical_platform(self, logical_platform=None):
        """
        update an existing instance
        @param logical_platform the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.logical_platform.update_one(logical_platform)


    def read_logical_platform(self, logical_platform_id=''):
        """
        fetch a resource by ID
        @param logical_platform_id the id of the object to be fetched
        @retval LogicalPlatform resource

        """
        return self.logical_platform.read_one(logical_platform_id)

    def delete_logical_platform(self, logical_platform_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param logical_platform_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.logical_platform.delete_one(logical_platform_id)

    def find_logical_platforms(self, filters=None):
        """

        """
        return self.logical_platform.find_some(filters)







    #FIXME: args need to change
    def assign_platform(self, logical_platform_id='', parent_site_id=''):
        """
        @todo the arguments for this function seem incorrect and/or mismatched
        """
        raise NotImplementedError()
        #return self.instrument_agent.assign(instrument_agent_id, instrument_id, instrument_agent_instance)

    def unassign_platform(self, logical_platform_id='', parent_site_id=''):
        """@todo document this interface!!!

        @param logical_platform_id    str
        @param parent_site_id    str
        @retval success    bool
        """
        raise NotImplementedError()
        #return self.instrument_agent.unassign(instrument_agent_id, instrument_device_id, instrument_agent_instance)



    def assign_site(self, child_site_id='', parent_site_id=''):
        assert child_site_id and parent_site_id, "Arguments not set"
        aid = self.clients.resource_registry.create_association(parent_site_id, AT.hasSite, child_site_id)
        return True

    def unassign_site(self, parent_site_id='', child_site_id=''):
        assert child_site_id and parent_site_id, "Arguments not set"
        assoc_id, _ = self.clients.resource_registry.find_associations(parent_site_id, AT.hasSite, child_site_id, True)
        if not assoc_id:
            raise NotFound("Association ParentSite hasSite ChildSite does not exist: parent site: %s  child site: %s" % parent_site_id, child_site_id)

        aid = self.clients.resource_registry.delete_association(assoc_id)
        return True


    def assign_instrument(self, logical_instrument_id='', parent_site_id=''):
        #todo: is platform associated with a Site?
        assert logical_instrument_id and parent_site_id, "Arguments not set"
        aid = self.clients.resource_registry.create_association(parent_site_id, AT.hasInstrument, logical_instrument_id)
        return True


    def unassign_instrument(self, logical_instrument_id='', parent_site_id=''):
        #todo: is instrument associated with a Site?
        assert logical_instrument_id and parent_site_id, "Arguments not set"
        assoc_id, _ = self.clients.resource_registry.find_associations(parent_site_id, AT.hasInstrument, logical_instrument_id, True)
        if not assoc_id:
            raise NotFound("Association Site hasPlatform LogicalInstrument does not exist: site: %s  instrument: %s" % parent_site_id, logical_instrument_id)

        aid = self.clients.resource_registry.delete_association(assoc_id)
        return True

    def define_observatory_policy(self):
        """method docstring
        """
        # Return Value
        # ------------
        # null
        # ...
        #
        pass

  
