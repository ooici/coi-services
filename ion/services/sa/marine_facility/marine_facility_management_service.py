#!/usr/bin/env python

'''
@package ion.services.sa.marine_facility_management.marine_facility Implementation of IMarineFacilityManagementService interface
@file ion/services/sa/marine_facility/marine_facility.py
@author M Manning
@brief Marine Facility Management service to keep track of Marine Facilities, sites, logical platforms, etc
and the relationships between them
'''

from pyon.core.exception import NotFound
from pyon.public import CFG, IonObject, log, RT, PRED, LCS



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



    ############################
    #
    #  ASSOCIATIONS
    #
    ############################


    def assign_platform_to_logical_platform(self, platform_id='', logical_platform_id=''):
        self.logical_platform.link_platform(logical_platform_id, platform_id)


    def unassign_platform_from_logical_platform(self, platform_id='', logical_platform_id=''):
        self.logical_platform.unlink_platform(logical_platform_id, platform_id)


    def assign_logical_instrument_to_logical_platform(self, logical_instrument_id='', logical_platform_id=''):
        self.logical_platform.link_instrument(logical_platform_id, logical_instrument_id)

    def unassign_logical_instrument_from_logical_platform(self, logical_instrument_id='', logical_platform_id=''):
        self.logical_platform.unlink_instrument(logical_platform_id, logical_instrument_id)


    def assign_site_to_marine_facility(self, site_id='', marine_facility_id=''):
        self.marine_facility.link_site(marine_facility_id, site_id)

    def unassign_site_from_marine_facility(self, site_id="", marine_facility_id=''):
        self.marine_facility.unlink_site(marine_facility_id, site_id)


    def assign_site_to_site(self, child_site_id='', parent_site_id=''):
        self.site.link_site(parent_site_id, child_site_id)

    def unassign_site_from_site(self, child_site_id="", parent_site_id=''):
        self.site.unlink_site(parent_site_id, child_site_id)

    def assign_logical_platform_to_site(self, logical_platform_id='', site_id=''):
        self.site.link_platform(site_id, logical_platform_id)

    def unassign_logical_platform_from_site(self, logical_platform_id='', site_id=''):
        self.site.unlink_platform(site_id, logical_platform_id)






    def define_observatory_policy(self):
        """method docstring
        """
        # Return Value
        # ------------
        # null
        # ...
        #
        pass

  
    ############################
    #
    #  LIFECYCLE TRANSITIONS
    #
    ############################

    def declare_logical_instrument_planned(self, logical_instrument_id=""):
       """
       declare a logical_instrument to be in the PLANNED state
       @param logical_instrument_id the resource id
       """
       return self.logical_instrument.advance_lcs(logical_instrument_id, LCS.PLANNED)

    def declare_logical_instrument_developed(self, logical_instrument_id=""):
       """
       declare a logical_instrument to be in the DEVELOPED state
       @param logical_instrument_id the resource id
       """
       return self.logical_instrument.advance_lcs(logical_instrument_id, LCS.DEVELOPED)

    def declare_logical_instrument_integrated(self, logical_instrument_id=""):
       """
       declare a logical_instrument to be in the INTEGRATED state
       @param logical_instrument_id the resource id
       """
       return self.logical_instrument.advance_lcs(logical_instrument_id, LCS.INTEGRATED)

    def declare_logical_instrument_discoverable(self, logical_instrument_id=""):
       """
       declare a logical_instrument to be in the DISCOVERABLE state
       @param logical_instrument_id the resource id
       """
       return self.logical_instrument.advance_lcs(logical_instrument_id, LCS.DISCOVERABLE)

    def declare_logical_instrument_available(self, logical_instrument_id=""):
       """
       declare a logical_instrument to be in the AVAILABLE state
       @param logical_instrument_id the resource id
       """
       return self.logical_instrument.advance_lcs(logical_instrument_id, LCS.AVAILABLE)

    def declare_logical_instrument_retired(self, logical_instrument_id=""):
       """
       declare a logical_instrument to be in the RETIRED state
       @param logical_instrument_id the resource id
       """
       return self.logical_instrument.advance_lcs(logical_instrument_id, LCS.RETIRED)

    def declare_logical_platform_planned(self, logical_platform_id=""):
       """
       declare a logical_platform to be in the PLANNED state
       @param logical_platform_id the resource id
       """
       return self.logical_platform.advance_lcs(logical_platform_id, LCS.PLANNED)

    def declare_logical_platform_developed(self, logical_platform_id=""):
       """
       declare a logical_platform to be in the DEVELOPED state
       @param logical_platform_id the resource id
       """
       return self.logical_platform.advance_lcs(logical_platform_id, LCS.DEVELOPED)

    def declare_logical_platform_integrated(self, logical_platform_id=""):
       """
       declare a logical_platform to be in the INTEGRATED state
       @param logical_platform_id the resource id
       """
       return self.logical_platform.advance_lcs(logical_platform_id, LCS.INTEGRATED)

    def declare_logical_platform_discoverable(self, logical_platform_id=""):
       """
       declare a logical_platform to be in the DISCOVERABLE state
       @param logical_platform_id the resource id
       """
       return self.logical_platform.advance_lcs(logical_platform_id, LCS.DISCOVERABLE)

    def declare_logical_platform_available(self, logical_platform_id=""):
       """
       declare a logical_platform to be in the AVAILABLE state
       @param logical_platform_id the resource id
       """
       return self.logical_platform.advance_lcs(logical_platform_id, LCS.AVAILABLE)

    def declare_logical_platform_retired(self, logical_platform_id=""):
       """
       declare a logical_platform to be in the RETIRED state
       @param logical_platform_id the resource id
       """
       return self.logical_platform.advance_lcs(logical_platform_id, LCS.RETIRED)

    def declare_marine_facility_planned(self, marine_facility_id=""):
       """
       declare a marine_facility to be in the PLANNED state
       @param marine_facility_id the resource id
       """
       return self.marine_facility.advance_lcs(marine_facility_id, LCS.PLANNED)

    def declare_marine_facility_developed(self, marine_facility_id=""):
       """
       declare a marine_facility to be in the DEVELOPED state
       @param marine_facility_id the resource id
       """
       return self.marine_facility.advance_lcs(marine_facility_id, LCS.DEVELOPED)

    def declare_marine_facility_integrated(self, marine_facility_id=""):
       """
       declare a marine_facility to be in the INTEGRATED state
       @param marine_facility_id the resource id
       """
       return self.marine_facility.advance_lcs(marine_facility_id, LCS.INTEGRATED)

    def declare_marine_facility_discoverable(self, marine_facility_id=""):
       """
       declare a marine_facility to be in the DISCOVERABLE state
       @param marine_facility_id the resource id
       """
       return self.marine_facility.advance_lcs(marine_facility_id, LCS.DISCOVERABLE)

    def declare_marine_facility_available(self, marine_facility_id=""):
       """
       declare a marine_facility to be in the AVAILABLE state
       @param marine_facility_id the resource id
       """
       return self.marine_facility.advance_lcs(marine_facility_id, LCS.AVAILABLE)

    def declare_marine_facility_retired(self, marine_facility_id=""):
       """
       declare a marine_facility to be in the RETIRED state
       @param marine_facility_id the resource id
       """
       return self.marine_facility.advance_lcs(marine_facility_id, LCS.RETIRED)

    def declare_site_planned(self, site_id=""):
       """
       declare a site to be in the PLANNED state
       @param site_id the resource id
       """
       return self.site.advance_lcs(site_id, LCS.PLANNED)

    def declare_site_developed(self, site_id=""):
       """
       declare a site to be in the DEVELOPED state
       @param site_id the resource id
       """
       return self.site.advance_lcs(site_id, LCS.DEVELOPED)

    def declare_site_integrated(self, site_id=""):
       """
       declare a site to be in the INTEGRATED state
       @param site_id the resource id
       """
       return self.site.advance_lcs(site_id, LCS.INTEGRATED)

    def declare_site_discoverable(self, site_id=""):
       """
       declare a site to be in the DISCOVERABLE state
       @param site_id the resource id
       """
       return self.site.advance_lcs(site_id, LCS.DISCOVERABLE)

    def declare_site_available(self, site_id=""):
       """
       declare a site to be in the AVAILABLE state
       @param site_id the resource id
       """
       return self.site.advance_lcs(site_id, LCS.AVAILABLE)

    def declare_site_retired(self, site_id=""):
       """
       declare a site to be in the RETIRED state
       @param site_id the resource id
       """
       return self.site.advance_lcs(site_id, LCS.RETIRED)

