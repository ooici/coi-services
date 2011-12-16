#!/usr/bin/env python

'''
@package ion.services.sa.marine_facility_management.marine_facility_management_service Implementation of IMarineFacilityManagementService interface
@file ion/services/sa/marine_facility_management/marine_facility_management_service.py
@author M Manning
@brief Marine Facility Management service to keep track of Marine Facilities, sites, logical platforms, etc
and the relationships between them
'''

from interface.services.sa.imarine_facility_management_service import BaseMarineFacilityManagementService 
from pyon.core.exception import NotFound
from pyon.public import CFG, IonObject, log, RT, AT, LCS



from interface.services.sa.imarine_facility_management_service import BaseMarineFacilityManagementService


class MarineFacilityManagementService(BaseMarineFacilityManagementService):


    def create_marine_facility(self, marine_facility={}):
        '''
        Create a new marine_facility.

        @param marine_facility New marine facility properties.
        @retval id New data_source id.
        '''
        log.debug("Creating marine_facility object")
        marine_facility_id, rev = self.clients.resource_registry.create(marine_facility)

        return marine_facility_id

    def update_marine_facility(self, marine_facility={}):
        '''
        Update an existing marine_facility.

        @param marine_facility The marine_facility object with updated properties.
        @retval success Boolean to indicate successful update.
        @todo Add logic to validate optional attributes. Is this interface correct?
        '''

        return self.clients.resource_registry.update(marine_facility)


    def read_marine_facility(self, marine_facility_id=''):
        '''
        Get an existing marine_facility object.

        @param marine_facility_id The id of the stream.
        @retval marine_facility_obj The marine_facility object.
        @throws NotFound when data_source doesn't exist.
        '''

        log.debug("Reading Marine Facility object id: %s" % marine_facility_id)
        marine_facility_obj = self.clients.resource_registry.read(marine_facility_id)
        if not marine_facility_obj:
            raise NotFound("Marine Facility %s does not exist" % marine_facility_id)
        return marine_facility_obj

    def delete_marine_facility(self, marine_facility_id=''):
        # Read and delete specified Marine Facility object
        marine_facility = self.clients.resource_registry.read(marine_facility_id)
        if not marine_facility:
            raise NotFound("Marine Facility %s does not exist" % marine_facility_id)
        self.clients.resource_registry.delete(marine_facility)

    def find_marine_facilities(self, filters={}):
        # Find marine_facilities - as list of resource objects
        # todo : add filtering
        marine_facility_list, _ = self.clients.resource_registry.find_resources(RT.MarineFacility, None, None, False)
        return marine_facility_list

    def create_site(self, site={}):
        # Persist Site object and return object _id as OOI id
        site_id, version = self.clients.resource_registry.create(site)
        return site_id

    def update_site(self, site={}):
        # Overwrite Site object
        self.clients.resource_registry.update(site)

    def read_site(self, site_id=''):
        # Read Site object with _id matching passed site id
        site = self.clients.resource_registry.read(site_id)
        if not site:
            raise NotFound("Site %s does not exist" % site_id)
        return site

    def delete_site(self, site_id=''):
        # Read and delete specified Site object
        site = self.clients.resource_registry.read(site_id)
        if not site:
            raise NotFound("Site %s does not exist" % site_id)
        self.clients.resource_registry.delete(site)

    def find_sites(self, filters={}):
        # Find sites - as list of resource objects
        # todo : add filtering
        site_list, _ = self.clients.resource_registry.find_resources(RT.Site, None, None, False)
        return site_list

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

    def create_logical_platform(self, logical_platform={}):
        # Persist logical platform object and return object _id as OOI id
        logical_platform_id, version = self.clients.resource_registry.create(logical_platform)
        return logical_platform_id

    def update_logical_platform(self, logical_platform={}):
        # Overwrite logical platform object
        self.clients.resource_registry.update(logical_platform)

    def read_logical_platform(self, logical_platform_id=''):
        # Read logical platform object with _id matching passed logical platform id
        logical_platform = self.clients.resource_registry.read(logical_platform_id)
        if not logical_platform:
            raise NotFound("Logical platform %s does not exist" % logical_platform_id)
        return logical_platform

    def delete_logical_platform(self, logical_platform_id=''):
        # Read and delete specified logical platform object
        logical_platform = self.clients.resource_registry.read(logical_platform_id)
        if not logical_platform:
            raise NotFound("Logical platform %s does not exist" % logical_platform_id)
        self.clients.resource_registry.delete(logical_platform)

    def find_logical_platforms(self, filters={}):
        # Find logical_platforms - as list of resource objects
        # todo : add filtering
        logical_platforms_list, _ = self.clients.resource_registry.find_resources(RT.LogicalPlatform, None, None, False)
        return logical_platforms_list

    def assign_platform(self, logical_platform_id='', parent_site_id=''):
        assert logical_platform_id and parent_site_id, "Arguments not set"
        aid = self.clients.resource_registry.create_association(parent_site_id, AT.hasPlatform, logical_platform_id)
        return True

    def unassign_platform(self, logical_platform_id='', parent_site_id=''):
        assert logical_platform_id and parent_site_id, "Arguments not set"
        assoc_id, _ = self.clients.resource_registry.find_associations(parent_site_id, AT.hasPlatform, logical_platform_id, True)
        if not assoc_id:
            raise NotFound("Association Site hasPlatform LogicalPlatfrom does not exist: site: %s  platfrom: %s" % parent_site_id, logical_platform_id)

        aid = self.clients.resource_registry.delete_association(assoc_id)
        return True

    def create_logical_instrument(self, logical_instrument={}):
        # Persist logical instrument object and return object _id as OOI id
        logical_instrument_id, version = self.clients.resource_registry.create(logical_instrument)
        return logical_instrument_id

    def update_logical_instrument(self, logical_instrument={}):
        # Overwrite logical instrument object
        self.clients.resource_registry.update(logical_instrument)

    def read_logical_instrument(self, logical_instrument_id=''):
        # Read logical instrument object with _id matching passed logical instrument id
        logical_instrument = self.clients.resource_registry.read(logical_instrument_id)
        if not logical_instrument:
            raise NotFound("Logical instrument %s does not exist" % logical_instrument_id)
        return logical_instrument

    def delete_logical_instrument(self, logical_instrument_id=''):
        # Read and delete specified logical instrument object
        logical_instrument = self.clients.resource_registry.read(logical_instrument_id)
        if not logical_instrument:
            raise NotFound("Logical instrument %s does not exist" % logical_instrument_id)
        self.clients.resource_registry.delete(logical_instrument)

    def find_logical_instruments(self, filters={}):
        # Find logical_instruments - as list of resource objects
        # todo : add filtering
        logical_instruments_list, _ = self.clients.resource_registry.find_resources(RT.LogicalInstrument, None, None, False)
        return logical_instruments_list

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

  