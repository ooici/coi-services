#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.sa.imarine_facility_management_service import BaseMarineFacilityManagementService


class MarineFacilityManagementService(BaseMarineFacilityManagementService):


    def create_marine_facility(self, marine_facility={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {marine_facility_id: ''}
        #
        pass

    def update_marine_facility(self, marine_facility={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_marine_facility(self, marine_facility_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # marine_facility: {}
        #
        pass

    def delete_marine_facility(self, marine_facility_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_marine_facilities(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # marine_facility_list: []
        #
        pass

    def create_site(self, site={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {site_id: ''}
        #
        pass

    def update_site(self, site={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_site(self, site_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # site: {}
        #
        pass

    def delete_site(self, site_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_sites(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # site_list: []
        #
        pass

    def assign_site(self, child_site_id='', parent_site_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_site(self, parent_site_id='', child_site_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_logical_platform(self, logical_platform={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {logical_platform_id: ''}
        #
        pass

    def update_logical_platform(self, logical_platform={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_logical_platform(self, logical_platform_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # logical_platform: {}
        #
        pass

    def delete_logical_platform(self, logical_platform_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_logical_platforms(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # logical_platform_list: []
        #
        pass

    def assign_platform(self, logical_platform_id='', parent_site_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_platform(self, logical_platform_id='', parent_site_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_logical_instrument(self, logical_instrument={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {logical_instrument_id: ''}
        #
        pass

    def update_logical_instrument(self, logical_instrument={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_logical_instrument(self, logical_instrument_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # logical_instrument: {}
        #
        pass

    def delete_logical_instrument(self, logical_instrument_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_logical_instruments(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # logical_instrument_list: []
        #
        pass

    def assign_instrument(self, logical_instrument_id='', parent_site_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_instrument(self, logical_instrument_id='', parent_site_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def define_observatory_policy(self):
        """method docstring
        """
        # Return Value
        # ------------
        # null
        # ...
        #
        pass

  