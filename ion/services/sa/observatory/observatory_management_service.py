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

from pyon.util.log import log



from interface.services.sa.iobservatory_management_service import BaseObservatoryManagementService



class ObservatoryManagementService(BaseObservatoryManagementService):


    def create_observatory(self, observatory=None):
        """Create a Observatory resource. An observatory  is coupled
        with one Org. The Org is created and associated as part of this call.

        @param observatory    Observatory
        @retval observatory_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        pass

    def update_observatory(self, observatory=None):
        """Update a Observatory resource

        @param observatory    Observatory
        @throws NotFound    object with specified id does not exist
        """
        pass

    def read_observatory_id(self, observatory_id=''):
        """Read a Observatory resource

        @param observatory_id    str
        @retval observatory    Observatory
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_observatory(self, observatory_id=''):
        """Delete a Observatory resource

        @param observatory_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def create_subsite(self, subsite=None, parent_id=''):
        """Create a Subsite resource. A subsite is a frame of reference within an observatory. It's parent is
        either the observatory or another subsite.

        @param subsite    Subsite
        @param parent_id    str
        @retval subsite_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        pass

    def update_subsite(self, subsite=None):
        """Update a Subsite resource

        @param subsite    Subsite
        @throws NotFound    object with specified id does not exist
        """
        pass

    def read_subsite(self, subsite_id=''):
        """Read a Subsite resource

        @param subsite_id    str
        @retval subsite    Subsite
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_subsite(self, subsite_id=''):
        """Delete a LogicalPlatform resource, removes assocations to parents

        @param subsite_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def create_platformsite(self, platformsite=None, parent_id=''):
        """Create a PlatformSite resource. A platformsite is a frame of reference within an observatory. It's parent is
        either the observatory or another platformsite.

        @param platformsite    PlatformSite
        @param parent_id    str
        @retval platformsite_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        pass

    def update_platformsite(self, platformsite=None):
        """Update a PlatformSite resource

        @param platformsite    PlatformSite
        @throws NotFound    object with specified id does not exist
        """
        pass

    def read_platformsite(self, platformsite_id=''):
        """Read a PlatformSite resource

        @param platformsite_id    str
        @retval platformsite    PlatformSite
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_platformsite(self, platformsite_id=''):
        """Delete a PlatformSite resource, removes assocations to parents

        @param platformsite_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def create_instrumentsite(self, instrumentsite=None, parent_id=''):
        """Create a InstrumentSite resource. A instrumentsite is a frame of reference within an observatory. It's parent is
        either the observatory or another instrumentsite.

        @param instrumentsite    InstrumentSite
        @param parent_id    str
        @retval instrumentsite_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        pass

    def update_instrumentsite(self, instrumentsite=None):
        """Update a InstrumentSite resource

        @param instrumentsite    InstrumentSite
        @throws NotFound    object with specified id does not exist
        """
        pass

    def read_instrumentsite(self, instrumentsite_id=''):
        """Read a InstrumentSite resource

        @param instrumentsite_id    str
        @retval instrumentsite    InstrumentSite
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_instrumentsite(self, instrumentsite_id=''):
        """Delete a PlatformSite resource, removes assocations to parents

        @param instrumentsite_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def assign_site_to_site(self, child_site_id='', parent_site_id=''):
        """Connects a child site (any subtype) to a parent site (any subtype)

        @param child_site_id    str
        @param parent_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def unassign_site_to_site(self, child_site_id='', parent_site_id=''):
        """Disconnects a child site (any subtype) from a parent site (any subtype)

        @param child_site_id    str
        @param parent_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass







  