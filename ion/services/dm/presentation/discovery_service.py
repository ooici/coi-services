#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.idiscovery_service import BaseDiscoveryService


class DiscoveryService(BaseDiscoveryService):

    """
    class docstring
    """

    def create_view(self, view={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {view_id: ''}
        #
        pass

    def update_view(self, view={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_view(self, view_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # view: {}
        #
        pass

    def delete_view(self, view_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def define_search(self, query=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_by_metadata(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # results: []
        #
        pass

    def find_by_type(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # results: []
        #
        pass
  