#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.itransform_management_service import BaseTransformManagementService

class TransformManagementService(BaseTransformManagementService):

    def create_transform(self, in_subscription_id='', out_stream_id='', process_definition_id='', configuration={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {transform_id: ''}
        #
        pass

    def update_transform(self, configuration={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_transform(self, transform_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # transform: {}
        #
        pass

    def delete_transform(self, transform_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def activate_transform(self, transform_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def schedule_transform(self, transform_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass
