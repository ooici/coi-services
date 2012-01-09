#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.public import log
from interface.services.dm.itransform_management_service import BaseTransformManagementService

class TransformManagementService(BaseTransformManagementService):

    def create_transform(self, transform={}):
        """Creates the transform and registers it with the resource registry
        @param transform The transform to register
        @return The transform_id to the transform
        """
        log.debug('Creating Transfrom: %s' % str(transform))
        transform_id, rev_id = self.clients.resource_registry.create(transform)
        return transform_id


    def update_transform(self, transform={}):
        """Updates a given transform
        @param transform The transform to update
        @return The transform_id to the transform
        """
        log.debug('Updating Transform: %s' % str(transform))
        transform_id, rev_id = self.clients.resource_registry.update(transform)
        return transform_id


    def read_transform(self, transform_id=''):
        """Reads a transform from the resource registry
        @param transform_id The unique transform identifier
        @return IonObject of the transform
        @throws NotFound when transform doesn't exist
        """

        log.debug('Reading Transform: %s' % transform_id)
        transform = self.clients.resource_registry.read(object_id=transform_id,rev_id='')
        return transform
        

    def delete_transform(self, transform_id=''):
        """Deletes a transform from the resource registry
        @param transform_id The unique transform identifier
        @throws NotFound when a transform doesn't exist
        """
        transform = self.read_transform(transform_id=transform_id)
        log.debug('Deleting Transform: %s' % transform_id)
        self.clients.resource_registry.delete(transform)

    def bind_transform(self, transform_id=''):
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
