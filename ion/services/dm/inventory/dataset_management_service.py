#!/usr/bin/env python
from pyon.core.exception import BadRequest

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.idataset_management_service import BaseDatasetManagementService

class DatasetManagementService(BaseDatasetManagementService):
    def __init__(self, *args, **kwargs):
        super(DatasetManagementService, self).__init__(*args,**kwargs)
        self.logging_name = '(DatasetManagementService %s)' % (self.name or self.id)

    """
    class docstring
    """

    def create_dataset(self, dataset=None):
        """
        method docstring
        """
        if not dataset:
            raise BadRequest('%s: No dataset provided.' % self.logging_name)
        dataset_id, _ = self.clients.resource_registry.create(dataset)
        return dataset_id

    def update_dataset(self, dataset=None):
        """
        method docstring
        """
        if not (dataset and dataset._id):
            raise BadRequest('%s: Dataset either not provided or malformed.' % self.logging_name)
        self.clients.resource_registry.update(dataset)
        #@todo: Check to make sure retval is boolean
        return True

    def read_dataset(self, dataset_id=''):
        """

        @throws NotFound if resource does not exist.
        """
        return self.clients.resource_registry.read(dataset_id)

    def delete_dataset(self, dataset_id=''):
        """
        @throws NotFound if resource does not exist.
        """
        self.clients.resource_registry.delete(dataset_id)

    def find_datasets(self, filters=None):
        """
        method docstring
        """
        pass
  