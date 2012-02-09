#!/usr/bin/env python
from pyon.core.exception import BadRequest

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.idataset_management_service import BaseDatasetManagementService
from interface.objects import DataSet
from pyon.datastore.datastore import DataStore

class DatasetManagementService(BaseDatasetManagementService):
    def __init__(self, *args, **kwargs):
        super(DatasetManagementService, self).__init__(*args,**kwargs)
        self.logging_name = '(DatasetManagementService %s)' % (self.name or self.id)

    def on_start(self):
        super(DatasetManagementService,self).on_start()
        self.db = self.container.datastore_manager.get_datastore('scidata',DataStore.DS_PROFILE.SCIDATA)

    """
    class docstring
    """

    def create_dataset(self, stream_id='', name='', description='', contact=None, user_metadata={}):
        """@brief Create a resource which defines a dataset. For LCA it is assumed that datasets are organized by stream.
        @param stream_id is the primary key used in the couch view to retrieve the content or metadata
        @param contact is the contact information for the dataset adminstrator
        @param user_metadata is user defined metadata which can be added to this dataset. Should be annotation via association
        @param name is the name of the dataset resource
        @param description is a description of the dataset resource

        @param stream_id    str
        @param name    str
        @param description    str
        @param contact    ContactInformation
        @param user_metadata    Unknown
        @retval dataset_id    str
        """

        dataset = DataSet()
        dataset.description=description
        dataset.name=name
        dataset.primary_view_key=stream_id
        #@todo: fill this in
        dataset.view_name='dataset_by_id'


        dataset_id, _ = self.clients.resource_registry.create(dataset)
        return dataset_id

    def update_dataset(self, dataset=None):
        """@todo document this interface!!!

        @param dataset    DataSet
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

    def get_dataset_bounds(self, dataset_id=''):
        """@brief Get the bounding coordinates of the dataset using a couch map/reduce query
        @param dataset_id
        @result bounds is a dictionary containing spatial and temporal bounds of the dataset in standard units

        @param dataset_id    str
        @retval bounds    Unknown
        """
        #dataset = self.read_dataset(dataset_id=dataset_id)
        bounds = {
            'geo':{
                'latmin':0,
                'lonmin':0,
                'latmax':0,
                'lonmax':0
            },
            'height':[0,0],
            'time':[0,0]
        }

        bounds['geo']['latmin'] = self.db.query_view('datasets/lattitude_min')[0]['value']
        bounds['geo']['latmax'] = self.db.query_view('datasets/lattitude_max')[0]['value']
        bounds['geo']['lonmin'] = self.db.query_view('datasets/longitude_min')[0]['value']
        bounds['geo']['lonmax'] = self.db.query_view('datasets/longitude_max')[0]['value']
        bounds['height'][0] = self.db.query_view('datasets/depth_min')[0]['value']
        bounds['height'][1] = self.db.query_view('datasets/depth_max')[0]['value']
        bounds['time'][0] = self.db.query_view('datasets/time_min')[0]['value']
        bounds['time'][1] = self.db.query_view('datasets/time_max')[0]['value']
        return bounds

    def get_dataset_metadata(self, dataset_id=''):
        """@brief Get the metadata for the dataset using a couch map/reduce query
        @param dataset_id
        @result the aggregated available metadata for the specified dataset

        @param dataset_id    str
        @retval metadata    Unknown
        """
        dataset = self.read_dataset(dataset_id=dataset_id)
        #@todo: Perform Query
        return ''
    def find_datasets(self, filters=None):
        """
        method docstring
        """
        pass
  