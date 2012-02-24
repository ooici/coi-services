#!/usr/bin/env python
import gevent
from gevent.greenlet import Greenlet
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
        self.datastore_name = self.CFG.get('process',{}).get('datastore_name','scidata')
        self.db = self.container.datastore_manager.get_datastore(self.datastore_name,DataStore.DS_PROFILE.SCIDATA)

    """
    class docstring
    """

    def create_dataset(self, stream_id='', datastore_name='', view_name='', name='', description='', contact=None, user_metadata=None):
        """@brief Create a resource which defines a dataset. For LCA it is assumed that datasets are organized by stream.
        @param stream_id is the primary key used in the couch view to retrieve the content or metadata
        @param datastore_name is the name of the datastore where this dataset resides.
        @param view_name is the name of the view which joins the dataset definition to the dataset
        @param contact is the contact information for the dataset adminstrator
        @param user_metadata is user defined metadata which can be added to this dataset. Should be annotation via association
        @param name is the name of the dataset resource
        @param description is a description of the dataset resource

        @param stream_id    str
        @param datastore_name    str
        @param view_name    str
        @param name    str
        @param description    str
        @param contact    ContactInformation
        @param user_metadata    Unknown
        @retval dataset_id    str
        """
        if not (stream_id and datastore_name):
            raise BadRequest("You must provide a stream_id and datastore name by which to identify this dataset.")

        dataset = DataSet()
        dataset.description=description
        dataset.name=name or stream_id
        dataset.primary_view_key=stream_id
        dataset.datastore_name = datastore_name
        #@todo: fill this in
        dataset.view_name=view_name or 'dataset_by_id'


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
        dataset = self.read_dataset(dataset_id=dataset_id)
        key = dataset.primary_view_key # stream_id
        ar = gevent.event.AsyncResult()
        def ar_timeout(db):
            opts = {
                'start_key':[key,0],
                'end_key':[key,2]
            }
            try:
                results = db.query_view("datasets/bounds",opts=opts)[0]['value']
            except IndexError:
                # Means there are no results
                results = {}
            ar.set(results)
        db = self.container.datastore_manager.get_datastore(dataset.datastore_name)
        g = Greenlet(ar_timeout,db)
        g.start()
        bounds = ar.get(timeout=5)

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

