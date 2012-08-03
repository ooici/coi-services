#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/inventory/dataset_management_service.py
@date Tue Jul 24 08:59:29 EDT 2012
@brief Dataset Management Service implementation
'''
from pyon.core.exception import BadRequest
from pyon.public import PRED
from pyon.datastore.datastore import DataStore
from interface.services.dm.idataset_management_service import BaseDatasetManagementService
from interface.objects import DataSet
from ion.services.dm.utility.granule_utils import SimplexCoverage, ParameterDictionary, GridDomain
from pyon.util.arg_check import validate_is_instance, validate_true, validate_is_not_none
from pyon.util.file_sys import FileSystem, FS


class DatasetManagementService(BaseDatasetManagementService):
    DEFAULT_DATASTORE = 'datasets'
    DEFAULT_VIEW      = 'manifest/by_dataset'
    def __init__(self, *args, **kwargs):
        super(DatasetManagementService, self).__init__(*args,**kwargs)
        self.logging_name = '(DatasetManagementService %s)' % (self.name or self.id)

    def on_start(self):
        super(DatasetManagementService,self).on_start()
        self.datastore_name = self.CFG.get_safe('process.datastore_name', self.DEFAULT_DATASTORE)
        #--------------------------------------------------------------------------------
        # Create the datastore if it doesn't already exist.
        #--------------------------------------------------------------------------------
        self.db = self.container.datastore_manager.get_datastore(self.datastore_name,DataStore.DS_PROFILE.SCIDATA)

    """
    class docstring
    """

    def create_dataset(self, name='', datastore_name='', view_name='', stream_id='', parameter_dict=None, spatial_domain=None,temporal_domain=None, description=''):
#        validate_true(name and parameter_dict and temporal_domain and spatial_domain, 'Datasets require name, parameter dictionary, temporal and spatial domains.')

        validate_is_not_none(parameter_dict, 'A parameter dictionary must be supplied to register a new dataset.')
        validate_is_not_none(spatial_domain, 'A spatial domain must be supplied to register a new dataset.')
        validate_is_not_none(temporal_domain, 'A temporal domain must be supplied to register a new dataset.')

        dataset                      = DataSet()
        dataset.description          = description
        dataset.name                 = name
        dataset.primary_view_key     = stream_id or None
        dataset.datastore_name       = datastore_name or self.DEFAULT_DATASTORE
        dataset.view_name            = view_name or self.DEFAULT_VIEW
        dataset.parameter_dictionary = parameter_dict
        dataset.temporal_domain      = temporal_domain
        dataset.spatial_domain       = spatial_domain


        dataset_id, _ = self.clients.resource_registry.create(dataset)
        if stream_id:
            self.add_stream(dataset_id,stream_id)

        coverage = self._create_coverage(description or dataset_id, parameter_dict, spatial_domain, temporal_domain) 
        self._persist_coverage(dataset_id, coverage)

        return dataset_id


    def _create_coverage(self, description, parameter_dict, spatial_domain,temporal_domain):
        #--------------------------------------------------------------------------------
        # Create coversions and deserialization then craft the coverage
        #--------------------------------------------------------------------------------

        pdict = ParameterDictionary.load(parameter_dict)
        sdom = GridDomain.load(spatial_domain)
        tdom = GridDomain.load(temporal_domain)

        scov = SimplexCoverage(description, parameter_dictionary=pdict, spatial_domain=sdom, temporal_domain=tdom)
        return scov

    @classmethod
    def _persist_coverage(cls, dataset_id, coverage):
        validate_is_instance(coverage,SimplexCoverage,'Coverage is not an instance of SimplexCoverage: %s' % type(coverage))
        filename = FileSystem.get_hierarchical_url(FS.CACHE, dataset_id, '.cov')
        SimplexCoverage.save(coverage, filename, use_ascii=False)

    @classmethod
    def _get_coverage(cls,dataset_id):
        filename = FileSystem.get_hierarchical_url(FS.CACHE, dataset_id, '.cov')
        coverage = SimplexCoverage.load(filename)
        return coverage


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
        retval = self.clients.resource_registry.read(dataset_id)
        validate_is_instance(retval,DataSet)
        return retval

    def delete_dataset(self, dataset_id=''):
        """
        @throws NotFound if resource does not exist.

        """
        assocs = self.clients.resource_registry.find_associations(subject=dataset_id,predicate=PRED.hasStream)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)
        self.clients.resource_registry.delete(dataset_id)

    def add_stream(self,dataset_id='', stream_id=''):
        validate_true(dataset_id and stream_id, 'Clients must provide both the dataset_id and stream_id')
        self.clients.resource_registry.create_association(subject=dataset_id, predicate=PRED.hasStream,object=stream_id)

    def remove_stream(self,dataset_id='', stream_id=''):
        validate_true(dataset_id and stream_id, 'Clients must provide both the dataset_id and stream_id')
        assocs = self.clients.resource_registry.find_associations(subject=dataset_id, predicate=PRED.hasStream,object=stream_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)


    def get_dataset_info(self,dataset_id=''):
        coverage = self._get_coverage(dataset_id)
        return coverage.info

    def get_dataset_parameters(self, dataset_id=''):
        coverage = self._get_coverage(dataset_id)
        return coverage.parameter_dictionary.dump()

    def get_dataset_bounds(self, dataset_id=''):
        raise NotImplementedError('This function is deprecated and does not exist')

    def get_dataset_metadata(self, dataset_id=''):
        raise NotImplementedError('This function does not exist')

    def find_datasets(self, filters=None):
        raise NotImplementedError('This function does not exist')

