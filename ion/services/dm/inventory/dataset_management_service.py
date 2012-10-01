#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/inventory/dataset_management_service.py
@date Tue Jul 24 08:59:29 EDT 2012
@brief Dataset Management Service implementation
'''
from pyon.public import PRED
from pyon.core.exception import BadRequest
from pyon.datastore.datastore import DataStore
from pyon.util.arg_check import validate_is_instance, validate_true, validate_is_not_none
from pyon.util.file_sys import FileSystem, FS
from pyon.util.log import log

from ion.services.dm.utility.granule_utils import SimplexCoverage, ParameterDictionary, GridDomain, ParameterContext

from interface.objects import ParameterContextResource, ParameterDictionaryResource
from interface.objects import DataSet
from interface.services.dm.idataset_management_service import BaseDatasetManagementService, DatasetManagementServiceClient


class DatasetManagementService(BaseDatasetManagementService):
    DEFAULT_DATASTORE = 'datasets'
    DEFAULT_VIEW      = 'manifest/by_dataset'
    def __init__(self, *args, **kwargs):
        super(DatasetManagementService, self).__init__(*args,**kwargs)
        self.logging_name = '(DatasetManagementService %s)' % (self.name or self.id)

    def on_start(self):
        super(DatasetManagementService,self).on_start()
        self.datastore_name = self.CFG.get_safe('process.datastore_name', self.DEFAULT_DATASTORE)
        self.db = self.container.datastore_manager.get_datastore(self.datastore_name,DataStore.DS_PROFILE.SCIDATA)

#--------

    def create_dataset(self, name='', datastore_name='', view_name='', stream_id='', parameter_dict=None, spatial_domain=None,temporal_domain=None, description=''):

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

    def read_dataset(self, dataset_id=''):
        retval = self.clients.resource_registry.read(dataset_id)
        validate_is_instance(retval,DataSet)
        return retval

    def update_dataset(self, dataset=None):
        if not (dataset and dataset._id):
            raise BadRequest('%s: Dataset either not provided or malformed.' % self.logging_name)
        self.clients.resource_registry.update(dataset)
        #@todo: Check to make sure retval is boolean
        return True

    def delete_dataset(self, dataset_id=''):
        assocs = self.clients.resource_registry.find_associations(subject=dataset_id,predicate=PRED.hasStream)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)
        self.clients.resource_registry.delete(dataset_id)

#--------

    def add_stream(self,dataset_id='', stream_id=''):
        log.info('Adding stream %s to dataset %s', stream_id, dataset_id)
        validate_true(dataset_id and stream_id, 'Clients must provide both the dataset_id and stream_id')
        self.clients.resource_registry.create_association(subject=dataset_id, predicate=PRED.hasStream,object=stream_id)

    def remove_stream(self,dataset_id='', stream_id=''):
        log.info('Removing stream %s from dataset %s', stream_id, dataset_id)
        validate_true(dataset_id and stream_id, 'Clients must provide both the dataset_id and stream_id')
        assocs = self.clients.resource_registry.find_associations(subject=dataset_id, predicate=PRED.hasStream,object=stream_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)

#--------

    def get_dataset_info(self,dataset_id=''):
        coverage = self._get_coverage(dataset_id)
        return coverage.info

    def get_dataset_parameters(self, dataset_id=''):
        coverage = self._get_coverage(dataset_id)
        return coverage.parameter_dictionary.dump()

#--------

    def create_parameter_context(self, name='', parameter_context=None, description=''):
        validate_true(name, 'Name field may not be empty')
        validate_is_instance(parameter_context, dict, 'parameter_context field is not dictable.')
        pc_res = ParameterContextResource(name=name, parameter_context=parameter_context, description=description)
        pc_id, ver = self.clients.resource_registry.create(pc_res)
        return pc_id

    def read_parameter_context(self, parameter_context_id=''):
        res = self.clients.resource_registry.read(parameter_context_id)
        validate_is_instance(res,ParameterContextResource)
        return res

    def delete_parameter_context(self, parameter_context_id=''):
        self.read_parameter_context(parameter_context_id)
        self.clients.resource_registry.delete(parameter_context_id)
        return True

#--------

    def create_parameter_dictionary(self, name='', parameter_context_ids=None, description=''):
        validate_true(name, 'Name field may not be empty.')
        parameter_context_ids = parameter_context_ids or []
        pd_res = ParameterDictionaryResource(name=name, description=description)
        pd_res_id, ver = self.clients.resource_registry.create(pd_res)
        for pc_id in parameter_context_ids:
            self._link_pcr_to_pdr(pc_id, pd_res_id)
        return pd_res_id

    def read_parameter_dictionary(self, parameter_dictionary_id=''):
        res = self.clients.resource_registry.read(parameter_dictionary_id)
        validate_is_instance(res, ParameterDictionaryResource, 'Resource is not a valid ParameterDictionaryResource')
        return res

    def delete_parameter_dictionary(self, parameter_dictionary_id=''):
        self.read_parameter_dictionary(parameter_dictionary_id)
        self._unlink_pdr(parameter_dictionary_id)
        self.clients.resource_registry.delete(parameter_dictionary_id)
        return True

#--------

    def add_context(self, parameter_dictionary_id='', parameter_context_ids=None):
        for pc_id in parameter_context_ids:
            self._link_pcr_to_pdr(pc_id, parameter_dictionary_id)
        return True

    def remove_context(self, parameter_dictionary_id='', parameter_context_ids=None):
        for pc_id in parameter_context_ids:
            self._unlink_pcr_to_pdr(pc_id, parameter_dictionary_id)
        return True

#--------

    def read_parameter_contexts(self, parameter_dictionary_id='', id_only=False):
        pcs, assocs = self.clients.resource_registry.find_objects(subject=parameter_dictionary_id, predicate=PRED.hasParameterContext, id_only=id_only)
        return pcs

#--------

    @classmethod
    def get_parameter_dictionary(cls, parameter_dictionary_id=''):
        '''
        Preferred client-side class method for constructing a parameter dictionary
        from a service call.
        '''
        dms_cli = DatasetManagementServiceClient()
        pcs = dms_cli.read_parameter_contexts(parameter_dictionary_id=parameter_dictionary_id, id_only=False)

        pdict = ParameterDictionary()
        for pc_res in pcs:
            pc = ParameterContext.load(pc_res.parameter_context)
            pdict.add_context(pc)

        pdict._identifier = parameter_dictionary_id

        return pdict

    @classmethod
    def get_parameter_context(cls, parameter_context_id=''):
        '''
        Preferred client-side class method for constructing a parameter context
        from a service call.
        '''
        dms_cli = DatasetManagementServiceClient()
        pc_res = dms_cli.read_parameter_context(parameter_context_id=parameter_context_id)
        pc = ParameterContext.load(pc_res.parameter_context)
        pc._identifier = pc_res._id
        return pc

#--------

    def _link_pcr_to_pdr(self, pcr_id, pdr_id):
        self.clients.resource_registry.create_association(subject=pdr_id, predicate=PRED.hasParameterContext,object=pcr_id)

    def _unlink_pcr_to_pdr(self, pcr_id, pdr_id):
        assocs = self.clients.resource_registry.find_associations(subject=pdr_id, predicate=PRED.hasParameterContext, object=pcr_id, id_only=True)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)

    def _unlink_pdr(self, pdr_id):
        objects, assocs = self.clients.resource_registry.find_objects(subject=pdr_id, predicate=PRED.hasParameterContext, id_only=True)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)

    def _create_coverage(self, description, parameter_dict, spatial_domain,temporal_domain):

        pdict = ParameterDictionary.load(parameter_dict)
        sdom = GridDomain.load(spatial_domain)
        tdom = GridDomain.load(temporal_domain)

        scov = SimplexCoverage(description, parameter_dictionary=pdict, temporal_domain=tdom, spatial_domain=sdom)
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

