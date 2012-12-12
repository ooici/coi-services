#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/inventory/dataset_management_service.py
@date Tue Jul 24 08:59:29 EDT 2012
@brief Dataset Management Service implementation
'''
from pyon.public import PRED, RT
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.datastore.datastore import DataStore
from pyon.net.endpoint import RPCClient
from pyon.util.arg_check import validate_is_instance, validate_true, validate_is_not_none
from pyon.util.file_sys import FileSystem, FS
from pyon.util.log import log

from ion.services.dm.utility.granule_utils import SimplexCoverage, ParameterDictionary, GridDomain, ParameterContext

from interface.objects import ParameterContextResource, ParameterDictionaryResource
from interface.objects import DataSet
from interface.services.dm.idataset_management_service import BaseDatasetManagementService, DatasetManagementServiceClient

from coverage_model.basic_types import AxisTypeEnum

import os


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

    def create_dataset(self, name='', datastore_name='', view_name='', stream_id='', parameter_dict=None, spatial_domain=None, temporal_domain=None, parameter_dictionary_id='', description=''):

        validate_true(parameter_dict or parameter_dictionary_id, 'A parameter dictionary must be supplied to register a new dataset.')
        validate_is_not_none(spatial_domain, 'A spatial domain must be supplied to register a new dataset.')
        validate_is_not_none(temporal_domain, 'A temporal domain must be supplied to register a new dataset.')
        
        if parameter_dictionary_id:
            pd = self.read_parameter_dictionary(parameter_dictionary_id)
            pcs = self.read_parameter_contexts(parameter_dictionary_id, id_only=False)
            parameter_dict = self._merge_contexts([ParameterContext.load(i.parameter_context) for i in pcs], pd.temporal_context)
            parameter_dict = parameter_dict.dump()

        dataset                      = DataSet()
        dataset.description          = description
        dataset.name                 = name
        dataset.primary_view_key     = stream_id or None
        dataset.datastore_name       = datastore_name or self.DEFAULT_DATASTORE
        dataset.view_name            = view_name or self.DEFAULT_VIEW
        dataset.parameter_dictionary = parameter_dict
        dataset.temporal_domain      = temporal_domain
        dataset.spatial_domain       = spatial_domain
        dataset.registered           = False


        dataset_id, _ = self.clients.resource_registry.create(dataset)
        if stream_id:
            self.add_stream(dataset_id,stream_id)


        cov = self._create_coverage(dataset_id, description or dataset_id, parameter_dict, spatial_domain, temporal_domain) 
        self._save_coverage(cov)
        cov.close()

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

    def register_dataset(self, dataset_id='', external_data_product_name=''):
        dataset_obj = self.read_dataset(dataset_id)
        dataset_obj.registered = True
        self.update_dataset(dataset=dataset_obj)
        external_data_product_name = external_data_product_name or dataset_obj.name

        procs,_ = self.clients.resource_registry.find_resources(restype=RT.Process, id_only=True)
        pid = None
        for p in procs:
            if 'registration_worker' in p:
                pid = p
        if not pid: 
            log.warning('No registration worker found')
            return
        rpc_cli = RPCClient(to_name=pid)
        rpc_cli.request({'dataset_id':dataset_id, 'data_product_name':external_data_product_name}, op='register_dap_dataset')


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

    def get_dataset_length(self, dataset_id=''):
        coverage = self._get_coverage(dataset_id)
        return coverage.num_timesteps

#--------

    def create_parameter_context(self, name='', parameter_context=None, description=''):
        res, _ = self.clients.resource_registry.find_resources(restype=RT.ParameterContextResource, name=name, id_only=False)
        if len(res):
            if res[0].name == name and self._compare_pc(res[0].parameter_context, parameter_context):
                return res[0]._id
            else:
                raise Conflict('Parameter Context %s already exists with a different definition' % name)
        
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

    def read_parameter_context_by_name(self, name='', id_only=False):
        res, _ = self.clients.resource_registry.find_resources(restype=RT.ParameterContextResource, name=name, id_only=id_only)
        if not len(res):
            raise NotFound('Unable to locate context with name: %s' % name)
        return res[0]

#--------

    def create_parameter_dictionary(self, name='', parameter_context_ids=None, temporal_context='', description=''):
        res, _ = self.clients.resource_registry.find_resources(restype=RT.ParameterDictionaryResource, name=name, id_only=True)
        if len(res):
            context_ids,_ = self.clients.resource_registry.find_objects(subject=res[0], predicate=PRED.hasParameterContext, id_only=True)
            context_ids.sort()
            parameter_context_ids.sort()
            if context_ids == parameter_context_ids:
                return res[0]
            else:
                raise Conflict('A parameter dictionary with name %s already exists and has a different definition' % name)
        validate_true(name, 'Name field may not be empty.')
        parameter_context_ids = parameter_context_ids or []
        pd_res = ParameterDictionaryResource(name=name, temporal_context=temporal_context, description=description)
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

    def read_parameter_contexts(self, parameter_dictionary_id='', id_only=False):
        pcs, assocs = self.clients.resource_registry.find_objects(subject=parameter_dictionary_id, predicate=PRED.hasParameterContext, id_only=id_only)
        return pcs

    def read_parameter_dictionary_by_name(self, name='', id_only=False):
        res, _ = self.clients.resource_registry.find_resources(restype=RT.ParameterDictionaryResource, name=name, id_only=id_only)
        if not len(res):
            raise NotFound('Unable to locate context with name: %s' % name)
        return res[0]

#--------

    def dataset_bounds(self, dataset_id='', parameters=None):
        self.read_dataset(dataset_id) # Validates proper dataset
        parameters = parameters or None
        coverage = self._get_coverage(dataset_id)
        if not coverage.num_timesteps:
            if isinstance(parameters,list):
                return {i:(coverage.get_parameter_context(i).fill_value,coverage.get_parameter_context(i).fill_value) for i in parameters}
            elif not parameters: 
                return {i:(coverage.get_parameter_context(i).fill_value,coverage.get_parameter_context(i).fill_value) for i in coverage.list_parameters()}
            else:
                return (coverage.get_parameter_context(parameters).fill_value, coverage.get_parameter_context(parameters).fill_value)
        return coverage.get_data_bounds(parameters)

    def dataset_bounds_by_axis(self, dataset_id='', axis=None):
        self.read_dataset(dataset_id) # Validates proper dataset
        axis = axis or None
        coverage = self._get_coverage(dataset_id)
        if not coverage.num_timesteps:
            temporal = coverage.temporal_parameter_name
            if isinstance(axis,list):
                return {temporal:(coverage.get_parameter_context(temporal).fill_value, coverage.get_parameter_context(temporal).fill_value)}
            elif not axis:
                return {temporal:(coverage.get_parameter_context(temporal).fill_value, coverage.get_parameter_context(temporal).fill_value)}
            else:
                return (coverage.get_parameter_context(temporal).fill_value, coverage.get_parameter_context(temporal).fill_value)
        return coverage.get_data_bounds_by_axis(axis)

    def dataset_extents(self, dataset_id='', parameters=None):
        self.read_dataset(dataset_id)
        parameters = parameters or None
        coverage = self._get_coverage(dataset_id)
        return coverage.get_data_extents(parameters)

    def dataset_extents_by_axis(self, dataset_id='', axis=None):
        self.read_dataset(dataset_id) 
        axis = axis or None
        coverage = self._get_coverage(dataset_id)
        return coverage.get_data_extents_by_axis(axis)

    def dataset_size(self,dataset_id='', parameters=None, slice_=None, in_bytes=False):
        self.read_dataset(dataset_id) 
        parameters = parameters or None
        slice_     = slice_ if isinstance(slice_, slice) else None

        coverage = self._get_coverage(dataset_id)
        return coverage.get_data_size(parameters, slice_, in_bytes)

#--------

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

    @classmethod
    def get_parameter_context_by_name(cls, name=''):
        dms_cli = DatasetManagementServiceClient()
        pc_res = dms_cli.read_parameter_context_by_name(name=name, id_only=False)
        pc = ParameterContext.load(pc_res.parameter_context)
        pc._identifier = pc_res._id
        return pc
    
    @classmethod
    def get_parameter_dictionary(cls, parameter_dictionary_id=''):
        '''
        Preferred client-side class method for constructing a parameter dictionary
        from a service call.
        '''
        dms_cli = DatasetManagementServiceClient()
        pd  = dms_cli.read_parameter_dictionary(parameter_dictionary_id)
        pcs = dms_cli.read_parameter_contexts(parameter_dictionary_id=parameter_dictionary_id, id_only=False)

        pdict = cls._merge_contexts([ParameterContext.load(i.parameter_context) for i in pcs], pd.temporal_context)
        pdict._identifier = parameter_dictionary_id

        return pdict

    @classmethod
    def get_parameter_dictionary_by_name(cls, name=''):
        dms_cli = DatasetManagementServiceClient()
        pd_res = dms_cli.read_parameter_dictionary_by_name(name=name, id_only=True)
        return cls.get_parameter_dictionary(pd_res)

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

    def _create_coverage(self, dataset_id, description, parameter_dict, spatial_domain,temporal_domain):
        pdict = ParameterDictionary.load(parameter_dict)
        sdom = GridDomain.load(spatial_domain)
        tdom = GridDomain.load(temporal_domain)
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        scov = SimplexCoverage(file_root,dataset_id,description or dataset_id,parameter_dictionary=pdict, temporal_domain=tdom, spatial_domain=sdom)
        return scov

    @classmethod
    def _save_coverage(cls, coverage):
        coverage.flush()

    @classmethod
    def _get_coverage(cls,dataset_id,mode='w'):
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        coverage = SimplexCoverage(file_root, dataset_id,mode=mode)
        return coverage

    @classmethod
    def _get_coverage_path(cls, dataset_id):
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        return os.path.join(file_root, '%s' % dataset_id)
    
    @classmethod
    def _compare_pc(cls, pc1, pc2):
        if pc1:
            pc1 = ParameterContext.load(pc1) or {}
        if pc2:
            pc2 = ParameterContext.load(pc2) or {}
        return bool(pc1 == pc2)
            
    @classmethod
    def _merge_contexts(cls, contexts, temporal):
        pdict = ParameterDictionary()
        for context in contexts:
            if context.name == temporal:
                context.axis = AxisTypeEnum.TIME
                pdict.add_context(context, is_temporal=True)
            else:
                pdict.add_context(context)
        return pdict

