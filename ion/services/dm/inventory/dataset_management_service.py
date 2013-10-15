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
from ion.util.time_utils import TimeUtils

from interface.objects import ParameterContext as ParameterContextResource, ParameterDictionary as ParameterDictionaryResource, ParameterFunction as ParameterFunctionResource
from interface.objects import Dataset
from interface.services.dm.idataset_management_service import BaseDatasetManagementService, DatasetManagementServiceClient

from coverage_model.basic_types import AxisTypeEnum
from coverage_model import AbstractCoverage, ViewCoverage, ComplexCoverage, ComplexCoverageType
from coverage_model.parameter_functions import AbstractFunction

from uuid import uuid4

import os
import numpy as np

class DatasetManagementService(BaseDatasetManagementService):
    DEFAULT_DATASTORE = 'datasets'
    DEFAULT_VIEW      = 'manifest/by_dataset'
    
    def __init__(self, *args, **kwargs):
        super(DatasetManagementService, self).__init__(*args,**kwargs)
        self.logging_name = '(DatasetManagementService %s)' % (self.name or self.id)

    def on_start(self):
        super(DatasetManagementService,self).on_start()
        self.datastore_name = self.CFG.get_safe('process.datastore_name', self.DEFAULT_DATASTORE)
        self.inline_data_writes  = self.CFG.get_safe('service.ingestion_management.inline_data_writes', True)
        #self.db = self.container.datastore_manager.get_datastore(self.datastore_name,DataStore.DS_PROFILE.SCIDATA)

#--------

    def create_dataset(self, name='', datastore_name='', view_name='', stream_id='', parameter_dict=None, spatial_domain=None, temporal_domain=None, parameter_dictionary_id='', description='', parent_dataset_id=''):
        validate_true(parameter_dict or parameter_dictionary_id, 'A parameter dictionary must be supplied to register a new dataset.')
        validate_is_not_none(spatial_domain, 'A spatial domain must be supplied to register a new dataset.')
        validate_is_not_none(temporal_domain, 'A temporal domain must be supplied to register a new dataset.')
        
        if parameter_dictionary_id:
            pd = self.read_parameter_dictionary(parameter_dictionary_id)
            pcs = self.read_parameter_contexts(parameter_dictionary_id, id_only=False)
            parameter_dict = self._merge_contexts([ParameterContext.load(i.parameter_context) for i in pcs], pd.temporal_context)
            parameter_dict = parameter_dict.dump()

        parameter_dict = self.numpy_walk(parameter_dict)

        dataset                      = Dataset()
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

        log.debug('creating dataset: %s', dataset_id)
        if parent_dataset_id:
            vcov = self._create_view_coverage(dataset_id, description or dataset_id, parent_dataset_id)
            vcov.close()
            return dataset_id

        cov = self._create_coverage(dataset_id, description or dataset_id, parameter_dict, spatial_domain, temporal_domain) 
        self._save_coverage(cov)
        cov.close()

        return dataset_id

    def read_dataset(self, dataset_id=''):
        retval = self.clients.resource_registry.read(dataset_id)
        validate_is_instance(retval,Dataset)
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

    def register_dataset(self, data_product_id=''):
        procs,_ = self.clients.resource_registry.find_resources(restype=RT.Process, id_only=True)
        pid = None
        for p in procs:
            if 'registration_worker' in p:
                pid = p
        if not pid: 
            log.warning('No registration worker found')
            return
        rpc_cli = RPCClient(to_name=pid)
        rpc_cli.request({'data_product_id':data_product_id}, op='register_dap_dataset')


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

    @classmethod
    def numpy_walk(cls,obj):
        try:
            if np.isnan(obj):
                return {'__nan__':0}
        except TypeError:
            pass
        except NotImplementedError:
            pass

        if isinstance(obj, np.number):
            return np.asscalar(obj)
        if isinstance(obj, np.dtype):
            return {'__np__':obj.str}
        if isinstance(obj,dict):
            if '__nan__' in obj and len(obj)==1:
                return np.nan
            if '__np__' in obj and len(obj)==1:
                return np.dtype(obj['__np__'])
            return {k:cls.numpy_walk(v) for k,v in obj.iteritems()}
        if isinstance(obj,list):
            return map(cls.numpy_walk, obj)
        if isinstance(obj, tuple):
            return tuple(map(cls.numpy_walk, obj))
        return obj


    def create_parameter_context(self, name='', parameter_context=None, description='', reference_urls=None, parameter_type='', internal_name='', value_encoding='', code_report='', units='', fill_value='', display_name='', parameter_function_id='', parameter_function_map='', standard_name='', ooi_short_name='', precision='', visible=True):
        
        validate_true(name, 'Name field may not be empty')
        validate_is_instance(parameter_context, dict, 'parameter_context field is not dictable.')
        parameter_context = self.numpy_walk(parameter_context)
        pc_res = ParameterContextResource(name=name, parameter_context=parameter_context, description=description)
        pc_res.reference_urls = reference_urls or []
        pc_res.parameter_type = parameter_type
        pc_res.internal_name = internal_name or name
        pc_res.value_encoding = value_encoding
        pc_res.code_report = code_report or ''
        pc_res.units = units
        pc_res.fill_value = fill_value
        pc_res.display_name = display_name
        pc_res.parameter_function_id = parameter_function_id
        pc_res.parameter_function_map = parameter_function_map
        pc_res.standard_name = standard_name
        pc_res.ooi_short_name = ooi_short_name
        pc_res.precision = precision or '5'
        pc_res.visible = visible

        pc_id, ver = self.clients.resource_registry.create(pc_res)
        if parameter_function_id:
            self.read_parameter_function(parameter_function_id)
            self.clients.resource_registry.create_association(subject=pc_id, predicate=PRED.hasParameterFunction, object=parameter_function_id)
        
        return pc_id

    def read_parameter_context(self, parameter_context_id=''):
        res = self.clients.resource_registry.read(parameter_context_id)
        validate_is_instance(res,ParameterContextResource)
        res.parameter_context = self.numpy_walk(res.parameter_context)
        return res

    def delete_parameter_context(self, parameter_context_id=''):
        self.read_parameter_context(parameter_context_id)
        self.clients.resource_registry.delete(parameter_context_id)
        return True

#--------

    def read_parameter_context_by_name(self, name='', id_only=False):
        res, _ = self.clients.resource_registry.find_resources(restype=RT.ParameterContext, name=name, id_only=id_only)
        if not len(res):
            raise NotFound('Unable to locate context with name: %s' % name)
        retval = res[0]
        if not id_only:
            retval.parameter_context = self.numpy_walk(retval.parameter_context)
        return retval

#--------

    def create_parameter_function(self, name='', parameter_function=None, description=''):
        validate_true(name, 'Name field may not be empty')
        validate_is_instance(parameter_function, dict, 'parameter_function field is not dictable.')
        parameter_function = self.numpy_walk(parameter_function)
        pf_res = ParameterFunctionResource(name=name, parameter_function=parameter_function, description=description)
        pf_id, ver = self.clients.resource_registry.create(pf_res)
        return pf_id

    def read_parameter_function(self, parameter_function_id=''):
        res = self.clients.resource_registry.read(parameter_function_id)
        validate_is_instance(res, ParameterFunctionResource)
        res.parameter_function = self.numpy_walk(res.parameter_function)
        return res

    def delete_parameter_function(self, parameter_function_id=''):
        self.read_parameter_function(parameter_function_id)
        self.clients.resource_registry.delete(parameter_function_id)
        return True

#--------

    def read_parameter_function_by_name(self, name='', id_only=False):
        res, _ = self.clients.resource_registry.find_resources(restype=RT.ParameterFunction,name=name, id_only=id_only)
        if not len(res):
            raise NotFound('Unable to locate parameter function with name: %s' % name)
        retval = res[0]
        retval.parameter_function = self.numpy_walk(retval.parameter_function)
        return retval

#--------

    def create_parameter_dictionary(self, name='', parameter_context_ids=None, temporal_context='', description=''):
        res, _ = self.clients.resource_registry.find_resources(restype=RT.ParameterDictionary, name=name, id_only=True)
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
        if not id_only:
            for pc in pcs:
                pc.parameter_context = self.numpy_walk(pc.parameter_context)
        return pcs

    def read_parameter_dictionary_by_name(self, name='', id_only=False):
        res, _ = self.clients.resource_registry.find_resources(restype=RT.ParameterDictionary, name=name, id_only=id_only)
        if not len(res):
            raise NotFound('Unable to locate dictionary with name: %s' % name)
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

    def dataset_temporal_bounds(self, dataset_id):
        coverage = self._get_coverage(dataset_id)
        temporal_param = coverage.temporal_parameter_name
        try:
            bounds = coverage.get_data_bounds(temporal_param)
        except ValueError:
            return (coverage.get_parameter_context(temporal_param).fill_value,) * 2
        uom = coverage.get_parameter_context(temporal_param).uom
        new_bounds = (TimeUtils.units_to_ts(uom,bounds[0]), TimeUtils.units_to_ts(uom,bounds[1]))
        return new_bounds

    def dataset_extents(self, dataset_id='', parameters=None):
        self.read_dataset(dataset_id)
        parameters = parameters or None
        coverage = DatasetManagementService._get_coverage(dataset_id)
        extents = coverage.get_data_extents(parameters)
        coverage.close()
        return extents

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
    def get_parameter_function(cls, parameter_function_id=''):
        '''
        Preferred client-side class method for constructing a parameter function
        '''
        dms_cli = DatasetManagementServiceClient()
        pf_res = dms_cli.read_parameter_function(parameter_function_id=parameter_function_id)
        pf = AbstractFunction.load(pf_res.parameter_function)
        pf._identifier = pf._id
        return pf

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

        return cls.build_parameter_dictionary(pd, pcs)

    @classmethod
    def build_parameter_dictionary(cls, parameter_dictionary_obj, parameter_contexts):
        pdict = cls._merge_contexts([ParameterContext.load(i.parameter_context) for i in parameter_contexts],
                                    parameter_dictionary_obj.temporal_context)
        pdict._identifier = parameter_dictionary_obj._id

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
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        pdict = ParameterDictionary.load(parameter_dict)
        sdom = GridDomain.load(spatial_domain)
        tdom = GridDomain.load(temporal_domain)
        scov = self._create_simplex_coverage(dataset_id, pdict, sdom, tdom, self.inline_data_writes)
        vcov = ViewCoverage(file_root, dataset_id, description or dataset_id, reference_coverage_location=scov.persistence_dir)
        scov.close()
        return vcov

    def _create_view_coverage(self, dataset_id, description, parent_dataset_id):
        # As annoying as it is we need to load the view coverage belonging to parent dataset id and use the information
        # inside to build the new one...
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        pscov = self._get_simplex_coverage(parent_dataset_id, mode='r')
        scov_location = pscov.persistence_dir
        pscov.close()
        vcov = ViewCoverage(file_root, dataset_id, description or dataset_id, reference_coverage_location=scov_location)
        return vcov


    @classmethod
    def _create_simplex_coverage(cls, dataset_id, parameter_dictionary, spatial_domain, temporal_domain, inline_data_writes=True):
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        scov = SimplexCoverage(file_root,uuid4().hex,'Simplex Coverage for %s' % dataset_id, parameter_dictionary=parameter_dictionary, temporal_domain=temporal_domain, spatial_domain=spatial_domain, inline_data_writes=inline_data_writes)
        return scov

    @classmethod
    def _splice_coverage(cls, dataset_id, scov):
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        vcov = cls._get_coverage(dataset_id,mode='a')
        scov_pth = scov.persistence_dir
        if isinstance(vcov.reference_coverage, SimplexCoverage):
            ccov = ComplexCoverage(file_root, uuid4().hex, 'Complex coverage for %s' % dataset_id, 
                    reference_coverage_locs=[vcov.head_coverage_path,],
                    parameter_dictionary=ParameterDictionary(),
                    complex_type=ComplexCoverageType.TEMPORAL_AGGREGATION)
            log.info('Creating Complex Coverage: %s', ccov.persistence_dir)
            ccov.append_reference_coverage(scov_pth)
            ccov_pth = ccov.persistence_dir
            ccov.close()
            vcov.replace_reference_coverage(ccov_pth)
        elif isinstance(vcov.reference_coverage, ComplexCoverage):
            log.info('Appending simplex coverage to complex coverage')
            vcov.reference_coverage.append_reference_coverage(scov_pth)
        vcov.refresh()
        vcov.close()




    @classmethod
    def _save_coverage(cls, coverage):
        coverage.flush()

    @classmethod
    def _get_coverage(cls,dataset_id,mode='w'):
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        coverage = AbstractCoverage.load(file_root, dataset_id, mode=mode)
        return coverage

    @classmethod
    def _get_nonview_coverage(cls, dataset_id, mode='w'):
        cov = cls._get_coverage(dataset_id, mode)
        if isinstance(cov, ViewCoverage):
            rcov = cov.reference_coverage
            pdir = rcov.persistence_dir
            rcov = None
            cov.close()
            cov = AbstractCoverage.load(pdir, mode=mode)
        return cov

    @classmethod
    def _get_simplex_coverage(cls, dataset_id, mode='w'):
        cov = cls._get_coverage(dataset_id, mode=mode)
        if isinstance(cov, SimplexCoverage):
            return cov
        if isinstance(cov, ViewCoverage):
            path = cov.head_coverage_path
            guid = os.path.basename(path)
            cov.close()
            return cls._get_simplex_coverage(guid, mode=mode)
        raise BadRequest('Unsupported coverage type found: %s' % type(cov))

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
        if hasattr(pc1,'lookup_value') or hasattr(pc2,'lookup_value'):
            if hasattr(pc1,'lookup_value') and hasattr(pc2,'lookup_value'):
                return bool(pc1 == pc2) and pc1.document_key == pc2.document_key
            return False
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

