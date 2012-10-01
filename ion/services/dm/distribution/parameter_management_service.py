#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Mon Oct  1 13:26:07 EDT 2012
@file ion/services/dm/distribution/parameter_management_service.py
@description Service Implementation for Parameter Management Service
'''
from interface.services.dm.iparameter_management_service import BaseParameterManagementService, ParameterManagementServiceClient
from interface.objects import ParameterContextResource, ParameterDictionaryResource
from pyon.util.arg_check import validate_true, validate_is_instance
from coverage_model.parameter import ParameterDictionary, ParameterContext
from pyon.public import PRED

class ParameterManagementService(BaseParameterManagementService):
    
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


    @classmethod
    def get_parameter_dictionary(cls, parameter_dictionary_id=''):
        '''
        Preferred client-side class method for constructing a parameter dictionary
        from a service call.
        '''
        pms_cli = ParameterManagementServiceClient()
        pcs = pms_cli.read_parameter_contexts(parameter_dictionary_id=parameter_dictionary_id, id_only=False)

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
        pms_cli = ParameterManagementServiceClient()
        pc_res = pms_cli.read_parameter_context(parameter_context_id=parameter_context_id)
        pc = ParameterContext.load(pc_res.parameter_context)
        pc._identifier = pc_res._id
        return pc

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


