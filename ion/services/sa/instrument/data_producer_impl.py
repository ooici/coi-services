#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.management.data_producer_impl
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class DataProducerImpl(ResourceSimpleImpl):
    """
    @brief resource management for DataProducer resources
    """

    def _primary_object_name(self):
        return RT.DataProducer

    def _primary_object_label(self):
        return "data_producer"

    def link_data_set(self, data_producer_id='', data_set_id=''):
        return self._link_resources(data_producer_id, PRED.hasDataset, data_set_id)

    def unlink_data_set(self, data_producer_id='', data_set_id=''):
        return self._unlink_resources(data_producer_id, PRED.hasDataset, data_set_id)

    def link_child_data_producer(self, data_producer_id='', data_producer_child_id=''):
        return self._link_resources(data_producer_id, PRED.hasChildDataProducer, data_producer_child_id)

    def unlink_child_data_producer(self, data_producer_id='', data_producer_child_id=''):
        return self._unlink_resources(data_producer_id, PRED.hasChildDataProducer, data_producer_child_id)

    def link_input_data_producer(self, data_producer_id='', data_producer_input_id=''):
        return self._link_resources(data_producer_id, PRED.hasInputDataProducer, data_producer_input_id)

    def unlink_input_data_producer(self, data_producer_id='', data_producer_input_id=''):
        return self._unlink_resources(data_producer_id, PRED.hasInputDataProducer, data_producer_input_id)

    def find_having_data_set(self, data_set_id):
        return self._find_having(PRED.hasDataset, data_set_id)

    def find_stemming_data_set(self, data_set_id):
        return self._find_stemming(data_set_id, PRED.hasDataset, RT.Dataset)

    def find_having_child_data_producer(self, data_producer_child_id):
        return self._find_having(PRED.hasChildDataProducer, data_producer_child_id)

    def find_stemming_child_data_producer(self, data_producer_id):
        return self._find_stemming(data_producer_id, PRED.hasChildDataProducer, RT.DataProducer)

    def find_having_input_data_producer(self, data_producer_input_id):
        return self._find_having(PRED.hasInputDataProducer, data_producer_input_id)

    def find_stemming_input_data_producer(self, data_producer_id):
        return self._find_stemming(data_producer_id, PRED.hasInputDataProducer, RT.DataProducer)

