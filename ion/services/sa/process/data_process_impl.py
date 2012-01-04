#!/usr/bin/env python

"""
@package  ion.services.sa.process.data_process_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.core.bootstrap import IonObject
from pyon.public import AT, RT
from pyon.util.log import log

######
"""
now TODO


Later TODO

 - fix lifecycle states... how?
 -

"""
######




from ion.services.sa.resource_impl import ResourceImpl

class DataProcessImpl(ResourceImpl):
    """
    @brief resource management for InstrumentDevice resources
    """
    
    def on_impl_init(self):
        pass

    def _primary_object_name(self):
        return RT.DataProcess

    def _primary_object_label(self):
        return "data_process"

    ### associations

    def link_transform(self, data_process_id='', transform_id=''):
        return self._link_resources(data_process_id, AT.hasTransform, transform_id)

    def unlink_transform(self, data_process_id='', transform_id=''):
        return self._unlink_resources(data_process_id, AT.hasTransform, transform_id)

    def link_input_product(self, data_process_id='', data_product_id=''):
        return self._link_resources(data_process_id, AT.hasInputProduct, data_product_id)

    def unlink_input_product(self, data_process_id='', data_product_id=''):
        return self._unlink_resources(data_process_id, AT.hasInputProduct, data_product_id)

    def link_output_product(self, data_process_id='', data_product_id=''):
        return self._link_resources(data_process_id, AT.hasOutputProduct, data_product_id)

    def unlink_output_product(self, data_process_id='', data_product_id=''):
        return self._unlink_resources(data_process_id, AT.hasOutputProduct, data_product_id)

    def link_data_producer(self, data_process_id='', data_producer_id=''):
        return self._link_resources(data_process_id, AT.hasDataProducer, data_producer_id)

    def unlink_data_producer(self, data_process_id='', data_producer_id=''):
        return self._unlink_resources(data_process_id, AT.hasDataProducer, data_producer_id)

    def link_device(self, data_process_id='', instrument_device_id=''):
        return self._link_resources(data_process_id, AT.hasDevice, instrument_device_id)

    def unlink_device(self, data_process_id='', instrument_device_id=''):
        return self._unlink_resources(data_process_id, AT.hasDevice, instrument_device_id)


    ### finds

    def find_having_transform(self, transform_id):
        return self._find_having(AT.hasSensor, transform_id)

    def find_stemming_transform(self, data_process_id):
        return self._find_stemming(data_process_id, AT.hasSensor, RT.SensorDevice)

    def find_having_input_product(self, data_product_id):
        return self._find_having(AT.hasInputProduct, data_product_id)

    def find_stemming_input_product(self, data_process_id):
        return self._find_stemming(data_process_id, AT.hasInputProduct, RT.DataProduct)

    def find_having_output_product(self, data_product_id):
        return self._find_having(AT.hasOutputProduct, data_product_id)

    def find_stemming_output_product(self, data_process_id):
        return self._find_stemming(data_process_id, AT.hasOutputProduct, RT.DataProduct)

    def find_having_data_producer(self, data_producer_id):
        return self._find_having(AT.hasDataProducer, data_producer_id)

    def find_stemming_data_producer(self, data_process_id):
        return self._find_stemming(data_process_id, AT.hasDataProducer, RT.DataProducer)

    def find_having_device(self, instrument_device_id):
        return self._find_having(AT.hasDevice, instrument_device_id)

    def find_stemming_device(self, data_process_id):
        return self._find_stemming(data_process_id, AT.hasDevice, RT.InstrumentDevice)



    ### lifecycles

    def lcs_precondition_PLANNED(self, data_process_id):
        return True

    def lcs_precondition_DEVELOPED(self, data_process_id):
        return True

    def lcs_precondition_TESTED(self, data_process_id):
        return True

    def lcs_precondition_INTEGRATED(self, data_process_id):
        return True

    def lcs_precondition_COMMISSIONED(self, data_process_id):
        return True

    def lcs_precondition_DEPLOYED(self, data_process_id):
        return True

    def lcs_precondition_ACTIVE(self, data_process_id):
        return True

    def lcs_precondition_INACTIVE(self, data_process_id):
        return True

    def lcs_precondition_RETIRED(self, data_process_id):
        return True



