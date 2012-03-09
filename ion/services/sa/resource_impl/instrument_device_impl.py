#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.instrument_device_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.core.bootstrap import IonObject
from pyon.public import PRED, RT
from pyon.util.log import log

######
"""
now TODO


Later TODO

 - fix lifecycle states... how?
 -

"""
######




from ion.services.sa.resource_impl.resource_impl import ResourceImpl

class InstrumentDeviceImpl(ResourceImpl):
    """
    @brief resource management for InstrumentDevice resources
    """
    
    def on_impl_init(self):
        #data acquisition management pointer
        if hasattr(self.clients, "data_acquisition_management"):
            self.DAMS = self.clients.data_acquisition_management

        #data product management pointer
        if hasattr(self.clients, "data_product_management"):
            self.DPMS = self.clients.data_product_management

    def _primary_object_name(self):
        return RT.InstrumentDevice

    def _primary_object_label(self):
        return "instrument_device"

    ### associations

    def link_agent_instance(self, instrument_device_id='', instrument_agent_instance_id=''):
        return self._link_resources(instrument_device_id, PRED.hasAgentInstance, instrument_agent_instance_id)

    def unlink_agent_instance(self, instrument_device_id='', instrument_agent_instance_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasAgentInstance, instrument_agent_instance_id)

    def link_deployment(self, instrument_device_id='', logical_instrument_id=''):
        return self._link_resources_single_object(instrument_device_id, PRED.hasDeployment, logical_instrument_id)

    def unlink_deployment(self, instrument_device_id='', logical_instrument_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasDeployment, logical_instrument_id)

    def link_primary_deployment(self, instrument_device_id='', logical_instrument_id=''):
        return self._link_resources_single_object(instrument_device_id, PRED.hasPrimaryDeployment, logical_instrument_id)

    def unlink_primary_deployment(self, instrument_device_id='', logical_instrument_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasPrimaryDeployment, logical_instrument_id)

    def link_data_producer(self, instrument_device_id='', data_producer_id=''):
        return self._link_resources(instrument_device_id, PRED.hasDataProducer, data_producer_id)

    def unlink_data_producer(self, instrument_device_id='', data_producer_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasDataProducer, data_producer_id)

    def link_output_product(self, instrument_device_id='', data_product_id=''):
        return self._link_resources(instrument_device_id, PRED.hasOutputProduct, data_product_id)

    def unlink_output_product(self, instrument_device_id='', data_product_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasOutputProduct, data_product_id)

    def link_model(self, instrument_device_id='', instrument_model_id=''):
        return self._link_resources_single_object(instrument_device_id, PRED.hasModel, instrument_model_id)

    def unlink_model(self, instrument_device_id='', instrument_model_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasModel, instrument_model_id)

    def link_sensor(self, instrument_device_id='', sensor_device_id=''):
        return self._link_resources(instrument_device_id, PRED.hasSensor, sensor_device_id)

    def unlink_sensor(self, instrument_device_id='', sensor_device_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasSensor, sensor_device_id)


    ### finds

    def find_having_agent_instance(self, instrument_agent_instance_id):
        return self._find_having(PRED.hasAgentInstance, instrument_agent_instance_id)

    def find_stemming_agent_instance(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, PRED.hasAgentInstance, RT.InstrumentAgentInstance)

    def find_having_assignment(self, logical_instrument_id):
        return self._find_having_single(PRED.hasAssignment, logical_instrument_id)

    def find_stemming_assignment(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, PRED.hasAssignment, RT.LogicalInstrument)

    def find_having_deployment(self, logical_instrument_id):
        return self._find_having_single(PRED.hasDeployment, logical_instrument_id)

    def find_stemming_deployment(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, PRED.hasDeployment, RT.LogicalInstrument)

    def find_having_data_producer(self, data_producer_id):
        return self._find_having(PRED.hasDataProducer, data_producer_id)

    def find_stemming_data_producer(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, PRED.hasDataProducer, RT.DataProducer)

    def find_having_output_product(self, data_product_id):
        return self._find_having(PRED.hasOutputProduct, data_product_id)

    def find_stemming_output_product(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, PRED.hasOutputProduct, RT.DataProduct)

    def find_having_model(self, instrument_model_id):
        return self._find_having(PRED.hasModel, instrument_model_id)

    def find_stemming_model(self, instrument_device_id):
        return self._find_stemming_single(instrument_device_id, PRED.hasModel, RT.InstrumentModel)

    def find_having_sensor(self, sensor_device_id):
        return self._find_having(PRED.hasSensor, sensor_device_id)

    def find_stemming_sensor(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, PRED.hasSensor, RT.SensorDevice)



    # LIFECYCLE STATE PRECONDITIONS

    # FIXME


