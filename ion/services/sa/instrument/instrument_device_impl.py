#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.instrument_device_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.core.bootstrap import IonObject
from pyon.public import PRED, RT, LCS, LCE
from pyon.util.log import log
from pyon.core.exception import NotFound, BadRequest


from ion.services.sa.resource_impl.resource_impl import ResourceImpl

class InstrumentDeviceImpl(ResourceImpl):
    """
    @brief resource management for InstrumentDevice resources
    """
    
    def on_impl_init(self):
        #data acquisition management pointer
        if hasattr(self.clients, "data_acquisition_management"):
            self.DAMS = self.clients.data_acquisition_management

        self.add_lce_precondition(LCE.PLAN, self.lce_precondition_plan)
        self.add_lce_precondition(LCE.DEVELOP, self.lce_precondition_develop)
        self.add_lce_precondition(LCE.INTEGRATE, self.lce_precondition_integrate)

    def _primary_object_name(self):
        return RT.InstrumentDevice

    def _primary_object_label(self):
        return "instrument_device"

    ### associations

    def link_agent_instance(self, instrument_device_id='', instrument_agent_instance_id=''):
        return self._link_resources(instrument_device_id, PRED.hasAgentInstance, instrument_agent_instance_id)

    def unlink_agent_instance(self, instrument_device_id='', instrument_agent_instance_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasAgentInstance, instrument_agent_instance_id)

    def link_deployment(self, instrument_device_id='', deployment_id=''):
        return self._link_resources_single_object(instrument_device_id, PRED.hasDeployment, deployment_id)

    def unlink_deployment(self, instrument_device_id='', deployment_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasDeployment, deployment_id)

    def link_data_producer(self, instrument_device_id='', data_producer_id=''):
        return self._link_resources(instrument_device_id, PRED.hasDataProducer, data_producer_id)

    def unlink_data_producer(self, instrument_device_id='', data_producer_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasDataProducer, data_producer_id)

    def link_model(self, instrument_device_id='', instrument_model_id=''):
        return self._link_resources_single_object(instrument_device_id, PRED.hasModel, instrument_model_id)

    def unlink_model(self, instrument_device_id='', instrument_model_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasModel, instrument_model_id)

    def link_device(self, instrument_device_id='', sensor_device_id=''):
        return self._link_resources(instrument_device_id, PRED.hasDevice, sensor_device_id)

    def unlink_device(self, instrument_device_id='', sensor_device_id=''):
        return self._unlink_resources(instrument_device_id, PRED.hasDevice, sensor_device_id)


    ### finds

    def find_having_agent_instance(self, instrument_agent_instance_id):
        return self._find_having(PRED.hasAgentInstance, instrument_agent_instance_id)

    def find_stemming_agent_instance(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, PRED.hasAgentInstance, RT.InstrumentAgentInstance)

    def find_having_deployment(self, deployment_id):
        return self._find_having_single(PRED.hasDeployment, deployment_id)

    def find_stemming_deployment(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, PRED.hasDeployment, RT.Deployment)

    def find_having_data_producer(self, data_producer_id):
        return self._find_having(PRED.hasDataProducer, data_producer_id)

    def find_stemming_data_producer(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, PRED.hasDataProducer, RT.DataProducer)

    def find_having_model(self, instrument_model_id):
        return self._find_having(PRED.hasModel, instrument_model_id)

    def find_stemming_model(self, instrument_device_id):
        return self._find_stemming_single(instrument_device_id, PRED.hasModel, RT.InstrumentModel)

    def find_having_device(self, sensor_device_id):
        return self._find_having(PRED.hasDevice, sensor_device_id)

    def find_stemming_device(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, PRED.hasDevice, RT.SensorDevice)



    # LIFECYCLE STATE PRECONDITIONS

    def lce_precondition_plan(self, instrument_device_id):
        if 0 < len(self.find_stemming_model(instrument_device_id)):
            return ""
        return "Can't have a planned instrument_device without associated instrument_model"


    def lce_precondition_develop(self, instrument_device_id):
        if 0 < len(self.find_stemming_agent_instance(instrument_device_id)):
            return ""
        return "Can't have a developed instrument_device without associated instrument_agent_instance"


    def lce_precondition_integrate(self, instrument_device_id):
        has_passing_certification = True #todo.... get this programmatically somehow
        if has_passing_certification:
            return ""
        return "Can't have an integrated instrument_device without certification"



