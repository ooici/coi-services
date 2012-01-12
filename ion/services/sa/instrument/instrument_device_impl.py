#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.instrument_device_impl
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

class InstrumentDeviceImpl(ResourceImpl):
    """
    @brief resource management for InstrumentDevice resources
    """
    
    def on_impl_init(self):
        #data acquisition management pointer
        if hasattr(self.clients, "data_acquisition_management_service"):
            self.DAMS = self.clients.data_acquisition_management_service

    def _primary_object_name(self):
        return RT.InstrumentDevice

    def _primary_object_label(self):
        return "instrument_device"

    ### associations

    def link_agent_instance(self, instrument_device_id='', instrument_agent_instance_id=''):
        return self._link_resources(instrument_device_id, AT.hasAgentInstance, instrument_agent_instance_id)

    def unlink_agent_instance(self, instrument_device_id='', instrument_agent_instance_id=''):
        return self._unlink_resources(instrument_device_id, AT.hasAgentInstance, instrument_agent_instance_id)

    def link_assignment(self, instrument_device_id='', logical_instrument_id=''):
        return self._link_resources(instrument_device_id, AT.hasAssignment, logical_instrument_id)

    def unlink_assignment(self, instrument_device_id='', logical_instrument_id=''):
        return self._unlink_resources(instrument_device_id, AT.hasAssignment, logical_instrument_id)

    def link_data_producer(self, instrument_device_id='', data_producer_id=''):
        return self._link_resources(instrument_device_id, AT.hasDataProducer, data_producer_id)

    def unlink_data_producer(self, instrument_device_id='', data_producer_id=''):
        return self._unlink_resources(instrument_device_id, AT.hasDataProducer, data_producer_id)

    def link_model(self, instrument_device_id='', instrument_model_id=''):
        return self._link_resources(instrument_device_id, AT.hasModel, instrument_model_id)

    def unlink_model(self, instrument_device_id='', instrument_model_id=''):
        return self._unlink_resources(instrument_device_id, AT.hasModel, instrument_model_id)

    def link_sensor(self, instrument_device_id='', sensor_device_id=''):
        return self._link_resources(instrument_device_id, AT.hasSensor, sensor_device_id)

    def unlink_sensor(self, instrument_device_id='', sensor_device_id=''):
        return self._unlink_resources(instrument_device_id, AT.hasSensor, sensor_device_id)


    ### finds

    def find_having_agent_instance(self, instrument_agent_instance_id):
        return self._find_having(AT.hasAgentInstance, instrument_agent_instance_id)

    def find_stemming_agent_instance(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, AT.hasAgentInstance, RT.InstrumentAgentInstance)

    def find_having_assignment(self, logical_instrument_id):
        return self._find_having(AT.hasAssignment, logical_instrument_id)

    def find_stemming_assignment(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, AT.hasAssignment, RT.LogicalInstrument)

    def find_having_data_producer(self, data_producer_id):
        return self._find_having(AT.hasDataProducer, data_producer_id)

    def find_stemming_data_producer(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, AT.hasDataProducer, RT.DataProducer)

    def find_having_model(self, instrument_model_id):
        return self._find_having(AT.hasModel, instrument_model_id)

    def find_stemming_model(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, AT.hasModel, RT.InstrumentModel)

    def find_having_sensor(self, sensor_device_id):
        return self._find_having(AT.hasSensor, sensor_device_id)

    def find_stemming_sensor(self, instrument_device_id):
        return self._find_stemming(instrument_device_id, AT.hasSensor, RT.SensorDevice)



    ### lifecycles

    def lcs_precondition_PLANNED(self, instrument_device_id):
        return True

    def lcs_precondition_DEVELOPED(self, instrument_device_id):
        return True

    def lcs_precondition_TESTED(self, instrument_device_id):
        return True

    def lcs_precondition_INTEGRATED(self, instrument_device_id):
        return True

    def lcs_precondition_COMMISSIONED(self, instrument_device_id):
        return True

    def lcs_precondition_DEPLOYED(self, instrument_device_id):
        return True

    def lcs_precondition_ACTIVE(self, instrument_device_id):
        return True

    def lcs_precondition_INACTIVE(self, instrument_device_id):
        return True

    def lcs_precondition_RETIRED(self, instrument_device_id):
        return True



    ##### SPECIAL METHODS


    def setup_data_production_chain(self, instrument_device_id=''):

        #get instrument object and instrument's data producer
        inst_obj = self.read_one(instrument_device_id)
        assoc_ids, _ = self.RR.find_associations(instrument_device_id, AT.hasDataProducer, None, True)
        pducer_id = assoc_ids[0]

        #get data product id from data product management service
        dpms_pduct_obj = IonObject("DataProduct",
                                   name=str(inst_obj.name + " L0 Product"),
                                   description=str("DataProduct for " + inst_obj.name))

        dpms_pducer_obj = IonObject("DataProducer",
                                    name=str(inst_obj.name + " L0 Producer"),
                                    description=str("DataProducer for " + inst_obj.name))

        pduct_id = self.DPMS.create_data_product(data_product=dpms_pduct_obj, data_producer=dpms_pducer_obj)


        # get data product's data produceer (via association)
        assoc_ids, _ = self.RR.find_associations(pduct_id, AT.hasDataProducer, None, True)
        # (FIXME: there should only be one assoc_id.  what error to raise?)
        # FIXME: what error to raise if there are no assoc ids?

        # instrument data producer is the parent of the data product producer
        associate_success = self.RR.create_association(pducer_id, AT.hasChildDataProducer, assoc_ids[0])
        log.debug("Create hasChildDataProducer Association: %s" % str(associate_success))


        #FIXME: error checking

        return self._return_update(True)
