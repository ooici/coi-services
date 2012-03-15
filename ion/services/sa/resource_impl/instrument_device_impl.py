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

    ### orchestrations


    def assign_primary_deployment(self, instrument_device_id='', logical_instrument_id=''):
        """
        associate a logical instrument with a physical one.
        """

        #verify that resources ids are valid, if not found RR will raise the error
        instrument_device_obj = self.clients.resource_registry.read(instrument_device_id)
        logical_instrument_obj = self.clients.resource_registry.read(logical_instrument_id)

        log.debug("assign_primary_deployment: instrument_device_obj  %s", str(instrument_device_obj))

        log.debug("assign_primary_deployment: instrument_device_obj  lcstate %s", str(instrument_device_obj.lcstate))

        # Check that the InstrumentDevice is in DEPLOYED_* lifecycle state
        #todo: check if there are other valid states
        if instrument_device_obj.lcstate != LCS.DEPLOYED_AVAILABLE \
            and instrument_device_obj.lcstate != LCS.DEPLOYED_PRIVATE \
            and instrument_device_obj.lcstate != LCS.DEPLOYED_DISCOVERABLE:
            raise BadRequest("This Instrument Device is in an invalid life cycle state  %s " %instrument_device_obj.name)

        # Check that the InstrumentDevice has association hasDeployment to this LogicalInstrument
        # Note: the device may also be deployed to other logical instruments, this is valid
        lgl_platform_assocs = self.clients.resource_registry.get_association(instrument_device_id, PRED.hasDeployment, logical_instrument_id)
        if not lgl_platform_assocs:
            raise BadRequest("This Instrument Device is not deployed to the specified Logical Instrument  %s" %instrument_device_obj.name)

        # Make sure InstrumentDevice and LogicalInstrument associations to InstrumentModel match (same type)
        dev_model_ids, _ = self.clients.resource_registry.find_objects(instrument_device_id, PRED.hasModel, RT.InstrumentModel, True)
        if not dev_model_ids or len(dev_model_ids) > 1:
            raise BadRequest("This Instrument Device has invalid assignment to an Instrument Model  %s" %instrument_device_obj.name)

        lgl_inst__model_ids, _ = self.clients.resource_registry.find_objects(logical_instrument_id, PRED.hasModel, RT.InstrumentModel, True)
        if not lgl_inst__model_ids or len(lgl_inst__model_ids) > 1:
            raise BadRequest("This Logical Instrument  has invalid assignment to an Instrument Model  %s" % logical_instrument_obj.name)

        if  dev_model_ids[0] != lgl_inst__model_ids[0]:
            raise BadRequest("The Instrument Device does not match the model type of the intended Logical Instrument  %s" %instrument_device_obj.name)

        # Remove potential previous device hasPrimaryDeployment
        primary_dev__ids, assocs = self.clients.resource_registry.find_subjects(RT.InstrumentDevice, PRED.hasPrimaryDeployment, logical_instrument_id, True)
        if primary_dev__ids:
            if len(primary_dev__ids) > 1:
                raise BadRequest("The Logical Instrument has multiple primary devices assigned  %s" %logical_instrument_obj.name)
            #check that the new device is not already linked as the primary device to this logical instrument
            if primary_dev__ids[0] == instrument_device_id :
                raise BadRequest("The Instrument Device is already the primary deployment to this Logical Instrument  %s" %instrument_device_obj.name)

            # remove the previous primary device association
            self.clients.resource_registry.delete_association(assocs[0])


        # Create association hasPrimaryDeployment
        log.debug("mfms:reassign_instrument_device_to_logical_instrument: call deploy_as_primary_instrument_device_to_logical_instrument")
        self._link_resources(instrument_device_id, PRED.hasPrimaryDeployment, logical_instrument_id)

        # If no prior deployment existed for this LI, create a "logical merge transform" (see below)


        # Create a subscription to the parsed stream of the instrument device for the logical merge transform


        # If a prior device existed, remove the subscription to this device from the transform




    def unassign_primary_deployment(self, instrument_device_id='', logical_instrument_id=''):
        return


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


