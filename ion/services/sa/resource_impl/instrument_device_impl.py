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

    def lce_precondition_plan(self, instrument_device_id):
        return 0 < len(self.find_stemming_model(instrument_device_id))


    def lce_precondition_develop(self, instrument_device_id):
        return 0 < len(self.find_stemming_agent_instance(instrument_device_id))


    def lce_precondition_integrate(self, instrument_device_id):
        has_passing_certification = True #todo.... get this programmatically somehow
        return has_passing_certification


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

            self.clients.resource_registry.delete_association(assocs[0])

            old_product_stream_def_id = ""
            old_product_stream_id = ""
            stream_def_map = {}
            old_hasInputProduct_assoc = ""
            # for each data product from this instrument, find any data processes that are using it as input
            data_product_ids, prod_assocs = self.clients.resource_registry.find_objects(primary_dev__ids[0], PRED.hasOutputProduct, RT.DataProduct, True)
            if data_product_ids:
                stream_def_map = self.find_stream_defs_for_output_products(instrument_device_id)

                for data_product_id in data_product_ids:

                    # look for data processes that use this product as input
                    data_process_ids, assocs = self.clients.resource_registry.find_subjects( RT.DataProcess, PRED.hasInputProduct, data_product_id, True)
                    log.debug("assign_primary_deployment: data_process_ids using the data product for input %s ", str(data_process_ids) )
                    if data_process_ids:
                        #get the stream def of the data product
                        stream_ids, _ = self.clients.resource_registry.find_objects( data_product_id, PRED.hasStream, RT.Stream, True)
                        #Assume one stream-to-one product for now
                        if stream_ids:
                            old_product_stream_id = stream_ids[0]
                            stream_def_ids, _ = self.clients.resource_registry.find_objects( stream_ids[0], PRED.hasStreamDefinition, RT.StreamDefinition, True)
                            #Assume one streamdef-to-one stream for now
                            if stream_def_ids:
                                old_product_stream_def_id = stream_def_ids[0]

                    if not old_product_stream_def_id:
                        continue

                    #get the corresponding data product on the new device
                    replacement_data_product = stream_def_map[old_product_stream_def_id]
                    log.debug("assign_primary_deployment: replacement_data_product %s ", str(replacement_data_product) )
                    new_stream_ids, _ = self.clients.resource_registry.find_objects( replacement_data_product, PRED.hasStream, RT.Stream, True)
                    #Assume one stream-to-one product for now
                    new_product_stream_id = new_stream_ids[0]

                    for data_process_id in data_process_ids:
                        log.debug("assign_primary_deployment: data_process_id  %s ", str(data_process_id) )
                        transform_ids, _ = self.clients.resource_registry.find_objects( data_process_id, PRED.hasTransform, RT.Transform, True)
                        log.debug("assign_primary_deployment: transform_ids  %s ", str(transform_ids) )

                        if transform_ids:
                            #Assume one transform per data process for now
                            subscription_ids, _ = self.clients.resource_registry.find_objects( transform_ids[0], PRED.hasSubscription, RT.Subscription, True)
                            log.debug("assign_primary_deployment: subscription_ids  %s ", str(subscription_ids) )
                            if subscription_ids:
                                #assume one subscription per transform for now
                                #read the subscription object
                                subscription_obj = self.clients.pubsub_management.read_subscription(subscription_ids[0])
                                log.debug("assign_primary_deployment: subscription_obj %s ", str(subscription_obj))
                                #remove the old stream that is no longer primary
                                subscription_obj.query.stream_ids.remove(old_product_stream_id)
                                #add the new stream from the new primary device that has THE SAME stream def
                                subscription_obj.query.stream_ids = [new_product_stream_id]
                                self.clients.pubsub_management.update_subscription(subscription_obj)
                                log.debug("assign_primary_deployment: updated subscription_obj %s ", str(subscription_obj))

                                #change the hasInputProduct association for this DataProcess
                                log.debug("assign_primary_deployment: switch hasInputProd data_process_id %s ", str(data_process_id))
                                log.debug("assign_primary_deployment: switch hasInputProd data_product_id %s ", str(data_product_id))

                                old_assoc = self.clients.resource_registry.get_association( data_process_id, PRED.hasInputProduct, data_product_id)
                                self.clients.resource_registry.delete_association(old_assoc)
                                log.debug("assign_primary_deployment: switch hasInputProd replacement_data_product %s ", str(replacement_data_product))
                                self.clients.resource_registry.create_association(data_process_id, PRED.hasInputProduct, replacement_data_product)

        # Create association hasPrimaryDeployment
        self._link_resources(instrument_device_id, PRED.hasPrimaryDeployment, logical_instrument_id)

        # todo: If no prior deployment existed for this LI, create a "logical merge transform" (see below)


        # todo: Create a subscription to the parsed stream of the instrument device for the logical merge transform


        # todo: If a prior device existed, remove the subscription to this device from the transform




    def unassign_primary_deployment(self, instrument_device_id='', logical_instrument_id=''):

        #verify that resources ids are valid, if not found RR will raise the error
        instrument_device_obj = self.clients.resource_registry.read(instrument_device_id)
        logical_instrument_obj = self.clients.resource_registry.read(logical_instrument_id)

        # Check that the InstrumentDevice has association hasDeployment to this LogicalInstrument
        # Note: the device may also be deployed to other logical instruments, this is valid
        lgl_platform_assocs = self.clients.resource_registry.get_association(instrument_device_id, PRED.hasDeployment, logical_instrument_id)
        if not lgl_platform_assocs:
            raise BadRequest("This Instrument Device is not deployed to the specified Logical Instrument  %s" %instrument_device_obj.name)

        # Check that the InstrumentDevice has association hasDeployment to this LogicalInstrument
        # Note: the device may also be deployed to other logical instruments, this is valid
        lgl_platform_assoc = self.clients.resource_registry.get_association(instrument_device_id, PRED.hasPrimaryDeployment, logical_instrument_id)
        if not lgl_platform_assoc:
            raise BadRequest("This Instrument Device is not deployed as primary to the specified Logical Instrument  %s" %instrument_device_obj.name)

        # remove the association
        self.clients.resource_registry.delete_association(lgl_platform_assoc)

        # remove the L0 subscription (keep the transform alive)
        # for each data product from this instrument, find any data processes that are using it as input
        data_product_ids, prod_assocs = self.clients.resource_registry.find_objects(instrument_device_id, PRED.hasOutputProduct, RT.DataProduct, True)
        log.debug("unassign_primary_deployment: data_product_ids for this instrument %s ", str(data_product_ids) )

        product_stream_id = ''
        for data_product_id in data_product_ids:
            # look for data processes that use this product as input
            data_process_ids, assocs = self.clients.resource_registry.find_subjects( RT.DataProcess, PRED.hasInputProduct, data_product_id, True)
            log.debug("unassign_primary_deployment: data_process_ids using the data product for input %s ", str(data_process_ids) )
            if data_process_ids:
                #get the stream def of the data product
                stream_ids, _ = self.clients.resource_registry.find_objects( data_product_id, PRED.hasStream, RT.Stream, True)
                # assume one stream per data product for now
                product_stream_id = stream_ids[0]
            for data_process_id in data_process_ids:
                log.debug("unassign_primary_deployment: data_process_id  %s ", str(data_process_id) )
                transform_ids, _ = self.clients.resource_registry.find_objects( data_process_id, PRED.hasTransform, RT.Transform, True)
                log.debug("unassign_primary_deployment: transform_ids  %s ", str(transform_ids) )

                if transform_ids:
                    #Assume one transform per data process for now
                    subscription_ids, _ = self.clients.resource_registry.find_objects( transform_ids[0], PRED.hasSubscription, RT.Subscription, True)
                    log.debug("unassign_primary_deployment: subscription_ids  %s ", str(subscription_ids) )
                    if subscription_ids:
                        #assume one subscription per transform for now
                        #read the subscription object
                        subscription_obj = self.clients.pubsub_management.read_subscription(subscription_ids[0])
                        log.debug("unassign_primary_deployment: subscription_obj %s ", str(subscription_obj))
                        #remove the old stream that is no longer primary
                        subscription_obj.query.stream_ids.remove(product_stream_id)
                        self.clients.pubsub_management.update_subscription(subscription_obj)
                        log.debug("unassign_primary_deployment: updated subscription_obj %s ", str(subscription_obj))

        return


    def find_stream_defs_for_output_products(self, instrument_device_id=''):

        stream_def_map = {}
        # for each data product from this instrument, find the streamdefs
        data_product_ids, prod_assocs = self.clients.resource_registry.find_objects(instrument_device_id, PRED.hasOutputProduct, RT.DataProduct, True)
        for data_product_id in data_product_ids:
            # get the streams from the data product
            stream_ids, _ = self.clients.resource_registry.find_objects( data_product_id, PRED.hasStream, RT.Stream, True)
            #Assume one stream-to-one product for now
            if stream_ids:
                stream_def_ids, _ = self.clients.resource_registry.find_objects( stream_ids[0], PRED.hasStreamDefinition, RT.StreamDefinition, True)
                #Assume one streamdef-to-one streamfor now
                if stream_def_ids:
                    stream_def_map[stream_def_ids[0]] = data_product_id

        log.debug("mfms:reassign_instrument_device_to_logical_instrument:   stream_def_map: %s ", str(stream_def_map))
        return stream_def_map

