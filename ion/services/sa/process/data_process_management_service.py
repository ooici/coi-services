#!/usr/bin/env python

"""
@package ion.services.sa.process.test.test_int_data_process_management_service
@author  Maurice Manning
"""

from pyon.util.log import log
from pyon.util.ion_time import IonTime
from interface.services.sa.idata_process_management_service import BaseDataProcessManagementService
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from pyon.public import   log, RT, PRED, OT
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound
from pyon.util.containers import create_unique_identifier
from interface.objects import ProcessDefinition
from pyon.core.object import IonObjectSerializer, IonObjectBase
from interface.objects import Transform
from pyon.util.containers import DotDict
from ion.services.sa.instrument.data_process_impl import DataProcessImpl


class DataProcessManagementService(BaseDataProcessManagementService):

    def on_init(self):
        IonObject("Resource")  # suppress pyflakes error

        self.override_clients(self.clients)

    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        #shortcut names for the import sub-services
        if hasattr(self.clients, "resource_registry"):
            self.RR   = self.clients.resource_registry

        #farm everything out to the impls

        self.data_process = DataProcessImpl(self.clients)

    def create_data_process_definition(self, data_process_definition=None):

        result, _ = self.clients.resource_registry.find_resources(RT.DataProcessDefinition, None, data_process_definition.name, True)
        if result:
            raise BadRequest("A data process definition named '%s' already exists" % data_process_definition.name)

        #todo: determine validation checks for a data process def

        data_process_definition_id, version = self.clients.resource_registry.create(data_process_definition)

        #-------------------------------
        # Process Definition
        #-------------------------------
        # Create the underlying process definition
        process_definition = ProcessDefinition()
        process_definition.name = data_process_definition.name
        process_definition.description = data_process_definition.description

        process_definition.executable = {'module':data_process_definition.module, 'class':data_process_definition.class_name}
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)

        self.clients.resource_registry.create_association(data_process_definition_id, PRED.hasProcessDefinition, process_definition_id)

        return data_process_definition_id

    def update_data_process_definition(self, data_process_definition=None):
        # TODO: If executable has changed, update underlying ProcessDefinition

        # Overwrite DataProcessDefinition object
        self.clients.resource_registry.update(data_process_definition)

    def read_data_process_definition(self, data_process_definition_id=''):
        # Read DataProcessDefinition object with _id matching id
        log.debug("Reading DataProcessDefinition object id: %s" % data_process_definition_id)
        data_proc_def_obj = self.clients.resource_registry.read(data_process_definition_id)

        return data_proc_def_obj

    def delete_data_process_definition(self, data_process_definition_id=''):
        # Delete the data process
        self.clients.resource_registry.delete(data_process_definition_id)

    def find_data_process_definitions(self, filters=None):
        """
        @param      filters: dict of parameters to filter down
                        the list of possible data proc.
        @retval
        """
        #todo: add filtering
        data_process_def_list , _ = self.clients.resource_registry.find_resources(RT.DataProcessDefinition, None, None, True)
        return data_process_def_list

    def assign_input_stream_definition_to_data_process_definition(self, stream_definition_id='', data_process_definition_id=''):
        """Connect the input  stream with a data process definition
        """
        # Verify that both ids are valid, RR will throw if not found
        stream_definition_obj = self.clients.resource_registry.read(stream_definition_id)
        data_process_definition_obj = self.clients.resource_registry.read(data_process_definition_id)

        self.clients.resource_registry.create_association(data_process_definition_id,  PRED.hasInputStreamDefinition,  stream_definition_id)

    def unassign_input_stream_definition_from_data_process_definition(self, stream_definition_id='', data_process_definition_id=''):
        """
        Disconnect the Data Product from the Data Producer

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """

        # Remove the link between the Stream Definition resource and the Data Process Definition resource
        associations = self.clients.resource_registry.find_associations(data_process_definition_id, PRED.hasInputStreamDefinition, stream_definition_id, id_only=True)
        if not associations:
            raise NotFound("No Input Stream Definitions associated with data process definition ID " + str(data_process_definition_id))
        for association in associations:
            self.clients.resource_registry.delete_association(association)

    def assign_stream_definition_to_data_process_definition(self, stream_definition_id='', data_process_definition_id=''):
        """Connect the output  stream with a data process definition
        """
        # Verify that both ids are valid, RR will throw if not found
        stream_definition_obj = self.clients.resource_registry.read(stream_definition_id)
        data_process_definition_obj = self.clients.resource_registry.read(data_process_definition_id)

        self.clients.resource_registry.create_association(data_process_definition_id,  PRED.hasStreamDefinition,  stream_definition_id)

    def unassign_stream_definition_from_data_process_definition(self, stream_definition_id='', data_process_definition_id=''):
        """
        Disconnect the Data Product from the Data Producer

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """

        # Remove the link between the Stream Definition resource and the Data Process Definition resource
        associations = self.clients.resource_registry.find_associations(data_process_definition_id, PRED.hasStreamDefinition, stream_definition_id, id_only=True)
        if not associations:
            raise NotFound("No Stream Definitions associated with data process definition ID " + str(data_process_definition_id))
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    # ------------------------------------------------------------------------------------------------
    # Working with DataProcess

    def create_data_process(self, data_process_definition_id='', in_data_product_ids=None, out_data_products=None, configuration=None):
        """
        @param  data_process_definition_id: Object with definition of the
                    transform to apply to the input data product
        @param  in_data_product_ids: ID of the input data products
        @param  out_data_products: list of IDs of the output data products
        @retval data_process_id: ID of the newly created data process object
        """

        inform = "Input Data Product:       "+str(in_data_product_ids)+\
                 "Transformed by:           "+str(data_process_definition_id)+\
                 "To create output Product: "+str(out_data_products)
        log.debug("DataProcessManagementService:create_data_process()\n" +
                  inform)
        log.critical('configuration: %s', configuration)

        if configuration is None:
            configuration = DotDict()

        if in_data_product_ids is None:
            in_data_product_ids = []

        if out_data_products is None:
            out_data_products = {}

        # Create and store a new DataProcess with the resource registry
        log.debug("DataProcessManagementService:create_data_process - Create and store a new DataProcess with the resource registry")
        data_process_def_obj = self.read_data_process_definition(data_process_definition_id)
        log.critical('Create data process: %s', data_process_def_obj.name)

        data_process_name = create_unique_identifier("process_" + data_process_def_obj.name)
        configuration.process.queue_name = data_process_name

        self.data_process = IonObject(RT.DataProcess, name=data_process_name)
        data_process_id, version = self.clients.resource_registry.create(self.data_process)
        self.data_process._id = data_process_id
        self.data_process._rev = version
        log.debug("DataProcessManagementService:create_data_process - Create and store a new DataProcess with the resource registry  data_process_id: %s" +  str(data_process_id))


        # Associate with dataProcessDefinition
        aid = self.clients.resource_registry.create_association(data_process_id,  PRED.hasProcessDefinition, data_process_definition_id)
        log.debug("DataProcessManagementService:create_data_process assoc to data process definition: %s", str(aid))

        # Register the data process instance as a data producer with DataAcquisitionMgmtSvc
        data_producer_id = self.clients.data_acquisition_management.register_process(data_process_id)
        log.debug("DataProcessManagementService:create_data_process register process with DataAcquisitionMgmtSvc: data_producer_id: %s   (L4-CI-SA-RQ-181)", str(data_producer_id) )


        # Register each output data product with DAMS to create DataProducer links
        output_stream_dict = {}
        if out_data_products is None:
            raise BadRequest("Data Process must have output product(s) specified %s",  str(data_process_definition_id) )
        for name, out_data_product_id in out_data_products.iteritems():

            # check that the product is not already associated with a producer
            producer_ids, _ = self.clients.resource_registry.find_objects(out_data_product_id, PRED.hasDataProducer, RT.DataProducer, True)
            if producer_ids:
                raise BadRequest("Data Product should not already be associated to a DataProducer %s hasDataProducer %s", str(data_process_id), str(producer_ids[0]))

            #Assign each output Data Product to this producer resource
            out_data_product_obj = self.clients.resource_registry.read(out_data_product_id)
            if not out_data_product_obj:
                raise NotFound("Output Data Product %s does not exist" % out_data_product_id)
            # Associate with DataProcess: register as an output product for this process
            log.debug("DataProcessManagementService:create_data_process link data process %s and output out data product: %s    (L4-CI-SA-RQ-260)", str(data_process_id), str(out_data_product_id))
            self.clients.data_acquisition_management.assign_data_product(data_process_id, out_data_product_id)

            # Retrieve the id of the OUTPUT stream from the out Data Product
            stream_ids, _ = self.clients.resource_registry.find_objects(out_data_product_id, PRED.hasStream, RT.Stream, True)

            log.debug("DataProcessManagementService:create_data_process retrieve out data prod streams: %s", str(stream_ids))
            if not stream_ids:
                raise NotFound("No Stream created for output Data Product " + str(out_data_product_id))
            if len(stream_ids) != 1:
                raise BadRequest("Data Product should only have ONE stream at this time" + str(out_data_product_id))
            output_stream_dict[name] = stream_ids[0]
            log.debug("DataProcessManagementService:create_data_process -Register the data process instance as a data producer with DataAcquisitionMgmtSvc, then retrieve the id of the OUTPUT stream  out_stream_id: " +  str(output_stream_dict[name]))


        #check if data process definition has lookup tables attached that should be passed to the tansfrom as part of configuration
        self._find_lookup_tables(data_process_definition_id, configuration)


        #Todo: currently this is handled explicitly after creating the data product, that code then calls DMAS:assign_data_product
        log.debug("DataProcessManagementService:create_data_process associate data process workflows with source data products %s hasInputProducts  %s   (L4-CI-SA-RQ-260)", str(data_process_id), str(in_data_product_ids))
        for  in_data_product_id in in_data_product_ids:
            self.clients.resource_registry.create_association(data_process_id, PRED.hasInputProduct, in_data_product_id)

            #check if in data product is attached to an instrument, check instrumentDevice and InstrumentModel for lookup table attachments
            instdevice_ids, _ = self.clients.resource_registry.find_subjects(RT.InstrumentDevice, PRED.hasOutputProduct, in_data_product_id, True)
            for instdevice_id in instdevice_ids:
                log.debug("DataProcessManagementService:create_data_process instrument device_id assoc to the input data product of this data process: %s   (L4-CI-SA-RQ-231)", str(instdevice_id))
                self._find_lookup_tables(instdevice_id, configuration)
                instmodel_ids, _ = self.clients.resource_registry.find_objects(instdevice_id, PRED.hasModel, RT.InstrumentModel, True)
                for instmodel_id in instmodel_ids:
                    log.debug("DataProcessManagementService:create_data_process instmodel_id assoc to the instDevice: %s", str(instmodel_id))
                    self._find_lookup_tables(instmodel_id, configuration)

        # Create subscription from in_data_product, which should already be associated with a stream via the Data Producer
        in_stream_ids = []
        # get the streams associated with this IN data products
        for  in_data_product_id in in_data_product_ids:
            log.debug("DataProcessManagementService:create_data_process - get the stream associated with this IN data product")
            stream_ids, _ = self.clients.resource_registry.find_objects(in_data_product_id, PRED.hasStream, RT.Stream, True)
            if not stream_ids:
                raise NotFound("No Stream created for this IN Data Product " + str(in_data_product_id))
            if len(stream_ids) != 1:
                raise BadRequest("IN Data Product should only have ONE stream at this time" + str(in_data_product_id))
            log.critical("DataProcessManagementService:create_data_process - get the stream associated with this IN data product:  %s  in_stream_id: %s ", str(in_data_product_id),  str(stream_ids[0]))
            in_stream_ids.append(stream_ids[0])
        log.critical('in_stream_ids: %s', in_stream_ids)
        # create a subscription to the input stream
        if in_stream_ids:
            log.debug("DataProcessManagementService:create_data_process - Finally - create a subscription to the input stream")
            input_subscription_id = self.clients.pubsub_management.create_subscription(name=data_process_name, stream_ids=in_stream_ids)
            log.debug("DataProcessManagementService:create_data_process - Finally - create a subscription to the input stream   input_subscription_id"  +  str(input_subscription_id))

            log.info("Adding the subscription id to the resource for clean up later.")
            # add the subscription id to the resource for clean up later
            self.data_process.input_subscription_id = input_subscription_id
        else:
            log.info('No input streams')

        procdef_ids,_ = self.clients.resource_registry.find_objects(data_process_definition_id, PRED.hasProcessDefinition, RT.ProcessDefinition, id_only=True)
        if not procdef_ids:
            raise BadRequest("Cannot find associated ProcessDefinition for DataProcessDefinition id=%s" % data_process_definition_id)
        process_definition_id = procdef_ids[0]

        log.info("Launching the process")
        debug_str = "\n\tQueue Name: %s\n\tOutput Streams: %s\n\tProcess Definition ID: %s\n\tConfiguration: %s" % (data_process_name, output_stream_dict, process_definition_id, configuration)
        log.debug(debug_str)

        pid = self._launch_process(
                           queue_name=data_process_name,
                           out_streams=output_stream_dict,
                           process_definition_id=process_definition_id,
                           configuration=configuration)



        log.debug("DataProcessManagementService:create_data_process - pid: %s", pid)
        self.data_process.process_id = pid
        log.debug("Updating data_process with pid: %s", pid)
        self.clients.resource_registry.update(self.data_process)

        return data_process_id

    def _launch_process(self, queue_name='', out_streams=None, process_definition_id='', configuration=None):
        """
        Launches the process
        """

        # ------------------------------------------------------------------------------------
        # Spawn Configuration and Parameters
        # ------------------------------------------------------------------------------------

        configuration['process'] = {
            'queue_name':queue_name,
            'publish_streams' : out_streams
        }
        configuration['process']['publish_streams'] = out_streams

        # ------------------------------------------------------------------------------------
        # Process Spawning
        # ------------------------------------------------------------------------------------
        # Spawn the process
        pid = self.clients.process_dispatcher.schedule_process(
            process_definition_id=process_definition_id,
            configuration=configuration
        )

        return pid



    def _find_lookup_tables(self, resource_id="", configuration=None):
        #check if resource has lookup tables attached
        attachment_objs, _ = self.clients.resource_registry.find_objects(resource_id, PRED.hasAttachment, RT.Attachment, False)
        for attachment_obj in attachment_objs:
            log.debug("DataProcessManagementService:_find_lookup_tables  attachment %s", str(attachment_obj))
            words = set(attachment_obj.keywords)
            if 'DataProcessInput' in words:
                configuration[attachment_obj.name] = attachment_obj.content
                log.debug("DataProcessManagementService:_find_lookup_tables lookup table found in attachment %s", attachment_obj.name)
            else:
                log.debug("DataProcessManagementService:_find_lookup_tables NO lookup table in attachment %s", attachment_obj.name)


    def update_data_process_inputs(self, data_process_id="", in_stream_ids=None):
        #@TODO: INPUT STREAM VALIDATION

        # first - get the data process
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        subscription_id = data_process_obj.input_subscription_id
        was_active = False 
        if subscription_id:
            # get rid of all the current streams
            try:
                self.clients.pubsub_management.deactivate_subscription(subscription_id)
                was_active = True

            except BadRequest:
                log.info('Subscription was not active')

            self.clients.pubsub_management.delete_subscription(subscription_id)

        subscription_id = self.clients.pubsub_management.create_subscription(data_process_obj.name, stream_ids=in_stream_ids)
        if was_active:
            self.clients.pubsub_management.activate_subscription(subscription_id)

            

    def update_data_process(self,):
        #todo: What are valid ways to update a data process?.

        return

    def read_data_process(self, data_process_id=""):
        # Read DataProcess object with _id matching  id
        log.debug("Reading DataProcess object id: %s" % data_process_id)
        data_proc_obj = self.clients.resource_registry.read(data_process_id)
        if not data_proc_obj:
            raise NotFound("DataProcess %s does not exist" % data_process_id)
        return data_proc_obj


    def delete_data_process(self, data_process_id=""):
        # Delete the specified DataProcessDefinition object
        data_process_obj = self.read_data_process(data_process_id)

        # delete the association with DataProcessDefinition
        dpd_assn_ids = self.clients.resource_registry.find_associations(subject=data_process_id,  predicate=PRED.hasProcessDefinition, id_only=True)
        for dpd_assn_id in dpd_assn_ids:
            log.debug("DataProcessManagementService:delete_data_process  delete data process definition assoc")
            self.clients.resource_registry.delete_association(dpd_assn_id)

        self._stop_process(data_process_obj)


        #--------------------------------------------------------------------------------
        # Finalize the Data Products by Removing streams associated with the dataset and product
        #--------------------------------------------------------------------------------

        out_products, assocs = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasOutputProduct, id_only=True)
        for out_product, assoc in zip(out_products, assocs):
            data_product_management = DataProductManagementServiceClient()
            data_product_management.remove_streams(out_product)
            self.clients.resource_registry.delete_association(assoc)

            self.clients.data_acquisition_management.unassign_data_product(data_process_id, out_product)




        # Delete the input products link
        inprod_associations = self.clients.resource_registry.find_associations(data_process_id, PRED.hasInputProduct)
        for inprod_association in inprod_associations:
            log.debug("DataProcessManagementService:delete_data_process  delete inprod assocs")
            self.clients.resource_registry.delete_association(inprod_association)


        try:
            self.deactivate_data_process(data_process_id=data_process_id)
            log.warn('Deleteing activated data process...')
        except BadRequest:
            pass



        # Deactivates and deletes the input subscription
        # todo: delete the subscription AND THE QUEUE!!
        # subscription is deactivate when deactivate_data_process is called
        log.debug("DataProcessManagementService:delete_data_process  delete subscription")
        self.clients.pubsub_management.delete_subscription(data_process_obj.input_subscription_id)
        data_process_obj.input_subscription_id = None

        #unregister the data process in DataAcquisitionMgmtSvc
        self.clients.data_acquisition_management.unregister_process(data_process_id)


        # Delete the data process
        log.debug("DataProcessManagementService:delete_data_process the data_process")

        self.clients.resource_registry.delete(data_process_id)
        return

    def _stop_process(self, data_process):
        pid = data_process.process_id
        self.clients.process_dispatcher.cancel_process(pid)


    def find_data_process(self, filters=None):
        """
        @param      filters: dict of parameters to filter down
                        the list of possible data proc.
        @retval
        """
        #todo: add filter processing
        data_process_list , _ = self.clients.resource_registry.find_resources(RT.DataProcess, None, None, True)
        return data_process_list

    def activate_data_process(self, data_process_id=""):

        data_process_obj = self.read_data_process(data_process_id)


        #update the producer context with the activation time and the configuration
        # todo: update the setting of this contect with the return vals from process_dispatcher:schedule_process after convert
        # todo: process_id, process_definition, schedule, configuration

        producer_obj = self._get_process_producer(data_process_id)
        if producer_obj.producer_context.type_ == OT.DataProcessProducerContext :
            producer_obj.producer_context.activation_time = IonTime().to_string()
            producer_obj.producer_context.execution_configuration = data_process_obj.configuration
            self.clients.resource_registry.update(producer_obj)

        subscription_id = data_process_obj.input_subscription_id
        self.clients.pubsub_management.activate_subscription(subscription_id=subscription_id)

    def deactivate_data_process(self, data_process_id=""):

        data_process_obj = self.read_data_process(data_process_id)


        #update the producer context with the deactivation time
        # todo: update the setting of this contect with the return vals from process_dispatcher:schedule_process after convert
        producer_obj = self._get_process_producer(data_process_id)
        if producer_obj.producer_context.type_ == OT.DataProcessProducerContext :
            producer_obj.producer_context.deactivation_time = IonTime().to_string()
            self.clients.resource_registry.update(producer_obj)

        subscription_id = data_process_obj.input_subscription_id
        self.clients.pubsub_management.deactivate_subscription(subscription_id=subscription_id)



    def attach_process(self, process=''):
        """
        @param      process: Should this be the data_process_id?
        @retval
        """
        # TODO: Determine the proper input param
        pass

    def _get_process_producer(self, data_process_id=""):
        producer_objs, _ = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasDataProducer, object_type=RT.DataProducer, id_only=False)
        if not producer_objs:
            raise NotFound("No Producers created for this Data Process " + str(data_process_id))
        return producer_objs[0]
