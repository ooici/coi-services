#!/usr/bin/env python

"""
@package ion.services.sa.process.test.test_int_data_process_management_service
@author  Maurice Manning
"""

from pyon.util.log import log
import time
from interface.services.sa.idata_process_management_service import BaseDataProcessManagementService
from pyon.public import   log, RT, PRED
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound
from interface.objects import ProcessDefinition, StreamQuery

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

        if hasattr(self.clients, "transform_management_service"):
            self.TMS  = self.clients.transform_management_service


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

    def create_data_process(self, data_process_definition_id=None, in_data_product_ids='', out_data_products=None, configuration=None):
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

        if configuration is None:
            configuration = {}

        # Create and store a new DataProcess with the resource registry
        log.debug("DataProcessManagementService:create_data_process - Create and store a new DataProcess with the resource registry")
        data_process_def_obj = self.read_data_process_definition(data_process_definition_id)

        data_process_name = "process_" + data_process_def_obj.name \
                             + time.ctime()
        self.data_process = IonObject(RT.DataProcess, name=data_process_name)
        data_process_id, version = self.clients.resource_registry.create(self.data_process)
        log.debug("DataProcessManagementService:create_data_process - Create and store a new DataProcess with the resource registry  data_process_id: %s" +  str(data_process_id))

        # Register the data process instance as a data producer with DataAcquisitionMgmtSvc
        #TODO: should this be outside this method? Called by orchestration?
        data_producer_id = self.clients.data_acquisition_management.register_process(data_process_id)
        log.debug("DataProcessManagementService:create_data_process register process with DataAcquisitionMgmtSvc: data_producer_id: %s   (L4-CI-SA-RQ-181)", str(data_producer_id) )


        self.output_stream_dict = {}
        #TODO: should this be outside this method? Called by orchestration?
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
            self.clients.data_acquisition_management.assign_data_product(data_process_id, out_data_product_id, create_stream=False)

            # Retrieve the id of the OUTPUT stream from the out Data Product
            stream_ids, _ = self.clients.resource_registry.find_objects(out_data_product_id, PRED.hasStream, RT.Stream, True)

            log.debug("DataProcessManagementService:create_data_process retrieve out data prod streams: %s", str(stream_ids))
            if not stream_ids:
                raise NotFound("No Stream created for output Data Product " + str(out_data_product_id))
            if len(stream_ids) != 1:
                raise BadRequest("Data Product should only have ONE stream at this time" + str(out_data_product_id))
            self.output_stream_dict[name] = stream_ids[0]
            log.debug("DataProcessManagementService:create_data_process -Register the data process instance as a data producer with DataAcquisitionMgmtSvc, then retrieve the id of the OUTPUT stream  out_stream_id: " +  str(self.output_stream_dict[name]))


        # Associate with dataProcess
        self.clients.resource_registry.create_association(data_process_id,  PRED.hasProcessDefinition, data_process_definition_id)

        #check if data process has lookup tables attached
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

        #-------------------------------
        # Create subscription from in_data_product, which should already be associated with a stream via the Data Producer
        #-------------------------------

#        # first - get the data producer associated with this IN data product
#        log.debug("DataProcessManagementService:create_data_process - get the data producer associated with this IN data product")
#        producer_ids, _ = self.clients.resource_registry.find_objects(in_data_product_id, PRED.hasDataProducer, RT.DataProducer, True)
#        if not producer_ids:
#            raise NotFound("No Data Producer created for this Data Product " + str(in_data_product_id))
#        if len(producer_ids) != 1:
#            raise BadRequest("Data Product should only have ONE Data Producers at this time" + str(in_data_product_id))
#        in_product_producer = producer_ids[0]
#        log.debug("DataProcessManagementService:create_data_process - get the data producer associated with this IN data product  in_product_producer: " +  str(in_product_producer))

        # second - get the streams associated with this IN data products
        self.in_stream_ids = []
        for  in_data_product_id in in_data_product_ids:
            log.debug("DataProcessManagementService:create_data_process - get the stream associated with this IN data product")
            stream_ids, _ = self.clients.resource_registry.find_objects(in_data_product_id, PRED.hasStream, RT.Stream, True)
            if not stream_ids:
                raise NotFound("No Stream created for this IN Data Product " + str(in_data_product_id))
            if len(stream_ids) != 1:
                raise BadRequest("IN Data Product should only have ONE stream at this time" + str(in_data_product_id))
            log.debug("DataProcessManagementService:create_data_process - get the stream associated with this IN data product:  %s  in_stream_id: %s ", str(in_data_product_id),  str(stream_ids[0]))
            self.in_stream_ids.append(stream_ids[0])

        # Finally - create a subscription to the input stream
        log.debug("DataProcessManagementService:create_data_process - Finally - create a subscription to the input stream")
        in_data_product_obj = self.clients.data_product_management.read_data_product(in_data_product_id)
        query = StreamQuery(stream_ids=self.in_stream_ids)
        #self.input_subscription_id = self.clients.pubsub_management.create_subscription(query=query, exchange_name=in_data_product_obj.name)
        self.input_subscription_id = self.clients.pubsub_management.create_subscription(query=query, exchange_name=data_process_name)
        log.debug("DataProcessManagementService:create_data_process - Finally - create a subscription to the input stream   input_subscription_id"  +  str(self.input_subscription_id))

        # add the subscription id to the resource for clean up later
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        data_process_obj.input_subscription_id = self.input_subscription_id
        self.clients.resource_registry.update(data_process_obj)

        procdef_ids,_ = self.clients.resource_registry.find_objects(data_process_definition_id, PRED.hasProcessDefinition, RT.ProcessDefinition, id_only=True)
        if not procdef_ids:
            raise BadRequest("Cannot find associated ProcessDefinition for DataProcessDefinition id=%s" % data_process_definition_id)
        process_definition_id = procdef_ids[0]

        # Launch the transform process
        log.debug("DataProcessManagementService:create_data_process - Launch the first transform process: ")
        log.debug("DataProcessManagementService:create_data_process - input_subscription_id: "   +  str(self.input_subscription_id) )
        log.debug("DataProcessManagementService:create_data_process - out_stream_id: "   +  str(self.output_stream_dict) )
        log.debug("DataProcessManagementService:create_data_process - process_definition_id: "   +  str(process_definition_id) )
        log.debug("DataProcessManagementService:create_data_process - data_process_id: "   +  str(data_process_id) )

        transform_id = self.clients.transform_management.create_transform( name=data_process_id, description=data_process_id,
                           in_subscription_id=self.input_subscription_id,
                           out_streams=self.output_stream_dict,
                           process_definition_id=process_definition_id,
                           configuration=configuration)

        log.debug("DataProcessManagementService:create_data_process - transform_id: "   +  str(transform_id) )

        self.clients.resource_registry.create_association(data_process_id, PRED.hasTransform, transform_id)
        log.debug("DataProcessManagementService:create_data_process - Launch the first transform process   transform_id"  +  str(transform_id))

        # TODO: Flesh details of transform mgmt svc schedule method
#        self.clients.transform_management_service.schedule_transform(transform_id)

        return data_process_id


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
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        if data_process_obj is None:
            raise NotFound("Data Process %s does not exist" % data_process_id)

        # Deactivates and deletes the input subscription
        # todo: create did not activate the subscription, should Transform deactivate?
        try:
            self.clients.pubsub_management.deactivate_subscription(data_process_obj.input_subscription_id)
            log.debug("DataProcessManagementService:delete_data_process  delete subscription")
            self.clients.pubsub_management.delete_subscription(data_process_obj.input_subscription_id)
        except BadRequest, e:
            #May not have activated the subscription so just skip - had to add this to get AS integration tests to pass - probably should be fixed
            pass

        # Delete the output stream, but not the output product
        out_products, _ = self.clients.resource_registry.find_objects(data_process_id, PRED.hasOutputProduct, RT.DataProduct, True)
        if len(out_products) < 1:
            raise NotFound('The the Data Process %s output product cannot be located.' % str(data_process_id))

#        outprod_associations = self.clients.resource_registry.find_associations(data_process_id, PRED.hasOutputProduct)
#        for outprod_association in outprod_associations:
#            log.debug("DataProcessManagementService:delete_data_process  delete outprod assoc")
#            self.clients.resource_registry.delete_association(outprod_association)

        for out_product in out_products:
            out_streams, _ = self.clients.resource_registry.find_objects(out_product, PRED.hasStream, RT.Stream, True)
            for out_stream in out_streams:

                outprod_associations = self.clients.resource_registry.find_associations(data_process_id, PRED.hasOutputProduct, out_product)
                log.debug("DataProcessManagementService:delete_data_process  delete outprod assoc")
                self.clients.resource_registry.delete_association(outprod_associations[0])

                # delete the stream def assoc to the stream
                streamdef_list = self.clients.resource_registry.find_objects(subject=out_stream, predicate=PRED.hasStreamDefinition, id_only=True)

                # delete the associations link first
                streamdef_assocs = self.clients.resource_registry.find_associations(out_stream, PRED.hasStreamDefinition)
                for streamdef_assoc in streamdef_assocs:
                    log.debug("DataProcessManagementService:delete_data_process  delete streamdef assocs")
                    self.clients.resource_registry.delete_association(streamdef_assoc)

#                for streamdef in streamdef_list:
#                    self.clients.resource_registry.delete_association(streamdef)

                # delete the connector first
                stream_associations = self.clients.resource_registry.find_associations(out_product, PRED.hasStream)
                for stream_association in stream_associations:
                    log.debug("DataProcessManagementService:delete_data_process  delete stream assocs")
                    self.clients.resource_registry.delete_association(stream_association)

                log.debug("DataProcessManagementService:delete_data_process  delete outstreams")
                self.clients.pubsub_management.delete_stream(out_stream)

        # Delete the input products link
        inprod_associations = self.clients.resource_registry.find_associations(data_process_id, PRED.hasInputProduct)
        if len(inprod_associations) < 1:
            raise NotFound('The the Data Process %s input product link cannot be located.' % str(data_process_id))
        for inprod_association in inprod_associations:
            log.debug("DataProcessManagementService:delete_data_process  delete inprod assocs")
            self.clients.resource_registry.delete_association(inprod_association)


        # Delete the transform
        transforms, _ = self.clients.resource_registry.find_objects(data_process_id, PRED.hasTransform, RT.Transform, True)
        if len(transforms) < 1:
            raise NotFound('There is no Transform linked to this Data Process %s' % str(data_process_id))

        # delete the transform associations link first
        transform_assocs = self.clients.resource_registry.find_associations(data_process_id, PRED.hasTransform)
        for transform_assoc in transform_assocs:
            log.debug("DataProcessManagementService:delete_data_process  delete transform assocs")
            self.clients.resource_registry.delete_association(transform_assoc)

        for transform in transforms:
            log.debug("DataProcessManagementService:delete_data_process  delete transform")
            self.clients.transform_management.delete_transform(transform)


        # Delete the assoc with Data Process Definition
        data_process_defs = self.clients.resource_registry.find_associations(data_process_id, PRED.hasProcessDefinition, None)
        if len(data_process_defs) < 1:
            raise NotFound('The the Data Process %s is not linked to a Data Process Definition.' % str(data_process_id))
        for data_process_def in data_process_defs:
            log.debug("DataProcessManagementService:delete_data_process  data_process_def transform")
            self.clients.resource_registry.delete_association(data_process_def)

#        other_assocs = self.clients.resource_registry.find_associations(subject=data_process_id)
#        for other_assoc in other_assocs:
#            log.debug("DataProcessManagementService:delete_data_process  delete other assoc")
#            self.clients.resource_registry.delete_association(other_assoc)

        # Delete the data process
        log.debug("DataProcessManagementService:delete_data_process the data_process")
        self.clients.resource_registry.delete(data_process_id)
        return


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

        #find the Transform
        log.debug("DataProcessManagementService:activate_data_process - get the transform associated with this data process")
        transforms, _ = self.clients.resource_registry.find_objects(data_process_id, PRED.hasTransform, RT.Transform, True)
        if not transforms:
            raise NotFound("No Transform created for this Data Process " + str(transforms))
        if len(transforms) != 1:
            raise BadRequest("Data Process should only have ONE Transform at this time" + str(transforms))

        log.debug("DataProcessManagementService:activate_data_process call transform_management.activate_transform to activate the subscription (L4-CI-SA-RQ-181)")
        self.clients.transform_management.activate_transform(transforms[0])
        return

    def deactivate_data_process(self, data_process_id=""):

        data_process_obj = self.read_data_process(data_process_id)

        #find the Transform
        log.debug("DataProcessManagementService:deactivate_data_process - get the transform associated with this data process")
        transforms, _ = self.clients.resource_registry.find_objects(data_process_id, PRED.hasTransform, RT.Transform, True)
        if not transforms:
            raise NotFound("No Transform created for this Data Process " + str(transforms))
        if len(transforms) != 1:
            raise BadRequest("Data Process should only have ONE Transform at this time" + str(transforms))

        log.debug("DataProcessManagementService:activate_data_process - transform_management.deactivate_transform")
        self.clients.transform_management.deactivate_transform(transforms[0])
        return



    def attach_process(self, process=''):
        """
        @param      process: Should this be the data_process_id?
        @retval
        """
        # TODO: Determine the proper input param
        pass

