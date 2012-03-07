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

from ion.services.sa.resource_impl.data_process_impl import DataProcessImpl


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

        if not data_process_definition.process_source:
            raise BadRequest("Data process definition has invalid process source.")

        data_process_definition_id, version = self.clients.resource_registry.create(data_process_definition)

        return data_process_definition_id

    def update_data_process_definition(self, data_process_definition=None):
        # Overwrite DataProcessDefinition object
        self.clients.resource_registry.update(data_process_definition)

    def read_data_process_definition(self, data_process_definition_id=''):
        # Read DataProcessDefinition object with _id matching id
        log.debug("Reading DataProcessDefinition object id: %s" % data_process_definition_id)
        data_proc_def_obj = self.clients.resource_registry.read(data_process_definition_id)
        if not data_proc_def_obj:
            raise NotFound("DataProcessDefinition %s does not exist" % data_process_definition_id)
        return data_proc_def_obj


    def delete_data_process_definition(self, data_process_definition_id=''):
        
        data_proc_def_obj = self.clients.resource_registry.read(data_process_definition_id)
        if data_proc_def_obj is None:
            raise NotFound("DataProcessDefinition %s does not exist" % data_process_definition_id)

        # Delete the data process
        self.clients.resource_registry.delete(data_process_definition_id)
        return

    def find_data_process_definitions(self, filters=None):
        """
        @param      filters: dict of parameters to filter down
                        the list of possible data proc.
        @retval
        """
        #todo: add filtering
        data_process_def_list , _ = self.clients.resource_registry.find_resources(RT.DataProcessDefinition, None, None, True)
        return data_process_def_list



    def create_data_process(self, data_process_definition_id='', in_data_product_id='', out_data_products=None):
        """
        @param  data_process_definition_id: Object with definition of the
                    transform to apply to the input data product
        @param  in_data_product_id: ID of the input data product
        @param  out_data_product_id: ID of the output data product
        @retval data_process_id: ID of the newly created data process object
        """

    #
        #
        #
        #todo: break this method up into: 1. create data process, 2. assign in/out products, 3. activate data process
        #
        #
        #
        inform = "Input Data Product:       "+str(in_data_product_id)+\
                 "Transformed by:           "+str(data_process_definition_id)+\
                 "To create output Product: "+str(out_data_products)
        log.debug("DataProcessManagementService:create_data_process()\n" +
                  inform)

        # Create and store a new DataProcess with the resource registry
        log.debug("DataProcessManagementService:create_data_process - Create and store a new DataProcess with the resource registry")
        data_process_def_obj = self.read_data_process_definition(data_process_definition_id)

        data_process_name = "process_" + data_process_def_obj.name \
                             + time.ctime()
        self.data_process = IonObject(RT.DataProcess, name=data_process_name)
        data_process_id, version = self.clients.resource_registry.create(self.data_process)
        log.debug("DataProcessManagementService:create_data_process - Create and store a new DataProcess with the resource registry  data_process_id: %s" +  str(data_process_id))

        # Register the data process instance as a data producer with DataAcquisitionMgmtSvc
        #TODO: should this be outside this method? Called by orchastration?
        data_producer_id = self.clients.data_acquisition_management.register_process(data_process_id)
        log.debug("DataProcessManagementService:create_data_process register process with DataAcquisitionMgmtSvc: data_producer_id: %s", str(data_producer_id) )


        self.output_stream_dict = {}
        #TODO: should this be outside this method? Called by orchastration?
        for name, out_data_product_id in out_data_products.iteritems():

            # check that the product is not already associated with a producer
            producer_ids, _ = self.clients.resource_registry.find_objects(out_data_product_id, PRED.hasDataProducer, RT.DataProducer, True)
            if producer_ids:
                raise BadRequest("Data Product should not already be associated to a DataProducer %s hasDtaProducer %s", str(data_process_id), str(producer_ids[0]))

            #Assign each output Data Product to this producer resource
            out_data_product_obj = self.clients.resource_registry.read(out_data_product_id)
            if not out_data_product_obj:
                raise NotFound("Output Data Product %s does not exist" % out_data_product_id)
            # register as an output product for this process
            self.clients.data_acquisition_management.assign_data_product(data_process_id, out_data_product_id, create_stream=False)

            # Retrieve the id of the OUTPUT stream from the out Data Product
            stream_ids, _ = self.clients.resource_registry.find_objects(out_data_product_id, PRED.hasStream, None, True)

            log.debug("DataProcessManagementService:create_data_process retrieve out data prod streams: %s", str(stream_ids))
            if not stream_ids:
                raise NotFound("No Stream created for output Data Product " + str(out_data_product_id))
            if len(stream_ids) != 1:
                raise BadRequest("Data Product should only have ONE stream at this time" + str(out_data_product_id))
            self.output_stream_dict[name] = stream_ids[0]
            log.debug("DataProcessManagementService:create_data_process -Register the data process instance as a data producer with DataAcquisitionMgmtSvc, then retrieve the id of the OUTPUT stream  out_stream_id: " +  str(self.output_stream_dict[name]))


        # Associate with dataProcess
        self.clients.resource_registry.create_association(data_process_definition_id,  PRED.hasInstance, data_process_id)

        #Todo: currently this is handled explicitly after creating the dat product, that code then calls DMAS:assign_data_product
        self.clients.resource_registry.create_association(data_process_id, PRED.hasInputProduct, in_data_product_id)

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

        # second - get the stream associated with this IN data product
        log.debug("DataProcessManagementService:create_data_process - get the stream associated with this IN data product")
        stream_ids, _ = self.clients.resource_registry.find_objects(in_data_product_id, PRED.hasStream, RT.Stream, True)
        if not stream_ids:
            raise NotFound("No Stream created for this IN Data Product " + str(in_data_product_id))
        if len(stream_ids) != 1:
            raise BadRequest("IN Data Product should only have ONE stream at this time" + str(in_data_product_id))
        in_stream_id = stream_ids[0]
        log.debug("DataProcessManagementService:create_data_process - get the stream associated with this IN data product   in_stream_id"  +  str(in_stream_id))

        # Finally - create a subscription to the input stream
        log.debug("DataProcessManagementService:create_data_process - Finally - create a subscription to the input stream")
        in_data_product_obj = self.clients.data_product_management.read_data_product(in_data_product_id)
        query = StreamQuery(stream_ids=[in_stream_id])
        self.input_subscription_id = self.clients.pubsub_management.create_subscription(query=query, exchange_name=in_data_product_obj.name)
        log.debug("DataProcessManagementService:create_data_process - Finally - create a subscription to the input stream   input_subscription_id"  +  str(self.input_subscription_id))

        # add the subscription id to the resource for clean up later
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        data_process_obj.input_subscription_id = self.input_subscription_id
        self.clients.resource_registry.update(data_process_obj)


        #-------------------------------
        # Process Definition
        #-------------------------------
        # Create the process definition for the basic transform
        transform_definition = ProcessDefinition()
        transform_definition.executable = {  'module':data_process_def_obj.module, 'class':data_process_def_obj.class_name }
        transform_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=transform_definition)

        # Launch the first transform process
        log.debug("DataProcessManagementService:create_data_process - Launch the first transform process: ")
        log.debug("DataProcessManagementService:create_data_process - input_subscription_id: "   +  str(self.input_subscription_id) )
        log.debug("DataProcessManagementService:create_data_process - out_stream_id: "   +  str(self.output_stream_dict) )
        log.debug("DataProcessManagementService:create_data_process - transform_definition_id: "   +  str(transform_definition_id) )
        log.debug("DataProcessManagementService:create_data_process - data_process_id: "   +  str(data_process_id) )



        transform_id = self.clients.transform_management.create_transform( name=data_process_id, description=data_process_id,
                           in_subscription_id=self.input_subscription_id,
                           out_streams=self.output_stream_dict,
                           process_definition_id=transform_definition_id,
                           configuration={})

        log.debug("DataProcessManagementService:create_data_process - transform_id: "   +  str(transform_id) )

        self.clients.resource_registry.create_association(data_process_id, PRED.hasTransform, transform_id)
        log.debug("DataProcessManagementService:create_data_process - Launch the first transform process   transform_id"  +  str(transform_id))

        # TODO: Flesh details of transform mgmt svc schedule and bind methods
#        self.clients.transform_management_service.schedule_transform(transform_id)
#        self.clients.transform_management_service.bind_transform(transform_id)

        # TODO: Where should activate take place?
        log.debug("DataProcessManagementService:create_data_process - transform_management.activate_transform")
        self.clients.transform_management.activate_transform(transform_id)

        return data_process_id


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
        self.clients.pubsub_management.deactivate_subscription(data_process_obj.input_subscription_id)
        self.clients.pubsub_management.delete_subscription(data_process_obj.input_subscription_id)

        # Delete the output stream, but not the output product
        out_products, _ = self.clients.resource_registry.find_objects(data_process_id, PRED.hasOutputProduct, RT.DataProduct, True)
        if len(out_products) < 1:
            raise NotFound('The the Data Process %s output product cannot be located.' % str(data_process_id))
        for out_product in out_products:
            out_streams, _ = self.clients.resource_registry.find_objects(out_product, PRED.hasStream, RT.Stream, True)
            for out_stream in out_streams:
                self.clients.pubsub_management.delete_stream(out_stream)
            # delete the connector as well
            associations = self.clients.resource_registry.find_associations(out_product, PRED.hasStream)
            for association in associations:
                self.clients.resource_registry.delete_association(association)

        # Delete the transform
        transforms, _ = self.clients.resource_registry.find_objects(data_process_id, PRED.hasTransform, RT.Transform, True)
        if len(transforms) < 1:
            raise NotFound('There is no Transform linked to this Data Process %s' % str(data_process_id))
        for transform in transforms:
            self.clients.transform_management.delete_transform(transform)
        # delete the transform associations link as well
        transforms = self.clients.resource_registry.find_associations(data_process_id, PRED.hasTransform)
        for transform in transforms:
            self.clients.resource_registry.delete_association(transform)

        # Delete the assoc with Data Process Definition
        data_process_defs = self.clients.resource_registry.find_associations(None, PRED.hasInstance, data_process_id)
        if len(data_process_defs) < 1:
            raise NotFound('The the Data Process %s is not linked to a Data Process Definition.' % str(data_process_id))
        for data_process_def in data_process_defs:
            self.clients.resource_registry.delete_association(data_process_def)

        # Delete the data process
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

    def attach_process(self, process=''):
        """
        @param      process: Should this be the data_process_id?
        @retval
        """
        # TODO: Determine the proper input param
        pass

