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
    """ @author Alon Yaari
        @file   ion/services/sa/
                    process/data_process_management_service.py
        @brief  Implementation of the data process management service
    """

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
        """
        @param      data_process_definition: dict with parameters to define
                        the data process def.
        @retval     data_process_definition_id: ID of the newly registered
                        data process def.
        """
        log.debug("DataProcessManagementService:create_data_process_definition: %s" % str(data_process_definition))
        
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
        # Read and delete specified DataProcessDefinition object
        log.debug("Deleting DataProcessDefinition id: %s" % data_process_definition_id)
        data_proc_def_obj = self.read_data_source(data_process_definition_id)
        if data_proc_def_obj is None:
            raise NotFound("DataSource %s does not exist" % data_process_definition_id)

        return self.clients.resource_registry.delete(data_process_definition_id)

    def find_data_process_definitions(self, filters=None):
        """
        @param      filters: dict of parameters to filter down
                        the list of possible data proc.
        @retval
        """
        #todo: add filtering
        data_process_def_list , _ = self.clients.resource_registry.find_resources(RT.DataProcessDefinition, None, None, True)
        return data_process_def_list

    def create_data_process(self,
                            data_process_definition_id='',
                            in_data_product_id='',
                            out_data_product_id=''):
        """
        @param  data_process_definition_id: Object with definition of the
                    transform to apply to the input data product
        @param  in_data_product_id: ID of the input data product
        @param  out_data_product_id: ID of the output data product
        @retval data_process_id: ID of the newly created data process object
        """
        inform = "Input Data Product:       "+str(in_data_product_id)+\
                 "Transformed by:           "+str(data_process_definition_id)+\
                 "To create output Product: "+str(out_data_product_id)
        log.debug("DataProcessManagementService:create_data_process()\n" +
                  inform)


        # Create and store a new DataProcess with the resource registry
        log.debug("DataProcessManagementService:create_data_process - Create and store a new DataProcess with the resource registry")
        data_process_def_obj = self.read_data_process_definition(data_process_definition_id)

        data_process_name = "process_" + data_process_def_obj.name \
                            + " - calculates " + \
                            str(out_data_product_id) + time.ctime()
        data_process = IonObject(RT.DataProcess, name=data_process_name)
        data_process_id, version = self.clients.resource_registry.create(data_process)
        log.debug("DataProcessManagementService:create_data_process - Create and store a new DataProcess with the resource registry  data_process_id: " +  str(data_process_id))

        # Associate with dataProcess
        self.clients.resource_registry.create_association(data_process_definition_id,  PRED.hasInstance, data_process_id)
        self.clients.resource_registry.create_association(data_process_id, PRED.hasInputProduct, in_data_product_id)
        self.clients.resource_registry.create_association(data_process_id, PRED.hasOutputProduct, out_data_product_id)

        #todo: is the data process definition or data process instance registerd as a data producer?
        # Register the data process instance as a data producer with DataAcquisitionMgmtSvc, then retrieve the id of the OUTPUT stream
        log.debug("DataProcessManagementService:create_data_process - Register the data process instance as a data producer with DataAcquisitionMgmtSvc, then retrieve the id of the OUTPUT stream")
        data_producer_id = self.clients.data_acquisition_management.register_process(data_process_id)
        stream_ids, _ = self.clients.resource_registry.find_objects(data_producer_id, PRED.hasStream, RT.Stream, True)
        if not stream_ids:
            raise NotFound("No Stream created for this Data Producer " + str(data_producer_id))
        if len(stream_ids) != 1:
            raise BadRequest("Data Producer should only have ONE stream at this time" + str(data_producer_id))
        out_stream_id = stream_ids[0]
        log.debug("DataProcessManagementService:create_data_process -Register the data process instance as a data producer with DataAcquisitionMgmtSvc, then retrieve the id of the OUTPUT stream  out_stream_id: " +  str(out_stream_id))

        # Connect the out_data_product with this process
        #todo: check that the product is not already associated with a producer
        self.clients.data_acquisition_management.assign_data_product(input_resource_id=data_process_id, data_product_id=out_data_product_id)

        #-------------------------------
        # Create subscription from in_data_product, which should already be associated with a stream via the Data Producer
        #-------------------------------

        # first - get the data producer associated with this IN data product
        log.debug("DataProcessManagementService:create_data_process - get the data producer associated with this IN data product")
        producer_ids, _ = self.clients.resource_registry.find_objects(in_data_product_id, PRED.hasDataProducer, RT.DataProducer, True)
        if not producer_ids:
            raise NotFound("No Data Producer created for this Data Product " + str(in_data_product_id))
        if len(producer_ids) != 1:
            raise BadRequest("Data Product should only have ONE Data Producers at this time" + str(in_data_product_id))
        in_product_producer = producer_ids[0]
        log.debug("DataProcessManagementService:create_data_process - get the data producer associated with this IN data product  in_product_producer: " +  str(in_product_producer))

        # second - get the stream associated with this IN data producer
        log.debug("DataProcessManagementService:create_data_process - get the stream associated with this IN data producer")
        stream_ids, _ = self.clients.resource_registry.find_objects(in_product_producer, PRED.hasStream, RT.Stream, True)
        if not stream_ids:
            raise NotFound("No Stream created for this IN Data Producer " + str(in_product_producer))
        if len(stream_ids) != 1:
            raise BadRequest("IN Data Producer should only have ONE stream at this time" + str(in_product_producer))
        in_stream_id = stream_ids[0]
        log.debug("DataProcessManagementService:create_data_process - get the stream associated with this IN data producer   in_stream_id"  +  str(in_stream_id))

        # Finally - create a subscription to the input stream
        log.debug("DataProcessManagementService:create_data_process - Finally - create a subscription to the input stream")
        in_data_product_obj = self.clients.data_product_management.read_data_product(in_data_product_id)
        query = StreamQuery(stream_ids=[in_stream_id])
        input_subscription_id = self.clients.pubsub_management.create_subscription(query=query, exchange_name=in_data_product_obj.name)
        log.debug("DataProcessManagementService:create_data_process - Finally - create a subscription to the input stream   input_subscription_id"  +  str(input_subscription_id))

        #-------------------------------
        # Process Definition
        #-------------------------------
        # Create the process definition for the basic transform
        process_definition = IonObject(RT.ProcessDefinition, name='basic_transform_definition')
        process_definition.executable = {
            'module': 'ion.processes.data.transforms.transform_example',
            'class':'TransformExample'
        }
        transform_definition_id, _ = self.clients.resource_registry.create(process_definition)


        # Launch the first transform process
        log.debug("DataProcessManagementService:create_data_process - Launch the first transform process")
        transform_id = self.clients.transform_management.create_transform( name='basic_transform',
                           in_subscription_id=input_subscription_id,
                           out_streams={'output':out_stream_id},
                           process_definition_id=transform_definition_id,
                           configuration={})
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
        """
        @param  data_process_id: ID of the data process object to update
        @param  data_process_definition_id: Object with definition of the
                    updated transform to apply to the input data product
        @param  in_subscription_id: Updated ID of the input data product
        @param  out_data_product_id: Updated ID of data product to publish
                    process output to
        @retval {"success": boolean}
        """
        log.debug("DataProcessManagementService:update_data_process: " +
                  str(data_process_id))

        # TODO: should these validations be performed here or in the interceptor?    
        # Validate inputs
        if not data_process_id:
            raise BadRequest("Missing ID of data process to update.")
        if not data_process_definition_id \
            and not in_subscription_id \
            and not out_data_product_id:
            raise BadRequest("No values provided to update.")
        if data_process_definition_id:
            data_def_obj = self.read_data_process_definition(data_process_definition_id)
            if not data_def_obj.process_source:
                raise BadRequest("Data definition has invalid process source code.")

        transform_ids, _ = self.clients.resource_registry.\
            find_associations(data_process_id, PRED.hasTransform)
        if not transform_ids:
            raise NotFound("No transform associated with data process ID " +
                           str(data_process_id))
        goodUpdate = True
        for x in transform_ids:
            transform_obj = self.clients.transform_management_service.read_transform(x)
            if data_process_definition_id:
                transform_obj.process_definition_id = data_process_definition_id
            if in_subscription_id:
                transform_obj.in_subscription_id = data_process.in_subscription_id
            if out_data_product_id:
                transform_obj.out_data_product_id = data_process.out_data_product_id
            goodUpdate = goodUpdate & \
                         self.clients.transform_management_service.update_transform(transform_obj)
        return goodUpdate

    def read_data_process(self, data_process_id=""):
        # Read DataProcess object with _id matching  id
        log.debug("Reading DataProcess object id: %s" % data_process_id)
        data_proc_obj = self.clients.resource_registry.read(data_process_id)
        if not data_proc_obj:
            raise NotFound("DataProcess %s does not exist" % data_process_id)
        return data_proc_obj

    def delete_data_process(self, data_process_id=""):
        """
        @param      data_process_id: ID of the data process resource to delete
        @retval     {"success": boolean}
        """
        log.debug("DataProcessManagementService:delete_data_process: " +
                  str(data_process_id))
        if not data_process_id:
            raise BadRequest("Delete failed.  Missing data_process_id.")
        
        # TODO: does the DPMS need to call the TMS to inform it that the process is being deleted?

        # Delete associations of the data process
        associations, _ = self.clients.resource_registry.\
            find_associations(data_process_id, None)
        if associations:
            for x in associations:
                self.clients.resource_registry.delete_association(x)

        # Delete the data process object
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        if not data_process_obj:
            raise NotFound("Data Process (ID: " +
                           data_process_id +
                           ") does not exist")
        self.clients.resource_registry.delete(data_process_obj)
        return {"success": True}

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

