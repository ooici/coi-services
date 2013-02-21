#!/usr/bin/env python

"""
@package ion.services.sa.process.test.test_int_data_process_management_service
@author  Maurice Manning
"""
from uuid import uuid4

from pyon.util.log import log
from pyon.util.ion_time import IonTime
from pyon.public import RT, PRED, OT, LCS
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound
from pyon.util.containers import create_unique_identifier
from pyon.util.containers import DotDict
from pyon.util.arg_check import validate_is_not_none, validate_true
from pyon.ion.resource import ExtendedResourceContainer
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessRestartMode, TransformFunction, DataProcess

from interface.services.sa.idata_process_management_service import BaseDataProcessManagementService
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient

from ion.services.sa.instrument.data_process_impl import DataProcessImpl
from pyon.util.arg_check import validate_is_instance
from ion.util.module_uploader import RegisterModulePreparerPy
import os
import pwd

class DataProcessManagementService(BaseDataProcessManagementService):

    def on_init(self):
        IonObject("Resource")  # suppress pyflakes error

        self.override_clients(self.clients)

        self.init_module_uploader()

        self.get_unique_id = (lambda : uuid4().hex)

        self.data_product_management = DataProductManagementServiceClient()

    def init_module_uploader(self):
        if self.CFG:
            #looking for forms like host=amoeba.ucsd.edu, remotepath=/var/www/release, user=steve
            cfg_host        = self.CFG.get_safe("service.data_process_management.process_release_host", None)
            cfg_remotepath  = self.CFG.get_safe("service.data_process_management.process_release_directory", None)
            cfg_user        = self.CFG.get_safe("service.data_process_management.process_release_user",
                                                pwd.getpwuid(os.getuid())[0])
            cfg_wwwprefix     = self.CFG.get_safe("service.data_process_management.process_release_wwwprefix", None)

            if cfg_host is None or cfg_remotepath is None or cfg_wwwprefix is None:
                raise BadRequest("Missing configuration items; host='%s', directory='%s', wwwprefix='%s'" %
                                 (cfg_host, cfg_remotepath, cfg_wwwprefix))

            self.module_uploader = RegisterModulePreparerPy(dest_user=cfg_user,
                                                            dest_host=cfg_host,
                                                            dest_path=cfg_remotepath,
                                                            dest_wwwprefix=cfg_wwwprefix)


    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        #shortcut names for the import sub-services
        if hasattr(self.clients, "resource_registry"):
            self.RR   = self.clients.resource_registry

        #farm everything out to the impls

        self.data_process = DataProcessImpl(self.clients)


    #todo: need to know what object will be worked with here
    def register_data_process_definition(self, process_code=''):
        """
        register a process module by putting it in a web-accessible location

        @process_code a base64-encoded python file
        """

#        # retrieve the resource
#        data_process_definition_obj = self.clients.resource_registry.read(data_process_definition_id)

        dest_filename = "process_code_%s.py" % self.get_unique_id() #data_process_definition_obj._id

        #process the input file (base64-encoded .py)
        uploader_obj, err = self.module_uploader.prepare(process_code, dest_filename)
        if None is uploader_obj:
            raise BadRequest("Process code failed validation: %s" % err)

        # actually upload
        up_success, err = uploader_obj.upload()
        if not up_success:
            raise BadRequest("Upload failed: %s" % err)

#        #todo: save module / class?
#        data_process_definition_obj.uri = uploader_obj.get_destination_url()
#        self.clients.resource_registry.update(data_process_definition_obj)

        return uploader_obj.get_destination_url()

    @classmethod
    def _cmp_transform_function(cls, tf1, tf2):
        return tf1.module        == tf2.module and \
               tf1.cls           == tf2.cls    and \
               tf1.uri           == tf2.uri    and \
               tf1.function_type == tf2.function_type

    def create_transform_function(self, transform_function=''):
        '''
        Creates a new transform function
        '''
        tfs, _ = self.clients.resource_registry.find_resources(name=transform_function.name, restype=RT.TransformFunction, id_only=False)
        if len(tfs):
            for tf in tfs:
                if self._cmp_transform_function(transform_function, tf):
                    return tf._id
            raise BadRequest('An existing TransformFunction with name %s exists with a different definition' % transform_function.name)

        tf_id, rev = self.clients.resource_registry.create(transform_function)
        return tf_id

    def read_transform_function(self, transform_function_id=''):
        tf = self.clients.resource_registry.read(transform_function_id)
        validate_is_instance(tf,TransformFunction, 'Resource %s is not a TransformFunction: %s' %(transform_function_id, type(tf)))
        return tf

    def delete_transform_function(self, transform_function_id=''):
        self.read_transform_function(transform_function_id)
        self.clients.resource_registry.delete(transform_function_id)
        
    def create_data_process_definition(self, data_process_definition=None):

        result, _ = self.clients.resource_registry.find_resources(RT.DataProcessDefinition, None, data_process_definition.name, True)

        validate_true( len(result) ==0, "A data process definition named '%s' already exists" % data_process_definition.name)

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
        data_proc_def_obj = self.clients.resource_registry.read(data_process_definition_id)
        return data_proc_def_obj

    def delete_data_process_definition(self, data_process_definition_id=''):

        # Delete all associations for this resource
        self._remove_associations(data_process_definition_id)

        # Delete the resource
        self.clients.resource_registry.retire(data_process_definition_id)

    def force_delete_data_process_definition(self, data_process_definition_id=''):

        processdef_ids, _ = self.clients.resource_registry.find_objects(subject=data_process_definition_id, predicate=PRED.hasProcessDefinition, object_type=RT.ProcessDefinition, id_only=True)
        self._remove_associations(data_process_definition_id)
        self.clients.resource_registry.delete(data_process_definition_id)
        for processdef_id in processdef_ids:
            self.clients.process_dispatcher.delete_process_definition(processdef_id)


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

        validate_is_not_none(stream_definition_obj, "No stream definition object found for stream definition id: %s" % stream_definition_id)
        validate_is_not_none(data_process_definition_obj, "No data process definition object found for data process" \
                                                          " definition id: %s" % data_process_definition_id)

        self.clients.resource_registry.create_association(data_process_definition_id,  PRED.hasInputStreamDefinition,  stream_definition_id)

    def unassign_input_stream_definition_from_data_process_definition(self, stream_definition_id='', data_process_definition_id=''):
        """
        Disconnect the Data Product from the Data Producer

        @param stream_definition_id    str
        @param data_process_definition_id    str
        @throws NotFound    object with specified id does not exist
        """

        # Remove the link between the Stream Definition resource and the Data Process Definition resource
        associations = self.clients.resource_registry.find_associations(data_process_definition_id, PRED.hasInputStreamDefinition, stream_definition_id, id_only=True)
        validate_is_not_none(associations, "No Input Stream Definitions associated with data process definition ID " + str(data_process_definition_id))

        for association in associations:
            self.clients.resource_registry.delete_association(association)

    def assign_stream_definition_to_data_process_definition(self, stream_definition_id='', data_process_definition_id='', binding=''):
        """Connect the output  stream with a data process definition
        """
        # Verify that both ids are valid, RR will throw if not found
        stream_definition_obj = self.clients.resource_registry.read(stream_definition_id)
        data_process_definition_obj = self.clients.resource_registry.read(data_process_definition_id)

        validate_is_not_none(stream_definition_obj, "No stream definition object found for stream definition id: %s" % stream_definition_id)
        validate_is_not_none(data_process_definition_obj, "No data process definition object found for data process"\
                                                          " definition id: %s" % data_process_definition_id)

        self.clients.resource_registry.create_association(data_process_definition_id,  PRED.hasStreamDefinition,  stream_definition_id)
        data_process_definition_obj.output_bindings[binding] = stream_definition_id
        self.clients.resource_registry.update(data_process_definition_obj)

    def unassign_stream_definition_from_data_process_definition(self, stream_definition_id='', data_process_definition_id=''):
        """
        Disconnect the Data Product from the Data Producer

        @param stream_definition_id    str
        @param data_process_definition_id    str
        @throws NotFound    object with specified id does not exist
        """

        # Remove the link between the Stream Definition resource and the Data Process Definition resource
        associations = self.clients.resource_registry.find_associations(data_process_definition_id, PRED.hasStreamDefinition, stream_definition_id, id_only=True)

        validate_is_not_none(associations, "No Stream Definitions associated with data process definition ID " + str(data_process_definition_id))
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def create_data_process2(self, data_process_definition_id='', in_data_product_ids=None, out_data_product_ids=None, configuration=None):
        '''
        Creates a DataProcess resource and launches the process.
        A DataProcess is a process that receives one (or more) data products and produces one (or more) data products.

        @param data_process_definition_id : The Data Process Definition to use, if none is specified the standard TransformDataProcess is used
        @param in_data_product_ids : A list of input data product identifiers
        @param out_data_product_ids : A list of output data product identifiers
        @param configuration : The configuration dictionary for the process, and the routing table:

        The routing table is defined as such:
            { (in_data_product_id, out_data_product_id) : actor }

        Routes are specified in the configuration dictionary under the item "routes"
        actor is either None (for ParameterFunctions) or a valid TransformFunction identifier
        '''
        configuration = configuration or DotDict()
        routes = configuration.get_safe('process.routes', {})
        self.validate_compatibility(in_data_product_ids, out_data_product_ids)
        configuration.process.routes = self._manage_routes(routes)

        dproc = DataProcess()
        dproc.name = 'data_process_%s' % self.get_unique_id()
        dproc.configuration = configuration
        dproc_id, rev = self.clients.resource_registry.create(dproc)

        self._manage_producers()

        self._manage_attachments()

        queue_name = self._create_subscription(dproc, in_data_product_ids)

        pid = self._launch_data_process(
                queue_name=queue_name,
                data_process_definition_id=data_process_definition_id,
                out_data_product_ids=out_data_product_ids,
                configuration=configuration)

        self.clients.resource_registry.create_association(subject=dproc_id, predicate=PRED.hasProcess, object=pid)


        return dproc_id
    # ------------------------------------------------------------------------------------------------
    # Working with DataProcess

    def create_data_process(self, data_process_definition_id='', in_data_product_ids=None, out_data_products=None, configuration=None):
        """
        @param  data_process_definition_id: Object with definition of the
                    process to apply to the input data product
        @param  in_data_product_ids: ID of the input data products
        @param  out_data_products: list of IDs of the output data products
        @retval data_process_id: ID of the newly created data process object
        """

        inform = "Input Data Product: "+str(in_data_product_ids)+\
                 "\nTransformed by: "+str(data_process_definition_id)+\
                 "\nTo create output Product: "+str(out_data_products) + "\n"
        log.debug("DataProcessManagementService:create_data_process() method called with parameters:\n" +
                  inform)

        #---------------------------------------------------------------------------------------
        # Initialize
        #---------------------------------------------------------------------------------------

        configuration = configuration or DotDict()

        #validate_is_not_none( out_data_products, "No output data products passed in")

        #---------------------------------------------------------------------------------------
        # Read the data process definition
        #---------------------------------------------------------------------------------------
        data_process_definition = self.read_data_process_definition(data_process_definition_id)

        #---------------------------------------------------------------------------------------
        # Read the output bindings from the definition
        #---------------------------------------------------------------------------------------

        output_bindings = data_process_definition.output_bindings

        #---------------------------------------------------------------------------------------
        # Find the process definition associated with this data process definition.
        # From the process definition, we can get the module and class to run....
        #---------------------------------------------------------------------------------------

        procdef_ids,_ = self.clients.resource_registry.find_objects(data_process_definition_id, PRED.hasProcessDefinition, RT.ProcessDefinition, id_only=True)
        if not procdef_ids:
            raise BadRequest("Cannot find associated ProcessDefinition for DataProcessDefinition id=%s" % data_process_definition_id)

        process_definition_id = procdef_ids[0]

        #---------------------------------------------------------------------------------------
        # Create a data process object and register it
        #---------------------------------------------------------------------------------------

        # get the name of the data process and create an IONObject for it
        data_process_name = create_unique_identifier("process_" + data_process_definition.name)
        data_process_obj = IonObject(RT.DataProcess, name=data_process_name)

        # register the data process
        data_process_id, version = self.clients.resource_registry.create(data_process_obj)

        data_process_obj = self.clients.resource_registry.read(data_process_id)

        #---------------------------------------------------------------------------------------
        # Make the necessary associations, registering
        #---------------------------------------------------------------------------------------

        #todo check if this assoc is needed?
        # Associate the data process with the data process definition
        self.clients.resource_registry.create_association(data_process_id,  PRED.hasProcessDefinition, data_process_definition_id)

        # Register the data process instance as a data producer with DataAcquisitionMgmtSvc
        data_producer_id = self.clients.data_acquisition_management.register_process(data_process_id)
        log.debug("DataProcessManagementService:create_data_process register process "
                  "with DataAcquisitionMgmtSvc: data_producer_id: %s   (L4-CI-SA-RQ-181)", str(data_producer_id) )

        #---------------------------------------------------------------------------------------
        # Register each output data product with DAMS to create DataProducer links
        #---------------------------------------------------------------------------------------

        output_stream_dict = {}

#        if out_data_products is None:
#            raise BadRequest("Data Process must have output product(s) specified %s",  str(data_process_definition_id) )
        if out_data_products:
            for binding, output_data_product_id in out_data_products.iteritems():

                # check that the product is not already associated with a producer
                producer_ids, _ = self.clients.resource_registry.find_objects(output_data_product_id, PRED.hasDataProducer, RT.DataProducer, True)
                if producer_ids:
                    raise BadRequest("Data Product should not already be associated to a DataProducer %s hasDataProducer %s", str(data_process_id), str(producer_ids[0]))

                #Assign each output Data Product to this producer resource
                output_data_product_obj = self.clients.resource_registry.read(output_data_product_id)
                if not output_data_product_obj:
                    raise NotFound("Output Data Product %s does not exist" % output_data_product_id)

                # Associate with DataProcess: register as an output product for this process
                log.debug("Link data process %s and output out data product: %s  (L4-CI-SA-RQ-260)", str(data_process_id), str(output_data_product_id))
                self.clients.data_acquisition_management.assign_data_product(input_resource_id= data_process_id,data_product_id= output_data_product_id)

                # Retrieve the id of the OUTPUT stream from the out Data Product
                stream_ids, _ = self.clients.resource_registry.find_objects(output_data_product_id, PRED.hasStream, RT.Stream, True)

                if not stream_ids:
                    raise NotFound("No Stream created for output Data Product " + str(output_data_product_id))

                if len(stream_ids) != 1:
                    raise BadRequest("Data Product should only have ONE stream at this time" + str(output_data_product_id))

                output_stream_dict[binding] = stream_ids[0]

        #------------------------------------------------------------------------------------------------------------------------------------------
        #Check for attached objects and put them into the configuration
        #------------------------------------------------------------------------------------------------------------------------------------------

        # check for attachments in data process definition
        configuration = self._find_lookup_tables(data_process_definition_id, configuration)
        input_stream_ids = []

        if in_data_product_ids:
            for  in_data_product_id in in_data_product_ids:

                self.clients.resource_registry.create_association(data_process_id, PRED.hasInputProduct, in_data_product_id)
                log.debug("Associate data process workflows with source data products %s "
                          "hasInputProducts  %s   (L4-CI-SA-RQ-260)", str(data_process_id), str(in_data_product_ids))

                #check if in data product is attached to an instrument, check instrumentDevice and InstrumentModel for lookup table attachments
                instdevice_ids, _ = self.clients.resource_registry.find_subjects(RT.InstrumentDevice, PRED.hasOutputProduct, in_data_product_id, True)

                for instdevice_id in instdevice_ids:
                    log.debug("Instrument device_id assoc to the input data product of this data process: %s   (L4-CI-SA-RQ-231)", str(instdevice_id))

                    # check for attachments in instrument device
                    configuration = self._find_lookup_tables(instdevice_id, configuration)
                    instmodel_ids, _ = self.clients.resource_registry.find_objects(instdevice_id, PRED.hasModel, RT.InstrumentModel, True)

                    for instmodel_id in instmodel_ids:
                        # check for attachments in instrument model
                        configuration = self._find_lookup_tables(instmodel_id, configuration)

            #------------------------------------------------------------------------------------------------------------------------------------------
            # Get the input stream from the input_data_product, which should already be associated with a stream via the Data Producer
            #------------------------------------------------------------------------------------------------------------------------------------------
            input_stream_ids = self._get_input_stream_ids(in_data_product_ids)

        #------------------------------------------------------------------------------------------------------------------------------------------
        # Create subscription to the input stream
        #------------------------------------------------------------------------------------------------------------------------------------------
        input_subscription_id = self.clients.pubsub_management.create_subscription(name=data_process_name, stream_ids=input_stream_ids)

        #------------------------------------------------------------------------------------------------------------------------------------------
        # Add the subscription id to the data process
        #------------------------------------------------------------------------------------------------------------------------------------------
        data_process_obj.input_subscription_id = input_subscription_id

        log.info("Launching the process")
        debug_str = "\n\tQueue Name: %s\n\tOutput Streams: %s\n\tProcess Definition ID: %s\n\tConfiguration: %s" % (data_process_name, output_stream_dict, process_definition_id, configuration)
        log.debug(debug_str)

        pid = self._launch_process(
                           queue_name=data_process_name,
                           out_streams=output_stream_dict,
                           process_definition_id=process_definition_id,
                           configuration=configuration)

        data_process_obj.process_id = pid
        self.clients.resource_registry.update(data_process_obj)
        return data_process_id

    def _get_input_stream_ids(self, in_data_product_ids = None):

        input_stream_ids = []

        #------------------------------------------------------------------------------------------------------------------------------------------
        # get the streams associated with this IN data products
        #------------------------------------------------------------------------------------------------------------------------------------------
        for  in_data_product_id in in_data_product_ids:

            # Get the stream associated with this input data product
            stream_ids, _ = self.clients.resource_registry.find_objects(in_data_product_id, PRED.hasStream, RT.Stream, True)

            validate_is_not_none( stream_ids, "No Stream created for this input Data Product " + str(in_data_product_id))
            validate_is_not_none( len(stream_ids) != 1, "Input Data Product should only have ONE stream" + str(in_data_product_id))

            # We take for now one stream_id associated with each input data product
            input_stream_ids.append(stream_ids[0])

        return input_stream_ids

    def _launch_process(self, queue_name='', out_streams=None, process_definition_id='', configuration=None):
        """
        Launches the process
        """

        # ------------------------------------------------------------------------------------
        # Spawn Configuration and Parameters
        # ------------------------------------------------------------------------------------

        if 'process' not in configuration:
            configuration['process'] = {}
        configuration['process']['queue_name'] = queue_name
        configuration['process']['publish_streams'] = out_streams

        # Setting the restart mode
        schedule = ProcessSchedule()
        schedule.restart_mode = ProcessRestartMode.ABNORMAL

        # ------------------------------------------------------------------------------------
        # Process Spawning
        # ------------------------------------------------------------------------------------
        # Spawn the process
        pid = self.clients.process_dispatcher.schedule_process(
            process_definition_id=process_definition_id,
            schedule= schedule,
            configuration=configuration
        )
        validate_is_not_none( pid, "Process could not be spawned")

        return pid


    def _find_lookup_tables(self, resource_id="", configuration=None):
        #check if resource has lookup tables attached

        configuration = configuration or DotDict()

        attachment_objs, _ = self.clients.resource_registry.find_objects(resource_id, PRED.hasAttachment, RT.Attachment, False)

        for attachment_obj in attachment_objs:

            words = set(attachment_obj.keywords)

            if 'DataProcessInput' in words:
                configuration[attachment_obj.name] = attachment_obj.content
                log.debug("Lookup table, %s, found in attachment %s" % (attachment_obj.content, attachment_obj.name))
            else:
                log.debug("NO lookup table in attachment %s" % attachment_obj.name)

        return configuration

    def update_data_process_inputs(self, data_process_id="", in_stream_ids=None):
        #@TODO: INPUT STREAM VALIDATION
        log.debug("Updating inputs to data process '%s'", data_process_id)
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        subscription_id = data_process_obj.input_subscription_id
        was_active = False 
        if subscription_id:
            # get rid of all the current streams
            try:
                log.debug("Deactivating subscription '%s'", subscription_id)
                self.clients.pubsub_management.deactivate_subscription(subscription_id)
                was_active = True

            except BadRequest:
                log.info('Subscription was not active')

            self.clients.pubsub_management.delete_subscription(subscription_id)

        new_subscription_id = self.clients.pubsub_management.create_subscription(data_process_obj.name,
                                                                                 stream_ids=in_stream_ids)
        data_process_obj.input_subscription_id = new_subscription_id

        self.clients.resource_registry.update(data_process_obj)

        if was_active:
            log.debug("Activating subscription '%s'", new_subscription_id)
            self.clients.pubsub_management.activate_subscription(new_subscription_id)

            

    def update_data_process(self,):
        #todo: What are valid ways to update a data process?.

        return

    def read_data_process(self, data_process_id=""):

        data_proc_obj = self.clients.resource_registry.read(data_process_id)
        return data_proc_obj


    def delete_data_process(self, data_process_id=""):

        # Delete the specified DataProcessDefinition object
        data_process_obj = self.read_data_process(data_process_id)

        log.debug("delete the association with DataProcessDefinition")
        dpd_assn_ids = self.clients.resource_registry.find_associations(subject=data_process_id,  predicate=PRED.hasProcessDefinition, id_only=True)
        for dpd_assn_id in dpd_assn_ids:
            log.debug("Trying to delete the association with this data process definition: %s", dpd_assn_id)
            self.clients.resource_registry.delete_association(dpd_assn_id)

        self._stop_process(data_process_obj)


        log.debug("Finalizing data products by removing streams associated with the dataset and product")
        out_products, assocs = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasOutputProduct, id_only=True)
        for out_product, assoc in zip(out_products, assocs):
            self.data_product_management.remove_streams(out_product)
            log.debug("deleting association with output data product '%s'" % out_product)
            self.clients.resource_registry.delete_association(assoc)

            self.clients.data_acquisition_management.unassign_data_product(data_process_id, out_product)


        log.debug("Delete the input product links")
        inprod_associations = self.clients.resource_registry.find_associations(data_process_id, PRED.hasInputProduct)
        for inprod_association in inprod_associations:
            self.clients.resource_registry.delete_association(inprod_association)


        try:
            self.deactivate_data_process(data_process_id=data_process_id)
            log.warn('Deleteing activated data process...')
        except BadRequest:
            pass

        subscription_id = data_process_obj.input_subscription_id
        self.clients.pubsub_management.delete_subscription(subscription_id)
        data_process_obj.input_subscription_id = None
        self.clients.resource_registry.update(data_process_obj)

        #unregister the data process in DataAcquisitionMgmtSvc
        self.clients.data_acquisition_management.unregister_process(data_process_id)

        # Delete the data process
        self.clients.resource_registry.retire(data_process_id)
        return

    def force_delete_data_process(self, data_process_id=""):

        # if not yet deleted, the first execute delete logic
        dp_obj = self.read_data_process(data_process_id)
        if dp_obj.lcstate != LCS.RETIRED:
            self.delete_data_process(data_process_id)

        self._remove_associations(data_process_id)
        self.clients.resource_registry.delete(data_process_id)

    def _stop_process(self, data_process):
        log.debug("stopping data process '%s'" % data_process.process_id)
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
        log.debug("activate_data_process:data_process_obj  %s ", str(data_process_obj))


#        #update the producer context with the activation time and the configuration


        # todo: update the setting of this context with the return vals from process_dispatcher:schedule_process after convert
        # todo: process_id, process_definition, schedule, configuration

        producer_obj = self._get_process_producer(data_process_id)
        producertype = type(producer_obj).__name__
        #todo: producer_obj.producer_context.type_ is returning the base type, not the derived type.
        if producer_obj.producer_context.type_ == OT.DataProcessProducerContext :
            log.debug("activate_data_process:activation_time  %s ", str(IonTime().to_string()))
            producer_obj.producer_context.activation_time = IonTime().to_string()
            producer_obj.producer_context.configuration = data_process_obj.configuration
            self.clients.resource_registry.update(producer_obj)

        subscription_id = data_process_obj.input_subscription_id
        self.clients.pubsub_management.activate_subscription(subscription_id=subscription_id)

    def deactivate_data_process(self, data_process_id=""):

        data_process_obj = self.read_data_process(data_process_id)

        if not data_process_obj.input_subscription_id:
            log.warn("data process '%s' has no subscription id to deactivate", data_process_id)
            return

        subscription_obj = self.clients.pubsub_management.read_subscription(data_process_obj.input_subscription_id)

        if subscription_obj.activated:

            #update the producer context with the deactivation time
            # todo: update the setting of this contect with the return vals from process_dispatcher:schedule_process after convert
            producer_obj = self._get_process_producer(data_process_id)
            producertype = type(producer_obj).__name__
            if producer_obj.producer_context.type_ == OT.DataProcessProducerContext :
                log.debug("data_process '%s' (producer '%s'): deactivation_time = %s ",
                          data_process_id, producer_obj._id, str(IonTime().to_string()))
                producer_obj.producer_context.deactivation_time = IonTime().to_string()
                self.clients.resource_registry.update(producer_obj)

            subscription_id = data_process_obj.input_subscription_id
            log.debug("Deactivating subscription '%s'", subscription_id)
            self.clients.pubsub_management.deactivate_subscription(subscription_id=subscription_id)



    def attach_process(self, process=''):
        """
        @param      process: Should this be the data_process_id?
        @retval
        """
        # TODO: Determine the proper input param
        pass


    def _get_stream_from_dp(self, dp_id):
        stream_ids, _ = self.clients.resource_registry.find_objects(subject=dp_id, predicate=PRED.hasStream, id_only=True)
        if not stream_ids: raise BadRequest('No streams associated with this data product')
        return stream_ids[0]


    def _manage_routes(self, routes):
        retval = {}
        for path,actor in routes.iteritems():
            in_data_product_id, out_data_product_id = path
            in_stream_id = self._get_stream_from_dp(in_data_product_id)
            out_stream_id = self._get_stream_from_dp(out_data_product_id)
            if actor:
                self.clients.resource_registry.read(actor)
                if isinstance(actor,TransformFunction):
                    actor = 'TODO: figure this out' 
                    
            retval[(in_stream_id, out_stream_id)] = actor
        return retval

    def _manage_producers(self):
        pass

    def _manage_attachments(self):
        pass

    def _create_subscription(self, dproc, in_data_product_ids):
        stream_ids = [self._get_stream_from_dp(i) for i in in_data_product_ids]
        #@TODO Maybe associate a data process with an exchange point but in the mean time: 
        queue_name = 'sub_%s' % dproc.name
        subscription_id = self.clients.pubsub_management.create_subscription(name=queue_name, stream_ids=stream_ids)
        self.clients.resource_registry.create_association(subject=dproc._id, predicate=PRED.hasSubscription, object=subscription_id)
        return queue_name

    def _get_process_definition(self, data_process_definition_id=''):
        process_definition_id = ''
        if data_process_definition_id:
            process_definitions, _ = self.clients.resource_registry.find_objects(subject=data_process_definition_id, predicate=PRED.hasProcessDefinition, id_only=True)
            if process_definitions:
                process_definition_id = process_definitions[0]
        else:
            process_definitions, _ = self.clients.resource_registry.find_resources(name='transform_data_process', restype=RT.ProcessDefinition,id_only=True)
            if process_definitions:
                process_definition_id = process_definitions[0]
            else:
                process_definition = ProcessDefinition()
                process_definition.name = 'transform_data_process'
                process_definition.executable['module'] = 'ion.processes.data.transforms.transform_prime'
                process_definition.executable['class'] = 'TransformPrime'
                process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition)
        return process_definition_id

    def _launch_data_process(self, queue_name='', data_process_definition_id='', out_data_product_ids=[], configuration={}):
        process_definition_id = self._get_process_definition(data_process_definition_id)

        out_streams = {}
        for dp_id in out_data_product_ids:
            stream_id = self._get_stream_from_dp(dp_id)
            out_streams[stream_id] = stream_id

        return self._launch_process(queue_name, out_streams, process_definition_id, configuration)







    def _validator(self, in_data_product_id, out_data_product_id):
        return True
    
    def validate_compatibility(self, in_data_product_ids=None, out_data_product_ids=None, routes=None):
        '''
        Validates compatibility between input and output data products
        routes are in this form:
        { (in_data_product_id, out_data_product_id) : actor }
            if actor is None then the data process is assumed to use parameter functions.
            if actor is a TransformFunction, the validation is done at runtime
        '''
        if len(out_data_product_ids)>1 and not routes:
            raise BadRequest('Multiple output data products but no routes defined')
        if len(out_data_product_ids)==1:
            return all( [self._validator(i, out_data_product_ids[0]) for i in in_data_product_ids] )
        elif len(out_data_product_ids)>1:
            for path,actor in routes.iteritems():
                in_dp_id, out_dp_id = path
                if not self._validator(in_dp_id, out_dp_id):
                    return False
            return True
        else:
            raise BadRequest('No input data products specified')


    def _get_process_producer(self, data_process_id=""):
        producer_objs, _ = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasDataProducer, object_type=RT.DataProducer, id_only=False)
        if not producer_objs:
            raise NotFound("No Producers created for this Data Process " + str(data_process_id))
        return producer_objs[0]


    ############################
    #
    #  EXTENDED RESOURCES
    #
    ############################



    def get_data_process_definition_extension(self, data_process_definition_id='', ext_associations=None, ext_exclude=None, user_id=''):
        #Returns an DataProcessDefinition Extension object containing additional related information

        if not data_process_definition_id:
            raise BadRequest("The data_process_definition_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_data_process_definition = extended_resource_handler.create_extended_resource_container(
            extended_resource_type=OT.DataProcessDefinitionExtension,
            resource_id=data_process_definition_id,
            computed_resource_type=OT.DataProcessDefinitionComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id)

        #Loop through any attachments and remove the actual content since we don't need
        #   to send it to the front end this way
        #TODO - see if there is a better way to do this in the extended resource frame work.
        if hasattr(extended_data_process_definition, 'attachments'):
            for att in extended_data_process_definition.attachments:
                if hasattr(att, 'content'):
                    delattr(att, 'content')

        # replace list of lists with single list
        replacement_data_products = []
        for inner_list in extended_data_process_definition.data_products:
            if inner_list:
                for actual_data_product in inner_list:
                    if actual_data_product:
                        replacement_data_products.append(actual_data_product)
        extended_data_process_definition.data_products = replacement_data_products

        return extended_data_process_definition

    def get_data_process_extension(self, data_process_id='', ext_associations=None, ext_exclude=None, user_id=''):
        #Returns an DataProcessDefinition Extension object containing additional related information

        if not data_process_id:
            raise BadRequest("The data_process_definition_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_data_process = extended_resource_handler.create_extended_resource_container(
            extended_resource_type=OT.DataProcessExtension,
            resource_id=data_process_id,
            computed_resource_type=OT.DataProcessComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id)

        #Loop through any attachments and remove the actual content since we don't need
        #   to send it to the front end this way
        #TODO - see if there is a better way to do this in the extended resource frame work.
        if hasattr(extended_data_process, 'attachments'):
            for att in extended_data_process.attachments:
                if hasattr(att, 'content'):
                    delattr(att, 'content')

        return extended_data_process


    def _remove_associations(self, resource_id=''):
        """
        delete all associations to/from a resource
        """

        # find all associations where this is the subject
        _, obj_assns = self.clients.resource_registry.find_objects(subject=resource_id, id_only=True)

        # find all associations where this is the object
        _, sbj_assns = self.clients.resource_registry.find_subjects(object=resource_id, id_only=True)

        log.debug("pluck will remove %s subject associations and %s object associations",
                 len(sbj_assns), len(obj_assns))

        for assn in obj_assns:
            log.debug("pluck deleting object association %s", assn)
            self.clients.resource_registry.delete_association(assn)

        for assn in sbj_assns:
            log.debug("pluck deleting subject association %s", assn)
            self.clients.resource_registry.delete_association(assn)

        # find all associations where this is the subject
        _, obj_assns = self.clients.resource_registry.find_objects(subject=resource_id, id_only=True)

        # find all associations where this is the object
        _, sbj_assns = self.clients.resource_registry.find_subjects(object=resource_id, id_only=True)

        log.debug("post-deletions, pluck found %s subject associations and %s object associations",
                 len(sbj_assns), len(obj_assns))
