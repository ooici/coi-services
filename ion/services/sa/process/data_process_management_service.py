#!/usr/bin/env python

"""
@package ion.services.sa.process.test.test_int_data_process_management_service
@author  Maurice Manning
"""
from uuid import uuid4
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from pyon.util.log import log
from pyon.util.ion_time import IonTime
from pyon.public import RT, PRED, OT, LCS
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound
from pyon.util.containers import create_unique_identifier
from pyon.util.containers import DotDict
from pyon.util.arg_check import validate_is_not_none, validate_true
from pyon.ion.resource import ExtendedResourceContainer
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessRestartMode, TransformFunction, DataProcess, ProcessQueueingMode, ComputedValueAvailability

from interface.services.sa.idata_process_management_service import BaseDataProcessManagementService
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient

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

        self.RR2 = EnhancedResourceRegistryClient(self.clients.resource_registry)

        #shortcut names for the import sub-services
        if hasattr(self.clients, "resource_registry"):
            self.RR   = self.clients.resource_registry


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

        return self.RR2.create(transform_function, RT.TransformFunction)

    def read_transform_function(self, transform_function_id=''):
        tf = self.RR2.read(transform_function_id, RT.TransformFunction)
        return tf

    def update_transform_function(self, transform_function=None):
        self.RR2.update(transform_function, RT.TransformFunction)

    def delete_transform_function(self, transform_function_id=''):
        self.RR2.retire(transform_function_id, RT.TransformFunction)

    def force_delete_transform_function(self, transform_function_id=''):
        self.RR2.pluck_delete(transform_function_id, RT.TransformFunction)

    def create_data_process_definition(self, data_process_definition=None):

        data_process_definition_id = self.RR2.create(data_process_definition, RT.DataProcessDefinition)

        #-------------------------------
        # Process Definition
        #-------------------------------
        # Create the underlying process definition
        process_definition = ProcessDefinition()
        process_definition.name = data_process_definition.name
        process_definition.description = data_process_definition.description

        process_definition.executable = {'module':data_process_definition.module, 'class':data_process_definition.class_name}
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)

        self.RR2.assign_process_definition_to_data_process_definition_with_has_process_definition(process_definition_id,
                                                                                                  data_process_definition_id)

        return data_process_definition_id

    def update_data_process_definition(self, data_process_definition=None):
        # TODO: If executable has changed, update underlying ProcessDefinition

        # Overwrite DataProcessDefinition object
        self.RR2.update(data_process_definition, RT.DataProcessDefinition)

    def read_data_process_definition(self, data_process_definition_id=''):
        data_proc_def_obj = self.RR2.read(data_process_definition_id, RT.DataProcessDefinition)
        return data_proc_def_obj

    def delete_data_process_definition(self, data_process_definition_id=''):

        # Delete the resource
        self.RR2.retire(data_process_definition_id, RT.DataProcessDefinition)

    def force_delete_data_process_definition(self, data_process_definition_id=''):

        processdef_ids, _ = self.clients.resource_registry.find_objects(subject=data_process_definition_id, predicate=PRED.hasProcessDefinition, object_type=RT.ProcessDefinition, id_only=True)

        self.RR2.pluck_delete(data_process_definition_id, RT.DataProcessDefinition)

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
        if binding:
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


    def create_data_process(self, data_process_definition_id='', in_data_product_ids=None, out_data_product_ids=None, configuration=None):
        '''
        Creates a DataProcess resource and launches the process.
        A DataProcess is a process that receives one (or more) data products and produces one (or more) data products.

        @param data_process_definition_id : The Data Process Definition to use, if none is specified the standard TransformDataProcess is used
        @param in_data_product_ids : A list of input data product identifiers
        @param out_data_product_ids : A list of output data product identifiers
        @param configuration : The configuration dictionary for the process, and the routing table:

        The routing table is defined as such:
            { in_data_product_id: {out_data_product_id : actor }}

        Routes are specified in the configuration dictionary under the item "routes"
        actor is either None (for ParameterFunctions) or a valid TransformFunction identifier
        '''
        configuration = DotDict(configuration or {}) 
        in_data_product_ids = in_data_product_ids or []
        out_data_product_ids = out_data_product_ids or []
        routes = configuration.get_safe('process.routes', {})
        if not routes and (1==len(in_data_product_ids)==len(out_data_product_ids)):
            routes = {in_data_product_ids[0]: {out_data_product_ids[0]:None}}
        # Routes are not supported for processes with discrete data process definitions
        elif not routes and not data_process_definition_id:
            raise BadRequest('No valid route defined for this data process.')

        self.validate_compatibility(data_process_definition_id, in_data_product_ids, out_data_product_ids, routes)
        routes = self._manage_routes(routes)
        configuration.process.input_products = in_data_product_ids
        configuration.process.output_products = out_data_product_ids
        configuration.process.routes = routes
        if 'lookup_docs' in configuration.process:
            configuration.process.lookup_docs.extend(self._get_lookup_docs(in_data_product_ids, out_data_product_ids))
        else:
            configuration.process.lookup_docs = self._get_lookup_docs(in_data_product_ids, out_data_product_ids)
        dproc = DataProcess()
        dproc.name = 'data_process_%s' % self.get_unique_id()
        dproc.configuration = configuration
        dproc_id, rev = self.clients.resource_registry.create(dproc)
        dproc._id = dproc_id
        dproc._rev = rev

        for data_product_id in in_data_product_ids:
            self.clients.resource_registry.create_association(subject=dproc_id, predicate=PRED.hasInputProduct, object=data_product_id)

        if data_process_definition_id:
            self.clients.resource_registry.create_association(data_process_definition_id, PRED.hasDataProcess ,dproc_id)

        self._manage_producers(dproc_id, out_data_product_ids)

        self._manage_attachments()

        queue_name = self._create_subscription(dproc, in_data_product_ids)

        pid = self._launch_data_process(
                queue_name=queue_name,
                data_process_definition_id=data_process_definition_id,
                out_data_product_ids=out_data_product_ids,
                configuration=configuration)

        self.clients.resource_registry.create_association(subject=dproc_id, predicate=PRED.hasProcess, object=pid)


        return dproc_id

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
        schedule.queueing_mode = ProcessQueueingMode.ALWAYS

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

            

    def update_data_process(self):
        raise BadRequest('Cannot update an existing data process.')


    def read_data_process(self, data_process_id=''):
        data_proc_obj = self.clients.resource_registry.read(data_process_id)
        return data_proc_obj


    def delete_data_process(self, data_process_id=""):

        #Stops processes and deletes the data process associations
        #TODO: Delete the processes also?
        self.deactivate_data_process(data_process_id)
        processes, assocs = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasProcess, id_only=False)
        for process, assoc in zip(processes,assocs):
            self._stop_process(data_process=process)
            self.clients.resource_registry.delete_association(assoc)

        #Delete all subscriptions associations
        subscription_ids, assocs = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasSubscription, id_only=True)
        for subscription_id, assoc in zip(subscription_ids, assocs):
            self.clients.resource_registry.delete_association(assoc)
            self.clients.pubsub_management.delete_subscription(subscription_id)

        #Unassign data products
        data_product_ids, assocs = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasOutputProduct, id_only=True)
        for data_product_id, assoc in zip(data_product_ids, assocs):
            self.clients.data_acquisition_management.unassign_data_product(input_resource_id=data_process_id, data_product_id=data_product_id)

        #Unregister the data process with acquisition
        self.clients.data_acquisition_management.unregister_process(data_process_id=data_process_id)

        #Delete the data process from the resource registry
        self.RR2.retire(data_process_id, RT.DataProcess)

    def force_delete_data_process(self, data_process_id=""):

        # if not yet deleted, the first execute delete logic
        dp_obj = self.read_data_process(data_process_id)
        if dp_obj.lcstate != LCS.RETIRED:
            self.delete_data_process(data_process_id)

        self.RR2.pluck_delete(data_process_id, RT.DataProcess)

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

    def activate_data_process(self, data_process_id=''):
        #@Todo: Data Process Producer context stuff
        subscription_ids, assocs = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasSubscription, id_only=True)
        for subscription_id in subscription_ids:
            if not self.clients.pubsub_management.subscription_is_active(subscription_id):
                self.clients.pubsub_management.activate_subscription(subscription_id)
        return True

    def deactivate_data_process(self, data_process_id=''):
        #@todo: data process producer context stuff
        subscription_ids, assocs = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasSubscription, id_only=True)
        for subscription_id in subscription_ids:
            if self.clients.pubsub_management.subscription_is_active(subscription_id):
                self.clients.pubsub_management.deactivate_subscription(subscription_id)

        return True


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

    def _has_lookup_values(self, data_product_id):
        stream_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        if not stream_ids:
            raise BadRequest('No streams found for this data product')
        stream_def_ids, _ = self.clients.resource_registry.find_objects(subject=stream_ids[0], predicate=PRED.hasStreamDefinition, id_only=True)
        if not stream_def_ids:
            raise BadRequest('No stream definitions found for this stream')
        stream_def_id = stream_def_ids[0]
        retval = self.clients.pubsub_management.has_lookup_values(stream_def_id)
        
        return retval
    
    def _get_lookup_docs(self, input_data_product_ids=[], output_data_product_ids=[]):
        retval = []
        need_lookup_docs = False
        for data_product_id in output_data_product_ids:
            if self._has_lookup_values(data_product_id):
                need_lookup_docs = True
                break
        if need_lookup_docs:
            for data_product_id in input_data_product_ids:
                retval.extend(self.clients.data_acquisition_management.list_qc_references(data_product_id))
            for data_product_id in output_data_product_ids:
                retval.extend(self.clients.data_acquisition_management.list_qc_references(data_product_id))
        return retval

    def _manage_routes(self, routes):
        retval = {}
        for in_data_product_id,route in routes.iteritems():
            for out_data_product_id, actor in route.iteritems():
                in_stream_id = self._get_stream_from_dp(in_data_product_id)
                out_stream_id = self._get_stream_from_dp(out_data_product_id)
                if actor:
                    actor = self.clients.resource_registry.read(actor)
                    if isinstance(actor,TransformFunction):
                        actor = {'module': actor.module, 'class':actor.cls}
                    else:
                        raise BadRequest('This actor type is not currently supported')
                        
                if in_stream_id not in retval:
                    retval[in_stream_id] =  {}
                retval[in_stream_id][out_stream_id] = actor
        return retval

    def _manage_producers(self, data_process_id, data_product_ids):
        self.clients.data_acquisition_management.register_process(data_process_id)
        for data_product_id in data_product_ids:
            producer_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataProducer, object_type=RT.DataProducer, id_only=True)
            if len(producer_ids):
                raise BadRequest('Only one DataProducer allowed per DataProduct')

            # Validate the data product
            self.clients.data_product_management.read_data_product(data_product_id)

            self.clients.data_acquisition_management.assign_data_product(input_resource_id=data_process_id, data_product_id=data_product_id)

            if not self._get_stream_from_dp(data_product_id):
                raise BadRequest('No Stream was found for this DataProduct')


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
                process_definition = ProcessDefinition()
                process_definition.name = 'transform_data_process'
                process_definition.executable['module'] = 'ion.processes.data.transforms.transform_prime'
                process_definition.executable['class'] = 'TransformPrime'
                process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition)

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
        if data_process_definition_id:
            dpd = self.read_data_process_definition(data_process_definition_id)

        for dp_id in out_data_product_ids:
            stream_id = self._get_stream_from_dp(dp_id)
            if data_process_definition_id:
                stream_definition = self.clients.pubsub_management.read_stream_definition(stream_id=stream_id)
                stream_definition_id = stream_definition._id

                # Check the binding to see if it applies here

                for binding,stream_def_id in dpd.output_bindings.iteritems():
                    if stream_def_id == stream_definition_id:
                        out_streams[binding] = stream_id
                        break
            else:
                out_streams[stream_id] = stream_id

        return self._launch_process(queue_name, out_streams, process_definition_id, configuration)


    def _validator(self, in_data_product_id, out_data_product_id):
        in_stream_id = self._get_stream_from_dp(dp_id=in_data_product_id)
        in_stream_defs, _ = self.clients.resource_registry.find_objects(subject=in_stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
        if not len(in_stream_defs):
            raise BadRequest('No valid stream definition defined for data product stream')
        out_stream_id = self._get_stream_from_dp(dp_id=out_data_product_id)
        out_stream_defs, _  = self.clients.resource_registry.find_objects(subject=out_stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
        if not len(out_stream_defs):
            raise BadRequest('No valid stream definition defined for data product stream')
        return self.clients.pubsub_management.compatible_stream_definitions(in_stream_definition_id=in_stream_defs[0], out_stream_definition_id=out_stream_defs[0])
    
    def validate_compatibility(self, data_process_definition_id='', in_data_product_ids=None, out_data_product_ids=None, routes=None):
        '''
        Validates compatibility between input and output data products
        routes are in this form:
        { (in_data_product_id, out_data_product_id) : actor }
            if actor is None then the data process is assumed to use parameter functions.
            if actor is a TransformFunction, the validation is done at runtime
        '''
        if data_process_definition_id:
            input_stream_def_ids, _ = self.clients.resource_registry.find_objects(subject=data_process_definition_id, predicate=PRED.hasInputStreamDefinition, id_only=True)
            output_stream_def_ids, _ = self.clients.resource_registry.find_objects(subject=data_process_definition_id, predicate=PRED.hasStreamDefinition, id_only=True)
            for in_data_product_id in in_data_product_ids:
                input_stream_def = self.stream_def_from_data_product(in_data_product_id)
                if input_stream_def not in input_stream_def_ids:
                    log.warning('Creating a data process with an unmatched stream definition input')
            for out_data_product_id in out_data_product_ids:
                output_stream_def = self.stream_def_from_data_product(out_data_product_id)
                if output_stream_def not in output_stream_def_ids:
                    log.warning('Creating a data process with an unmatched stream definition output')
        
        if not out_data_product_ids and data_process_definition_id:
            return True
        if len(out_data_product_ids)>1 and not routes and not data_process_definition_id:
            raise BadRequest('Multiple output data products but no routes defined')
        if len(out_data_product_ids)==1:
            return all( [self._validator(i, out_data_product_ids[0]) for i in in_data_product_ids] )
        elif len(out_data_product_ids)>1:
            for in_dp_id,out in routes.iteritems():
                for out_dp_id, actor in out.iteritems():
                    if not self._validator(in_dp_id, out_dp_id):
                        return False
            return True
        else:
            raise BadRequest('No input data products specified')


    def stream_def_from_data_product(self, data_product_id=''):
        stream_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        validate_true(stream_ids, 'No stream found for this data product: %s' % data_product_id)
        stream_id = stream_ids.pop()
        stream_def_ids, _ = self.clients.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
        validate_true(stream_def_ids, 'No stream definition found for this stream: %s' % stream_def_ids)
        stream_def_id = stream_def_ids.pop()
        return stream_def_id


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

        data_process_ids = set()
        for dp in extended_data_process_definition.data_processes:
            data_process_ids.add(dp._id)

        #create the list of output data_products from the data_processes that is aligned
        products_map = {}
        objects, associations = self.clients.resource_registry.find_objects_mult(subjects=list(data_process_ids), id_only=False)
        for obj,assoc in zip(objects,associations):
            # if this is a hasOutputProduct association...
            if assoc.p == PRED.hasOutputProduct:
                #collect the set of output products from each data process in a map
                if not products_map.has_key(assoc.s):
                    products_map[assoc.s] = [assoc.o]
                else:
                    products_map[assoc.s].append(assoc.o)
        #now set up the final list to align
        extended_data_process_definition.data_products = []
        for dp in extended_data_process_definition.data_processes:
            extended_data_process_definition.data_products.append( products_map[dp._id] )

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

    def get_data_process_subscriptions_count(self, data_process_id=""):
        if not data_process_id:
            raise BadRequest("The data_process_definition_id parameter is empty")

        subscription_ids, _ = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasSubscription, id_only=True)
        log.debug("get_data_process_subscriptions_count(id=%s): %s subscriptions", data_process_id, len(subscription_ids))
        return len(subscription_ids)

    def get_data_process_active_subscriptions_count(self, data_process_id=""):
        if not data_process_id:
            raise BadRequest("The data_process_definition_id parameter is empty")

        subscription_ids, _ = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasSubscription, id_only=True)
        active_count = 0
        for subscription_id in subscription_ids:
            if self.clients.pubsub_management.subscription_is_active(subscription_id):
                active_count += 1
        log.debug("get_data_process_active_subscriptions_count(id=%s): %s subscriptions", data_process_id, active_count)
        return active_count


    def get_operational_state(self, taskable_resource_id):   # from Device
        retval = IonObject(OT.ComputedStringValue)
        retval.value = ''
        retval.status = ComputedValueAvailability.NOTAVAILABLE
        return retval
