#!/usr/bin/env python

"""
@package ion.services.sa.process.test.test_int_data_process_management_service
@author  Maurice Manning
"""
from uuid import uuid4
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from pyon.util.log import log
from pyon.util.ion_time import IonTime
from pyon.public import RT, PRED, OT, LCS, CFG
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound
from pyon.util.containers import create_unique_identifier
from pyon.util.containers import DotDict
from pyon.util.arg_check import validate_is_not_none, validate_true
from pyon.ion.resource import ExtendedResourceContainer
from pyon.event.event import EventPublisher

from interface.objects import ProcessDefinition, ProcessSchedule, ProcessRestartMode, DataProcess, ProcessQueueingMode, ComputedValueAvailability
from interface.objects import ParameterFunction, TransformFunction, DataProcessTypeEnum, DataProcessStatusType

from interface.services.sa.idata_process_management_service import BaseDataProcessManagementService
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from coverage_model.parameter_functions import AbstractFunction, PythonFunction, NumexprFunction
from coverage_model import ParameterContext, ParameterFunctionType, ParameterDictionary
from ion.processes.data.replay.replay_client import ReplayClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

from pyon.util.arg_check import validate_is_instance
from ion.util.module_uploader import RegisterModulePreparerPy
import inspect
import os
import pwd

class DataProcessManagementService(BaseDataProcessManagementService):

    def on_init(self):
        IonObject("Resource")  # suppress pyflakes error

        self.override_clients(self.clients)

        self.event_publisher = EventPublisher(OT.DataProcessStatusEvent)

        self.init_module_uploader()

        self.init_transform_worker()

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


    def init_transform_worker(self):

        #todo: temporary? where to store for use on restart?

        # map contains worker_process_id : exchange_name
        #self.transform_worker_subscription_map = {}
        # map contains worker_process_id : [dp_id1, dp_id2, etc]
        #self.transform_worker_dp_map = {}

        self.TRANSFORM_WORKER_HOST_LIMIT = 10



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

    def create_transform_function(self, transform_function=None):
        '''
        Creates a new transform function
        '''
        if not transform_function:
            raise BadRequest("Improper TransformFunction specified")
        return self.RR2.create(transform_function, RT.TransformFunction)

    def read_transform_function(self, transform_function_id=''):
        tf = self.RR2.read(transform_function_id, RT.TransformFunction)
        return tf

    def update_transform_function(self, transform_function=None):
        self.RR2.update(transform_function, RT.TransformFunction)

    def delete_transform_function(self, transform_function_id=''):
        self.RR2.lcs_delete(transform_function_id, RT.TransformFunction)

    def force_delete_transform_function(self, transform_function_id=''):
        self.RR2.force_delete(transform_function_id, RT.TransformFunction)


    def create_data_process_definition(self, data_process_definition=None, function_id=''):
        function_definition = self.clients.resource_registry.read(function_id)

        if isinstance(function_definition,  ParameterFunction):
            data_process_definition.data_process_type = DataProcessTypeEnum.PARAMETER_FUNCTION
            dpd_id, _ = self.clients.resource_registry.create(data_process_definition)
            self.clients.resource_registry.create_association(subject=dpd_id, object=function_id, predicate=PRED.hasParameterFunction)
            return dpd_id

        elif isinstance(function_definition, TransformFunction):
            # TODO: Need service methods for this stuff
            if data_process_definition.data_process_type not in (DataProcessTypeEnum.TRANSFORM_PROCESS, DataProcessTypeEnum.RETRIEVE_PROCESS):
                data_process_definition.data_process_type = DataProcessTypeEnum.TRANSFORM_PROCESS

            dpd_id, _ = self.clients.resource_registry.create(data_process_definition)
            self.clients.resource_registry.create_association(subject=dpd_id, object=function_id, predicate=PRED.hasTransformFunction)
            return dpd_id

        else:
            raise BadRequest('function_definition is not a function type')


    def update_data_process_definition(self, data_process_definition=None):
        # TODO: If executable has changed, update underlying ProcessDefinition

        # Overwrite DataProcessDefinition object
        self.RR2.update(data_process_definition, RT.DataProcessDefinition)

    def read_data_process_definition(self, data_process_definition_id=''):
        data_proc_def_obj = self.RR2.read(data_process_definition_id, RT.DataProcessDefinition)
        return data_proc_def_obj

    def delete_data_process_definition(self, data_process_definition_id=''):
        self.RR2.lcs_delete(data_process_definition_id, RT.DataProcessDefinition)

    def force_delete_data_process_definition(self, data_process_definition_id=''):

        processdef_ids, _ = self.clients.resource_registry.find_objects(subject=data_process_definition_id, predicate=PRED.hasProcessDefinition, object_type=RT.ProcessDefinition, id_only=True)

        self.RR2.force_delete(data_process_definition_id, RT.DataProcessDefinition)

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



    def create_data_process(self, data_process_definition_id='', inputs=None, outputs=None, configuration=None, argument_map=None, out_param_name=''):
        '''
        Creates a DataProcess resource.
        A DataProcess can be of a few types:
           - a data transformation function hosted on a transform worker that receives granules and produces granules.
           - a parameter function in a coverage that transforms data on request

        @param data_process_definition_id : The Data Process Definition parent which contains the transform or parameter funcation specification
        @param inputs: A list of inputs
        @param outputs: A list of outputs

        @param configuration : The configuration dictionary for the process, and the routing table:
        '''

        #todo: out publishers can be either stream or event
        #todo: determine if/how routing tables will be managed
        dpd_obj = self.read_data_process_definition(data_process_definition_id)
        configuration = DotDict(configuration or {})
        if dpd_obj.data_process_type == DataProcessTypeEnum.PARAMETER_FUNCTION:
            # A different kind of data process
            # this function creates a data process resource for each data product and appends the parameter
            return self._initialize_parameter_function(data_process_definition_id, inputs, argument_map, out_param_name)
        elif dpd_obj.data_process_type == DataProcessTypeEnum.TRANSFORM_PROCESS:
            return self._initialize_transform_process(dpd_obj, inputs, outputs, configuration, argument_map, out_param_name)
        elif dpd_obj.data_process_type == DataProcessTypeEnum.RETRIEVE_PROCESS:
            return self._initialize_retrieve_process(dpd_obj, inputs, outputs, configuration, argument_map, out_param_name)


    def _initialize_transform_process(self, data_process_definition, in_data_product_ids, out_data_product_ids, configuration=None, argument_map=None, out_param_name=''):
        dpd_obj = data_process_definition
        configuration = DotDict(configuration or {})
        # work with empty lists if nothing provided for inputs / outputs
        in_data_product_ids = (in_data_product_ids or [])
        out_data_product_ids = (out_data_product_ids or [])

        configuration.process.output_products = out_data_product_ids

        dataproduct_name = ''
        if in_data_product_ids:
            in_dataprod_obj = self.clients.data_product_management.read_data_product(in_data_product_ids[0])
            dataproduct_name = in_dataprod_obj.name

        if 'lookup_docs' in configuration.process:
            configuration.process.lookup_docs.extend(self._get_lookup_docs(in_data_product_ids, out_data_product_ids))
        else:
            configuration.process.lookup_docs = self._get_lookup_docs(in_data_product_ids, out_data_product_ids)

        #Validate input and output mappings
        for in_data_product_id in in_data_product_ids:
            if not self.validate_argument_input(data_product_id=in_data_product_id, argument_map=argument_map):
                raise BadRequest('Input data product does not contain the parameters defined in argument map')
        for out_data_product_id in out_data_product_ids:
            if not self.validate_argument_input(data_product_id=out_data_product_id, output_param=out_param_name):
                raise BadRequest('Output data product does not contain the output parameter name provided')

        dproc = DataProcess()
        #name the data process the DPD name + in_data_product name to make more readable
        dproc.name = ''.join([dpd_obj.name, '_on_', dataproduct_name])
        dproc.configuration = configuration
        # todo: document that these two attributes must be added into the configuration dict of the create call
        dproc.argument_map = argument_map
        dproc.output_param = out_param_name

        dproc_id, rev = self.clients.resource_registry.create(dproc)
        dproc._id = dproc_id
        dproc._rev = rev

        # create links
        for data_product_id in in_data_product_ids:
            self.clients.resource_registry.create_association(subject=dproc_id, predicate=PRED.hasInputProduct, object=data_product_id)
        for data_product_id in out_data_product_ids:
            self.clients.resource_registry.create_association(subject=dproc_id, predicate=PRED.hasOutputProduct, object=data_product_id)
        if data_process_definition._id:
            self.clients.resource_registry.create_association(data_process_definition._id, PRED.hasDataProcess ,dproc_id)

        exchange_name = self._assign_worker(dproc_id)
        #exchange_name = self.transform_worker_subscription_map[transform_worker_pid]
        log.debug('create_data_process  exchange_name: %s', exchange_name)
        queue_name = self._create_subscription(dproc, in_data_product_ids, exchange_name)
        log.debug('create_data_process  queue_name: %s', queue_name)

        return dproc_id

    def _assign_worker(self, data_process_id):
        #todo: assign to a transform worker
        #if no workers, start the first one
        #todo: check if TW has reached limit of dps
        transform_worker_pid = ''
        exchange_name = ''
        transform_worker_dp_map = self._find_transform_workers()
        log.debug('_start_transform_worker  transform_worker_dp_map:  %s', transform_worker_dp_map)
        if not transform_worker_dp_map:
            log.debug('create_data_process start first worker')
            #for the first data process in a TW, generate the exchange name
            transform_worker_pid, exchange_name = self._start_transform_worker()
        else:
            for worker_pid, dp_list in transform_worker_dp_map.iteritems():
                if len(dp_list) < self.TRANSFORM_WORKER_HOST_LIMIT:
                    # slots available on this worker, add the dp input stream to its queue
                    transform_worker_pid = worker_pid
                    # get the exchange name for a hosted data process
                    exchange_name = self._get_transform_worker_subscription_name(transform_worker_pid)

        #link the Process to the DataProcess that it is hosting
        self.clients.resource_registry.create_association(subject=data_process_id, predicate=PRED.hasProcess, object=transform_worker_pid)

        self.event_publisher.publish_event(origin=data_process_id, origin_type='DataProcess', status=DataProcessStatusType.NORMAL,
                               description='data process assigned to transform worker: ' + transform_worker_pid )

        return exchange_name

    def _initialize_retrieve_process(self, data_process_definition, in_data_product_ids, out_data_product_ids, configuration, argument_map, out_param_name):
        '''
        Initializes a retreive process

        1. Launch the process
        2. Create streams for the inputs to replay on
        3. Subscribe and activate
        4. Initiate replay
        '''

        configuration = configuration or DotDict()
        # Manage cases and misconfigurations
        if not in_data_product_ids:
            raise BadRequest('Input data products are required')

        data_product_id = in_data_product_ids[0]
        data_product = self.clients.data_product_management.read_data_product(data_product_id)

        data_process = DataProcess()
        data_process.name = '_on_'.join([data_process_definition.name, data_product.name])
        data_process.configuration = configuration
        data_process.argument_map = argument_map
        data_process.output_param = out_param_name

        data_process_id, _ = self.clients.resource_registry.create(data_process)

        # Create the associations
        for data_product_id in in_data_product_ids:
            self.clients.resource_registry.create_association(subject=data_process_id, predicate=PRED.hasInputProduct, object=data_product_id)
        for data_product_id in out_data_product_ids:
            self.clients.resource_registry.create_association(subject=data_process_id, predicate=PRED.hasOutputProduct, object=data_product_id)
        if data_process_definition._id:
            self.clients.resource_registry.create_association(data_process_definition._id, PRED.hasDataProcess ,data_process_id)

        # Launch the worker
        exchange_name = self._assign_worker(data_process_id)

        # Create streams and subscription for a replay
        self._create_replay(data_process_id, in_data_product_ids, exchange_name, configuration)
        return data_process_id


    def _create_replay(self, data_process_id, in_data_product_ids, exchange_name, configuration):
        streams = []
        replay_ids = []
        for data_product_id in in_data_product_ids:
            '''
            For each data product
                1. Create a stream for the replay
                2. Setup the query
                3. Define the replay
            '''
            data_product = self.clients.data_product_management.read_data_product(data_product_id)
            stream_defs, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=True)
            stream_def_id = stream_defs[0]
            # Create the stream for replay
            stream_id, stream_route = self.clients.pubsub_management.create_stream(
                    name='Replay stream for %s' % data_product.name,
                    exchange_point=configuration.get_safe('exchange_point','science_data'),
                    stream_definition_id=stream_def_id)

            # Associate the stream with the data process
            # We have to cleanup the streams when we're done with the data process
            self.clients.resource_registry.create_association(data_process_id, PRED.hasStream, stream_id)
            # Store the stream for the subscription
            streams.append(stream_id)

            # Get the dataset id for the replay definition
            dataset_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)
            if not dataset_ids:
                raise BadRequest("Data Product has no dataset")
            dataset_id = dataset_ids[0]

            # Setup the replay query
            query = {"start_time" : configuration.get_safe("start_time"),
                    "end_time" : configuration.get_safe("end_time"),
                    "parameters" : configuration.get_safe("parameters"),
                    "publish_limit": configuration.get_safe("publish_limit")}
            
            # Create the replay
            replay_id, pid = self.clients.data_retriever.define_replay(dataset_id, 
                    query=query, delivery_format=stream_def_id, stream_id=stream_id)
            replay_ids.append(replay_id)
            self.clients.data_retriever.start_replay_agent(replay_id)

            # Link the data process to this replay
            self.clients.resource_registry.create_association(data_process_id, PRED.hasReplay, replay_id)

            log.debug('data process _create_replay: %s. ', data_product_id)
            self.event_publisher.publish_event(origin=data_process_id, origin_type='DataProcess', status=DataProcessStatusType.NORMAL,
                                   description='data process replay created from data product: '+ data_product_id )

        subscription_id = self.clients.pubsub_management.create_subscription(name=exchange_name, 
                                                                             stream_ids=streams)
        self.clients.resource_registry.create_association(data_process_id, PRED.hasSubscription, object=subscription_id)
        return replay_ids




    #--------------------------------------------------------------------------------
    # Validation
    #--------------------------------------------------------------------------------
    def validate_argument_input(self, data_product_id='', argument_map=None, output_param=''):
        '''
        Returns true if the argument map or output parameter is valid for a particular data product
        '''
        if not ( argument_map or output_param):
            raise BadRequest("A valid arugment map or output parameter is required")
        if not data_product_id:
            raise BadRequest("A valid data product is required")

        parameters = self.parameters_for_data_product(data_product_id)
        parameter_names = parameters.keys()
        if argument_map:
            for argument_name in argument_map.itervalues():
                if argument_name not in parameter_names:
                    return False
        else:
            if output_param not in parameter_names:
                return False

        return True

    def parameters_for_data_product(self, data_product_id='', filtered=False):
        '''
        Returns a dict of parameter and parameter ids for a data product. Applies a
        filter if specified (using the stream def)
        '''

        stream_defs, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=False)
        if not stream_defs:
            raise BadRequest("No Stream Definition Found for data product %s" % data_product_id)
        stream_def = stream_defs[0]

        pdicts, _ = self.clients.resource_registry.find_objects(stream_def._id, PRED.hasParameterDictionary, id_only=True)
        if not pdicts:
            raise BadRequest("No Parameter Dictionary Found for data product %s" % data_product_id)
        pdict_id = pdicts[0]
        parameters, _ = self.clients.resource_registry.find_objects(pdict_id, PRED.hasParameterContext, id_only=False)

        # too complicated for one line of code
        #retval = { p.name : p._id for p in parameters if not filtered or (filtered and p in stream_def.available_fields) }
        retval = {}
        for p in parameters:
            if filtered and p.name in stream_def.available_fields:
                retval[p.name] = p._id
            elif not filtered:
                retval[p.name] = p._id
        return retval




    #--------------------------------------------------------------------------------
    # Transform Worker Management
    #--------------------------------------------------------------------------------


    def _start_transform_worker(self):

        def create_queue_name():
            import uuid
            id = str(uuid.uuid4())
            #import re
            #queue_name = re.sub(r'[ -]','_',id)
            queue_name = id.replace('-', '_')
            log.debug('_create_queue_name:  %s', queue_name)
            return queue_name

        log.debug('_start_transform_worker: ')
        config = DotDict()
        config.process.queue_name = create_queue_name()
        log.debug('_create_queue_name config  %s', config)

        #create the process definition
        process_definition = ProcessDefinition()
        process_definition.name = 'TransformWorker'
        process_definition.executable['module'] = 'ion.processes.data.transforms.transform_worker'
        process_definition.executable['class'] = 'TransformWorker'
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition)

        # Setting the restart mode
        schedule = ProcessSchedule()
        schedule.restart_mode = ProcessRestartMode.ALWAYS
        schedule.queueing_mode = ProcessQueueingMode.ALWAYS

        # Spawn the process
        pid = self.clients.process_dispatcher.schedule_process(
            process_definition_id=process_definition_id,
            schedule= schedule,
            configuration=config
        )
        log.debug('_create_queue_name pid  %s', pid)

        return pid, config.process.queue_name


    def _find_transform_workers(self):

        transform_worker_map = {}
        # returns a map of process resources that are transform workers and the list of data processes they are hosting
        # { transformworker1 : [ dataprocess1, dataprocess2] }

        # DataProcess hasProcess Process
        #    Subj        Pred       Object

        dataprocess_ids, _ = self.clients.resource_registry.find_resources(restype=RT.DataProcess, id_only=True)
        log.debug('_find_transform_workers data_process_ids: %s ', dataprocess_ids)

        #todo: look for Process resources that are assoc with DataProcess resources

        # now look for hasProcess associations to determine which Processes are TransformWorkers
        objects, associations = self.clients.resource_registry.find_objects_mult(subjects=dataprocess_ids)
        log.debug('have %d dataprocess-associated objects, %d are hasProcess', len(associations), sum([1 if assoc.p==PRED.hasProcess else 0 for assoc in associations]))
        for subject, assoc in zip(objects, associations):
            if assoc.p == PRED.hasProcess:
                if assoc.o in transform_worker_map:
                    transform_worker_map[assoc.o].append(assoc.s)
                else:
                    transform_worker_map[assoc.o] = [assoc.s]

        log.debug('find_transform_workers  transform_worker_map:  %s', transform_worker_map)
        return transform_worker_map



    def _get_transform_worker_subscription_name(self, transform_worker_pid=''):

        # returns the

        #todo: find a data process assoc (hosted by) this transform worker process and retrieve its subscription to get exchange name
        data_process_ids, _ = self.clients.resource_registry.find_subjects(subject_type=RT.DataProcess, predicate=PRED.hasProcess, object=transform_worker_pid, id_only=True)
        if not data_process_ids: raise BadRequest('No data processes associated with this transform worker process')

        subscription_ids, _ = self.clients.resource_registry.find_objects(subject=data_process_ids[0], predicate=PRED.hasSubscription, object_type=RT.Subscription, id_only=True)
        if not subscription_ids: raise BadRequest('No Subscription associated with this transform worker process')

        #todo: retrieve the subscrption exchange name

        subscription_obj = self.clients.pubsub_management.read_subscription(subscription_ids[0])
        log.debug('get_transform_worker_subscription_name subscription_obj: %s ', subscription_obj)

        return subscription_obj.exchange_name


    #--------------------------------------------------------------------------------
    # Inspection
    #--------------------------------------------------------------------------------

    def inspect_data_process_definition(self, data_process_definition_id=''):
        '''
        Returns the source code for the data process definition
        '''
        dpd = self.read_data_process_definition(data_process_definition_id)
        if dpd.data_process_type == DataProcessTypeEnum.PARAMETER_FUNCTION:
            return self._inspect_parameter_function(data_process_definition_id)
        if dpd.data_process_type == DataProcessTypeEnum.TRANSFORM_PROCESS:
            return self._inspect_transform_function(data_process_definition_id)
        #TODO: add the rest of the types
        else:
            raise BadRequest("Data process type not yet supported")

    def _inspect_parameter_function(self, data_process_definition_id):
        '''
        Returns the source code definition for the DPD's parameter function
        '''
        pfuncs, _ = self.clients.resource_registry.find_objects(data_process_definition_id, PRED.hasParameterFunction, id_only=False)
        if not pfuncs:
            raise BadRequest('Data Process Definition %s has no parameter functions' % data_process_definition_id)
        func = DatasetManagementService.get_coverage_function(pfuncs[0])
        if isinstance(func, PythonFunction):
            func._import_func()
            # Gets a list of lines of the source code
            src = inspect.getsourcelines(func._callable)[0]
            # Join the lines into one contiguous string
            src = ''.join(src)

        elif isinstance(func, NumexprFunction):
            src = func.expression

        else:
            raise BadRequest('%s is not supported for inspection' % type(func))

        return src


    def _inspect_transform_function(self, data_process_definition_id):
        '''
        Returns the source code definition for the DPD's transform function
        '''
        import importlib

        data_process_definition_obj = self.read_data_process_definition(data_process_definition_id)

        tfuncs, _ = self.clients.resource_registry.find_objects(data_process_definition_id, PRED.hasTransformFunction, id_only=False)
        if not tfuncs:
            raise BadRequest('Data Process Definition %s has no transform functions' % data_process_definition_id)
        tfunc_obj =  tfuncs[0]

        module = None
        #first try to load the module, if not avaialable, then import the egg
        try:
            module = importlib.import_module(tfunc_obj.module)
        except ImportError:
        #load the associated transform function
            import ion.processes.data.transforms.transform_worker.TransformWorker
            if tfunc_obj.uri:
                egg = TransformWorker.download_egg(tfunc_obj.uri)
                import pkg_resources
                pkg_resources.working_set.add_entry(egg)

                module = importlib.import_module(tfunc_obj.module)
            else:
                log.warning('No uri provided for module in data process definition.')

        function = getattr(module, tfunc_obj.function)
        # Gets a list of lines of the source code
        src = inspect.getsourcelines(function)[0]
        # Join the lines into one contiguous string
        src = ''.join(src)

        return src


    #--------------------------------------------------------------------------------
    # Parameter Function Support
    #--------------------------------------------------------------------------------

    def _initialize_parameter_function(self, data_process_definition_id, data_product_ids, param_map, out_param_name):
        '''
        For each data product, append a parameter according to the dpd

        1. Get a parameter context
        2. Load the data product's datasets and check if the name is in the dict
        3. Append parameter to all datasets
        4. Associate context with data products' parameter dicts
        '''
        dpd = self.read_data_process_definition(data_process_definition_id)
        ctxt, ctxt_id = self._get_param_ctx_from_dpd(data_process_definition_id, param_map, out_param_name)
        # For each data product check for and add the parameter
        data_process_ids = []
        for data_product_id in data_product_ids:
            self._check_data_product_for_parameter(data_product_id, out_param_name)
            self._add_parameter_to_data_products(ctxt_id, data_product_id)
            # Create a data process and we'll return all the ids we make
            dp_id = self._create_data_process_for_parameter_function(dpd, data_product_id, param_map)
            data_process_ids.append(dp_id)
        return data_process_ids

    def _get_param_ctx_from_dpd(self, data_process_definition_id, param_map, out_param_name):
        '''
        Creates a parameter context for the data process definition using the param_map
        '''
        # Get the parameter function associated with this data process definition
        parameter_functions, _ = self.clients.resource_registry.find_objects(data_process_definition_id, PRED.hasParameterFunction, id_only=False)
        if not parameter_functions:
            raise BadRequest("No associated parameter functions with data process definition %s" % data_process_definition_id)
        # Make a context specific to this data process
        pf = DatasetManagementService.get_coverage_function(parameter_functions[0])

        # Assign the parameter map
        if not param_map:
            raise BadRequest('A parameter map must be specified for ParameterFunctions')
        if not out_param_name:
            raise BadRequest('A parameter name is required')
        # TODO: Add sanitary check
        pf.param_map = param_map
        ctxt = ParameterContext(out_param_name, param_type=ParameterFunctionType(pf))
        ctxt_id = self.clients.dataset_management.create_parameter_context(out_param_name, ctxt.dump())
        return ctxt, ctxt_id

    def _check_data_product_for_parameter(self, data_product_id, param_name):
        '''
        Verifies that the named parameter does not exist in the data product already
        '''
        # Get the dataset for this data product
        # DataProduct -> Dataset [ hasDataset ]
        datasets, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=False)
        if not datasets:
            raise BadRequest("No associated dataset with this data product %s" % data_product_id)
        dataset = datasets[0]
        # Load it with the class ParameterDictionary so I can see if the name
        # is in the parameter dictionary
        pdict = ParameterDictionary.load(dataset.parameter_dictionary)
        if param_name in pdict:
            raise BadRequest("Named parameter %s already exists in this dataset: %s" % (param_name, dataset._id))

    def _add_parameter_to_data_products(self, parameter_context_id, data_product_id):
        '''
        Adds the parameter context to the data product
        '''
        # Get the dataset id
        dataset_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)
        dataset_id = dataset_ids[0]

        # Add the parameter
        self.clients.dataset_management.add_parameter_to_dataset(parameter_context_id, dataset_id)

    def _create_data_process_for_parameter_function(self, data_process_definition, data_product_id, param_map):
        '''
        Creates a data process to represent a parameter function on a data product
        '''
        # Get the data product object for the name
        data_product = self.clients.data_product_management.read_data_product(data_product_id)
        # There may or may not be a need to keep more information here
        dp = DataProcess()
        dp.name = 'Data Process %s for Data Product %s' % (data_process_definition.name, data_product.name)
        dp.argument_map = param_map
        # We make a data process
        dp_id, _ = self.clients.resource_registry.create(dp)
        self.clients.resource_registry.create_association(data_process_definition._id, PRED.hasDataProcess, dp_id)
        return dp_id




    def update_data_process(self):
        raise BadRequest('Cannot update an existing data process.')


    def read_data_process(self, data_process_id=''):
        data_proc_obj = self.clients.resource_registry.read(data_process_id)
        return data_proc_obj


    def read_data_process_for_stream(self, stream_id="", worker_process_id=""):
        dataprocess_details_list = []

        #get the data product assoc with this stream
        dataprocess_ids = []
        dataproduct_id = self._get_dataproduct_from_stream(stream_id)
        if dataproduct_id:
            dataprocess_ids.extend(self._get_dataprocess_from_input_product(dataproduct_id))
        
        replay_id = self._get_replay_from_stream(stream_id)
        if replay_id:
            dataprocess_ids.extend(self._get_data_process_from_replay(replay_id))


        #create a dict of information for each data process assoc with this stream is
        for dataprocess_id in dataprocess_ids:

            #only send the info required by the TW
            dataprocess_details = DotDict()

            dp_obj = self.read_data_process(dataprocess_id)
            dataprocess_details.dataprocess_id = dataprocess_id
            dataprocess_details.in_stream_id = stream_id

            out_stream_id, out_stream_route, out_stream_definition = self.load_out_stream_info(dataprocess_id)
            dataprocess_details.out_stream_id = out_stream_id
            dataprocess_details.out_stream_route = out_stream_route
            dataprocess_details.output_param = dp_obj.output_param
            dataprocess_details.out_stream_def = out_stream_definition

            dpd_obj, pfunction_obj = self.load_data_process_definition_and_transform(dataprocess_id)

            dataprocess_details.module = pfunction_obj.module
            dataprocess_details.function = pfunction_obj.function
            dataprocess_details.arguments = pfunction_obj.arguments
            dataprocess_details.argument_map=dp_obj.argument_map
            dataprocess_details.uri=pfunction_obj.uri

            log.debug('read_data_process_for_stream   dataprocess_details:  %s', dataprocess_details)
            dataprocess_details_list.append(dataprocess_details)

        return dataprocess_details_list

    def delete_data_process(self, data_process_id=""):

        data_process_definitions, _ = self.clients.resource_registry.find_subjects(object=data_process_id, predicate=PRED.hasDataProcess, subject_type=RT.DataProcessDefinition, id_only=False)
        dpd = data_process_definitions[0]

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

        #Remove assocs to the TransformWorker process which hosts this data process
        process_ids, assocs = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasProcess, object_type=RT.Process)
        for process_id, assoc in zip(process_ids, assocs):
            self.clients.resource_registry.delete_association(assoc)

        # Remove the streams associated with the data process if it's a retrieve process
        if dpd.data_process_type == DataProcessTypeEnum.RETRIEVE_PROCESS:
            stream_ids, _ = self.clients.find_objects(data_process_id, PRED.hasStream, id_only=True)
            for stream_id in stream_ids:
                self.clients.pubsub_management.delete_stream(stream_id)

        #Unregister the data process with acquisition
        #todo update when Data acquisition/Data Producer is ready
        self.clients.data_acquisition_management.unregister_process(data_process_id=data_process_id)

        #Delete the data process from the resource registry
        self.RR2.lcs_delete(data_process_id, RT.DataProcess)

    def force_delete_data_process(self, data_process_id=""):

        # if not yet deleted, the first execute delete logic
        dp_obj = self.read_data_process(data_process_id)
        if dp_obj.lcstate != LCS.DELETED:
            self.delete_data_process(data_process_id)

        self.RR2.force_delete(data_process_id, RT.DataProcess)

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

        #used for DataProcessTypeEnum.RETRIEVE_PROCESS to activate the replay.
        #for other DataProcess types this is a no-op

         # Start the replays
         replays, _ = self.clients.resource_registry.find_objects(data_process_id, PRED.hasReplay, id_only=False)
         # replays only get associated if it's a RETRIEVE_PROCESS, so I don't need to make that check
         for replay in replays:
             process_id = replay.process_id
             replay_client = ReplayClient(process_id)
             replay_client.start_replay()
         return True


    def deactivate_data_process(self, data_process_id=''):
        # Stop the replays
        replays, _ = self.clients.resource_registry.find_objects(data_process_id, PRED.hasReplay, id_only=False)
        # replays only get associated if it's a RETRIEVE_PROCESS, so I don't need to make that check
        for replay in replays:
            process_id = replay.process_id
            replay_client = ReplayClient(process_id)
            replay_client.stop_replay()
        #@todo: data process producer context stuff
        subscription_ids, assocs = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasSubscription, id_only=True)
        for subscription_id in subscription_ids:
            if self.clients.pubsub_management.subscription_is_active(subscription_id):
                self.clients.pubsub_management.deactivate_subscription(subscription_id)

        return True

    def _get_stream_from_dataproduct(self, dataproduct_id):
        stream_ids, _ = self.clients.resource_registry.find_objects(subject=dataproduct_id, predicate=PRED.hasStream, id_only=True)
        if not stream_ids: raise BadRequest('No streams associated with this data product')
        return stream_ids[0]

    def _get_dataproduct_from_stream(self, stream_id):
        data_product_ids, _ = self.clients.resource_registry.find_subjects(subject_type=RT.DataProduct, predicate=PRED.hasStream, object=stream_id, id_only=True)
        if not data_product_ids: return None
        return data_product_ids[0]


    def _get_dataprocess_from_input_product(self, data_product_id):
        data_process_ids, _ = self.clients.resource_registry.find_subjects(subject_type=RT.DataProcess, predicate=PRED.hasInputProduct, object=data_product_id, id_only=True)
        if not data_process_ids: raise BadRequest('No data processes associated with this input product')
        log.debug('_get_dataprocess_from_input_product data_process_ids: %s ', data_process_ids)
        return data_process_ids

    def _get_replay_from_stream(self, stream_id):
        replays, _ = self.clients.resource_registry.find_subjects(object=stream_id, predicate=PRED.hasStream, subject_type=RT.Replay, id_only=True)
        if not replays: return None
        return replays[0]

    def _get_data_process_from_replay(self, replay_id):
        data_process_ids, _ = self.clients.resource_registry.find_subjects(object=replay_id, predicate=PRED.hasReplay, subject_type=RT.DataProcess, id_only=True)
        return data_process_ids

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
                in_stream_id = self._get_stream_from_dataproduct(in_data_product_id)
                out_stream_id = self._get_stream_from_dataproduct(out_data_product_id)
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

            if not self._get_stream_from_dataproduct(data_product_id):
                raise BadRequest('No Stream was found for this DataProduct')


    def _manage_attachments(self):
        pass

    def _create_subscription(self, dproc, in_data_product_ids=None, queue_name=None):

        if not queue_name:
            queue_name = 'sub_%s' % dproc.name

        stream_ids = [self._get_stream_from_dataproduct(i) for i in in_data_product_ids]

        #@TODO Maybe associate a data process with an exchange point but in the mean time:

        subscription_id = self.clients.pubsub_management.create_subscription(name=queue_name, stream_ids=stream_ids)
        log.debug('_create_subscription subscription_id: %s', subscription_id)
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


    def _validator(self, in_data_product_id, out_data_product_id):
        in_stream_id = self._get_stream_from_dataproduct(dataproduct_id=in_data_product_id)
        in_stream_defs, _ = self.clients.resource_registry.find_objects(subject=in_stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
        if not len(in_stream_defs):
            raise BadRequest('No valid stream definition defined for data product stream')
        out_stream_id = self._get_stream_from_dataproduct(dataproduct_id=out_data_product_id)
        out_stream_defs, _  = self.clients.resource_registry.find_objects(subject=out_stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
        if not len(out_stream_defs):
            raise BadRequest('No valid stream definition defined for data product stream')
        return self.clients.pubsub_management.compatible_stream_definitions(in_stream_definition_id=in_stream_defs[0], out_stream_definition_id=out_stream_defs[0])



    def stream_def_from_data_product(self, data_product_id=''):
        stream_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        validate_true(stream_ids, 'No stream found for this data product: %s' % data_product_id)
        stream_id = stream_ids.pop()
        stream_def_ids, _ = self.clients.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
        validate_true(stream_def_ids, 'No stream definition found for this stream: %s' % stream_def_ids)
        stream_def_id = stream_def_ids.pop()
        return stream_def_id


    def load_out_stream_info(self, data_process_id=None):

        #get the input stream id
        out_stream_id = ''
        out_stream_route = ''
        out_stream_definition = ''

        out_dataprods_objs, _ = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasOutputProduct, object_type=RT.DataProduct, id_only=False)
        if len(out_dataprods_objs) != 1:
            log.exception('The data process is not correctly associated with a ParameterFunction resource.')
        else:
            out_stream_definition = self.stream_def_from_data_product(out_dataprods_objs[0]._id)
            stream_ids, assoc_ids = self.clients.resource_registry.find_objects(out_dataprods_objs[0], PRED.hasStream, RT.Stream, True)
            out_stream_id = stream_ids[0]

            self.pubsub_client = PubsubManagementServiceClient()

            out_stream_route = self.pubsub_client.read_stream_route(out_stream_id)

        return out_stream_id, out_stream_route, out_stream_definition


    def load_data_process_definition_and_transform(self, data_process_id=None):

        data_process_def_obj = None
        transform_function_obj = None

        dpd_objs, _ = self.clients.resource_registry.find_subjects(subject_type=RT.DataProcessDefinition, predicate=PRED.hasDataProcess, object=data_process_id, id_only=False)

        if len(dpd_objs) != 1:
            log.exception('The data process not correctly associated with a Data Process Definition resource.')
        else:
            data_process_def_obj = dpd_objs[0]

        if data_process_def_obj.data_process_type in (DataProcessTypeEnum.TRANSFORM_PROCESS, DataProcessTypeEnum.RETRIEVE_PROCESS):

            tfunc_objs, _ = self.clients.resource_registry.find_objects(subject=data_process_def_obj, predicate=PRED.hasTransformFunction, id_only=False)

            if len(tfunc_objs) != 1:
                log.exception('The data process definition for a data process is not correctly associated with a TransformFunction resource.')
            else:
                transform_function_obj = tfunc_objs[0]


        return data_process_def_obj, transform_function_obj




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
