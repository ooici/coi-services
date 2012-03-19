
'''
@author Luke Campbell
@file ion/services/dm/transformation/transform_management_service.py
@description Implementation for TransformManagementService
'''
from pyon.public import log, RT, PRED
from pyon.core.exception import BadRequest, NotFound
from pyon.core.object import IonObjectSerializer, IonObjectBase
from interface.services.dm.itransform_management_service import BaseTransformManagementService
import gevent
from interface.objects import Transform

class TransformManagementService(BaseTransformManagementService):
    """Provides the main orchestration for stream processing
    subscription, data process definition and computation
    request (scheduling). The transformation service handles content format
    transformation, mediation, qualification, verification and validation
    """
    def __init__(self):
        BaseTransformManagementService.__init__(self)

        self.serializer = IonObjectSerializer()

    def on_start(self):
        super(TransformManagementService,self).on_start()
        restart_flag = self.CFG.get_safe('process.restart', False)
        if restart_flag:
            transform_ids, meta = self.clients.resource_registry.find_resources(restype=RT.Transform, id_only=True)
            for transform_id in transform_ids:
                self._restart_transform(transform_id)

    def _restart_transform(self, transform_id):
        transform = self.clients.resource_registry.read(transform_id)
        configuration = transform.configuration
        proc_def_ids,other = self.clients.resource_registry.find_objects(subject=transform_id,predicate=PRED.hasProcessDefinition,id_only=True)

        if len(proc_def_ids) < 1:
            log.warning('Transform did not have a correct process definition.')
            return

        self.clients.process_dispatcher.schedule_process(
            process_definition_id=proc_def_ids[0],
            configuration=configuration
        )




    def _strip_types(self, obj):
        if not isinstance(obj, dict):
            return
        for k,v in obj.iteritems():
            if isinstance(v,dict):
                self._strip_types(v)
        if "type_" in obj:
            del obj['type_']


    def create_transform(self,
                         name='',
                         description='',
                         in_subscription_id='',
                         out_streams=None,
                         process_definition_id='',
                         configuration=None):

        """Creates the transform and registers it with the resource registry
        @param process_definition_id The process definition contains the module and class of the process to be spawned
        @param in_subscription_id The subscription id corresponding to the input subscription
        @param out_stream_id The stream id for the output
        @param configuration {}

        @return The transform_id to the transform
        """

        # ------------------------------------------------------------------------------------
        # Resources and Initial Configs
        # ------------------------------------------------------------------------------------
        # Determine Transform Name

        if isinstance(configuration, IonObjectBase):
            configuration = self.serializer.serialize(configuration)
            # strip the type
            self._strip_types(configuration)


        elif not configuration:
            configuration = {}

        # Handle the name uniqueness factor
        res, _ = self.clients.resource_registry.find_resources(name=name, id_only=True)
        if len(res)>0:
            raise BadRequest('The transform resource with name: %s, already exists.' % name)

        transform_name=name

        if not process_definition_id:
            raise NotFound('No process definition was provided')


        # Transform Resource for association management and pid
        transform_res = Transform(name=name, description=description)
        
        # ------------------------------------------------------------------------------------
        # Spawn Configuration and Parameters
        # ------------------------------------------------------------------------------------

        subscription = self.clients.pubsub_management.read_subscription(subscription_id = in_subscription_id)
        listen_name = subscription.exchange_name


        configuration['process'] = dict({
            'name':transform_name,
            'type':'stream_process',
            'listen_name':listen_name
        }, **configuration)
        if out_streams:
            configuration['process']['publish_streams'] = out_streams
            stream_ids = list(v for k,v in out_streams.iteritems())
        else:
            stream_ids = []
        transform_res.configuration = configuration


        # ------------------------------------------------------------------------------------
        # Process Spawning
        # ------------------------------------------------------------------------------------
        # Spawn the process
        pid = self.clients.process_dispatcher.schedule_process(
            process_definition_id=process_definition_id,
            configuration=configuration
        )
        transform_res.process_id =  pid
        
        # ------------------------------------------------------------------------------------
        # Handle Resources
        # ------------------------------------------------------------------------------------
        transform_id, _ = self.clients.resource_registry.create(transform_res)


        self.clients.resource_registry.create_association(transform_id,PRED.hasProcessDefinition,process_definition_id)
        self.clients.resource_registry.create_association(transform_id,PRED.hasSubscription,in_subscription_id)


        for stream_id in stream_ids:
            self.clients.resource_registry.create_association(transform_id,PRED.hasOutStream,stream_id)

        return transform_id



    def update_transform(self, configuration=None):
        """Not currently possible to update a transform
        @throws NotImplementedError
        """
        raise NotImplementedError


    def read_transform(self, transform_id=''):
        """Reads a transform from the resource registry
        @param transform_id The unique transform identifier
        @return Transform resource
        @throws NotFound when transform doesn't exist
        """

        log.debug('(%s): Reading Transform: %s' % (self.name,transform_id))
        transform = self.clients.resource_registry.read(object_id=transform_id,rev_id='')
        return transform
        

    def delete_transform(self, transform_id=''):
        """Deletes and stops an existing transform process
        @param transform_id The unique transform identifier
        @throws NotFound when a transform doesn't exist
        """

        # get the transform resource (also verifies it's existence before continuing)
        transform_res = self.read_transform(transform_id=transform_id)
        pid = transform_res.process_id

        # get the resources
        process_definition_ids, _ = self.clients.resource_registry.find_objects(transform_id,
                                PRED.hasProcessDefinition, RT.ProcessDefinition, True)
        in_subscription_ids, _ = self.clients.resource_registry.find_objects(transform_id,
                                PRED.hasSubscription, RT.Subscription, True)
        out_stream_ids, _ = self.clients.resource_registry.find_objects(transform_id,
                                PRED.hasOutStream, RT.Stream, True)

        # build a list of all the ids above
        id_list = process_definition_ids + in_subscription_ids + out_stream_ids

        # stop the transform process

        #@note: terminate_process does not raise or confirm if there termination was successful or not

        self.clients.process_dispatcher.cancel_process(pid)

        log.debug('(%s): Terminated Process (%s)' % (self.name,pid))


        # delete the associations
        for predicate in [PRED.hasProcessDefinition, PRED.hasSubscription, PRED.hasOutStream]:
            associations = self.clients.resource_registry.find_associations(transform_id,predicate)
            for association in associations:
                self.clients.resource_registry.delete_association(association)


        #@todo: should I delete the resources, or should dpms?

        # iterate through the list and delete each
        #for res_id in id_list:
        #    self.clients.resource_registry.delete(res_id)

        self.clients.resource_registry.delete(transform_id)
        return True



# ---------------------------------------------------------------------------

    def execute_transform(self, process_definition_id='', data=None, configuration=None):
        process_definition = self.clients.process_dispatcher.read_process_definition(process_definition_id)
        module = process_definition.executable.get('module')
        cls = process_definition.executable.get('class')



        module = __import__(module, fromlist=[cls])
        cls = getattr(module,cls)
        instance = cls()

        result = gevent.event.AsyncResult()
        def execute(data):
            result.set(instance.execute(data))

        g = gevent.greenlet.Greenlet(execute, data)
        g.start()

        retval = result.get(timeout=10)


        return retval


    def activate_transform(self, transform_id=''):
        """Activate the subscription to bind (start) the transform
        @param transform_id
        @retval True on success
        @throws NotFound if either the subscription doesn't exist or the transform object doesn't exist.
        """
        subscription_ids, _ = self.clients.resource_registry.find_objects(transform_id,
                                                            PRED.hasSubscription, RT.Subscription, True)
        if len(subscription_ids) < 1:
            raise NotFound

        for subscription_id in subscription_ids:
            self.clients.pubsub_management.activate_subscription(subscription_id)


        return True

    def deactivate_transform(self, transform_id=''):
        """Decativates the subscriptions for the specified transform
        @param transform_id
        @retval True on success
        @throws NotFound if either the subscription doesn't exist or the transform object doesn't exist
        """
        subscription_ids, _ = self.clients.resource_registry.find_objects(transform_id,
                                                            PRED.hasSubscription, RT.Subscription, True)
        if len(subscription_ids) < 1:
            raise NotFound

        for subscription_id in subscription_ids:
            self.clients.pubsub_management.deactivate_subscription(subscription_id)

        return True



    def schedule_transform(self, transform_id=''):
        """Not currently implemented
        @throws NotImplementedError
        """
        raise NotImplementedError



