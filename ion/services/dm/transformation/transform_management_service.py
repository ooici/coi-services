#!/usr/bin/env python
__license__ = 'Apache 2.0'
'''
@author Maurice Manning
@author Luke Campbell
@file ion/services/dm/transformation/transform_management_service.py
@description Implementation for TransformManagementService
'''
import time
from mock import Mock
from pyon.public import log, IonObject, RT, AT
from pyon.core.exception import BadRequest, NotFound
from pyon.util.containers import DotDict
from interface.services.dm.itransform_management_service import BaseTransformManagementService

class TransformManagementService(BaseTransformManagementService):
    """Provides the main orchestration for stream processing
    subscription, data process definition and computation
    request (scheduling). The transformation service handles content format
    transformation, mediation, qualification, verification and validation
    """
    def __init__(self):
        BaseTransformManagementService.__init__(self)



    def create_transform(self,in_subscription_id='', out_stream_id='', process_definition_id='', configuration={}):
        """Creates the transform and registers it with the resource registry
        @param process_definition_id The process defintion contains the module and class of the process to be spawned
        @param in_subscription_id The subscription id corresponding to the input subscription
        @param out_stream_id The stream id for the output
        @param configuration {'process': {'name' : <name>, 'type': <process_type>, 'listen_name': <exchange_name> }}

        @return The transform_id to the transform
        """

        # ------------------------------------------------------------------------------------
        # Configuration and Set Up
        # ------------------------------------------------------------------------------------
        # Determine Transform Name
        if not configuration:
            configuration = {}
        transform_name=configuration.get('name',None) or configuration.get('process',{}).get('name','transform')

        #@todo: fill in process schedule stuff (CEI->Process Dispatcher)
        #@note: In the near future, Process Dispatcher will do all of this
       
        # Get the Module and Class from the Process Definition Object
        # Default to ion.services.dm.transformation.example.transform_example, TransformExample

        # A process definition isn't required but it is preferred
        # If one doesn't exist, create it.
        if process_definition_id:
            process_definition = self.clients.resource_registry.read(process_definition_id)
        else:
            process_definition = IonObject(RT.ProcessDefinition,name='%s_definition' % transform_name)
            process_definition.executable['module'] = 'ion.services.dm.transformation.example.transform_example'
            process_definition.executable['class'] = 'TransformExample'



        module = process_definition.executable.get('module','ion.services.dm.transformation.example.transform_example')
        cls = process_definition.executable.get('class','TransformExample')

        # Transform Resource for association management and pid
        transform_res = IonObject(RT.Transform,name=transform_name)
        
        # ------------------------------------------------------------------------------------
        # Spawn Configuration and Parameters
        # ------------------------------------------------------------------------------------
       
        # If listen name wasn't passed in with config, determine it through subscription
        listen_name = configuration.get('process',{}).get('listen_name',None) or \
                configuration.get('process',{}).get('exchange_name',None)

        if not listen_name:
            subscription = self.clients.pubsub_management.read_subscription(subscription_id=in_subscription_id)
            listen_name = subscription.exchange_name

        if not configuration:
            configuration= {'process':{'name':transform_name,'type':"stream_process",'listen_name':listen_name}}

        if out_stream_id:
            configuration['process']['publish_streams'] = {'out_stream' : out_stream_id}


        # Update the resource_registry with this process_definition's configuration



        process_definition.config = configuration
        if not process_definition_id:
            process_definition_id, _ = self.clients.resource_registry.create(process_definition)
        else:
            self.clients.resource_registry.update(process_definition)

        # ------------------------------------------------------------------------------------
        # Process Spawning
        # ------------------------------------------------------------------------------------


        # Spawn the process
        pid = self.container.spawn_process(name=transform_name,
                        module=module,
                        cls=cls,
                        config=configuration)

        transform_res.process_id = '%s.%s' % (str(self.container.id), str(pid))
        
        # ------------------------------------------------------------------------------------
        # Handle Resources
        # ------------------------------------------------------------------------------------
        transform_id, _ = self.clients.resource_registry.create(transform_res)


        self.clients.resource_registry.create_association(transform_id,AT.hasProcessDefinition,process_definition_id)
        self.clients.resource_registry.create_association(transform_id,AT.hasSubscription,in_subscription_id)

        # Output stream is not necessary for a transform process
        if out_stream_id:
            self.clients.resource_registry.create_association(transform_id,AT.hasOutStream, out_stream_id)

        return transform_id



    def update_transform(self, configuration={}):
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
                                AT.hasProcessDefinition, RT.ProcessDefinition, True)
        in_subscription_ids, _ = self.clients.resource_registry.find_objects(transform_id,
                                AT.hasSubscription, RT.Subscription, True)
        out_stream_ids, _ = self.clients.resource_registry.find_objects(transform_id,
                                AT.hasOutStream, RT.Stream, True)

        # build a list of all the ids above
        id_list = process_definition_ids + in_subscription_ids + out_stream_ids

        # stop the transform process

        #@note: terminate_process does not raise or confirm if there termination was successful or not
        self.container.proc_manager.terminate_process(pid)
        log.debug('(%s): Terminated Process (%s)' % (self.name,pid))


        # delete the associations
        for predicate in [AT.hasProcessDefinition, AT.hasSubscription, AT.hasOutStream]:
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

    def activate_transform(self, transform_id=''):
        """Activate the subscription to bind (start) the transform
        @param transform_id
        @retval True on success
        @throws NotFound if either the subscription doesn't exist or the transform object doesn't exist.
        """
        subscription_ids, _ = self.clients.resource_registry.find_objects(transform_id,
                                                            AT.hasSubscription, RT.Subscription, True)
        if len(subscription_ids) < 1:
            raise NotFound

        self.clients.pubsub_management.activate_subscription(subscription_ids[0])
        return True





    def schedule_transform(self, transform_id=''):
        """Not currently implemented
        @throws NotImplementedError
        """
        raise NotImplementedError



