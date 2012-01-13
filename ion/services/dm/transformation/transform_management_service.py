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
from ion.services.dm.transformation.example.transform_example import TransformExample
from ion.services.dm.distribution.pubsub_management_service import PubsubManagementService
from interface.services.dm.itransform_management_service import BaseTransformManagementService

class TransformManagementService(BaseTransformManagementService):
    """Provides the main orchestration for stream processing
    subscription, data process definition and computation
    request (scheduling). The transformation service handles content format
    transformation, mediation, qualification, verification and validation
    """
    def __init__(self):
        BaseTransformManagementService.__init__(self)
        # Mocking interfaces

        #@todo: un-mock
        # PubSub
        #self.clients.pubsub_management = DotDict()
        #self.clients.pubsub_management['XP'] = 'science.data'
        #self.clients.pubsub_management["create_subscription"] = Mock()
        #self.clients.pubsub_management.create_subscription.return_value = 'subscription_id'
        #self.clients.pubsub_management["register_producer"] = Mock()
        #self.clients.pubsub_management.register_producer.return_value = {'StreamRoute':'Mocked'}
        #self.clients.pubsub_management["create_stream"] = Mock()
        #self.clients.pubsub_management.create_stream.return_value = 'unique_stream_id'
        #self.clients.pubsub_management['activate_subscription'] = Mock()
        #self.clients.pubsub_management.activate_subscription.return_value = True
        # ProcessDispatcher
        self.clients.process_dispatcher_service = DotDict()
        self.clients.process_dispatcher_service["create_process_definition"] = Mock()
        self.clients.process_dispatcher_service.create_process_definition.return_value = 'process_definition_id'
        self.clients.process_dispatcher_service["schedule_process"] = Mock()
        self.clients.process_dispatcher_service.schedule_process.return_value = True
        self.clients.process_dispatcher_service["cancel_process"] = Mock()
        self.clients.process_dispatcher_service.cancel_process.return_value = True
        self.clients.process_dispatcher_service["delete_process_definition"] = Mock()
        self.clients.process_dispatcher_service.delete_process_definition.return_value = True



    def create_transform(self,in_subscription_id='', out_stream_id='', process_definition_id='', configuration={}):
        """Creates the transform and registers it with the resource registry
        @param process_definition_id The ProcessDefinition that holds the configuration for the process to be launched
        @param in_subscription_id The subscription id corresponding to the input subscription
        @param out_stream_id The stream id for the output
        @param configuration { name: Name of the transform process, module: module, class: class name }

        @return The transform_id to the transform
        """
        #@todo: fix this

        transform_name=configuration['name']


        schedule = IonObject(RT.ProcessSchedule, name=transform_name+'_schedule')

        #@todo: fill in process schedule stuff
        # cei/process.yml
        # --
        # schedule.activation_mode =''
        # schedule.schedule = {}

        pid = self.clients.process_dispatcher_service.schedule_process(process_definition_id, schedule)
        transform_res = IonObject(RT.Transform,name=transform_name)
        transform_res.process_id = pid




        transform_id, _ = self.clients.resource_registry.create(transform_res)
        self.clients.resource_registry.create_association(transform_id,AT.hasProcessDefinition,process_definition_id)
        self.clients.resource_registry.create_association(transform_id,AT.hasSubscription,in_subscription_id)
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

        log.debug('Reading Transform: %s' % transform_id)
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
        out_stream_ids, _ = self.clients.resource_registry.find_objects(transform_id, AT.hasOutStream, RT.Stream, True)

        # build a list of all the ids above
        id_list = process_definition_ids + in_subscription_ids + out_stream_ids





        # stop the transform process
        self.clients.process_dispatcher_service.cancel_process(process_id=pid)

        # delete the associations
        for predicate in [AT.hasProcessDefinition, AT.hasSubscription, AT.hasOutStream]:
            associations = self.clients.resource_registry.find_associations(transform_id,predicate)
            for association in associations:
                self.clients.resource_registry.delete_association(association)


        #@todo: should I delete the resources, or should dpms?
        # iterate through the list and delete each
        for res_id in id_list:
            self.clients.resource_registry.delete(res_id)



        self.clients.resource_registry.delete(transform_res)
        return True





    def bind_transform(self, transform_id=''):
        """Activate the subscription to bind (start) the transform
        @param transform_id

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



