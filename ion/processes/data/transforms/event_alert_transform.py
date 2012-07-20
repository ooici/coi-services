#!/usr/bin/env python

'''
@brief The EventAlertTransform listens to events and publishes alert messages when the events
        satisfy a condition. Its uses an algorithm to check the latter
@author Swarbhanu Chatterjee
'''

from pyon.util.log import log
from ion.processes.data.transforms.transform import TransformEventListener, TransformEventPublisher
from ion.processes.data.transforms.transform import TransformAlgorithm
from interface.objects import ProcessDefinition
from ion.services.dm.utility.query_language import QueryLanguage

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient



class EventAlertTransform(TransformEventListener, TransformEventPublisher):

    def on_start(self):
        log.warn('TransformDataProcess.on_start()')

        self.process_dispatcher = ProcessDispatcherServiceClient()

        query_statement = self.CFG.get_safe('process.query_statement', '')
        event_type = self.CFG.get_safe('process.event_type', '')
        event_origin = self.CFG.get_safe('process.event_origin', '')
        event_origin_type = self.CFG.get_safe('process.event_origin_type', '')
        event_subtype = self.CFG.get_safe('process.event_subtype', '')


        # pass in the configs to the listener

        # config to the listener (event types etc and the algorithm)

        #-------------------------------------------------------------------------------------
        # Create a transform event listener
        #-------------------------------------------------------------------------------------

        # Create an algorithm object

        algorithm = TransformAlgorithm(statement=query_statement)

        # The configuration for the listener
        configuration_listener = {
                                    'process':{
                                                'algorithm': algorithm,
                                                'event_type': event_type,
                                                'event_origin': event_origin,
                                                'event_origin_type': event_origin_type,
                                                'event_subtype': event_subtype
                                        }
                                }
        # Create the process
        pid = self.create_process(  name= 'transform_event_listener',
                                    module='ion.processes.data.transforms.transform',
                                    class_name='TransformEventListener',
                                    configuration= configuration_listener)


        #-------------------------------------------------------------------------------------
        # Create a transform event publisher
        #-------------------------------------------------------------------------------------

        pid = self.create_process(  name= 'transform_event_publisher',
                                    module='ion.processes.data.transforms.transform',
                                    class_name='TransformEventPublisher')



    def create_process(self, name= '', module = '', class_name = '', configuration = None):
        '''
        A helper method to create a process
        '''

        producer_definition = ProcessDefinition(name=name)
        producer_definition.executable = {
            'module':module,
            'class': class_name
        }

        procdef_id = self.process_dispatcher.create_process_definition(process_definition=producer_definition)
        pid = self.process_dispatcher.schedule_process(process_definition_id= procdef_id, configuration=configuration)

        return pid

class Operation(object):
    '''
    Apply a user provided operator on a set of fields and return the result.
    This is meant to be an object that can be fed to an Algorithm object, which in turn will
    check whether the result is consistent with its own query dict obtained by parsing a query statement.
    '''

    def __init__(self, operator = ''):
        self.operator = operator

    def execute(self, fields = None):

        # apply the operator on the fields
        result = ''

        return result

class AlgorithmA(object):
    '''
    This is meant to be flexible, accept a query statement and return True/False.
    '''
    def __init__(self, statement = '', fields = None, operator = ''):
        self.ql = QueryLanguage()
        self.statement = statement
        self.fields = fields

        self.operation = Operation(operator=operator)

    def execute(self, fields = None):

        query_dict = self.ql.parse(self.statement)

        result = self.operation.execute(self.fields)

        match = self.evaluate_condition(result, query_dict)

        return match

    def evaluate_condition(self, result = '', query_dict = None):

        # if result matches the query dict return True

        # else return False

        pass






