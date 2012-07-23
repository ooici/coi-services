#!/usr/bin/env python

'''
@brief The EventAlertTransform listens to events and publishes alert messages when the events
        satisfy a condition. Its uses an algorithm to check the latter
@author Swarbhanu Chatterjee
'''
from pyon.ion.transforma import TransformEventListener, TransformStreamListener, TransformAlgorithm
from pyon.util.log import log
from ion.services.dm.utility.query_language import QueryLanguage
from pyon.core.exception import BadRequest
from pyon.event.event import EventPublisher

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
import operator

class EventAlertTransform(TransformEventListener):

    def on_start(self):
        log.warn('EventAlertTransform.on_start()')
        super(EventAlertTransform, self).on_start()

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # get the algorithm to use
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        algorithm_id = self.CFG.get_safe('process.algorithm_id', '')
        self.event_count = self.CFG.get_safe('process.event_count', 1)
        self.count = 0

        rr = ResourceRegistryServiceClient()
        algorithm = rr.read(algorithm_id)

        self.transform_algorithm = TransformAlgorithmExample(algorithm = algorithm)

        #-------------------------------------------------------------------------------------
        # Create the publisher that will publish the Alert message
        #-------------------------------------------------------------------------------------

        self.event_publisher = EventPublisher()

    def process_event(self, msg, headers):
        '''
        The callback method.
        If the events satisfy the criteria supplied through the algorithm object, publish an alert event.
        '''

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Get the list of relevant field values of the event
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        fields = []
        self.count += 1
        for field_name in self.transform_algorithm.algorithm.field_names and (self.count == self.event_count):
            fields.append(getattr(msg, field_name))
            self.count = 0

        log.warning("in process_event, got the following fields: %s" % fields)

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Apply the algorithm and if criteria check out, publish an alert event
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        if self.transform_algorithm.process(fields):
            self.publish()

    def publish(self):

        # publish an alert event
        self.event_publisher.publish_event( event_type= "DeviceEvent",
                                            origin="EventAlertTransform",
                                            description= "An alert event being published.")


class StreamAlertTransform(TransformStreamListener):

    def on_start(self):
        log.warn('StreamAlertTransform.on_start()')

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # get the algorithm to use
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        algorithm_id = self.CFG.get_safe('process.algorithm_id', None)

        rr = ResourceRegistryServiceClient()
        algorithm = rr.read(algorithm_id)

        rr = ResourceRegistryServiceClient()
        algorithm = rr.read(algorithm_id)

        self.transform_algorithm = TransformAlgorithmExample(algorithm = algorithm)

        #-------------------------------------------------------------------------------------
        # Create the publisher that will publish the Alert message
        #-------------------------------------------------------------------------------------

        self.event_publisher = EventPublisher()

    def recv_packet(self, msg, headers):
        '''
        The callback method.
        If the events satisfy the criteria supplied through the algorithm object, publish an alert event.
        '''

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Get the list of relevant field values of the event
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        fields = self._extract_parameters_from_stream(self.transform_algorithm.field_names)

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Apply the algorithm and if criteria check out, publish an alert event
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        if self.transform_algorithm.process(fields):
            self.publish()

    def publish(self):

        # publish an alert event
        self.event_publisher.publish_event( event_type= "DeviceEvent",
                                            origin="StreamAlertTransform",
                                            description= "An alert event being published.")

    def _extract_parameters_from_stream(self, field_names ):

        fields = []

        #todo implement this method according to use cases

#        for field_name in field_names:
#            fields.append(  )

        log.warning("in stream alert recv_packet method, got the following fields: %s" % fields)

        return fields

class Operation(object):
    '''
    Apply a user provided operator on a set of fields and return the result.
    This is meant to be an object that can be fed to an Algorithm object, which in turn will
    check whether the result is consistent with its own query dict obtained by parsing a query statement.
    '''

    operators = {   "+"  :  operator.add,
                    "-"  :  operator.sub,
                    "*"  :  operator.mul,
                    "/"  :  operator.div
                }


    def __init__(self, _operator = '+', _operator_list = None):

        #-------------------------------------------------------------------------------------
        # A simple operator
        #-------------------------------------------------------------------------------------
        self._operator = _operator

        #-------------------------------------------------------------------------------------
        # instead of a simple single operator, one could provide a list of operators,
        # Ex: ['+', '-', '+', '*'] ==> a + b - c + d * e
        #-------------------------------------------------------------------------------------

        self._operator_list = _operator_list

    def define_subtraction_constant(self, const):
        '''
        If we want to do a successive subtraction operation, it may be relevant to have a starting point unequal to 0.
        For example, subtract all field values from, let's say, 100.
        '''
        self.const = const

    def _initialize_the_result(self):

        # apply the operator on the fields
        if self._operator_list:
            result = 0
        elif self._operator == '+':
            result = 0
        elif  self._operator == '-':
            if self.const:
                result = self.const
            else: result = 0
        elif self._operator == '*' or '/':
            result = 1
        else:
            raise NotImplementedError("Unimplemented operator: %s" % self._operator)

        return result

    def operate(self, fields = None):

        #todo this method is most useful for additions

        #-------------------------------------------------------------------------------------
        # Initialize the result
        #-------------------------------------------------------------------------------------

        result = self._initialize_the_result()

        #-------------------------------------------------------------------------------------
        # Apply the operation
        #-------------------------------------------------------------------------------------

        if not self._operator_list: # if operator list is not provided, apply the simple SINGLE operator
            for field in fields:
                # get the Python operator
                self.operator =  Operation.operators[self._operator]
                result = self.operator(result, field)
        else: # apply operator list
            count = 0
            for field in fields:
                operator = Operation.operators[self._operator_list][count]
                result = operator(result, field)

        return result


class TransformAlgorithmExample(TransformAlgorithm):
    '''
    This is meant to be flexible, accept a query statement and return True/False.

    To use this object:

        algorithm = AlgorithmA( statement = "search 'result' is '5' from 'dummy_index' and SEARCH 'result' VALUES FROM 10 TO 20 FROM 'dummy_index",
                                fields = [1,20,3,10],
                                _operator = '+',
                                _operator_list = ['+','-','*'])

        algorithm.process()

        This going to check for (1 + 20 -3 * 10 is equal to 5 OR 1 + 20 -3 * 10 has a value between 10 and 20)



    '''

    def __init__(self, algorithm = None):

        self.ql = QueryLanguage()

        self.algorithm = algorithm

        self.operation = Operation(_operator= self.algorithm.operator_, _operator_list = self.algorithm.operator_list_)

    def process(self, fields):
        '''
        The method that parses the supplied statement (related to the query), operates on fields, checks the result against the
        query and returns a True/False depending on whether the result is within the query bounds

        @param fields [] A list of field values of type int or float
        @ret_val evaluation True/False
        '''

        # the number of operations have to be one less than the number of fields
        if self.algorithm.operator_list_ and len(self.algorithm.operator_list_) != len(fields) - 1:
            raise AssertionError("An operator list has been provided but does not correspond correctly with number of "\
                                 "field values to operate on" )

        #-------------------------------------------------------------------------------------
        # Construct the query dictionary after parsing the string statement
        #-------------------------------------------------------------------------------------

        query_dict = self.ql.parse(self.algorithm.query_statement)

        #-------------------------------------------------------------------------------------
        # Execute the operation on the fields and get the result out
        #-------------------------------------------------------------------------------------

        result = self.algorithm.operation.operate(fields)

        #-------------------------------------------------------------------------------------
        # Check if the result satisfies the query dictionary
        #-------------------------------------------------------------------------------------

        evaluation = self._evaluate_condition(result, query_dict)

        return evaluation

    def _evaluate_condition(self, result = None, query_dict = None):
        '''
        If result matches the query dict return True, else return False
        '''

        main_query = query_dict['query']
        or_queries= query_dict['or']
        and_queries = query_dict['and']

        #-------------------------------------------------------------------------------------
        # if any of the queries in the list of 'or queries' gives a match, publish an event
        #-------------------------------------------------------------------------------------
        if or_queries:
            for or_query in or_queries:
                if self._match(result, or_query):
                    return True

        #-------------------------------------------------------------------------------------
        # if an 'and query' or a list of 'and queries' is provided, return if the match returns false for any one of them
        #-------------------------------------------------------------------------------------
        if and_queries:
            for and_query in and_queries:
                if not self._match(result, and_query):
                    return False

        #-------------------------------------------------------------------------------------
        # The main query
        #-------------------------------------------------------------------------------------
        return self._match(result, main_query)


    def _match(self, result = None, query = None):
        '''
        Checks whether it is an "equals" matching or a "range" matching
        '''

        if QueryLanguage.query_is_term_search(query):
            # This is a term search - always a string
            if str(result) == query['value']:
                return True

        elif QueryLanguage.query_is_range_search(query):
            # always a numeric value - float or int
            if (result >=  query['range']['from']) and (result <= query['range']['to']):
                return True
            else:
                return False

            pass
        else:
            raise BadRequest("Missing parameters value and range for query: %s" % query)


#    ss = "search 'result' is '5' from 'dummy_index' and SEARCH 'result' VALUES FROM 10 TO 20 FROM 'dummy_index' "
#
#    query_dict = {       'and': [{'field': 'result',
#                                          'index': 'dummy_index',
#                                          'range': {'from': 10.0, 'to': 20.0}}],
#                                 'or': [],
#                                 'query': {'field': 'result', 'index': 'dummy_index', 'value': '5'}}






