#!/usr/bin/env python

"""
@package ion.agents.alarms.alarms
@file ion/agents/alarms/alarms.py
@author Edward Hunter
@brief Alarm objects to control construction of valid alarm expressions.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon imports
from pyon.public import IonObject, log

# Alarm types and events.
from interface.objects import StreamAlarmType

class BaseAlarm(object):
    """
    Base alarm object.
    """
    def __init__(self, name, stream_name, value_id, message, type):
        """
        Populate fields used by all alarms.
        """
        
        if not isinstance(name, str):
            raise TypeError('Invalid name.')
            
        if not isinstance(stream_name, str):
            raise TypeError('Invalid stream name.')

        if not isinstance(value_id, str):
            raise TypeError('Invalid value id.')

        if not isinstance(message, str):
            raise TypeError('Invalid message.')

        if not isinstance(type, int):
            raise TypeError('Invalid alarm type.')

        if not type in StreamAlarmType._value_map.values():
            raise TypeError('Invalid alarm type.')

        self.name = name
        self.stream_name = stream_name
        self.value_id = value_id
        self.message = message
        self.type = type
        self.expr = ''
        self.status = None
        self.current_val = None

    def eval_alarm(self, x):
        """
        Evalueate boolean alarm expression.
        """
        self.current_val = x
        old_status = self.status
        self.status = eval(self.expr)
        
        retval = None
        if old_status != self.status:
            
            event_data = {
                'name' : self.name,
                'message' : self.message,
                'expr' : self.expr,
                'stream_name' : self.stream_name,
                'value_id' : self.value_id,
                'value' : x
            }
            
            if not self.status:
                event_data['event_type'] = 'StreamAllClearAlarrmEvent'
                event_data['message'] = 'The alarm %s has cleared.' % self.name
                retval = event_data
                
            elif self.type == StreamAlarmType.WARNING:
                event_data['event_type'] = 'StreamWarningAlaramEvent'
                retval = event_data
    
            elif self.type == StreamAlarmType.ALERT:
                event_data['event_type'] = 'StreamAlertAlarmEvent'
                retval = event_data

            else:
                log.error('Unknown alarm type.')
        
        return retval

    def __str__(self):
        """
        Pretty print the alarm object.
        """
        fmt = ('Alarm object of class:%s \n   name:%s \n   message:%s \n'
            '   expr:%s \n   stream_name:%s \n   value_id:%s \n   value:%s')
        
        s = fmt % (self.__class__.__name__,
                  self.name,
                  self.message,
                  self.expr,
                  self.stream_name,
                  self.value_id,
                  str(self.current_val))

        return s

class IntervalAlarm(BaseAlarm):
    """
    An alarm that specifies an interval range. Can be one sided or closed.
    """
    def __init__(self, name, stream_name, value_id, message, type,
                 lower_bound=None, lower_rel_op=None,
                 upper_bound=None, upper_rel_op=None):
        """
        Call superclass and construct interval expression.
        """
        super(IntervalAlarm, self).__init__(name, stream_name, value_id, message,
                                            type)
        
        self.lower_bound = lower_bound
        self.lower_rel_op = lower_rel_op
        self.upper_bound = upper_bound
        self.upper_rel_op = upper_rel_op
        
        if self.lower_bound:
            if not isinstance(self.lower_bound, (int, float)):
                raise TypeError('Bad lower bound value.')
            if self.lower_rel_op not in ('<','<='):
                raise TypeError('Bad lower bound relational op.')

        if self.upper_bound:
            if not isinstance(self.upper_bound, (int, float)):
                raise TypeError('Bad upper bound value.')
            if self.upper_rel_op not in ('<','<='):
                raise TypeError('Bad upper bound relational op.')
        
        if self.lower_bound and self.upper_bound:
            if self.lower_bound >= self.upper_bound:
                raise ValueError('Lower bound >= upper bound.')

        if self.lower_bound:
            self.expr += str(self.lower_bound)
            self.expr += self.lower_rel_op
        
        self.expr += 'x'
        
        if self.upper_bound:
            self.expr += self.upper_rel_op
            self.expr += str(self.upper_bound)

class DoubleIntervalAlarm(BaseAlarm):
    """
    An alarm providing a double interval. Either the left lower bound,
    the right upper bound or both may be open.
    """
    def __init__(self, name, stream_name, value_id, message, type):
        raise Exception('Not implemented.')

class SetMembershipAlarm(BaseAlarm):
    """
    An alarm providing membership in a discrete set of objects.
    """
    def __init__(self, name, stream_name, value_id, message, type):
        raise Exception('Not implemented.')

class UserDefinedAlarm(BaseAlarm):
    """
    An alarm provided by a user supplied expression.
    This must be used with caution.
    """
    def __init__(self, name, stream_name, value_id, message, type, expr=None):
        """
        """
        super(UserDefinedAlarm, self).__init__(name, stream_name, value_id, message,
                                            type)

        if not isinstance(self.expr, str):
            raise TypeError('Alarm expression must be a string.')
    
        self.expr = expr        


