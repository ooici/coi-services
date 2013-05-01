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

# Standard imports.
import time
import copy

# gevent.
import gevent

# Alarm types and events.
from interface.objects import StreamAlertType
from interface.objects import AggregateStatusType
from interface.objects import DeviceStatusType

# Events.
from pyon.event.event import EventPublisher

# Resource agent.
from pyon.agent.agent import ResourceAgentState


class BaseAlert(object):
    """
    Base class for all alert types.
    """
    schema = {
        'display_name' : '',
        'description' : '',
        "type" : {
            "name" :
            {
                "display_name" : "Alert Name",
                "description" : "Alert name unique to this agent.",
                "required" : True,
                "type" : "str"
            
            },
            "description" : {
                "display_name" : "Alert Description",
                "description" : "Descriptive purpose of the alert.",
                "required" : True,
                "type" : "str"
            
            },
            "alert_type" : {
                "display_name" : "Alert Type",
                "description" : "Severity type of the alert.",
                "required" : True,
                "type" : "int",
                "string_map" : StreamAlertType._str_map,
                "value_map" : StreamAlertType._value_map
            
            },
            "aggregate_type" : {
                "display_name" : "Aggregate Type",
                "description" : "Functional type of the alert for aggregate status.",
                "required" : False,
                "type" : "int",        
                "string_map" : AggregateStatusType._str_map,
                "value_map" : AggregateStatusType._value_map
                },
            "value" : {
                "display_name" : "Current Value",
                "description" : "Current value of the alert.",
                "required" : False,                
                "type" : "alert specific"
                },
            "status" : {
                "display_name" : "Alert Status",
                "description" : "Current boolean status of the alert.",
                "required" : False,
                "type" : "bool"
                }
            }
        }
    
    @classmethod
    def get_schema(cls):
        return copy.deepcopy(cls.schema)

    def __init__(self, name=None, description=None, alert_type=None,
                 resource_id=None, origin_type=None, aggregate_type=None):
        assert isinstance(name, str)
        assert alert_type in StreamAlertType._str_map.keys()
        assert isinstance(description, str)
        if aggregate_type != None:
            assert aggregate_type in AggregateStatusType._str_map.keys()
        assert isinstance(resource_id, str)
        assert isinstance(origin_type, str)
        
        self._name = name
        self._description = description
        self._alert_type = alert_type
        self._aggregate_type = aggregate_type
        self._resource_id = resource_id
        self._origin_type = origin_type
        
        self._status = None
        self._prev_status = None
        self._current_value = None

    def get_status(self):
        """
        Base get_status returns the common members.
        """        
        status = {
            'name' : self._name,
            'description' : self._description,
            'alert_type' : self._alert_type,
            'aggregate_type' : self._aggregate_type,
            'alert_class' : self.__class__.__name__,
            'value' : self._current_value,
            'status' : self._status
        }

        return status

    def make_event_data(self):
        """
        Make data event creates an event dict for publishing.
        """
        event_data = {
            'name' : self._name,
            'description' : self._description,
            'values' : [self._current_value],
            'event_type' : 'DeviceStatusAlertEvent',
            'origin' : self._resource_id,
            'origin_type' : self._origin_type
        }
        
        if self._status:
            event_data['sub_type'] = 'ALL_CLEAR'
            event_data['description'] = 'The alert is cleared.'
            
        elif self._alert_type == StreamAlertType.WARNING:
            event_data['sub_type'] = 'WARNING'
        
        elif self._alert_type == StreamAlertType.ALERT:
            event_data['sub_type'] = 'ALERT'

        return event_data

    def publish_alert(self):
        """
        Publishes the alert to ION.
        """
        event_data = self.make_event_data()
        pub = EventPublisher()
        pub.publish_event(**event_data)

    def stop(self):
        """
        Some alerts have greenlets that need to be stopped. Override where
        necessary.
        """
        pass

    def eval_alert(**kwargs):
        """
        Override in derived classes to perform alert logic when triggered
        by appropriate events.
        """
        pass

class StreamAlert(BaseAlert):
    
    schema = {
            "stream_name" : {
                "display_name" : "Stream Name",
                "description" : "Name of the stream this alert listens on.",
                "required" : True,
                "type" : "str"
            }
        }

    @classmethod
    def get_schema(cls):
        retval = BaseAlert.get_schema()
        retval['type'].update(cls.schema)
        return retval
    
    def __init__(self, name=None, description=None, alert_type=None, resource_id=None,
                    origin_type=None, aggregate_type=None, stream_name=None):
        
        super(StreamAlert, self).__init__(name, description, alert_type, resource_id,
                                          origin_type, aggregate_type)
        
        assert isinstance(stream_name, str)
        self._stream_name = stream_name

    def get_status(self):
        status = super(StreamAlert, self).get_status()
        status['stream_name'] = self._stream_name
        return status

    def make_event_data(self):
        event_data = super(StreamAlert, self).make_event_data()
        event_data['stream_name'] = self._stream_name
        return event_data
    
class StreamValueAlert(StreamAlert):
    
    schema = {
        "value_id" : {
            "display_name" : "Value ID",
            "description" : "Name of data value field within stream.",
            "required" : True,
            "type" : "str"                
        }
    }
    
    @classmethod
    def get_schema(cls):
        retval = StreamAlert.get_schema()
        retval['type'].update(cls.schema)
        return retval
    
    
    def __init__(self, name=None, description=None, alert_type=None, resource_id=None,
                    origin_type=None, aggregate_type=None, stream_name=None, value_id=None):
        
        super(StreamValueAlert, self).__init__(name, description, alert_type, resource_id,
                                          origin_type, aggregate_type, stream_name)
        
        assert isinstance(value_id, str)
        self._value_id = value_id

    def get_status(self):
        status = super(StreamValueAlert, self).get_status()
        status['value_id'] = self._value_id
        return status

    def make_event_data(self):
        event_data = super(StreamValueAlert, self).make_event_data()
        event_data['value_id'] = self._value_id
        return event_data

class IntervalAlert(StreamValueAlert):
    """
    An alert that triggers when values leave a defined range.
    """
    rel_ops = ['<', '<=']

    schema = {
        "lower_bound" : {
            "display_name" : "Lower Bound",
            "description" : "Lower bound of the data valid interval.",
            "required" : False,
            "type" : "float"                            
        },
        "upper_bound" : {
            "display_name" : "Upper Bound",
            "description" : "Upper bound of the data valid interval.",
            "required" : False,
            "type" : "float"                            
        },
        "lower_rel_op" : {
            "display_name" : "Lower Bound Operator",
            "description" : "Lower bound inequallity operator of the data valid interval.",
            "required" : False,
            "type" : "str",
            "valid_values" : ['<','<=']
        },
        "upper_rel_op" : {
            "display_name" : "Upper Bound Operator",
            "description" : "Upper bound inequallity operator of the data valid interval.",
            "required" : False,
            "type" : "str",
            "valid_values" : ['<','<=']
        },
        "class_name" : {
            "display_name" : "Alert Class Name",
            "descritpion" : "Name of class implementing alert.",
            "required" : True,
            "type" : "str",
            "value" : "IntervalAlert"
            }            
        }

    @classmethod
    def get_schema(cls):
        retval = StreamValueAlert.get_schema()
        retval['type'].update(cls.schema)
        retval['display_name'] = 'Data Interval Alert'
        retval['description'] = 'Alert triggered by out of range data values.'
        retval['type']['value']['type'] = "float"
        return retval
    
    def __init__(self, name=None, stream_name=None, description=None, alert_type=None,
                 value_id=None, resource_id=None, origin_type=None, aggregate_type=None,
                 lower_bound=None, lower_rel_op=None, upper_bound=None,
                 upper_rel_op=None, **kwargs):

        super(IntervalAlert, self).__init__(name, description, alert_type, resource_id,
                        origin_type, aggregate_type, stream_name, value_id)
        
        assert isinstance(value_id, str)
        self._value_id = value_id

        self._lower_bound = None
        self._upper_bound = None
        self._upper_rel_op = None
        self._lower_rel_op = None

        assert (isinstance(lower_bound, (int, float)) \
                or isinstance(upper_bound, (int, float)))
        
        if isinstance(lower_bound, (int, float)):
            assert lower_rel_op in IntervalAlert.rel_ops
            self._lower_rel_op = lower_rel_op
            self._lower_bound = lower_bound

        if isinstance(upper_bound, (int, float)):
            assert upper_rel_op in IntervalAlert.rel_ops
            self._upper_rel_op = upper_rel_op
            self._upper_bound= upper_bound

    def get_status(self):
        status = super(IntervalAlert, self).get_status()
        status['lower_bound'] = self._lower_bound
        status['upper_bound'] = self._upper_bound
        status['lower_rel_op'] = self._lower_rel_op
        status['upper_rel_op'] = self._upper_rel_op
        return status

    def eval_alert(self, stream_name=None, value=None, value_id=None, **kwargs):

        if stream_name != self._stream_name or value_id != self._value_id \
                          or not value:
            return

        self._current_value = value
        self._prev_status = self._status
        
        if self._lower_bound and self._upper_bound:
            if self._lower_rel_op == '<=':
                if self._upper_rel_op == '<=':
                    self._status = (self._lower_bound <= self._current_value <= self._upper_bound)
                
                else:
                    self._status = (self._lower_bound <= self._current_value < self._upper_bound)
                    
            else:
                if self._upper_rel_op == '<=':
                    self._status = (self._lower_bound < self._current_value <= self._upper_bound)
                
                else:
                    self._status = (self._lower_bound < self._current_value < self._upper_bound)
                        
        elif self._lower_bound:
            if self._lower_rel_op == '<=':
                self._status = (self._lower_bound <= self._current_value)
            else:
                self._status = (self._lower_bound < self._current_value)
            
        elif self._upper_bound:
            if self._upper_rel_op == '<=':
                self._status = (self._current_value <= self._upper_bound)
            else:
                self._status = (self._current_value < self._upper_bound)
                
        if self._prev_status != self._status:
            self.publish_alert()


class RSNEventAlert(BaseAlert):
    """
    An alert that represents an RSN push alert notification.
    """

    # value_id represents the name of the monitorable in an RSNAlert
    #

    def __init__(self, name=None, stream_name=None, description=None, alert_type=None,
                 value_id=None, resource_id=None, origin_type=None, aggregate_type=None,
                 **kwargs):

        super(RSNEventAlert, self).__init__(name, '', description,
                alert_type, value_id, resource_id, origin_type, aggregate_type)

        assert isinstance(value_id, str)
        self._value_id = value_id

#        {
#        "group": "power",
#        "name" : "low_voltage_warning",
#        "value_id" : "input_voltage",
#        "value" : "1.2",
#        "alert_type" : "warning",
#        "url": "http://localhost:8000",
#        "timestamp": 3573569514.295556,
#        "ref_id": "44.78",
#        "platform_id": "TODO_some_platform_id_of_type_UPS",
#        "message": "low battery (synthetic event generated from simulator)"
#        }

        self._name = name
        self._stream_name = stream_name
        self._message = message
        self._alert_type = alert_type
        self._aggregate_type = aggregate_type
        self._value_id = value_id
        self._resource_id = resource_id
        self._origin_type = origin_type

        self._status = None
        self._prev_status = None
        self._current_value = None

    def get_status(self):
        status = super(RSNEventAlert, self).get_status()

        return status

    def eval_alert(self, rsn_alert=None):

        # x is an RSN event struct TBD
        assert isinstance(rsn_alert, dict)

        #print 'x: %s',x.keys()
        #print 'x: %s',x

        self._current_value = rsn_alert['value']
        self._prev_status = self._status

        self._message = rsn_alert['message']

        if rsn_alert['alert_type'] is "warning":
            self._alert_type = StreamAlertType.WARNING
            self._status = False
        elif rsn_alert['alert_type'] is "error":
            self._alert_type = StreamAlertType.ALERT
            self._status = False
        else:
            self._alert_type = StreamAlertType.ALL_CLEAR
            self._status = True

        if self._prev_status != self._status:
            self.publish_alert()

        return


class UserExpressionAlert(StreamValueAlert):
    """
    An alert that represents a user defined python expression.
    (Caution this can be dangerous!)
    """
    pass

class DeltaAlert(StreamValueAlert):
    """
    An alert that triggers when a large jump is seen in data streams.
    """
    pass

class LateDataAlert(StreamAlert):
    """
    An alert that triggers when data is late in streaming mode.
    """
    schema = {
        "time_delta" : {
            "display_name" : "Time Delta",
            "description" : "Wait time for new data to arrive on the stream.",
            "required" : True,
            "type" : "float"
        },
        "class_name" : {
            "display_name" : "Alert Class Name",
            "descritpion" : "Name of class implementing alert.",
            "required" : True,
            "type" : "str",
            "value" : "LateDataAlert"
        }            
    }
    
    @classmethod
    def get_schema(cls):
        retval = StreamAlert.get_schema()
        retval['type'].update(cls.schema)
        retval['display_name'] = 'Late Data Alert'
        retval['description'] = 'Alert triggered by late data in streaming mode.'
        retval['type']['value']['type'] = "float"        
        return retval
    
    
    def __init__(self, name=None, stream_name=None, description=None, alert_type=None,
                 value_id=None, resource_id=None, origin_type=None, aggregate_type=None,
                 time_delta=None, get_state=None, **kwargs):

        super(LateDataAlert, self).__init__(name, description, alert_type, resource_id,
                        origin_type, aggregate_type, stream_name)

        assert isinstance(time_delta, (int, float))
        assert get_state
        assert callable(get_state)
        
        self._time_delta = time_delta
        self._get_state = get_state
        self._gl = gevent.spawn(self._check_data)

    def get_status(self):
        status = super(LateDataAlert, self).get_status()
        status['time_delta'] = self._time_delta
        return status

    def eval_alert(self, stream_name=None, **kwargs):
        if stream_name != self._stream_name:
            return
        
        self._current_value = time.time()
        if not self._status:
            self._status = True
            self.publish_alert()
        
    def _check_data(self):
        """
        """
        while True:
            prev_value = self._current_value
            prev_status = self._status
            gevent.sleep(self._time_delta)            
            if self._get_state() == ResourceAgentState.STREAMING:
                if self._current_value == prev_value and self._status:
                    self._status = False
                    self.publish_alert()
        
    def stop(self):
        if self._gl:
            self._gl.kill()
            self._gl.join()
            self._gl = None
            
class StateAlert(BaseAlert):
    """
    An alert that triggers for specified state changes.
    Useful for detecting spontaneous disconnects.
    """    
    schema = {
        "alert_states" : {
            "display_name" : "Alert States",
            "description" : "List of states that trigger the alert.",
            "required" : True,
            "type" : ["str"],
            },
        "clear_states" : {
            "display_name" : "Clear State",
            "description" : "List of states that clear the alert.",
            "required" : True,
            "type" : ["str"],
        },
        "class_name" : {
            "display_name" : "Alert Class Name",
            "descritpion" : "Name of class implementing alert.",
            "type" : "str",
            "required" : True,
            "value" : "StateAlert"
        }
    }
    
    @classmethod
    def get_schema(cls):
        retval = BaseAlert.get_schema()
        retval['type'].update(cls.schema)
        retval['display_name'] = 'State Alert'
        retval['description'] = 'Alert triggered by specified states.'
        retval['type']['value']['type'] = "str"        
        return retval
    
    def __init__(self, name=None, description=None, alert_type=None, resource_id=None,
                 origin_type=None, aggregate_type=None, alert_states=None,
                 clear_states=None, **kwargs):

        super(StateAlert, self).__init__(name, description, alert_type, resource_id,
                                         origin_type, aggregate_type)

        assert isinstance(alert_states, (list, tuple))
        assert isinstance(clear_states, (list, tuple))
        assert all([isinstance(x, str) for x in alert_states])
        assert all([isinstance(x, str) for x in clear_states])
        
        self._alert_states = alert_states
        self._clear_states = clear_states
        
    def get_status(self):
        status = super(StateAlert, self).get_status()
        status['alert_states'] = self._alert_states
        status['clear_states'] = self._clear_states
        return status

    def eval_alert(self, state=None, **kwargs):
        if state == None or not isinstance(state,str):
            return
        
        self._current_value = state
        self._prev_status = self._status
        
        if self._prev_status == None:
            # No previous status, set to alert only if in alert states.
            if state in self._alert_states:
                self._status = False

            else:
                self._status = True
                
        elif self._prev_status == False:
            # Previous status false, set to clear if state in clear states.
            if state in self._clear_states:
                self._status = True
            
        else:
            # Previuos status true, set to alert if state in alert states.
            if state in self._alert_states:
                self._status = False

        if self._prev_status != self._status:
            self.publish_alert()

class CommandErrorAlert(BaseAlert):
    """
    An alert that triggers for specified command errors.
    Useful for detecting failed connection attmepts.
    """
    schema = {
        "command" : {
            "display_name" : "Command Name",
            "description" : "Execute agent command whos failure triggers the alert.",
            "required" : True,
            "type" : "str",
            },
        "clear_states" : {
            "display_name" : "Clear State",
            "description" : "List of states that clear the alert.",
            "required" : True,
            "type" : ["str"],
        },
        "class_name" : {
            "display_name" : "Alert Class Name",
            "descritpion" : "Name of class implementing alert.",
            "type" : "str",
            "required" : True,
            "value" : "CommandErrorAlert"
        }
        
    }
    
    @classmethod
    def get_schema(cls):
        retval = BaseAlert.get_schema()
        retval['type'].update(cls.schema)
        retval['display_name'] = 'Command Alert'
        retval['description'] = 'Alert triggered by specified command errors.'
        retval['type']['value']['type'] = "str"                
        return retval
    
    
    def __init__(self, name=None, description=None, alert_type=None, resource_id=None,
                 origin_type=None, aggregate_type=None, command=None,
                 clear_states=None, **kwargs):

        super(CommandErrorAlert, self).__init__(name, description, alert_type, resource_id,
                                         origin_type, aggregate_type)

        assert isinstance(command, str)
        assert isinstance(clear_states, (list, tuple))
        assert all([isinstance(x, str) for x in clear_states])
        
        self._command = command
        self._clear_states = clear_states
        
    def get_status(self):
        status = super(CommandErrorAlert, self).get_status()
        status['command'] = self._command
        status['clear_states'] = self._clear_states
        return status

    def eval_alert(self, command=None, command_success=None, state=None, **kwargs):        
        if (not isinstance(command, str) or not isinstance(command_success, bool)) and \
            not isinstance(state, str):
            return
        
        self._prev_status = self._status

        if self._prev_status == None:
            if command != None:
                if command == self._command:
                    self._status = command_success
                else:
                    self._status = True
            
            elif state != None:
                self._status = True
        
        else:
            if command != None:
                if command == self._command:
                    self._status = command_success
                else:
                    self._status = self._prev_status
                    
            elif state != None:
                if self._prev_status == False:
                    self._status = (state in self._clear_states)
                else:
                    self._status = self._prev_status

        if self._prev_status != self._status:
            self.publish_alert()
    
def get_alerts_schema():
    
    return [
        IntervalAlert.get_schema(),
        LateDataAlert.get_schema(),
        StateAlert.get_schema(),
        CommandErrorAlert.get_schema()
        ]
    
