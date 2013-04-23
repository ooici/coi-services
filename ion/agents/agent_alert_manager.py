#!/usr/bin/env python

"""
@package ion.agents.agent_alert_manager 
@file ion/agents/agent_alert_manager.py
@author Edward Hunter
@brief Class for managing alerts and aggregated alerts based on data streams,
state changes, and command errors, for opt-in use by agents.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon imports
from pyon.public import log


from interface.objects import StreamAlertType
from interface.objects import DeviceStatusType
from interface.objects import AggregateStatusType

# Alarms.
from ion.agents.alerts.alerts import *

class AgentAlertManager(object):
    """
    """
    def __init__(self, agent):
        self._agent = agent
        agent.aparam_set_alerts = self.aparam_set_alerts
        agent.aparam_get_alerts = self.aparam_get_alerts
    
        # Always default the aggstatus to unknown.
        for aggregate_type in AggregateStatusType._str_map.keys():
            agent.aparam_aggstatus[aggregate_type] = DeviceStatusType.STATUS_UNKNOWN
        agent.aparam_set_aggstatus = self.aparam_set_aggstatus
    
    def process_alerts(self, **kwargs):
        
        for a in self._agent.aparam_alerts:
            a.eval_alert(**kwargs)

        # update the aggreate status for this device
        self._process_aggregate_alerts()
        
    def _process_aggregate_alerts(self):
        """
        loop thru alerts list and retrieve status of any alert that contributes to the aggregate status and update the state
        """
        #init working status
        updated_status = {}
        for aggregate_type in AggregateStatusType._str_map.keys():
            updated_status[aggregate_type] = DeviceStatusType.STATUS_OK

        for a in self._agent.aparam_alerts:
            curr_state = a.get_status()

            #if this alert does not contribue to an aggregate type then pass
            if a._aggregate_type:

                #get the current value for this aggregate status
                current_agg_state = updated_status[ a._aggregate_type ]
                if a._status:
                    # this alert is not 'tripped' so the status is OK
                    #check behavior here. if there are any unknowns then set to agg satus to unknown?
                    if current_agg_state is DeviceStatusType.STATUS_UNKNOWN:
                        updated_status[ a._aggregate_type ]  = DeviceStatusType.STATUS_OK

                else:
                    #the alert is active, either a warning or an alarm
                    if a._alert_type is StreamAlertType.ALARM:
                        updated_status[ a._aggregate_type ] = DeviceStatusType.STATUS_CRITICAL
                    elif  a._alert_type is StreamAlertType.WARNING and current_agg_state is not DeviceStatusType.STATUS_CRITICAL:
                        updated_status[ a._aggregate_type ] = DeviceStatusType.STATUS_WARNING

        #compare old state with new state and publish alerts for any agg status that has changed.
        for aggregate_type in AggregateStatusType._str_map.keys():
            if updated_status[aggregate_type] != self._agent.aparam_aggstatus[aggregate_type]:
                old_status = self._agent.aparam_aggstatus[aggregate_type]
                self._agent.aparam_aggstatus[aggregate_type] = updated_status[aggregate_type]
                self._publish_agg_status_event(aggregate_type, self._agent.aparam_aggstatus[aggregate_type], old_status)

        return

    def _publish_agg_status_event(self, status_type, new_status, old_status):
        """
        Publish resource config change event.
        """
        try:
            self._agent._event_publisher.publish_event(
                event_type='DeviceAggregateStatusEvent',
                origin_type=self._agent.__class__.ORIGIN_TYPE,
                origin=self._agent.resource_id,
                status_name=status_type,
                status=new_status,
                prev_status=old_status)
        except Exception as exc:
            log.error('Agent %s could not publish aggregate status change event. Exception message: %s',
                self._agent._proc_name, exc.message)


    def aparam_set_alerts(self, params):
        """
        Construct alert objects from kwarg dicts.
        """
        if not isinstance(params, (list,tuple)) or len(params)==0:
            return -1
        
        if isinstance(params[0], str):
            action = params[0]
            params = params[1:]
        else:
            action = 'set'
        
        if action not in ('set','add','remove','clear'):
            return -1
        
        if action in ('set', 'clear'):
            [x.stop() for x in self._agent.aparam_alerts]
            self._agent.aparam_alerts = []
                
        if action in ('set', 'add'):
            for alert_def in params:
                try:
                    cls = alert_def.pop('alert_class')
                    alert_def['resource_id'] = self._agent.resource_id
                    alert_def['origin_type'] = self._agent.__class__.ORIGIN_TYPE
                    if cls == 'LateDataAlert':
                        alert_def['get_state'] = self._agent._fsm.get_current_state                    
                    alert = eval('%s(**alert_def)' % cls)
                    self._agent.aparam_alerts.append(alert)
                except Exception as ex:
                    log.error('Agent %s error constructing alert %s. Exception: %s.',
                              self._agent._proc_name, str(alert_def), str(ex))
                    
        elif action == 'remove':
            new_alerts = copy.deepcopy(self._agent.aparam_alerts)
            new_alerts = [x for x in new_alerts if x.name not in params]
            old_alerts = [x for x in new_alerts if x.name in params]
            [x.stop() for x in old_alerts]
            self._agent.aparam_alerts = new_alerts

        for a in self._agent.aparam_alerts:
            log.info('Agent alert: %s', str(a))
                       
    def aparam_get_alerts(self):
        """
        Return kwarg representationn of all alerts.
        """
        result = [x.get_status() for x in self._agent.aparam_alerts]
        return result
    
    def stop_all(self):
        """
        """
        [a.stop for a in self._agent.aparam_alerts]
        
    def aparam_set_aggstatus(self, params):
        return -1
        