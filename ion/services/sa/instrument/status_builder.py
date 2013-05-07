#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.agent_status_builder
@author   Ian Katz
"""
from ooi.logging import log
from pyon.agent.agent import ResourceAgentClient
from pyon.core.bootstrap import IonObject
from pyon.core.exception import NotFound, Unauthorized


from interface.objects import ComputedValueAvailability, ComputedIntValue, ComputedDictValue
from interface.objects import AggregateStatusType, DeviceStatusType


class AgentStatusBuilder(object):

    def __init__(self, process=None):
        """
        the process should be the "self" of a service instance
        """
        assert process
        self.process = process


    def set_status_computed_attributes(self, computed_attrs, values_dict=None, availability=None, reason=None):
        mappings = {AggregateStatusType.AGGREGATE_COMMS    : "communications_status_roll_up",
                    AggregateStatusType.AGGREGATE_POWER    : "power_status_roll_up",
                    AggregateStatusType.AGGREGATE_DATA     : "data_status_roll_up",
                    AggregateStatusType.AGGREGATE_LOCATION : "location_status_roll_up"}

        if values_dict is None:
            values_dict = {}

        for k, a in mappings.iteritems():
            if k in values_dict:
                status = ComputedIntValue(status=availability, value=values_dict[k], reason=reason)
            else:
                status = ComputedIntValue(status=ComputedValueAvailability.NOTAVAILABLE, reason=None)
            setattr(computed_attrs, a, status)


    def set_status_computed_attributes_notavailable(self, computed_attrs, reason):
        self.set_status_computed_attributes(computed_attrs, None, ComputedValueAvailability.NOTAVAILABLE, reason)
        if hasattr(computed_attrs, "child_device_status"):
            computed_attrs.child_device_status = ComputedDictValue(status=ComputedValueAvailability.NOTAVAILABLE,
                                                                   reason=reason)


    def get_device_agent(self, device_id):
        if not device_id or device_id is None:
            return None, "No device ID was provided"

        try:
            h_agent = ResourceAgentClient(device_id, process=self.process)
            log.debug("got the agent client here: %s for the device id: %s and process: %s",
                      h_agent, device_id, self.process)
        except NotFound:
            return None, "Could not connect to agent instance -- may not be running"

        except Unauthorized:
            return None, "The requester does not have the proper role to access the status of this agent"

        except Exception as e:
            raise e

        return h_agent, ""

    # child_device_ids is None for instruments, a list for platforms
    def add_device_rollup_statuses_to_computed_attributes(self, device_id,
                                                          extension_computed,
                                                          child_device_ids=None):


        h_agent, reason = self.get_device_agent(device_id)
        if None is h_agent:
            self.set_status_computed_attributes_notavailable(extension_computed, reason)
            return

        if None is child_device_ids:
            return None

        # if it's not none, the developer had better be giving us a list
        assert isinstance(child_device_ids, list)

        child_agg_status = h_agent.get_agent(['child_agg_status'])['child_agg_status']
        log.debug('add_device_rollup_statuses_to_computed_attributes child_agg_status : %s', child_agg_status)

        # get child agg status if we can set child_device_status
        if child_agg_status and hasattr(extension_computed, "child_device_status"):
            crushed = dict([(k, self._crush_status_dict(v)) for k, v in child_agg_status.iteritems()])
            extension_computed.child_device_status = ComputedDictValue(status=ComputedValueAvailability.PROVIDED,
                                                                       value=crushed)

        #retrieve the platform status from the platform agent
        this_status = h_agent.get_agent(['aggstatus'])['aggstatus']
        log.debug("this_status is %s", this_status)

        rollup_statuses = {}
        # 1. loop through the items in this device status,
        # 2. append all the statuses of that type from child devices,
        # 3. crush
        for stype, svalue in this_status.iteritems():
            one_type_status_list = [svalue]
            for child_device_id in child_device_ids:
                if not child_device_id in child_agg_status:
                    log.warn("Child device '%s' of parent device '%s' not found in child_agg_status from top agent_client",
                             child_device_id, device_id)
                else:
                    # get the dict of AggregateStatusType -> DeviceStatusType
                    child_statuses = child_agg_status[child_device_id]
                    one_type_status_list.append(child_statuses.get(stype, DeviceStatusType.STATUS_UNKNOWN))

            rollup_statuses[stype] = self._crush_status_list(one_type_status_list)

        log.debug("combined_status is %s", this_status)
        self.set_status_computed_attributes(extension_computed, rollup_statuses,
                                            ComputedValueAvailability.PROVIDED)

        return child_agg_status


    def _crush_status_dict(self, values_dict):
        return self._crush_status_list(values_dict.values())


    def _crush_status_list(self, values_list):

        if DeviceStatusType.STATUS_CRITICAL in values_list:
            status = DeviceStatusType.STATUS_CRITICAL
        elif DeviceStatusType.STATUS_WARNING in values_list:
            status = DeviceStatusType.STATUS_WARNING
        elif DeviceStatusType.STATUS_OK  in values_list:
            status = DeviceStatusType.STATUS_OK
        else:
            status = DeviceStatusType.STATUS_UNKNOWN

        return status


    def obtain_agent_calculation(self, device_id, result_container):
        ret = IonObject(result_container)

        h_agent, reason = self.get_device_agent(device_id)
        if None is h_agent:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = reason
        else:
            ret.status = ComputedValueAvailability.PROVIDED

        return h_agent, ret


    def _get_status_of_device(self, device_id):

        a_client, reason = self.get_device_agent(device_id)
        if None is a_client:
            return None, reason

        aggstatus = a_client.get_agent(['aggstatus'])['aggstatus']
        log.debug('get_aggregate_status_of_device status: %s', aggstatus)
        return aggstatus, ""

    def get_status_of_device(self, device_id):
        status, _ = self._get_status_of_device(device_id)
        if None is status:
            status = {}
        return status


    def get_aggregate_status_of_device(self, device_id):
        aggstatus, reason = self._get_status_of_device(device_id)

        if None is aggstatus:
            DeviceStatusType.STATUS_UNKNOWN
        else:
            return self._crush_status_dict(aggstatus)


    def get_device_status_computed_dict(self, device_id):
        aggstatus, reason = self._get_status_of_device(device_id)

        if None is aggstatus:
            return ComputedDictValue(status=ComputedValueAvailability.NOTAVAILABLE,
                                    reason=reason)
        else:
            return ComputedDictValue(status=ComputedValueAvailability.PROVIDED,
                                    value=aggstatus)