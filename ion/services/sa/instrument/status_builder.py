#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.agent_status_builder
@author   Ian Katz
"""
from ooi.logging import log
from pyon.agent.agent import ResourceAgentClient
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound, Unauthorized


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


    def add_platform_device_aggregate_status_to_resource_extension(self, platform_device_id, extension):

        computed_attributes = extension.computed

        #retrieve the list of aggreate status for all children of this platform agent
        try:
            pa_client = self.obtain_agent_handle(platform_device_id)

            child_agg_status = pa_client.get_agent(['child_agg_status'])['child_agg_status']
            log.debug('get_platform_device_extension child_agg_status : %s', child_agg_status)

            if child_agg_status and hasattr(computed_attributes, "child_device_status"):
                computed_attributes.child_device_status = ComputedDictValue(status=ComputedValueAvailability.PROVIDED,
                                                                                   value=child_agg_status)

            #retrieve the platform status from the platform agent
            this_status = pa_client.get_agent(['aggstatus'])['aggstatus']
            log.debug("this_status is %s", this_status)

            combined_status = {}
            # 1. loop through the items in this device status,
            # 2. append all the statuses of that type from child devices,
            # 3. crush
            for stype, svalue in this_status.iteritems():
                status_list = [svalue]
                for dev_id, otherstats in child_agg_status.iteritems():
                    status_list.append(otherstats.get(stype, DeviceStatusType.STATUS_UNKNOWN))
                combined_status[stype] = self._crush_status_list(status_list)

            log.debug("combined_status is %s", this_status)
            self.set_status_computed_attributes(computed_attributes, combined_status,
                                                                     ComputedValueAvailability.PROVIDED)

            computed_attributes.aggregated_status = self._compute_aggregated_status_overall(combined_status)

        except NotFound:
            reason = "Could not connect to platform agent instance -- may not be running"
            if hasattr(computed_attributes, "child_device_status"):
                computed_attributes.child_device_status = ComputedDictValue(status=ComputedValueAvailability.NOTAVAILABLE,
                                                                                   value={},
                                                                                   reason=reason)
            computed_attributes.aggregated_status = self._compute_aggregated_status_overall({})

        except Unauthorized:
            reason = "The requester does not have the proper role to access the status of this platform agent"
            if hasattr(computed_attributes, "child_device_status"):
                computed_attributes.child_device_status = ComputedDictValue(status=ComputedValueAvailability.NOTAVAILABLE,
                                                                            value={},
                                                                            reason=reason)
            computed_attributes.aggregated_status = self._compute_aggregated_status_overall({})

        except Exception as e:
            raise e




    def add_device_aggregate_status_to_resource_extension(self, device_id, status_name, extended_resource):

        if not status_name or not extended_resource :
            raise BadRequest("The device or extended resource parameter is empty")

        if not device_id:
            self.set_status_computed_attributes(extended_resource.computed, {}, None, "No device")
            return

        try:
            agg = self.get_status_of_device(device_id, status_name)
            log.debug('add_device_aggregate_status_to_resource_extension status: %s', agg)
            print 'add_device_aggregate_status_to_resource_extension status: %s', agg

            if agg:
                self.set_status_computed_attributes(extended_resource.computed, agg, ComputedValueAvailability.PROVIDED)
                extended_resource.computed.aggregated_status = self._compute_aggregated_status_overall(agg)
            else:
                reason = "Status name '%s' not found" % status_name
                self.set_status_computed_attributes(extended_resource.computed, {}, None, reason)
                extended_resource.computed.aggregated_status = ComputedIntValue(status=ComputedValueAvailability.NOTAVAILABLE,
                                                                                reason=reason)

        except NotFound:
            self.set_status_computed_attributes(extended_resource.computed, {}, None,
                                                "Could not connect to instrument agent instance -- may not be running")

        except Unauthorized:
            self.set_status_computed_attributes(extended_resource.computed, {}, None,
                                                "The requester does not have the proper role to access the status of this instrument agent")

        except Exception as e:
            raise e

        return


    def get_status_of_device(self, device_id, status_name):
        try:
            a_client = self.obtain_agent_handle(device_id)
            return a_client.get_agent([status_name])[status_name]
        except:
            return {}

    def get_aggregate_status_of_device(self, device_id, status_name):
        if not device_id:
            return ComputedIntValue(status=ComputedValueAvailability.NOTAVAILABLE,
                                    value=DeviceStatusType.STATUS_UNKNOWN,
                                    reason="No device ID was provided")
        try:
            a_client = self.obtain_agent_handle(device_id)

            aggstatus = a_client.get_agent([status_name])[status_name]
            log.debug('get_aggregate_status_of_device status: %s', aggstatus)
            return self._compute_aggregated_status_overall(aggstatus)

        except NotFound:
            reason = "Could not connect to instrument agent instance -- may not be running"
            return ComputedIntValue(status=ComputedValueAvailability.NOTAVAILABLE,
                                    value=DeviceStatusType.STATUS_UNKNOWN, reason=reason)

    def _create_computed_status(cls, status=DeviceStatusType.STATUS_UNKNOWN):
        return ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status)


    def _compute_aggregated_status_overall (self, agg_status_dict=None):
        if agg_status_dict is None:
            agg_status_dict = {}

        values_list = agg_status_dict.values()

        status = self._crush_status_list(values_list)

        return ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status)


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


    # TODO: this causes a problem because an instrument agent must be running in order to look up extended attributes.
    def obtain_agent_handle(self, device_id):

        ia_client = ResourceAgentClient(device_id, process=self.process)
        log.debug("got the instrument agent client here: %s for the device id: %s and process: %s",
                  ia_client, device_id, self.process)
        #       #todo: any validation?
        #        cmd = AgentCommand(command='get_current_state')
        #        retval = cls._ia_client.execute_agent(cmd)
        #        state = retval.result
        #        cls.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        #

        return ia_client

    def obtain_agent_calculation(self, device_id, result_container):
        ret = IonObject(result_container)
        a_client = None
        try:
            a_client = self.obtain_agent_handle(device_id)
            ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Could not connect to instrument agent instance -- may not be running"
        except Exception as e:
            raise e

        return a_client, ret
