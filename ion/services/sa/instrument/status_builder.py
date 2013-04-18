#!/usr/bin/env python

"""
@package  ion.util.enhanced_resource_registry_client
@author   Ian Katz
"""
from interface.objects import ComputedIntValue
from ooi.logging import log
from pyon.agent.agent import ResourceAgentClient
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound


from interface.objects import AttachmentType, ComputedValueAvailability, ComputedIntValue, StatusType, ProcessDefinition
from interface.objects import AggregateStatusType, DeviceStatusType


class AgentStatusBuilder(object):

    @classmethod
    def add_device_aggregate_status_to_resource_extension(cls, device_id='', status_name='', extended_device_resource=None):

        if not device_id or not status_name or not extended_device_resource :
            raise BadRequest("The device or extended resource parameter is empty")

        try:
            ia_client = cls.obtain_agent_handle(device_id)

            aggstatus = ia_client.get_agent([status_name])[status_name]
            log.debug('add_device_aggregate_status_to_resource_extension status: %s', aggstatus)

            if aggstatus:
                extended_device_resource.computed.communications_status_roll_up = cls._create_computed_status ( aggstatus[AggregateStatusType.AGGREGATE_COMMS] )
                extended_device_resource.computed.power_status_roll_up          = cls._create_computed_status ( aggstatus[AggregateStatusType.AGGREGATE_POWER] )
                extended_device_resource.computed.data_status_roll_up           = cls._create_computed_status ( aggstatus[AggregateStatusType.AGGREGATE_DATA] )
                extended_device_resource.computed.location_status_roll_up       = cls._create_computed_status ( aggstatus[AggregateStatusType.AGGREGATE_LOCATION] )
                extended_device_resource.computed.aggregated_status             = cls._compute_aggregated_status_overall(aggstatus)

        except NotFound:
            reason = "Could not connect to instrument agent instance -- may not be running"
            extended_device_resource.computed.communications_status_roll_up =\
            extended_device_resource.computed.power_status_roll_up =\
            extended_device_resource.computed.data_status_roll_up =\
            extended_device_resource.computed.location_status_roll_up =\
            extended_device_resource.computed.aggregated_status = ComputedIntValue(status=ComputedValueAvailability.NOTAVAILABLE,
                                                                                   value=DeviceStatusType.STATUS_UNKNOWN, reason=reason)
        except Exception as e:
            raise e

        return

    @classmethod
    def get_aggregate_status_of_device(cls, device_id, status_name):
        try:
            ia_client = cls.obtain_agent_handle(device_id)

            aggstatus = ia_client.get_agent([status_name])[status_name]
            log.debug('get_aggregate_status_of_device status: %s', aggstatus)
            return cls._compute_aggregated_status_overall(aggstatus)

        except NotFound:
            reason = "Could not connect to instrument agent instance -- may not be running"
            return ComputedIntValue(status=ComputedValueAvailability.NOTAVAILABLE,
                                    value=DeviceStatusType.STATUS_UNKNOWN, reason=reason)

    @classmethod
    def _create_computed_status(cls, status=DeviceStatusType.STATUS_UNKNOWN):
        return ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status)


    @classmethod
    def _compute_aggregated_status_overall (cls, agg_status_dict=None):
        if agg_status_dict is None:
            agg_status_dict = {}

        status = DeviceStatusType.STATUS_UNKNOWN

        values_list = agg_status_dict.values()
        if DeviceStatusType.STATUS_CRITICAL in values_list:
            status = DeviceStatusType.STATUS_CRITICAL
        elif DeviceStatusType.STATUS_WARNING in values_list:
            status = DeviceStatusType.STATUS_WARNING
        elif DeviceStatusType.STATUS_OK  in values_list:
            status = DeviceStatusType.STATUS_OK

        return ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status)



    # TODO: this causes a problem because an instrument agent must be running in order to look up extended attributes.
    @classmethod
    def obtain_agent_handle(cls, device_id, process=None):
        ia_client = ResourceAgentClient(device_id,  process=process)
        log.debug("got the instrument agent client here: %s for the device id: %s and process: %s", ia_client, device_id, cls)

        #       #todo: any validation?
        #        cmd = AgentCommand(command='get_current_state')
        #        retval = cls._ia_client.execute_agent(cmd)
        #        state = retval.result
        #        cls.assertEqual(state, InstrumentAgentState.UNINITIALIZED)
        #

        return ia_client

    @classmethod
    def obtain_agent_calculation(cls, device_id, result_container):
        ret = IonObject(result_container)
        a_client = None
        try:
            a_client = cls.obtain_agent_handle(device_id)
            ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Could not connect to instrument agent instance -- may not be running"
        except Exception as e:
            raise e

        return a_client, ret
