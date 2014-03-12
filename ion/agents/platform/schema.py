#!/usr/bin/env python

"""
from ion.agents.platform.node_schema import get_schema, pp_schema
import ion.agents.platform.node_schema as ns
pp_schema()
x = get_schemna()
"""


import json

from ion.agents.alerts.alerts import get_alerts_schema
from ion.agents.platform.platform_agent import PlatformAgentCapability
from ion.agents.platform.platform_agent import PlatformAgentState
from interface.objects import DeviceStatusType
from interface.objects import AggregateStatusType
from interface.objects import StreamAlertType

# TBD TBD what is comms config for platforms?
"""
COMMS_CONFIG = {
    "display_name" : "Driver Comms Config",
    "description" : "Driver communications configuration parameters.",
    "type" : {
        "addr" : {
            "display_name" : "Address",
            "description" : "Address of port agent process.",
            "required" : True,
            "type" : "str"
        },
        "port"  : {
            "display_name" : "Data port",
            "description" : "Port agent data port.",
            "required" : True,
            "type" : "int"
        },
        "cmd_port" : {
            "display_name" : "Command port",
            "description" : "Port agent command port.",
            "required" : True,
            "type" : "int"            
        }
    }
}
"""

# TBD TBD what is driver config for platforms?
"""
DRIVER_CONFIG = {
    "display_name" : "Driver Config",
    "description" : "Driver configuration parameters.",
    "type" : {
        "dvr_egg" : {
            "display_name" : "Driver egg",
            "description" : "URI of the driver egg.",
            "required" : True,
            "type" : "str"
        },
        "dvr_mod" : {
            "display_name" : "Driver module",
            "description" : "Module of the driver.",
            "required" : True,
            "type" : "str"
        },
        "dvr_cls" : {
            "display_name" : "Driver class",
            "description" : "Class of the driver.",
            "required" : True,
            "type" : "str"
        },
        "workdir" : {
            "display_name" : "Work directory",
            "description" : "Address of port agent process.",
            "required" : True,
            "type" : "str"
        },
        "process_type" : {
            "display_name" : "Address",
            "description" : "Address of port agent process.",
            "required" : True,
            "type" : "str",
            "valid_values" : [
                DriverProcessType.EGG,
                DriverProcessType.PYTHON_MODULE
            ]
        },
        "mi_repo" : {
            "display_name" : "Driver Repository",
            "description" : "Filesystem path of the driver repository if driver launched from a python module.",
            "required" : False,
            "type" : "str"     
        },
        "comms_config" : COMMS_CONFIG
    }
}
"""
ALERTS = get_alerts_schema()    

AGENT_SCHEMA_V1 = {
    "commands" : {
        PlatformAgentCapability.INITIALIZE :
            {
                "display_name" : "Initialize",
                "description" : "Load the platform driver.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.GO_ACTIVE :
            {
                "display_name" : "Activate",
                "description" : "Activate platform connection.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.RUN :
            {
                "display_name" : "Run",
                "description" : "Enter command mode.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.PAUSE :
            {
                "display_name" : "Pause",
                "description" : "Pause command mode.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.RESUME :
            {
                "display_name" : "Resume",
                "description" : "Resume command mode.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.CLEAR :
            {
                "display_name" : "Clear",
                "description" : "Stop the agent and go idle.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.GO_INACTIVE :
            {
                "display_name" : "Deactivate",
                "description" : "Deactivate agent platform connection.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.START_MONITORING :
            {
                "display_name" : "Start Monitoring",
                "description" : "Activate platform stream publishing.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.STOP_MONITORING :
            {
                "display_name" : "Stop Monitoring",
                "description" : "Deactivate platform stream publishing.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.SHUTDOWN :
            {
                "display_name" : "Shutdown",
                "description" : "TBD. TBD. TBD.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.RESET :
            {
                "display_name" : "Reset",
                "description" : "Reset the agent to uninitialized mode.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.RUN_MISSION :
            {
                "display_name" : "Run Mission",
                "description" : "Commence mission execution.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.ABORT_MISSION :
            {
                "display_name" : "Abort Mission",
                "description" : "Shutdown mission gracefully at next appropriate time.",
                "args" : [],
                "kwargs" : {}
            },
        PlatformAgentCapability.KILL_MISSION :
            {
                "display_name" : "Kill Mission",
                "description" : "Immediately halt mission execution.",
                "args" : [],
                "kwargs" : {}
            },
        },
    "parameters" : {
        "streams" :
            {
                "display_name" : "Data Streams",
                "description" : "Data streams and fields published by agent.",
                "visibility" : "READ_ONLY",
                "type" : "dict"
            },        
        "alerts" :
            {
                "display_name" : "Agent Alerts.",
                "description" : "Definition and status of agent alerts.",
                "visibility" : "READ_WRITE",
                "type" : "list",
                "valid_values" : [
                    "/alert_defs/values*",
                    "'set', /alert_defs/values*",
                    "'add', /alert_defs/values+",
                    "'remove', /parameters/alerts/alert_name+",
                    "'clear'"
                    ],
                "set_options" : {
                    "set" : {
                        "display_name" : "Set",
                        "description" : "Reset all alerts to the new definitions."
                    },
                    "add" : {
                        "display_name" : "Add",
                        "description" : "Add alerts to the existing set."
                    },
                    "remove" : {
                        "display_name" : "Remove",
                        "description" : "Remove alerts with the supplied names."
                    },
                    "clear" : {
                        "display_name" : "Clear",
                        "description" : "Clear all alerts."
                    }
                }
            },
        "aggstatus" :
            {
                "display_name" : "Aggregate Status.",
                "description" : "Aggregate status of agent functions.",
                "visibility" : "READ_ONLY",
                "type" : "dict",
                "valid_values" :[{
                    "key" : {
                        "type" : "enum",
                        "string_map" : AggregateStatusType._str_map,
                        "value_map" : AggregateStatusType._value_map
                        },
                    "value" : {
                        "type" : "enum",
                        "string_map" : DeviceStatusType._str_map,
                        "value_map" : DeviceStatusType._value_map
                        }                    
                    }]
                },
        "child_agg_status" :
            {
                "display_name" : "Child Aggregate Status.",
                "description" : "Aggregate status of child agent functions.",
                "visibility" : "READ_ONLY",
                "type" : "dict",
                "valid_values" :[{
                    "key" : {
                        "type" : "enum",
                        "string_map" : AggregateStatusType._str_map,
                        "value_map" : AggregateStatusType._value_map
                        },
                    "value" : {
                        "type" : "enum",
                        "string_map" : DeviceStatusType._str_map,
                        "value_map" : DeviceStatusType._value_map
                        }                    
                    }]
                },
        "rollup_status" :
            {
                "display_name" : "Roll-up Status.",
                "description" : "Roll up status of agent and children functions.",
                "visibility" : "READ_ONLY",
                "type" : "dict",
                "valid_values" :[{
                    "key" : {
                        "type" : "enum",
                        "string_map" : AggregateStatusType._str_map,
                        "value_map" : AggregateStatusType._value_map
                        },
                    "value" : {
                        "type" : "enum",
                        "string_map" : DeviceStatusType._str_map,
                        "value_map" : DeviceStatusType._value_map
                        }                    
                    }]
                }
        },
    "states" : {
        PlatformAgentState.UNINITIALIZED : {
            "display_name" : "Uninitialized",
            "description" : "The agent has no resource loaded."
            },
        PlatformAgentState.INACTIVE : {
            "display_name" : "Inactive",
            "description" : "The agent is not connected to its resource."
            },
        PlatformAgentState.IDLE : {
            "display_name" : "Idle",
            "description" : "The agent is connected and idle."
            },
        PlatformAgentState.STOPPED : {
            "display_name" : "Stopped",
            "description" : "Agent command mode is paused."
            },
        PlatformAgentState.COMMAND : {
            "display_name" : "Command",
            "description" : "Agent resource can accept interactive resource commands."
            },
        PlatformAgentState.MONITORING : {
            "display_name" : "Monitoring",
            "description" : "Agent resource is monitoring data."
            },
        PlatformAgentState.LAUNCHING : {
            "display_name" : "Launching",
            "description" : "Agent resource is launching its child agents."
            },
        PlatformAgentState.LOST_CONNECTION : {
            "display_name" : "Lost Connection",
            "description" : "The resource connection has been lost."
            },
        },
    "command_args" : {},
    "alert_defs" : ALERTS
    }

def get_schema():
    return AGENT_SCHEMA_V1

def get_schema_json():
    return json.dumps(AGENT_SCHEMA_V1)
    
def pp_schema():
    print json.dumps(AGENT_SCHEMA_V1, sort_keys=True, indent=4)
    
    
    
"""
The following is an example of the schema returned by a platform node driver:

                           
"""