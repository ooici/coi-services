#!/usr/bin/env python

"""
@package ion.services.mi.instrument_agent Instrument resource agent
@file ion/services/mi/instrument_agent.py
@author Edward Hunter
@brief Resource agent derived class providing an instrument agent as a resource.
This resource fronts instruments and instrument drivers one-to-one in ION.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from pyon.core.exception import BadRequest, NotFound
from pyon.public import IonObject, AT, log
from pyon.agent.agent import ResourceAgent


class InstrumentAgent(ResourceAgent):
    '''The base instrument agent class
    
    The Instrument Agent is responsible for interfacing the ION infrastructure
    with the individual instrument drivers that then talk to the instruments.
    '''
    
    def __init__(self):
    
        self.instrument_agent_errors = None
        '''A list of errors that the Instrument Agent can return'''
    
        self.driver = None
        '''The instrument driver used to communicate with the instrument'''
    
    # May need some endpoint information in here somewhere.    
        # Build an instrument driver when instantiated
        
    # Methods are placeholders for now. To be improved/documented during port
    def execute_observatory(self, command, transaction_id):
        '''
        '''
    
    def get_observatory(self, params, transaction_id):
        '''
        '''

    def set_observatory(self, params, transaction_id):
        '''
        '''

    def get_observatory_metadata(self, params, transaction_id):
        '''
        '''

    def get_observatory_status(self, params, transaction_id):
        '''
        '''

    def get_observatory_capabilities(self, params, transaction_id):
        '''
        '''

    def execute_device(self, channels, command, transaction_id):
        '''
        '''

    def get_device(self, params, transaction_id):
        '''
        '''

    def set_device(self, params, transaction_id):
        '''
        '''

    def execute_device_direct(self, bytes, transaction_id):
        '''
        '''

    def get_device_metadata(self, params, transaction_id):
        '''
        '''

    def get_device_status(self, params, transaction_id):
        '''
        '''

    def start_transaction(self, acq_timeout, exp_timeout):
        '''
        '''

    def end_transaction(self, transaction_id):
        '''
        '''

    def driver_event_occurred(self, content):
        '''
        '''

    def set_agent_param(self, name='', value=''):
        pass
    
    def get_agent_param(self, name=''):
        pass
    
    def execute_agent(self, command={}):
        pass
    
    def set_param(self, name='', value=''):
        pass

    def get_param(self, name=''):
        pass
    
    def execute(self, command={}):
        pass
            
    def get_capabilities(self, capability_types=[]):
        pass
    
    def _get_agent_conv_types(self):
        pass

    def _get_agent_params(self):
        pass

    def _get_agent_commands(self):
        pass

    def _get_resource_params(self):
        pass

    def _get_resource_commands(self):
        pass

    def _get_names(self, obj, prefix):
        pass
    
            
    
    """
    def execute(self, command={}):
        return self._execute("rcmd_", command)

    def execute_agent(self, command={}):
        return self._execute("acmd_", command)

    def _execute(self, cprefix, command):
        if not command:
            raise iex.BadRequest("execute argument 'command' not present")
        if not command.command:
            raise iex.BadRequest("command not set")
        cmd_res = IonObject("AgentCommandResult", command_id=command.command_id, command=command.command)
        cmd_func = getattr(self, cprefix + str(command.command), None)
        if cmd_func:
            cmd_res.ts_execute = get_ion_ts()
            try:
                res = cmd_func(*command.args, **command.kwargs)
                cmd_res.status = 0
                cmd_res.result = res
            except Exception as ex:
                cmd_res.status = getattr(ex, 'status_code', -1)
                cmd_res.result = str(ex)
        else:
            ex = iex.NotFound("Command not supported: %s" % command.command)
            cmd_res.status = iex.NotFound.status_code
            cmd_res.result = str(ex)
        return cmd_res

    def get_capabilities(self, capability_types=[]):
        capability_types = capability_types or ["CONV_TYPE", "AGT_CMD", "AGT_PAR", "RES_CMD", "RES_PAR"]
        cap_list = []
        if "CONV_TYPE" in capability_types:
            cap_list.extend([("CONV_TYPE", cap) for cap in self._get_agent_conv_types()])
        if "AGT_CMD" in capability_types:
            cap_list.extend([("AGT_CMD", cap) for cap in self._get_agent_commands()])
        if "AGT_PAR" in capability_types:
            cap_list.extend([("AGT_PAR", cap) for cap in self._get_agent_params()])
        if "RES_CMD" in capability_types:
            cap_list.extend([("RES_CMD", cap) for cap in self._get_resource_commands()])
        if "RES_PAR" in capability_types:
            cap_list.extend([("RES_PAR", cap) for cap in self._get_resource_params()])
        return cap_list

    def set_param(self, name='', value=''):
        if not hasattr(self, "rpar_%s" % name):
            raise iex.NotFound('Resource parameter not existing: %s' % name)
        pvalue = getattr(self, "rpar_%s" % name)
        setattr(self, "rpar_%s" % name, value)
        return pvalue

    def get_param(self, name=''):
        try:
            return getattr(self, "rpar_%s" % name)
        except AttributeError:
            raise iex.NotFound('Resource parameter not found: %s' % name)

    def set_agent_param(self, name='', value=''):
        if not hasattr(self, "apar_%s" % name):
            raise iex.NotFound('Agent parameter not existing: %s' % name)
        pvalue = getattr(self, "apar_%s" % name)
        setattr(self, "apar_%s" % name, value)
        return pvalue

    def get_agent_param(self, name=''):
        try:
            return getattr(self, "apar_%s" % name)
        except AttributeError:
            raise iex.NotFound('Agent parameter not found: %s' % name)

    def _get_agent_conv_types(self):
        return []

    def _get_agent_params(self):
        return self._get_names(self, "apar_")

    def _get_agent_commands(self):
        return self._get_names(self, "acmd_")

    def _get_resource_params(self):
        return self._get_names(self, "rpar_")

    def _get_resource_commands(self):
        return self._get_names(self, "rcmd_")

    def _get_names(self, obj, prefix):
        return [name[len(prefix):] for name in dir(obj) if name.startswith(prefix)]
    """

