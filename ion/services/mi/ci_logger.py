#!/usr/bin/env python

"""
@package ion.services.mi.ci_logger CI_Logger
@file ion/services/mi/ci_logger.py
@author Bill Bollenbacher
@brief Resource agent derived class providing a CI Logger agent as a resource.
This resource fronts CG_Loggers and instrument drivers one-to-one in ION.
"""

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

import time
import socket
import SocketServer
import logging
from subprocess import Popen

from ion.services.mi.instrument_fsm_args import InstrumentFSM
from ion.services.mi.common import BaseEnum
from ion.services.mi.common import InstErrorCode
from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess
from ion.services.mi.drivers.sbe37.sbe37_driver import SBE37Channel

class CgMsgTypes(BaseEnum):
    ACK               = 0
    NACK              = 1
    PING              = 2
    PING_RET          = 3
    QUIT              = 6
    DLOG_INST_DAQ_ON  = 37
    DLOG_INST_DAQ_OFF = 38
    DLOG_INST_PWR_ON  = 39
    DLOG_INST_PWR_OFF = 40
    INITIALIZE        = 105

class CiLogger(object):
    """
    ResourceAgent derived class for the CI Logger. This class
    interfaces CG components to driver resources in the ION
    system. It provides support for the CG DCL_Control and Data_Mgr interfaces 
    and translates/forwards these on to the CI driver and CG_Logger.
    """

    def __init__(self):
        log.debug("CiLogger.__init__:")
        
        # start driver
        driver_config = {'svr_addr': 'localhost',
                         'cmd_port': 5556,
                         'evt_port': 5557,
                         'dvr_mod': 'ion.services.mi.drivers.sbe37.sbe37_driver',
                         'dvr_cls': 'SBE37Driver'}
        result = self._start_driver(driver_config)
        if not isinstance(result, int):
            log.error("CiLogger.__init__: " + result[1])
            return
        log.info("CiLogger.__init__: started driver with process id of " + str(result))
        
        # configure driver and have it start the port agent
        comms_config = {SBE37Channel.CTD: {'method':'ethernet',
                                           'device_addr': 'sbe37-simulator.oceanobservatories.org',
                                           'device_port': 4001,
                                           'server_addr': 'localhost',
                                           'server_port': 8888} 
                       }               
        cfg_result = self._dvr_client.cmd_dvr('configure', comms_config)
        channels = [key for (key, val) in cfg_result.iteritems() if not
            InstErrorCode.is_error(val)]
        con_result = self._dvr_client.cmd_dvr('connect', channels)
        result = cfg_result.copy()
        try:
            for (key, val) in con_result.iteritems():
                result[key] = val
        except:
            log.error("CiLogger.__init__: driver connection failure - " + str(con_result))
            return
        self._active_channels = self._dvr_client.cmd_dvr('get_active_channels')
        if len(self._active_channels) <= 0:
            log.error("CiLogger.__init__: driver connection failure - no active channels")
            return
        log.info("CiLogger.__init__: driver connected to instrument " + comms_config[SBE37Channel.CTD]['device_addr'])

    def start_streaming(self):
        log.info("CiLogger.start_streaming():")
        return CgMsgTypes.ACK, "Instrument started"
    
    
    ###############################################################################
    # Event callback and handling.
    ###############################################################################

    def evt_recv(self, evt):
        """
        Callback to receive asynchronous driver events.
        @param evt The driver event received.
        """
        log.info('CiLogger.evt_recv(): received driver event %s', str(evt))
        # TODO: send 'sample' events to the CG_Logger
        
    ###############################################################################
    # Private helpers.
    ###############################################################################

    def _go_inactive(self, *args, **kwargs):
        """
        Handler for go_inactive agent command in idle state.
        Attempt to disconnect and initialize all active driver channels.
        Swtich to inactive state if successful.
        """
        
        channels = self._dvr_client.cmd_dvr('get_active_channels')
        dis_result = self._dvr_client.cmd_dvr('disconnect', channels)
        
        [key for (key, val) in dis_result.iteritems() if not
            InstErrorCode.is_error(val)]
        self._stop_driver()
        
    def _go_streaming(self,  *args, **kwargs):
        """
        Handler for go_streaming agent command in observatory state.
        Send start autosample command to driver and switch to streaming
        state if successful.
        """
        result = self._dvr_client.cmd_dvr('start_autosample', *args, **kwargs)
    
        if isinstance(result, dict):
            if any([val == None for val in result.values()]):
                next_state = InstrumentAgentState.STREAMING

    def _go_observatory(self,  *args, **kwargs):
        """
        Handler for go_observatory agent command within streaming state. Command
        driver to stop autosampling, and switch to observatory mode if
        successful.
        """
        
        result = self._dvr_client.cmd_dvr('stop_autosample', *args, **kwargs)
        
        if isinstance(result, dict):
            if all([val == None for val in result.values()]):
                next_state = InstrumentAgentState.OBSERVATORY
                               
    def _start_driver(self, dvr_config):
        """
        Start the driver process and driver client.
        @param dvr_config The driver configuration.
        @param comms_config The driver communications configuration.
        @retval None or error.
        """
        try:        
            cmd_port = dvr_config['cmd_port']
            evt_port = dvr_config['evt_port']
            dvr_mod = dvr_config['dvr_mod']
            dvr_cls = dvr_config['dvr_cls']
            svr_addr = dvr_config['svr_addr']
            
        except (TypeError, KeyError):
            # Not a dict. or missing required parameter.
            log.error('CiLogger._start_driver(): missing required parameter in start_driver.')            
            return InstErrorCode.REQUIRED_PARAMETER
                
        # Launch driver process.
        self._dvr_proc = ZmqDriverProcess.launch_process(cmd_port, evt_port,
                                                         dvr_mod,  dvr_cls)

        self._dvr_proc.poll()
        if self._dvr_proc.returncode:
            # Error proc didn't start.
            log.error('CiLogger._start_driver(): driver process did not launch.')
            return InstErrorCode.AGENT_INIT_FAILED

        log.info('CiLogger._start_driver(): launched driver process.')
        
        # Create client and start messaging.
        self._dvr_client = ZmqDriverClient(svr_addr, cmd_port, evt_port)
        self._dvr_client.start_messaging(self.evt_recv)
        log.info('CiLogger._start_driver(): driver process client started.')
        time.sleep(1)

        try:        
            retval = self._dvr_client.cmd_dvr('process_echo', 'Test.')
            log.info('CiLogger._start_driver(): driver process echo test: %s.' %str(retval))
            
        except Exception:
            self._dvr_proc.terminate()
            self._dvr_proc.wait()
            self._dvr_proc = None
            self._dvr_client = None
            log.error('CiLogger._start_driver(): error commanding driver process.')            
            return InstErrorCode.AGENT_INIT_FAILED

        else:
            log.info('CiLogger._start_driver(): started its driver.')

        return self._dvr_proc.pid
        
    def _stop_driver(self):
        """
        Stop the driver process and driver client.
        @retval None.
        """
        if self._dvr_client:
            self._dvr_client.done()
            self._dvr_proc.wait()
            self._dvr_proc = None
            self._dvr_client = None
            log.info('CiLogger._stop_driver(): stopped its driver.')            
        time.sleep(1)

class UDPServerHandler(SocketServer.BaseRequestHandler):
    """
    This class works similar to the TCP handler class, except that
    self.request consists of a pair of data and client socket, and since
    there is no connection the client address must be given explicitly
    when sending data back via sendto().
    """

    def handle(self):
        data = self.request[0].strip()
        log.info("{} sent: ".format(self.client_address) + data)
        msg_items = data.split(',')
        
        # switch on msg type
        if msg_items[2] == str(CgMsgTypes.DLOG_INST_DAQ_ON):
            response_type, msg = self.server.ci_logger.start_streaming()
        elif msg_items[2] == str(CgMsgTypes.PING):
            response_type = CgMsgTypes.PING_RET
            msg = "Ping received at "
        else:
            response_type = CgMsgTypes.NACK
            msg = "unknown cmd type"
            
        response = "%s,%s,%d,%d,%s" %(msg_items[1],
                                      msg_items[0],
                                      response_type,
                                      len(msg),
                                      msg)
        socket = self.request[1]
        socket.sendto(response, self.client_address)

if __name__ == '__main__':
    log = logging
    log.basicConfig(level=logging.DEBUG)
    
    HOST, PORT = "localhost", 9999
    log.info("ci_logger: Starting UDP server on port %d" %PORT)
    server = SocketServer.UDPServer((HOST, PORT), UDPServerHandler)
    log.info('ci_logger: UDP server started on %s:%s' %(HOST, PORT))
    
    log.info("ci_logger: Starting cg_logger_sim")
    ci_logger_sim = Popen(['bin/python', '/Users/Bill/WorkSpace/coi-services/ion/services/mi/cg_logger_sim.py'])    

    log.info('Instantiating CiLogger')
    ci_logger = CiLogger()
    
    server.ci_logger = ci_logger
    
    try:
        server.serve_forever()
    except:
        ci_logger._go_inactive()
    log.info('UDP server & driver stopped')
