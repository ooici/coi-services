

__author__ = "Tim Giguere"

from pyon.public import log

from ion.services.mi.instrument_agent import InstrumentAgent, InstrumentAgentState, InstrumentAgentEvent
from ion.agents.eoi.handler.data_handler import DataHandler
import os

class ExternalDataAgent(InstrumentAgent):

    ###############################################################################
    # Private helpers.
    ###############################################################################

    def _start_driver(self, dvr_config):
        """
        Start the driver process and driver client.
        @param dvr_config The driver configuration.
        @param comms_config The driver communications configuration.
        @retval None or error.
        """
        # Get driver configuration and pid for test case.
        dvr_mod = self._dvr_config['dvr_mod']
        dvr_cls = self._dvr_config['dvr_cls']
        this_pid = os.getpid() if self._test_mode else None

        #(self._dvr_proc, cmd_port, evt_port) = ZmqDriverProcess.launch_process(dvr_mod, dvr_cls, '/tmp/', this_pid)
        #
        ## Verify the driver has started.
        #if not self._dvr_proc or self._dvr_proc.poll():
        #    raise InstDriverError('Error starting driver process.')

        #log.info('Started driver process for %d %d %s %s', cmd_port,
        #    evt_port, dvr_mod, dvr_cls)
        #log.info('Driver process pid %d', self._dvr_proc.pid)

        # Start client messaging and verify messaging.
        try:
            #self._dvr_client = ZmqDriverClient('localhost', cmd_port, evt_port)
            self._dvr_client = DataHandler()
            #self._dvr_client.start_messaging(self.evt_recv)
            #retval = self._dvr_client.cmd_dvr('process_echo', 'Test.')

        except Exception:
            #self._dvr_proc.kill()
            #self._dvr_proc.wait()
            #self._dvr_proc = None
            self._dvr_client = None
            #raise InstDriverError('Error starting driver client.')

        #self._construct_packet_factories(dvr_mod)

        log.info('Instrument agent %s started its driver.', self._proc_name)

    def _stop_driver(self):
        """
        Stop the driver process and driver client.
        @retval None.
        """
        return None

    def _validate_driver_config(self):
        """
        Test the driver config for validity.
        @retval True if the current config is valid, False otherwise.
        """
        return True

    #def _construct_data_publishers(self):
        """
        Construct the stream publishers from the stream_config agent
        config variable.
        @retval None
        """
    #    pass

    #def _construct_packet_factories(self, dvr_mod):
        """
        Construct packet factories from packet_config member of the
        driver_config.
        @retval None
        """
    #    pass

    #def _clear_packet_factories(self):
        """
        Delete packet factories.
        @retval None
        """
    #    pass

    #def _log_state_change_event(self, state):
    #    pass

    #def _publish_instrument_agent_event(self, event_type=None, description=None):
    #    pass