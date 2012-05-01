#!/usr/bin/env python

"""
@package ion.agents.eoi.external_data_agent
@file ion/agents/eoi/external_data_agent.py
@author Tim Giguere
@author Christopher Mueller
@brief Class derived from InstrumentAgent that provides a one-to-one relationship between an ExternalDataAgent instance
 and a given external dataset
"""

from pyon.public import log
from pyon.core.exception import InstDriverError

from ion.services.mi.instrument_agent import InstrumentAgent, InstrumentAgentState, InstrumentAgentEvent
from ion.agents.eoi.handler.data_handler import DataHandler
import os

class ExternalDataAgent(InstrumentAgent):

    def __init__(self, initial_state=InstrumentAgentState.UNINITIALIZED):
        log.debug('ExternalDataAgent.__init__: initial_state = {0}'.format(initial_state))
        InstrumentAgent.__init__(self, initial_state)

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

        # TODO: Retrieve all resources needed by the DataHandler and provide them during instantiation

        # Instantiate the DataHandler based on the configuration
        try:

            module = __import__(dvr_mod, fromlist=[dvr_cls])
            classobj = getattr(module, dvr_cls)
#            self._dvr_client = classobj(data_provider=edp_res, data_source=dsrc_res, ext_dataset=ext_ds_res)
            self._dvr_client = classobj()

        except Exception:
            self._dvr_client = None
            raise InstDriverError('Error instantiating DataHandler: {0}.{1}'.format(dvr_mod,dvr_cls))

        #self._construct_packet_factories(dvr_mod)

        log.info('ExternalDataAgent {0} loaded it\'s DataHandler: {1}'.format(self._proc_name,self._dvr_client))

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