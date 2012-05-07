#!/usr/bin/env python

"""
@package ion.agents.eoi.external_dataset_agent
@file ion/agents/eoi/external_dataset_agent.py
@author Tim Giguere
@author Christopher Mueller
@brief Class derived from InstrumentAgent that provides a one-to-one relationship between an ExternalDatasetAgent instance
 and a given external dataset
"""

from pyon.public import log
from pyon.core.exception import InstDriverError

from ion.services.mi.instrument_agent import InstrumentAgent, InstrumentAgentState, InstrumentAgentEvent

class ExternalDatasetAgent(InstrumentAgent):

    def __init__(self, initial_state=InstrumentAgentState.UNINITIALIZED):
        log.debug('ExternalDatasetAgent.__init__: initial_state = {0}'.format(initial_state))
        InstrumentAgent.__init__(self, initial_state)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.EXECUTE_RESOURCE, self._handler_streaming_execute_resource)
        # TODO: Do we need to (can we even?) remove handlers that aren't supported (i.e. Direct Access?)

    ###############################################################################
    # Private helpers.
    ###############################################################################

    def on_init(self):
        InstrumentAgent.on_init(self)

    def _start_driver(self, dvr_config):
        """
        Instantiate the DataHandler based on the configuration
        Called from:
                    InstrumentAgent._handler_uninitialized_initialize
        @param dvr_config The driver configuration, equivalent to self._dvr_config
        @retval None or error.
        """

        # Get driver configuration and pid for test case.
        dvr_mod = self._dvr_config['dvr_mod']
        dvr_cls = self._dvr_config['dvr_cls']

        # TODO: Retrieve all resources needed by the DataHandler, they will be provided during configuration
#        ## Here to !!!! END from external_observatory_agent
#        resreg_cli = ResourceRegistryServiceClient()
#
#        ext_ds_res = resreg_cli.read(object_id=ext_dataset_id)
#        log.debug("Retrieved ExternalDataset: %s" % ext_ds_res)
#
#        dsrc_res, dsrc_acc = resreg_cli.find_objects(subject=ext_dataset_id, predicate=PRED.hasSource, object_type=RT.DataSource)
#        dsrc_res = dsrc_res[0]
#        dsrc_id = dsrc_acc[0].o
#        log.debug("Found associated DataSource: %s" % dsrc_id)
#
#        edp_res, edp_acc = resreg_cli.find_objects(subject=dsrc_id, predicate=PRED.hasProvider, object_type=RT.ExternalDataProvider)
#        edp_res = edp_res[0]
#        edp_id = edp_acc[0].o
#        log.debug("Found associated ExternalDataProvider: %s" % edp_id)
#
#        mdl_res, mdl_acc = resreg_cli.find_objects(subject=dsrc_id, predicate=PRED.hasModel, object_type=RT.DataSourceModel)
#        mdl_res = mdl_res[0]
#        mdl_id = mdl_acc[0].o
#        log.debug("Found associated DataSourceModel: %s" % mdl_id)
#
#        dprod_id, _ = resreg_cli.find_objects(subject=ext_dataset_id, predicate=PRED.hasOutputProduct, object_type=RT.DataProduct, id_only=True)
#        dprod_id = dprod_id[0]
#        log.debug("Found associated DataProduct: %s" % dprod_id)
#
#        stream_id, _ = resreg_cli.find_objects(subject=dprod_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
#        log.debug(">>>>> stream_id: %s" % stream_id)
#        stream_id = stream_id[0]
#        log.debug("Found associated Stream: %s" % stream_id)
#
#        ## !!!! END
        # TODO: Add the bits the DataHandler needs to know about to the 'comms_config' portion of the _dvr_config


        # The 'comms_config' portion of dvr_config is passed to configure()
        self._dvr_config['comms_config'] = {'dataset_id':self.resource_id}

        # Instantiate the DataHandler based on the configuration
        try:

            module = __import__(dvr_mod, fromlist=[dvr_cls])
            classobj = getattr(module, dvr_cls)
            self._dvr_client = classobj()
#            self._dvr_client = classobj(data_provider=edp_res, data_source=dsrc_res, ext_dataset=ext_ds_res)
            self._dvr_client.set_event_callback(self.evt_recv)
            # Initialize the DataHandler
            self._dvr_client.cmd_dvr('initialize')

        except Exception:
            self._dvr_client = None
            raise InstDriverError('Error instantiating DataHandler: {0}.{1}'.format(dvr_mod,dvr_cls))

        #TODO: Temporarily construct packet factories to utilize pathways provided by IA
        self._construct_packet_factories(dvr_mod)

        log.info('ExternalDatasetAgent \'{0}\' loaded DataHandler \'{1}\''.format(self._proc_name,''.join([dvr_mod,'.',dvr_cls])))

    def _stop_driver(self):
        """
        Unload the DataHandler instance
        Called from:
                    InstrumentAgent._handler_inactive_reset,
                    InstrumentAgent._handler_idle_reset,
                    InstrumentAgent._handler_stopped_reset,
                    InstrumentAgent._handler_observatory_reset
        @retval None.
        """
        self._dvr_client = None
        log.info('ExternalDatasetAgent \'{0}\' unloaded DataHandler: {1}'.format(self._proc_name, self._dvr_client))
        return None

    def _validate_driver_config(self):
        """
        Test the driver config for validity.
        @retval True if the current config is valid, False otherwise.
        """
        try:
            dvr_mod = self._dvr_config['dvr_mod']
            dvr_cls = self._dvr_config['dvr_cls']

        except TypeError, KeyError:
            return False

        if not isinstance(dvr_mod, str) or not isinstance(dvr_cls, str):
            return False

        return True

    def _handler_streaming_execute_resource(self, command, *args, **kwargs):
        """
        Handler for execute_resource command in streaming state.
        Delegates to InstrumentAgent._handler_observatory_execute_resource
        """
        if command.command == 'execute_acquire_data':
            return self._handler_observatory_execute_resource(command, *args, **kwargs)
        else:
            raise StateError('Command \'{0}\' not allowed in current state {1}'.format(command.command, self._fsm.get_current_state()))

#    def _construct_data_publishers(self):
#        """
#        Construct the stream publishers from the stream_config agent
#        config variable.
#        @retval None
#        """
#        InstrumentAgent._construct_data_publishers(self)

#    def _construct_packet_factories(self, dvr_mod):
#        """
#        Construct packet factories from packet_config member of the
#        driver_config.
#        @retval None
#        """
#        pass

#    def _clear_packet_factories(self):
#        """
#        Delete packet factories.
#        @retval None
#        """
#        pass

#    def _log_state_change_event(self, state):
#        pass

#    def _publish_instrument_agent_event(self, event_type=None, description=None):
#        pass
