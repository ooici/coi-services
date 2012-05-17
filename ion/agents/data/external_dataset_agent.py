#!/usr/bin/env python

"""
@package ion.agents.data.external_dataset_agent
@file ion/agents/data/external_dataset_agent.py
@author Tim Giguere
@author Christopher Mueller
@brief Class derived from InstrumentAgent that provides a one-to-one relationship between an ExternalDatasetAgent instance
 and a given external dataset
"""
from pyon.public import log
from pyon.ion.resource import PRED, RT
from pyon.util.containers import get_safe
from pyon.core.exception import InstDriverError, NotFound

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.mi.exceptions import InstrumentStateException

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
        self._rr_cli = ResourceRegistryServiceClient()

    def _start_driver(self, dvr_config):
        """
        Instantiate the DataHandler based on the configuration
        Called from:
                    InstrumentAgent._handler_uninitialized_initialize
        @param dvr_config The driver configuration, equivalent to self._dvr_config
        @retval None or error.
        """

        # Get driver configuration and pid for test case.
        dvr_mod = get_safe(self._dvr_config, 'dvr_mod', None)
        dvr_cls = get_safe(self._dvr_config, 'dvr_cls', None)
        dh_cfg = get_safe(self._dvr_config, 'dh_cfg', {})

        log.debug('_start_driver: dvr_mod.dvr_cls={0}.{1} dh_cfg={2}'.format(dvr_mod,dvr_cls,dh_cfg))

        if not dvr_mod or not dvr_cls:
            raise InstDriverError('DataHandler module ({0}) and class ({1}) cannot be None'.format(dvr_mod, dvr_cls))

        #CBM: Could also follow the "stream_config" pattern and simply copy the stream_id(s) from agent.stream_config into dh_cfg
        if not 'stream_id' in dh_cfg:
            raise InstDriverError('DataHandler config must contain dh_cfg.stream_id: {0}'.format(self._dvr_config))

#        # TODO: Retrieve all resources needed by the DataHandler, they will be provided during configuration
#        ## Here to !!!! END from external_observatory_agent
        resreg_cli = ResourceRegistryServiceClient()

        # CBM: WTF - If not wrapped - exception DISAPPEARS!!  Why?

        ext_ds_res = None
        try:
            ext_ds_res = resreg_cli.read(object_id=self.resource_id)
            log.info('Dataset Resource (id={0}): {1}'.format(self.resource_id,ext_ds_res))
        except NotFound as ex:
            if not ext_ds_res and not get_safe(self._dvr_config, 'dh_cfg.TESTING'):
                raise InstDriverError('No resource associated with id = {0}'.format(self.resource_id))

        dh_cfg['external_dataset_res'] = ext_ds_res
        dh_cfg['external_dataset_res_id'] = self.resource_id

            #        ext_resources = {'dataset':ext_ds_res}
#        log.debug('Retrieved ExternalDataset: {0}'.format(ext_ds_res))
#
#        dsrc_res, dsrc_assn = resreg_cli.find_objects(subject=ext_dataset_id, predicate=PRED.hasSource, object_type=RT.DataSource)
#        dsrc_res = dsrc_res[0]
#        dsrc_id = dsrc_assn[0].o
#        ext_resources['datasource'] = dsrc_res
#        log.debug('Found associated DataSource: {0}'.format(dsrc_id))
#
#        edp_res, edp_assn = resreg_cli.find_objects(subject=dsrc_id, predicate=PRED.hasProvider, object_type=RT.ExternalDataProvider)
#        edp_res = edp_res[0]
#        edp_id = edp_assn[0].o
#        ext_resources['provider'] = edp_res
#        log.debug('Found associated ExternalDataProvider: {0}'.format(edp_id))
#
#        dsrc_mdl_res, dsrc_mdl_assn = resreg_cli.find_objects(subject=dsrc_id, predicate=PRED.hasModel, object_type=RT.DataSourceModel)
#        dsrc_mdl_res = dsrc_mdl_res[0]
#        dsrc_mdl_id = dsrc_mdl_assn[0].o
#        ext_resources['datasource_model'] = dsrc_mdl_res
#        log.debug('Found associated DataSourceModel: {0}'.format(dsrc_mdl_id))
#
#        dprod_res, dprod_assn = resreg_cli.find_objects(subject=ext_dataset_id, predicate=PRED.hasOutputProduct, object_type=RT.DataProduct)
#        dprod_res = dprod_res[0]
#        dprod_id = dprod_assn[0].o
#        ext_resources['data_products'] = dprod_res
#        log.debug('Found associated DataProduct: {0}'.format(dprod_id))
#
#        stream_res, stream_assn = resreg_cli.find_objects(subject=dprod_id, predicate=PRED.hasStream, object_type=RT.Stream)
#        stream_res = stream_res[0]
#        stream_id = stream_assn[0].o
#        ext_resources['stream_res'] = stream_res
#        log.debug('Found associated Stream: {0}'.format(stream_id))
#
#        comms_config = {'dataset_id':self.resource_id,'resources':ext_resources}
#        ## !!!! END
#        # TODO: Add the bits the DataHandler needs to know about to the 'comms_config' portion of the _dvr_config

#        comms_config = {}

        # The 'comms_config' portion of dvr_config is passed to configure()
#        self._dvr_config['comms_config'] = comms_config

        # Instantiate the DataHandler based on the configuration
        try:

            module = __import__(dvr_mod, fromlist=[dvr_cls])
            classobj = getattr(module, dvr_cls)

            log.debug('Load DataHandler: module={0}  classojb={1}'.format(module,classobj))

            self._dvr_client = classobj(self._rr_cli, self._stream_registrar, dh_cfg)
            self._dvr_client.set_event_callback(self.evt_recv)
            # Initialize the DataHandler
            self._dvr_client.cmd_dvr('initialize')

        except Exception as ex:
            self._dvr_client = None
            raise InstDriverError('Error instantiating DataHandler \'{0}.{1}\': {2}'.format(dvr_mod, dvr_cls, ex))

        #TODO: Temporarily construct packet factories to utilize pathways provided by IA
        self._construct_packet_factories(dvr_mod)

        log.info('ExternalDatasetAgent \'{0}\' loaded DataHandler \'{1}.{2}\''.format(self._proc_name,dvr_mod,dvr_cls))

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
        dvr_mod = get_safe(self._dvr_config, 'dvr_mod', None)
        dvr_cls = get_safe(self._dvr_config, 'dvr_cls', None)

        self._dvr_client = None
        log.info('ExternalDatasetAgent \'{0}\' unloaded DataHandler \'{1}.{2}\''.format(self._proc_name,dvr_mod,dvr_cls))
        return None

    def _validate_driver_config(self):
        """
        Test the driver config for validity.
        Called BEFORE comms_config is added to self._dvr_config, so only validate core portions
        @retval True if the current config is valid, False otherwise.
        """
        try:
            dvr_mod = self._dvr_config['dvr_mod']
            dvr_cls = self._dvr_config['dvr_cls']
            dvr_cfg = self._dvr_config['dh_cfg']
            stream_id = dvr_cfg['stream_id']

        except TypeError, KeyError:
            return False

        if not isinstance(dvr_mod, str) or not isinstance(dvr_cls, str) or not isinstance(dvr_cfg, dict) or not isinstance(stream_id, str):
            return False

        return True

    def _handler_streaming_execute_resource(self, command, *args, **kwargs):
        """
        Handler for execute_resource command in streaming state.
        Delegates to InstrumentAgent._handler_observatory_execute_resource
        """
        if command == 'execute_acquire_data':
            return self._handler_observatory_execute_resource(command, *args, **kwargs)
        else:
            raise InstrumentStateException('Command \'{0}\' not allowed in current state {1}'.format(command, self._fsm.get_current_state()))

    def _construct_data_publishers(self):
        pass
#        """
#        Construct the stream publishers from the stream_config agent
#        config variable.
#        @retval None
#        """
#        InstrumentAgent._construct_data_publishers(self)

    def _construct_packet_factories(self, dvr_mod):
        pass
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
