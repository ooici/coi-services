"""
bootstrap process for the IDK

uses the IDK configuration files to create custom entries for:
InstrumentAgent, InstrumentAgentInstance, InstrumentDevice, and DataProduct

assumes:
 - driver is always mi.instrument.FAMILY.MODEL.ooicore.driver.InstrumentDriver
 - port_agent will always run locally
 - container started with a command like:
            bin/pycc --rel res/deploy/r2idk.yml  idk_agent=IA5   idk_server_address=10.5.2.3    idk_comms_method=ethernet idk_comms_device_address=10.3.65.21   idk_comms_device_port=1234   idk_comms_server_address=3.4.5.65  idk_comms_server_port=3456   idk_comms_server_cmd_port=2435
"""

from ion.processes.bootstrap.ion_loader import IONLoader, COL_SCENARIO, COL_ID
from ooi.logging import log
from pyon.core.exception import BadRequest

class IDKLoader(IONLoader):
    def on_start(self):
        """ use cmd-line args for IDK settings that reconfigure preload values """
        self.idk_scenario = self.CFG.get('idk_scenario', 'IDK')
        self.idk_comms_method = self.CFG.get('idk_comms_method', 'ethernet')
        self.idk_agent_id = self.CFG.get('idk_agent')
        log.debug("idk agent is %s", self.idk_agent_id)
        self.idk_server_address = self.CFG.get('idk_server_address')
        self.idk_comms_device_address = self.CFG.get('idk_comms_device_address')
        self.idk_comms_device_port = self.CFG.get('idk_comms_device_port')
        self.idk_comms_server_address = self.CFG.get('idk_comms_server_address')
        self.idk_comms_server_port = self.CFG.get('idk_comms_server_port')
        self.idk_comms_server_cmd_port = self.CFG.get('idk_comms_server_cmd_port')
        self.idk_model_id = None
        self.idk_stream_config = None
        # kick off normal preload
        super(IDKLoader,self).on_start()

    def _load_StreamDefinition(self, row):
        if row[COL_SCENARIO]==self.idk_scenario and row['param_dict_name']=='UNKNOWN':
            # delay adding to resource registry until we know which param_dict from the InstrumentAgent
            self.saved_stream_definition_row = row
        else:
            super(IDKLoader,self)._load_StreamDefinition(row)

    def _load_InstrumentAgent(self, row):
        """ save device model and parsed stream config for use later with IDK device and data product"""
        if not self.idk_agent_id:
            raise BadRequest('no IDK agent ID specified')
        if row[COL_ID]==self.idk_agent_id:
            # TODO: should this be plural?
            self.idk_model_id = row['instrument_model_ids']
            self.idk_stream_config = row['stream_configurations'].split(',')[1]
            log.debug("idk model id is %s", self.idk_model_id)
            # now we can insert the stream definition saved from earlier
            self.saved_stream_definition_row['param_dict_name'] = self.stream_config[self.idk_stream_config].parameter_dictionary_name
            super(IDKLoader,self)._load_StreamDefinition(self.saved_stream_definition_row)
        else:
            log.debug("preload agent %s, not ours %s", row[COL_ID], self.idk_agent_id)
        super(IDKLoader,self)._load_InstrumentAgent(row)

    def _load_InstrumentDevice(self, row):
        """ apply device model of agent """
        if row[COL_SCENARIO]==self.idk_scenario:
            if not self.idk_model_id:
                raise BadRequest('no IDK model ID found')
            row['instrument_model_id'] = self.idk_model_id
        super(IDKLoader,self)._load_InstrumentDevice(row)

    def _load_InstrumentAgentInstance(self, row):
        """ override specific columns of IDK row defining instrument agent instance based on cmd-line arguments passed to container """
        if row[COL_SCENARIO]==self.idk_scenario:
            row['iai/comms_method'] = self.idk_comms_method
            row['iai/comms_device_address'] = self.idk_comms_device_address
            row['iai/comms_device_port'] = self.idk_comms_device_port
            row['iai/comms_device_address'] = self.idk_comms_device_port
            row['comms_server_address'] = self.idk_comms_server_address
            row['comms_server_port'] = self.idk_comms_server_port
            row['comms_server_cmd_port'] = self.idk_comms_server_cmd_port
            row['instrument_agent_id'] = self.idk_agent_id
        super(IDKLoader,self)._load_InstrumentAgentInstance(row)

    def _load_DataProduct(self, row, do_bulk=False):
        if row[COL_SCENARIO]==self.idk_scenario and row['stream_def_id']=='UNKNOWN':
            row['stream_def_id'] = self.saved_stream_definition_row[COL_ID]
            row['param_dict_type'] = self.stream_config[self.idk_stream_config].parameter_dictionary_name
        super(IDKLoader,self)._load_DataProduct(row)

