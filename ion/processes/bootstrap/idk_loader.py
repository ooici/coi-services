"""
bootstrap process for the IDK

uses the IDK configuration files to create custom entries for:
InstrumentAgent, InstrumentAgentInstance, InstrumentModel, and ParameterDictionary

assumes:
 - driver is always mi.instrument.FAMILY.MODEL.ooicore.driver.InstrumentDriver
 - port_agent will always run locally
 - container started with a command like:
            bin/pycc --rel res/deploy/r2idk.yml  idk_agent=IA5   idk_server_address=10.5.2.3    idk_comms_method=ethernet idk_comms_device_address=10.3.65.21   idk_comms_device_port=1234   idk_comms_server_address=3.4.5.65  idk_comms_server_port=3456   idk_comms_server_cmd_port=2435
"""

from ion.processes.bootstrap.ion_loader import IONLoader, COL_SCENARIO
from ooi.logging import log, TRACE
from pyon.core.exception import BadRequest, Conflict
import os
import yaml

class IDKLoader(IONLoader):
    def on_start(self):
        self.idk_scenario = self.CFG.get('idk_scenario', 'IDK')
        self.idk_comms_method = self.CFG.get('idk_comms_method', 'ethernet')

        self.idk_agent_id = self.CFG.get('idk_agent')
        self.idk_server_address = self.CFG.get('idk_server_address')
        self.idk_comms_device_address = self.CFG.get('idk_comms_device_address')
        self.idk_comms_device_port = self.CFG.get('idk_comms_device_port')
        self.idk_comms_server_address = self.CFG.get('idk_comms_server_address')
        self.idk_comms_server_port = self.CFG.get('idk_comms_server_port')
        self.idk_comms_server_cmd_port = self.CFG.get('idk_comms_server_cmd_port')

        # kick off normal preload
        super(IDKLoader,self).on_start()

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

        super(IDKLoader,self)._load_InstrumentAgentInstance(row)

