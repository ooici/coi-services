"""
bootstrap process for the IDK

uses the IDK configuration files to create custom entries for:
InstrumentAgent, InstrumentAgentInstance, InstrumentModel, and ParameterDictionary

assumes:
 - driver is always mi.instrument.FAMILY.MODEL.ooicore.driver.InstrumentDriver
 - port_agent will always run locally

"""

from ion.processes.bootstrap.ion_loader import IONLoader, COL_SCENARIO
from ooi.logging import log, TRACE
from pyon.core.exception import BadRequest, Conflict
import os
import yaml

class IDKLoader(IONLoader):
    def on_start(self):
        self.idk_scenario = self.CFG.get('idk_scenario', 'IDK')
        self._read_idk_config()
        # perform normal preload
        super(IDKLoader,self).on_start()

    def _read_idk_config(self):
        import pprint
        log.info(pprint.pformat(self.CFG))
        # parameters should be set on pycc command line
        family = self.CFG.get('idk_family')
        model = self.CFG.get('idk_model')
        idk_base = self.CFG.get('idk_home')
        if not family or not model or not idk_base:
            raise BadRequest('failed to set IDK properties idk_family, idk_model and idk_home')
        dirname = os.path.join(idk_base, 'mi/instrument', family, model, 'ooicore')

        # merge entries from both config files into a dictionary
        self.idk_config = { }
        for filename in 'comm_config.yml', 'metadata.yml':
            pathname = os.path.join(dirname, filename)
            with open(pathname, 'r') as f:
                data = yaml.load(f)
            for key in data:
                if key in self.idk_config:
                    raise Conflict('key %s found in both IDK configuration files for model %s' % (key, model))
                self.idk_config[key] = data[key]

        if log.isEnabledFor(TRACE):
            import pprint
            log.trace('idk configuration values:\n%s', pprint.pformat(self.idk_config))

    def _is_idk(self, row):
        return row[COL_SCENARIO]==self.idk_scenario

    def _load_InstrumentAgent(self, row):
        if self._is_idk(row):
            row['ia/driver_module'] = 'mi.instrument.%s.%s.ooicore.driver' % (self.idk_config['driver_metadata']['driver_family'], self.idk_config['driver_metadata']['driver_model'])
        super(IDKLoader,self)._load_InstrumentAgent(row)

    def _load_InstrumentAgentInstance(self, row):
        if self._is_idk(row):
            row['iai/comms_device_address'] = self.idk_config['comm']['device_addr']
            row['iai/comms_device_port'] = self.idk_config['comm']['device_port']
            row['iai/comms_server_port'] = self.idk_config['comm']['data_port']
            row['iai/comms_server_cmd_port'] = self.idk_config['comm']['command_port']
        super(IDKLoader,self)._load_InstrumentAgentInstance(row)

    def _load_InstrumentModel(self, row):
        if self._is_idk(row):
            row['im/instrument_family'] = self.idk_config['driver_metadata']['driver_family']
        super(IDKLoader,self)._load_InstrumentModel(row)

#    def _load_ParameterDictionary(self, row):
#        if self._is_idk(row):
#            row['parameters'] = self.idk_config['??']['??']
#        super(IDKLoader,self)._load_InstrumentModel(row)
