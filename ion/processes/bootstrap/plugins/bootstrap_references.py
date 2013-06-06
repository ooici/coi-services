
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceProcessClient
from ion.core.bootstrap_process import BootstrapPlugin

class BootstrapReferences(BootstrapPlugin):

    def on_initial_bootstrap(self, process, config, **kwargs):

        if not config.get_safe('bootstrap.load_references', False):
            return
        self.data_acquisition = DataAcquisitionManagementServiceProcessClient(process=process)


        global_range_test = config.get_safe('reference_tables.global_range_test', "res/preload/r2_ioc/attachments/Data_QC_Lookup_Table_Global_Range_Test_2013-2-21.csv")
        gradient_test = config.get_safe('reference_tables.gradient_test', "res/preload/r2_ioc/attachments/Data_QC_Lookup_Table_Gradient_Test.csv")
        stuck_value_test = config.get_safe('reference_tables.stuck_value_test', "res/preload/r2_ioc/attachments/Data_QC_Lookup_Table_Stuck_Value_Test.csv")
        trend_test = config.get_safe('reference_tables.trend_test', "res/preload/r2_ioc/attachments/Data_QC_Lookup_Table_Trend_Test.csv")
        spike_test = config.get_safe('reference_tables.spike_test', "res/preload/r2_ioc/attachments/Data_QC_Lookup_Table_spike_test_updated.csv")

        grt_parser_id = self.data_acquisition.create_parser(name='Global Range Test', module='ion.util.parsers.global_range_test', method='grt_parser')
        stuck_parser_id = self.data_acquisition.create_parser(name='Stuck Value Test', module='ion.util.parsers.stuck_value_test', method='stuck_value_test_parser')
        gradient_parser_id = self.data_acquisition.create_parser(name='Gradient Test', module='ion.util.parsers.gradient_test', method='gradient_test_parser')
        spike_parser_id = self.data_acquisition.create_parser(name='Spike Test', module='ion.util.parsers.spike_test', method='spike_parser')
        trend_parser_id = self.data_acquisition.create_parser(name='Trend Test', module='ion.util.parsers.trend_test', method='trend_parser')


        self.load_reference(grt_parser_id, global_range_test)
        self.load_reference(stuck_parser_id, stuck_value_test)
        self.load_reference(gradient_parser_id, gradient_test)
        self.load_reference(spike_parser_id, spike_test)
        self.load_reference(trend_parser_id, trend_test)


    def load_reference(self, parser_id, path):
        with open(path) as f:
            doc = f.read()
            self.data_acquisition.parse_qc_reference(parser_id, doc, timeout=120)






