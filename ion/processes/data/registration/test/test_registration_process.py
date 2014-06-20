from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from ion.processes.data.registration.registration_process import RegistrationProcess
from pyon.public import CFG, PRED
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule_utils import time_series_domain
from xml.dom.minidom import parseString
from coverage_model import SimplexCoverage, QuantityType, ArrayType, ConstantType, CategoryType
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from interface.objects import DataProduct, Dataset
from pydap.client import open_url
import unittest
import os
import gevent
import numpy as np

@attr('INT', group='dm')
class RegistrationProcessTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management      = DatasetManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()
        self.pubsub_management       = PubsubManagementServiceClient()
        self.resource_registry       = self.container.resource_registry
        
            

    def _make_dataset(self):
        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()
        parameter_dict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        dataset = Dataset(name='test_dataset')
        dataset_id = self.dataset_management.create_dataset(dataset, parameter_dictionary_id=parameter_dict_id)
        return dataset_id

    def test_pydap(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_extended_parsed()

        stream_def_id = self.pubsub_management.create_stream_definition('example', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)


        dp = DataProduct(name='example')

        data_product_id = self.data_product_management.create_data_product(dp, stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)
        
        self.data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, data_product_id)

        dataset_id = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)[0][0]
        monitor = DatasetMonitor(dataset_id)
        self.addCleanup(monitor.stop)

        rdt = ph.get_rdt(stream_def_id)
        ph.fill_rdt(rdt,10)
        ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(monitor.wait())


        gevent.sleep(1) # Yield to other greenlets, had an issue with connectivity

        pydap_host = CFG.get_safe('server.pydap.host','localhost')
        pydap_port = CFG.get_safe('server.pydap.port',8001)
        url = 'http://%s:%s/%s' %(pydap_host, pydap_port, data_product_id)

        for i in xrange(3): # Do it three times to test that the cache doesn't corrupt the requests/responses
            ds = open_url(url)

            np.testing.assert_array_equal(list(ds['data']['time']), np.arange(10))
            untested = []
            for k,v in rdt.iteritems():
                if k==rdt.temporal_parameter:
                    continue
                context = rdt.context(k)
                if isinstance(context.param_type, QuantityType):
                    np.testing.assert_array_equal(list(ds['data'][k]), rdt[k])
                elif isinstance(context.param_type, ArrayType):
                    if context.param_type.inner_encoding is None:
                        values = np.empty(rdt[k].shape, dtype='O')
                        for i,obj in enumerate(rdt[k]):
                            values[i] = str(obj)
                        np.testing.assert_array_equal(list(ds['data'][k]), values)
                    elif len(rdt[k].shape)>1:
                        values = np.empty(rdt[k].shape[0], dtype='O')
                        for i in xrange(rdt[k].shape[0]):
                            values[i] = ','.join(map(lambda x : str(x), rdt[k][i].tolist()))
                elif isinstance(context.param_type, ConstantType):
                    np.testing.assert_array_equal(list(ds['data'][k]), rdt[k])
                elif isinstance(context.param_type, CategoryType):
                    np.testing.assert_array_equal(list(ds['data'][k]), rdt[k].astype('|S'))
                else:
                    untested.append('%s (%s)' % (k,context.param_type))
            if untested:
                raise AssertionError('Untested parameters: %s' % untested)





