#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell at ASAScience dot com>
@date Tue Feb 12 09:54:27 EST 2013
@file ion/processes/data/transforms/test/test_transform_prime.py
'''

from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.utility.granule import RecordDictionaryTool
from pyon.ion.stream import StandaloneStreamPublisher
from pyon.util.int_test import IonIntegrationTestCase
from coverage_model import ParameterContext, AxisTypeEnum, QuantityType, ConstantType, NumexprFunction, ParameterFunctionType, VariabilityEnum, PythonFunction
from interface.objects import DataProcessDefinition, DataProduct, DataProducer
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient

from pyon.public import PRED

from nose.plugins.attrib import attr

import numpy as np
import unittest

@attr('INT',group='dm')
@unittest.skip('Not ready yet')
class TestTransformPrime(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml') # Because hey why not?!

        self.dataset_management      = DatasetManagementServiceClient()
        self.data_process_management = DataProcessManagementServiceClient()
        self.pubsub_management       = PubsubManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()


    def _create_proc_def(self):
        dpd_obj = DataProcessDefinition(
            name='Optimus',
            description='It\'s a transformer',
            module='ion.processes.data.transforms.transform_prime',
            class_name='TransformPrime')
        return self.data_process_management.create_data_process_definition(dpd_obj)
    

    def _L0_pdict(self):

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1900'
        t_ctxt.fill_value = -9999
        t_ctxt_id = self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump(), parameter_type='quantity<int64>', unit_of_measure=t_ctxt.uom)

        lat_ctxt = ParameterContext('lat', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))))
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = -9999
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='lat', parameter_context=lat_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=lat_ctxt.uom)

        lon_ctxt = ParameterContext('lon', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))))
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = -9999
        lon_ctxt_id = self.dataset_management.create_parameter_context(name='lon', parameter_context=lon_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=lon_ctxt.uom)


        temp_ctxt = ParameterContext('TEMPWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        temp_ctxt.uom = 'deg_C'
        temp_ctxt.fill_value = -9999
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='TEMPWAT_L0', parameter_context=temp_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=temp_ctxt.uom)

        # Conductivity - values expected to be the decimal results of conversion from hex
        cond_ctxt = ParameterContext('CONDWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        cond_ctxt.uom = 'S m-1'
        cond_ctxt.fill_value = -9999
        cond_ctxt_id = self.dataset_management.create_parameter_context(name='CONDWAT_L0', parameter_context=cond_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=cond_ctxt.uom)

        # Pressure - values expected to be the decimal results of conversion from hex
        press_ctxt = ParameterContext('PRESWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')))
        press_ctxt.uom = 'dbar'
        press_ctxt.fill_value = -9999
        press_ctxt_id = self.dataset_management.create_parameter_context(name='PRESWAT_L0', parameter_context=press_ctxt.dump(), parameter_type='quantity<float32>', unit_of_measure=press_ctxt.uom)


        context_ids = [t_ctxt_id, lat_ctxt_id, lon_ctxt_id, temp_ctxt_id, cond_ctxt_id, press_ctxt_id]

        pdict_id = self.dataset_management.create_parameter_dictionary('L0 SBE37', parameter_context_ids=context_ids, temporal_context='time')

        return pdict_id


    def _L1_pdict(self):
        pdict_id = self._L0_pdict()
        param_context_ids = self.dataset_management.read_parameter_contexts(pdict_id,id_only=True)


        # TEMPWAT_L1 = (TEMPWAT_L0 / 10000) - 10
        tl1_func = '(TEMPWAT_L0 / 10000) - 10'
        tl1_pmap = {'TEMPWAT_L0':'TEMPWAT_L0'}
        func = NumexprFunction('TEMPWAT_L1', tl1_func, tl1_pmap)
        tempL1_ctxt = ParameterContext('TEMPWAT_L1', param_type=ParameterFunctionType(function=func), variability=VariabilityEnum.TEMPORAL)
        tempL1_ctxt.uom = 'deg_C'

        tempL1_ctxt_id = self.dataset_management.create_parameter_context(name=tempL1_ctxt.name, parameter_context=tempL1_ctxt.dump(), parameter_type='pfunc', unit_of_measure=tempL1_ctxt.uom)
        param_context_ids.append(tempL1_ctxt_id)

        # CONDWAT_L1 = (CONDWAT_L0 / 100000) - 0.5
        cl1_func = '(CONDWAT_L0 / 100000) - 0.5'
        cl1_pmap = {'CONDWAT_L0':'CONDWAT_L0'}
        func = NumexprFunction('CONDWAT_L1', cl1_func, cl1_pmap)
        condL1_ctxt = ParameterContext('CONDWAT_L1', param_type=ParameterFunctionType(function=func), variability=VariabilityEnum.TEMPORAL)
        condL1_ctxt.uom = 'S m-1'
        condL1_ctxt_id = self.dataset_management.create_parameter_context(name=condL1_ctxt.name, parameter_context=condL1_ctxt.dump(), parameter_type='pfunc', unit_of_measure=condL1_ctxt.uom)
        param_context_ids.append(condL1_ctxt_id)
                

        # Equation uses p_range, which is a calibration coefficient - Fixing to 679.34040721
        #   PRESWAT_L1 = (PRESWAT_L0 * p_range / (0.85 * 65536)) - (0.05 * p_range)
        pl1_func = '(PRESWAT_L0 * 679.34040721 / (0.85 * 65536)) - (0.05 * 679.34040721)'
        pl1_pmap = {'PRESWAT_L0':'PRESWAT_L0'}
        func = NumexprFunction('PRESWAT_L1', pl1_func, pl1_pmap)
        presL1_ctxt = ParameterContext('PRESWAT_L1', param_type=ParameterFunctionType(function=func), variability=VariabilityEnum.TEMPORAL)
        presL1_ctxt.uom = 'S m-1'
        presL1_ctxt_id = self.dataset_management.create_parameter_context(name=presL1_ctxt.name, parameter_context=presL1_ctxt.dump(), parameter_type='pfunc', unit_of_measure=presL1_ctxt.uom)
        param_context_ids.append(presL1_ctxt_id)

        # Density & practical salinity calucluated using the Gibbs Seawater library - available via python-gsw project:
        #       https://code.google.com/p/python-gsw/ & http://pypi.python.org/pypi/gsw/3.0.1

        # PRACSAL = gsw.SP_from_C((CONDWAT_L1 * 10), TEMPWAT_L1, PRESWAT_L1)
        owner = 'gsw'
        sal_func = 'SP_from_C'
        sal_arglist = [NumexprFunction('CONDWAT_L1*10', 'C*10', {'C':'CONDWAT_L1'}), 'TEMPWAT_L1', 'PRESWAT_L1']
        sal_kwargmap = None
        func = PythonFunction('PRACSAL', owner, sal_func, sal_arglist, sal_kwargmap)
        sal_ctxt = ParameterContext('PRACSAL', param_type=ParameterFunctionType(func), variability=VariabilityEnum.TEMPORAL)
        sal_ctxt.uom = 'g kg-1'

        sal_ctxt_id = self.dataset_management.create_parameter_context(name=sal_ctxt.name, parameter_context=sal_ctxt.dump(), parameter_type='pfunc', unit_of_measure=sal_ctxt.uom)
        param_context_ids.append(sal_ctxt_id)

        # absolute_salinity = gsw.SA_from_SP(PRACSAL, PRESWAT_L1, longitude, latitude)
        # conservative_temperature = gsw.CT_from_t(absolute_salinity, TEMPWAT_L1, PRESWAT_L1)
        # DENSITY = gsw.rho(absolute_salinity, conservative_temperature, PRESWAT_L1)
        owner = 'gsw'
        abs_sal_func = PythonFunction('abs_sal', owner, 'SA_from_SP', ['PRACSAL', 'PRESWAT_L1', 'lon','lat'], None)
        cons_temp_func = PythonFunction('cons_temp', owner, 'CT_from_t', [abs_sal_func, 'TEMPWAT_L1', 'PRESWAT_L1'], None)
        dens_func = PythonFunction('DENSITY', owner, 'rho', [abs_sal_func, cons_temp_func, 'PRESWAT_L1'], None)
        dens_ctxt = ParameterContext('DENSITY', param_type=ParameterFunctionType(dens_func), variability=VariabilityEnum.TEMPORAL)
        dens_ctxt.uom = 'kg m-3'

        dens_ctxt_id = self.dataset_management.create_parameter_context(name=dens_ctxt.name, parameter_context=dens_ctxt.dump(), parameter_type='pfunc', unit_of_measure=dens_ctxt.uom)
        param_context_ids.append(dens_ctxt_id)

        pdict_id = self.dataset_management.create_parameter_dictionary('L1_SBE37', parameter_context_ids=param_context_ids, temporal_context='time')
        return pdict_id


    def _stream_definition(self,name,pdict_id):
        return self.pubsub_management.create_stream_definition(name, parameter_dictionary_id=pdict_id)


    def _data_product(self, name, stream_def):
        tdom, sdom = time_series_domain()
        dp_obj = DataProduct(name=name, description='blah', spatial_domain=sdom.dump(), temporal_domain=tdom.dump())
        dp_id = self.data_product_management.create_data_product(dp_obj, stream_def)
        return dp_id

    def _data_process(self, proc_def_id, input_products, output_product, stream_def):
        fake_producer = DataProducer(name='fake_producer')

        fake_producer_id, _  = self.container.resource_registry.create(fake_producer)

        self.data_process_management.assign_stream_definition_to_data_process_definition(stream_def,proc_def_id,binding='output')
        
        data_process_id = self.data_process_management.create_data_process(proc_def_id, input_products, {'output':output_product})
        self.container.resource_registry.create_association(subject=data_process_id, predicate=PRED.hasDataProducer, object=fake_producer_id)

        self.data_process_management.activate_data_process(data_process_id)

    def _fake_producer(self):
        if not hasattr(self, 'producer'):
            self.fake_producer_id,_ = self.container.resource_registry.create(DataProducer(name='fake_producer'))
        return self.fake_producer_id



    def _publisher(self, data_product_id):
        stream_ids, _ = self.container.resource_registry.find_resources(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        stream_id = stream_ids[0]

        route = self.pubsub_management.read_stream_route(stream_id)

        publisher = StandaloneStreamPublisher(stream_id, route)
        return publisher


    def test_prime(self):
        proc_def_id = self._create_proc_def()

        incoming_pdict_id = self._L0_pdict()
        outgoing_pdict_id = self._L1_pdict()

        incoming_stream_def_id = self._stream_definition('L0_stream_def', incoming_pdict_id)
        outgoing_stream_def_id = self._stream_definition('L1_stream_def', outgoing_pdict_id)

        L0_data_product_id = self._data_product('L0_SBE37', incoming_stream_def_id)
        L1_data_product_id = self._data_product('L1_SBE37', outgoing_stream_def_id)

        self._data_process(proc_def_id, [L0_data_product_id], L1_data_product_id, outgoing_stream_def_id)

        publisher = self._publisher(L0_data_product_id)
        
        rdt = RecordDictionaryTool(stream_definition_id=incoming_stream_def_id)
        rdt['time'] = np.arange(20)
        rdt['lat'] = 40.992469
        rdt['lon'] = -71.727069
        rdt['TEMPWAT_L0'] = np.arange([35]* 20)
        #rdt['CONDWAT_L0'] = np.arange(

        publisher.publish(rdt.to_granule())
    

        
