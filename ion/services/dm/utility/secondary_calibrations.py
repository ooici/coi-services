#!/usr/bin/env python
'''
ion/services/dm/utility/secondary_calibrations.py

Utility class for managing secondary calibrations for a data product
'''
import re

from pyon.core.exception import NotFound, BadRequest
from interface.objects import ParameterContext

from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

class SecondaryCalibrations(object):
    def __init__(self, data_product_management=None, dataset_management=None):
        self.data_product_management = data_product_management or DataProductManagementServiceClient()
        self.dataset_management = dataset_management or DatasetManagementServiceClient()

    def _get_base_parameter(self, data_product_id, parameter_name):
        # Get the parameter
        parameters = self.data_product_management.get_data_product_parameters(data_product_id)
        parameter_obj = None
        for parameter in parameters:
            if parameter.name == parameter_name:
                parameter_obj = parameter
                break

        if parameter_obj is None: # Still None
            raise NotFound("Data product %s doesn't have a parameter %s" % (data_product_id, parameter_name))

        # Get the DPS Code
        dps_code = parameter.ooi_short_name
        if not dps_code:
            raise BadRequest("Parameter %s (%s) has no defined data product code" % (parameter.name, parameter._id))

        # Make the dps_code + _l1b_pd

        if re.match(r'.*_L[0-2]', dps_code):
            print "matches"
            dps_code = dps_code[:-3]
        return dps_code, parameter

    def add_post_deployment(self, data_product_id, parameter_name):
        '''
        Adds the post-deployment calibration parameter

        E.g. 
        add_post_deployment(ctd_parsed_data_product_id, seawater_temperature)
        
        Creates a new parameter
        'tempwat_l1b_pd'
        'tempwat_l1b_start'
        '''
        dps_code, parameter = self._get_base_parameter(data_product_id, parameter_name)
        new_name = dps_code.lower() + '_l1b_pd'

        print new_name

        parameter_context = ParameterContext(name=new_name,
                                             description='%s Post-Deployment Secondary Calibration' % parameter.name,
                                             display_name='%s Post-Deployment Secondary Calibration' % parameter.display_name,
                                             units='1',
                                             parameter_type='sparse',
                                             value_encoding='float32')
        
        parameter_context_id = self.dataset_management.create_parameter(parameter_context)
        self.data_product_management.add_parameter_to_data_product(parameter_context_id, data_product_id)

        new_name = dps_code.lower() + '_l1b_start'
        print new_name
        parameter_context = ParameterContext(name=new_name,
                                             description='%s Secondary Calibration Start' % parameter.name,
                                             display_name='%s Secondary Calibration Start' % parameter.display_name,
                                             units='seconds since 1900-01-01',
                                             parameter_type='sparse',
                                             value_encoding='float64')

        parameter_context_id = self.dataset_management.create_parameter(parameter_context)
        self.data_product_management.add_parameter_to_data_product(parameter_context_id, data_product_id)

    def add_post_recovery(self, data_product_id, parameter_name):
        '''
        Adds the post-recovery calibration parameter

        E.g. 
        add_post_deployment(ctd_parsed_data_product_id, seawater_temperature)
        
        Creates a new parameter
        'tempwat_l1b_pr'
        'tempwat_l1b_end'
        '''

        dps_code, parameter = self._get_base_parameter(data_product_id, parameter_name)
        new_name = dps_code.lower() + '_l1b_pr'

        print new_name

        parameter_context = ParameterContext(name=new_name,
                                             description='%s Post-Recovery Secondary Calibration' % parameter.name,
                                             display_name='%s Post-Recovery Secondary Calibration' % parameter.display_name,
                                             units='1',
                                             parameter_type='sparse',
                                             value_encoding='float32')

        parameter_context_id = self.dataset_management.create_parameter(parameter_context)
        self.data_product_management.add_parameter_to_data_product(parameter_context_id, data_product_id)
        
        new_name = dps_code.lower() + '_l1b_end'
        print new_name

        parameter_context = ParameterContext(name=new_name,
                                             description='%s Secondary Calibration End' % parameter.name,
                                             display_name='%s Secondary Calibration End' % parameter.display_name,
                                             units='seconds since 1900-01-01',
                                             parameter_type='sparse',
                                             value_encoding='float64')

        parameter_context_id = self.dataset_management.create_parameter(parameter_context)
        self.data_product_management.add_parameter_to_data_product(parameter_context_id, data_product_id)

    def add_post_interpolated(self, data_product_id, parameter_name):
        '''
        Adds the post-recovery calibration parameter

        E.g. 
        add_post_deployment(ctd_parsed_data_product_id, seawater_temperature)
        
        Creates a new parameter
        'tempwat_l1b_interp'
        '''

        dps_code, parameter = self._get_base_parameter(data_product_id, parameter_name)
        new_name = dps_code.lower() + '_l1b_interp'
        print new_name
        parameter_context = ParameterContext(name=new_name,
                                             description='%s Secondary Calibration Interpolated' % parameter.name,
                                             display_name='%s Secondary Calibration Interpolated' % parameter.display_name,
                                             units='1',
                                             parameter_type='sparse',
                                             value_encoding='float32')

        parameter_context_id = self.dataset_management.create_parameter(parameter_context)
        self.data_product_management.add_parameter_to_data_product(parameter_context_id, data_product_id)


