#!/usr/bin/env python
'''
ion/services/dm/utility/secondary_calibrations.py

Utility class for managing secondary calibrations for a data product
'''
import re

from pyon.public import RT
from pyon.core.exception import NotFound, BadRequest
from pyon.container.cc import Container
from interface.objects import ParameterContext
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

class SecondaryCalibrations(object):
    def __init__(self, data_product_management=None, dataset_management=None):
        self.data_product_management = data_product_management or DataProductManagementServiceClient()
        self.dataset_management = dataset_management or DatasetManagementServiceClient()
        self.resource_registry = Container.instance.resource_registry

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

    def _level(self, parameter):
        '''
        Returns the _l1 or _l2 if it's an L1 or L2,
        otherwise None
        '''
        if parameter.ooi_short_name.lower().endswith('_l1'):
            return '_l1'
        elif parameter.ooi_short_name.lower().endswith('_l2'):
            return '_l2'
        
        return None
    
    def initialize_parameters(self, parameters, data_product_id):
        for param_name, param_def in parameters.iteritems():
            param = ParameterContext(name=param_name, **param_def)
            param_id = self.dataset_management.create_parameter(param)
            self.data_product_management.add_parameter_to_data_product(param_id, data_product_id)


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
        level = self._level(parameter)
        if not level:
            raise BadRequest("Parameter lacks a DPS level")

        pf_id = self.find_polyval_calibration()
        if not pf_id:
            raise BadRequest("Parameter function for polyval_calibration is not defined")

        prefix = dps_code.lower() + level

        parameters = {
            prefix + 'b_pd_cals' : {
                'description':'%s Post-Deployment Calibration Coefficients' % parameter.name,
                'display_name':'%s Post-Deployment Calibration Coefficients' % parameter.display_name,
                'units':'1',
                'parameter_type':'sparse',
                'value_encoding':'float32,float32,float32,float32,float32'
            },
            prefix + 'b_pd' : {
                "description":'%s Post-Deployment Secondary Calibration' % parameter.name,
                "display_name":'%s Post-Deployment Secondary Calibration' % parameter.display_name,
                "units":'1',
                "parameter_type":'function',
                "parameter_function_id":pf_id,
                "parameter_function_map":{
                    'coefficients': prefix + 'b_pd_cals',
                    'x' : parameter_name
                }
            },
            prefix + 'b_start' : {
                "description":'%s Secondary Calibration Start' % parameter.name,
                "display_name":'%s Secondary Calibration Start' % parameter.display_name,
                "units":'seconds since 1900-01-01',
                "parameter_type":'sparse',
                "value_encoding":'float64'
            }
        }

        self.initialize_parameters(parameters, data_product_id)


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
        level = self._level(parameter)
        if not level:
            raise BadRequest("Parameter lacks a DPS level")

        pf_id = self.find_polyval_calibration()
        if not pf_id:
            raise BadRequest("Parameter function for polyval_calibration is not defined")
        
        prefix = dps_code.lower() + level

        parameters = {
            prefix + 'b_pr_cals' : {
                'description':'%s Post-Recovery Calibration Coefficients' % parameter.name,
                'display_name':'%s Post-Recovery Calibration Coefficients' % parameter.display_name,
                'units':'1',
                'parameter_type':'sparse',
                'value_encoding':'float32,float32,float32,float32,float32'
            },
            prefix + 'b_pr' : {
                "description":'%s Post-Recovery Secondary Calibration' % parameter.name,
                "display_name":'%s Post-Recovery Secondary Calibration' % parameter.display_name,
                "units":'1',
                "parameter_type":'function',
                "parameter_function_id":pf_id,
                "parameter_function_map":{
                    'coefficients': prefix + 'b_pr_cals',
                    'x' : parameter_name
                }
            },
            prefix + 'b_end' : {
                "description":'%s Secondary Calibration End' % parameter.name,
                "display_name":'%s Secondary Calibration End' % parameter.display_name,
                "units":'seconds since 1900-01-01',
                "parameter_type":'sparse',
                "value_encoding":'float64'
            }
        }

        self.initialize_parameters(parameters, data_product_id)

    def add_post_interpolated(self, data_product_id, parameter_name):
        '''
        Adds the post-recovery calibration parameter

        E.g. 
        add_post_deployment(ctd_parsed_data_product_id, seawater_temperature)
        
        Creates a new parameter
        'tempwat_l1b_interp'
        '''

        dps_code, parameter = self._get_base_parameter(data_product_id, parameter_name)
        level = self._level(parameter)
        if not level:
            raise BadRequest("Parameter lacks a DPS level")
        pf_id = self.find_interpolate()
        if not pf_id:
            raise NotFound("secondary interpolation function not defined as a parameter function")
        prefix = dps_code.lower() + level
        parameters = {
            prefix + 'b_interp' : {
                "description":'%s Secondary Calibration Interpolated' % parameter.name,
                "display_name":'%s Secondary Calibration Interpolated' % parameter.display_name,
                "units":'1',
                "parameter_type":'function',
                "parameter_function_id":pf_id,
                "parameter_function_map":{
                    'x' : 'time',
                    'range0' : dps_code.lower() + '_l1b_pd',
                    'range1' : dps_code.lower() + '_l1b_pr',
                    'starts' : dps_code.lower() + '_l1b_start',
                    'ends' : dps_code.lower() + '_l1b_end'
                }
            }
        }
        self.initialize_parameters(parameters, data_product_id)


    def find_interpolate(self):
        '''
        Finds the interpolation parameter function
        '''
        pfs, _ = self.resource_registry.find_resources(name='interpolate', restype=RT.ParameterFunction)
        for pf in pfs:
            if pf.function == 'secondary_interpolation' and pf.owner == 'ion_functions.data.interpolation':
                return pf._id
        return None

    def find_identity(self):
        '''
        Finds the identity parameter function
        '''
        pfs, _ = self.resource_registry.find_resources(name='identity', restype=RT.ParameterFunction)
        for pf in pfs:
            if pf.function == 'identity' and pf.owner == 'ion_functions.data.interpolation':
                return pf._id
        return None

    def find_polyval_calibration(self):
        '''
        Finds the polyval_calibration parameter function
        '''
        pfs, _ = self.resource_registry.find_resources(name='polyval_calibration', restype=RT.ParameterFunction)
        for pf in pfs:
            if pf.function == 'polyval_calibration' and pf.owner == 'ion_functions.data.interpolation':
                return pf._id
        return None
