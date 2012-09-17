'''
This module dumps parameter context objects to a yml,param_context_defs.yml, using the method,
dump_param_contexts_to_yml().

After the parameter contexts are dumped to their yml, one can use, the method,
get_param_dict(param_dict_name), to load the ParameterDictionary object as laid out in
param_dict_defs.yml.

Usage:

from ion.util.parameter_yaml_IO import dump_param_contexts_to_yml, get_param_dict
dump_param_contexts_to_yml()

pdict = get_param_dict('sample_param_dict')
pdict_as_python_dictionary = pdict.dump()

'''

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
import yaml
from pyon.util.log import log
from pyon.util.containers import DotDict
import numpy as np

def build_contexts():
    '''
    Builds the relevant parameter context objects
    '''

    contexts = []

    cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.float32))
    cond_ctxt.uom = 'unknown'
    cond_ctxt.fill_value = 0e0
    contexts.append(cond_ctxt)

    pres_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=np.float32))
    pres_ctxt.uom = 'Pascal'
    pres_ctxt.fill_value = 0x0
    contexts.append(pres_ctxt)

    sal_ctxt = ParameterContext('salinity', param_type=QuantityType(value_encoding=np.float32))
    sal_ctxt.uom = 'PSU'
    sal_ctxt.fill_value = 0x0
    contexts.append(sal_ctxt)

    temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.float32))
    temp_ctxt.uom = 'degree_Celsius'
    temp_ctxt.fill_value = 0e0
    contexts.append(temp_ctxt)

    t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
    t_ctxt.reference_frame = AxisTypeEnum.TIME
    t_ctxt.uom = 'seconds since 1970-01-01'
    t_ctxt.fill_value = 0x0
    contexts.append(t_ctxt)

    lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
    lat_ctxt.reference_frame = AxisTypeEnum.LAT
    lat_ctxt.uom = 'degree_north'
    lat_ctxt.fill_value = 0e0
    contexts.append(lat_ctxt)

    lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
    lon_ctxt.reference_frame = AxisTypeEnum.LON
    lon_ctxt.uom = 'degree_east'
    lon_ctxt.fill_value = 0e0
    contexts.append(lon_ctxt)

    depth_ctxt = ParameterContext('depth', param_type=QuantityType(value_encoding=np.float32))
    depth_ctxt.reference_frame = AxisTypeEnum.HEIGHT
    depth_ctxt.uom = 'meters'
    depth_ctxt.fill_value = 0e0
    contexts.append(depth_ctxt)

    bin_ctxt = ParameterContext('binary', param_type=QuantityType(value_encoding=np.uint8))
    bin_ctxt.uom = 'unknown'
    bin_ctxt.fill_value = 0x0
    contexts.append(bin_ctxt)


    return contexts

def dump_param_contexts_to_yml():

    # Dumping the param context defs in the file below
    param_context_defs_file = "res/config/param_context_defs.yml"

    contexts = build_contexts()

    out_dict = {}
    for param_context in contexts:
        out_dict[param_context.name] = param_context.dump()

    yml_body = yaml.dump(out_dict)

    with open(param_context_defs_file, "w") as f:
        f.write(yml_body)

def get_param_dict(param_dict_name = None):

    # Dumping the param context defs in the file below
    param_context_defs_file = "res/config/param_context_defs.yml"
    param_dict_defs_file = "res/config/param_dict_defs.yml"

    with open(param_dict_defs_file, "r") as f_dict:
        dict_string = f_dict.read()
    with open(param_context_defs_file, "r") as f_ctxt:
        ctxt_string = f_ctxt.read()

    # look at param dict yml
    pdict_dict = yaml.load(dict_string)

    # load each parameter context based on name
    context_names = pdict_dict[param_dict_name]

    param_context_dict = yaml.load(ctxt_string)

    # validate that the context names mentioned in the parameter dictionary def in yml are correct
    for name in context_names:
        if not param_context_dict.has_key(name):
            raise AssertionError('The parameter dict has a context that does not exist in the parameter context defs specified in yml: %s' % name)

    pdict = ParameterDictionary()

    for ctxt_name in context_names:
        param_context = ParameterContext.load(param_context_dict[ctxt_name])
        pdict.add_context(param_context)

    return pdict
    

