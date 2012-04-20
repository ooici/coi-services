__author__ = 'cmueller'

from ion.agents.eoi.data_acquisition_management_service_Placeholder import *

EXTERNAL_DATA_PROVIDER = "ext_data_prov"
DATA_SOURCE = "data_src"
EXTERNAL_DATA_SET = "ext_data_set"
DAP_DS_DESC = "dap_ds_desc"

CWD = os.getcwd()

def get_dataset(x):
    damsP = DataAcquisitionManagementServicePlaceholder()
    dprov = damsP.create_external_data_provider(ds_id=x)
    dsrc = damsP.create_data_source(ds_id=x)
    dset = damsP.create_external_dataset(ds_id=x)
    dsdesc = damsP.create_dap_ds_desc(ds_id=x)

    if dsrc is None or dsdesc is None:
        raise Exception("invalid dataset specified: %s" % x)

    ret = {}
    ret[EXTERNAL_DATA_PROVIDER] = dprov
    ret[DATA_SOURCE] = dsrc
    ret[EXTERNAL_DATA_SET] = dset
    ret[DAP_DS_DESC] = dsdesc
    print ret[DAP_DS_DESC]
    return ret

if __name__ == '__main__':
    ret = get_dataset(AST2)
    dsh = DapExternalDataHandler(ret[EXTERNAL_DATA_PROVIDER], ret[DATA_SOURCE], ret[EXTERNAL_DATA_SET], ret[DAP_DS_DESC])
    print "Global Attrs: %s" % dsh.get_attributes()
    print "TimeVar Attrs: %s" % dsh.get_attributes(scope="time")


#    import pprint
#    pprint.pprint(dsh)
