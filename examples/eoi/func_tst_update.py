__author__ = 'cmueller'

from pyon.public import IonObject
from examples.eoi.func_tst_base import *
from ion.agents.eoi.handler.dap_external_data_handler import DapExternalDataHandler
from datetime import datetime, timedelta

if __name__ == '__main__':

    ret = get_dataset(HFR_LOCAL)

    dsh = DapExternalDataHandler(ret[EXTERNAL_DATA_PROVIDER], ret[DATA_SOURCE], ret[EXTERNAL_DATA_SET], ret[DAP_DS_DESC])

    td=timedelta(days=-1)
    edt=datetime.utcnow()
    sdt=edt+td

    req_obj = IonObject("ExternalDataRequest", name="test_request")
    req_obj.start_time = sdt
    req_obj.end_time = edt
    req_obj.bbox["lower_left_x"] = 40
    req_obj.bbox["lower_left_y"] = -80
    req_obj.bbox["upper_right_x"] = 50
    req_obj.bbox["upper_right_y"] = -70

    print req_obj

    print "Commented out dsh.acquire_data_old --> this func_tst superseded by unit/int tests"
#    resp = dsh.acquire_data_old(req_obj)
#    print resp
