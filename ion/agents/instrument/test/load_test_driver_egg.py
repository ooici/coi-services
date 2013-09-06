
from ion.agents.instrument.driver_process import DriverProcessType
from ion.agents.instrument.driver_process import ZMQEggDriverProcess
import sys

# A seabird driver.
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.1.5-py2.7.egg'
DRV_SHA_0_1_1 = '28e1b59708d72e008b0aa68ea7392d3a2467f393'
DRV_SHA_0_1_5 = '51ce182316f4f9dce336a76164276cb4749b77a5'
DRV_SHA = DRV_SHA_0_1_5
DRV_MOD = 'mi.instrument.seabird.sbe37smb.ooicore.driver'
DRV_CLS = 'SBE37Driver'
WORK_DIR = '/tmp/'

# Driver config.
DVR_CONFIG = {
    'dvr_egg' : DRV_URI,
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS,
    'workdir' : WORK_DIR,
    'process_type' : None
}

def load_egg():
    # Dynamically load the egg into the test path
    launcher = ZMQEggDriverProcess(DVR_CONFIG)
    egg = launcher._get_egg(DRV_URI)
    from hashlib import sha1
    with open(egg,'r') as f:
        doc = f.read()
        sha = sha1(doc).hexdigest()
        if sha != DRV_SHA:
            raise ImportError('Failed to load driver %s: incorrect checksum.  (%s!=%s)' % (DRV_URI, DRV_SHA, sha))
        if not egg in sys.path: sys.path.insert(0, egg)
        DVR_CONFIG['process_type'] = (DriverProcessType.EGG,)

    return DVR_CONFIG

def load_repo(mi_repo):
    if not mi_repo in sys.path: sys.path.insert(0, mi_repo)
    DVR_CONFIG['process_type'] = (DriverProcessType.PYTHON_MODULE,)
    DVR_CONFIG['mi_repo'] = mi_repo
    return DVR_CONFIG

