
from pyon.public import CFG
from ion.agents.instrument.driver_process import DriverProcessType
from ion.agents.instrument.driver_process import ZMQEggDriverProcess
import sys

# Driver config.

def load_egg():
    # Dynamically load the egg into the test path

    dvr_config = make_config()
    dvr_egg = dvr_config['dvr_egg']
    dvr_sha = CFG.device.sbe37.dvr_sha
    launcher = ZMQEggDriverProcess(dvr_config)

    egg = launcher._get_egg(dvr_egg)
    from hashlib import sha1
    with open(egg,'r') as f:
        doc = f.read()
        #This does not work uniformly
        #sha = sha1(doc).hexdigest()
        #if sha != dvr_sha:
        #    raise ImportError('Failed to load driver %s: incorrect checksum.  (%s!=%s)' % (dvr_egg, dvr_sha, sha))
        if not egg in sys.path: sys.path.insert(0, egg)
        dvr_config['process_type'] = (DriverProcessType.EGG,)

    return dvr_config

def load_repo(mi_repo):

    dvr_config = make_config()
    if not mi_repo in sys.path: sys.path.insert(0, mi_repo)
    dvr_config['process_type'] = (DriverProcessType.PYTHON_MODULE,)
    dvr_config['mi_repo'] = mi_repo
    return dvr_config


def make_config():

    dvr_config = {
        'dvr_egg' : CFG.device.sbe37.dvr_egg,
        'dvr_mod' : CFG.device.sbe37.dvr_mod,
        'dvr_cls' : CFG.device.sbe37.dvr_cls,
        'workdir' : CFG.device.sbe37.workdir,
        'process_type' : None
    }
    return dvr_config
