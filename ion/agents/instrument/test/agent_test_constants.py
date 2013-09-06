
# Work dir and logger delimiter.
WORK_DIR = '/tmp/'
DELIM = ['<<','>>']

# Agent parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.agents.instrument.instrument_agent'
IA_CLS = 'InstrumentAgent'

# A seabird driver.
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.1.5-py2.7.egg'
DRV_SHA_0_1_1 = '28e1b59708d72e008b0aa68ea7392d3a2467f393'
DRV_SHA_0_1_5 = '51ce182316f4f9dce336a76164276cb4749b77a5'
DRV_SHA = DRV_SHA_0_1_5
DRV_MOD = 'mi.instrument.seabird.sbe37smb.ooicore.driver'
DRV_CLS = 'SBE37Driver'

# these defintions will be referenced by other tests
#  404: bad URI; BAD: driver will launch but commands will fail; GOOD: launch and commanding will succeed
DRV_URI_GOOD = DRV_URI
DRV_URI_BAD  = "http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.1a-py2.7.egg"
DRV_URI_404  = "http://sddevrepo.oceanobservatories.org/releases/completely_made_up_404.egg"

# Driver config.
# DVR_CONFIG['comms_config']['port'] is set by the setup.
DVR_CONFIG = {
    'dvr_egg' : DRV_URI,
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS,
    'workdir' : WORK_DIR,
    'process_type' : None
}
