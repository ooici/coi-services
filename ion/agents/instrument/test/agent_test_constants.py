
# Agent parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.agents.instrument.instrument_agent'
IA_CLS = 'InstrumentAgent'


# these defintions will be referenced by other tests
#  404: bad URI; BAD: driver will launch but commands will fail; GOOD: launch and commanding will succeed
DRV_URI_BAD  = "http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.1a-py2.7.egg"
DRV_URI_404  = "http://sddevrepo.oceanobservatories.org/releases/completely_made_up_404.egg"

