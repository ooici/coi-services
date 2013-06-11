#!/usr/bin/env python

version = '0.2'

short_name = 'a'

constructor_name = 'DriverA'

versioned_module_name = 'driver_%s_%s' % (short_name, version.replace('.','_').replace('-','_'))

entry_point_group = 'drivers.platform.%s' % short_name
versioned_constructor = 'driver-%s = %s.FILENAME:%s' % (version,
                                               versioned_module_name,
                                               constructor_name)

setup_kwargs = {
    'name' : versioned_module_name,
    'version' : version,
    'author' : 'Edward Hunter',
    'author_email' : 'edwardhunter@ucsd.edu',
    'description' : 'Platform driver A version 0.2',
    'license' : 'Apache 2.0',
    'keywords' : ['ooici','ioncore', 'pyon', 'coi'],
    'url' : 'https://github.com/ooici/coi-services'
}


