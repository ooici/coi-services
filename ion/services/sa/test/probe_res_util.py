#!/usr/bin/env python

"""
@package ion.services.sa.test.prob_res_util
@file ion/services/sa/test/prob_res_util.py
@author Edward Hunter
@brief Interactive utility to query resources and associations to generate ground truth for
activation integration test cases. Output files formatted for cut and paste confluence expand blocks.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'


def probe_res(container=None, restype='', predicate=''):
    """
    @param container: The cabability container.
    @param resource_type: Type of resources to examine.
    """

    f = open('%s_resource.txt' % restype,'w')

    rr = container.resource_registry
    res = rr.find_resources(restype=restype)
    for x in res[0]:
        id = x['_id']
        name = x['name']
        sub_assoc = rr.find_associations(subject=id)
        obj_assoc = rr.find_associations(object=id)

        f.write('# {expand: %s resource %s}\n' % (restype, name))
        f.write('{code:xml}\n')
        f.write(str(x)+'\n')
        f.write('{code}\n')
        f.write('# {expand}\n')
        f.write('#* Subect Associations:\n')
        for y in sub_assoc:
            f.write('#* {expand: %s >> %s >> %s}\n' % (restype, y['p'], y['ot']))
            f.write('{code:xml}\n')
            z = rr.read(y['o'])
            f.write(str(z)+'\n')
            f.write('{code}\n')
            f.write('{expand}\n')
        f.write('#* Object Associations:\n')
        for y in obj_assoc:
            f.write('#* {expand: %s >> %s >> %s}\n' % (y['st'], y['p'], restype))
            f.write('{code:xml}\n')
            z = rr.read(y['s'])
            f.write(str(z)+'\n')
            f.write('{code}\n')
            f.write('{expand}\n')
        f.write('\n\n\n')
    f.close()
