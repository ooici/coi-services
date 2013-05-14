"""
for a list of deployment IDs,
generate a dict of dicts with information about the deployments
suitable for the UI table:
 { deployment_id: { 'ui_column': 'string_value'... } }
"""

from ooi.logging import log
from pyon.ion.resource import RT, PRED, LCS, OT

def describe_deployments(deployments, context):
    if not deployments:
        return {}
    rr=context.resource_registry
    deployment_ids = [ d._id for d in deployments ]
    descriptions = { d._id: { 'is_primary': 'False' } for d in deployments }

    # first get the all site and instrument objects
    site_ids = []
    objects,associations = rr.find_objects_mult(subjects=deployment_ids, id_only=False)
    for obj,assoc in zip(objects,associations):
        # if this is a hasDeployment association...
        if assoc.p == PRED.hasDeployment:
            id = assoc.s
            description = descriptions[id]

            # save site or device info in the description
            type = obj.type_
            if type==RT.InstrumentSite or type==RT.PlatformSite:
                description['site_id'] = obj._id
                description['site_name'] = obj.name
                description['site_type'] = type
                if obj._id not in site_ids:
                    site_ids.append(obj._id)
            elif type==RT.InstrumentDevice or type==RT.PlatformDevice:
                description['device_id'] = obj._id
                description['device_name'] = obj.name
                description['device_type'] = type
            else:
                log.warn('unexpected association: %s %s %s %s %s', assoc.st, assoc.s, assoc.p, assoc.ot, assoc.o)

    # now look for hasDevice associations to determine which deployments are "primary" or "active"
    objects2,associations = rr.find_objects_mult(subjects=site_ids)
    for obj,assoc in zip(objects2,associations):
        if assoc.p==PRED.hasDevice:
            found_match = False
            for description in descriptions.itervalues():
                if description['site_id']==assoc.s and description['device_id']==assoc.o:
                    if found_match:
                        log.warn('more than one primary deployment for site %s (%s) and device %s (%s)',
                                 assoc.s, description['site_name'], assoc.o, description['device_name'])
                    description['is_primary']=found_match=True
    log.trace('%d deployments, %d associated sites/devices, %d activations', len(deployments), len(objects), len(objects2))
    return descriptions