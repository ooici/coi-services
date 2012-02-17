
import os
import sys
import csv

from pyon.public import RT, PRED #, LCS
from pyon.core.exception import BadRequest #, NotFound

import requests, json
#from functools import wraps

SERVICE_REQUEST_TEMPLATE = {
    'serviceRequest': {
        'serviceName': '', 
        'serviceOp': '',
        'params': {
            #'object': [] # Ex. [BankObject, {'name': '...'}] 
            }
        }
    }

class PreloadCSV(object):
    
    def __init__(self, hostname, port):
        """
        need hostname and port of service gateway
        """
        self.host = hostname
        self.port = port

        #cache 2 dicts
        self.lookup_svc = self._get_svc_by_resource()
        self.lookup_pyname = self._get_python_by_yaml()


    # actual action function to call
    def preload(self, csv_files):
        associations_reader = None
        resource_ids = {}
        for c in csv_files:
            b = os.path.basename(c).split(".")[0]

            #determine type of csv
            csvfile = open(c, "rb")
            dialect = csv.Sniffer().sniff(csvfile.read(1024))
            csvfile.seek(0)
            reader = csv.DictReader(csvfile, dialect=dialect)


            #determine type of resource
            if "associations" == b.lower():
                #save for later
                associations_reader = reader
            else:
                #enter now and save ids
                resource_ids[b] = self._preload_resources(b, reader)

        #now that all resources are in, do the associations
        if associations_reader:
            self._preload_associations(resource_ids, associations_reader)
                



    # get the service responsible for a given resource
    def _get_svc_by_resource(self):
        return {
            RT.InstrumentAgent:          "instrument_management_service",
            RT.InstrumentAgentInstance:  "instrument_management_service",
            RT.InstrumentModel:          "instrument_management_service",
            RT.InstrumentDevice:         "instrument_management_service",
            RT.PlatformAgent:            "instrument_management_service",
            RT.PlatformAgentInstance:    "instrument_management_service",
            RT.PlatformModel:            "instrument_management_service",
            RT.PlatformDevice:           "instrument_management_service",
            RT.SensorModel:              "instrument_management_service",
            RT.SensorDevice:             "instrument_management_service",
            
            RT.MarineFacility:           "marine_facility_management",
            RT.Site:                     "marine_facility_management",
            RT.LogicalPlatform:          "marine_facility_management",
            RT.LogicalInstrument:        "marine_facility_management",

            RT.DataProduct:              "data_product_management",
            RT.DataProducer:             "data_acquisition_management",

            }
        
    # get the pythonic (some_widget) name of a yaml (SomeWidget) name
    def _get_python_by_yaml(self):
        return {
            RT.InstrumentAgent:          "instrument_agent",
            RT.InstrumentAgentInstance:  "instrument_agent_instance",
            RT.InstrumentModel:          "instrument_model",
            RT.InstrumentDevice:         "instrument_device",
            RT.PlatformAgent:            "platform_agent",
            RT.PlatformAgentInstance:    "platform_agent_instance",
            RT.PlatformModel:            "platform_model",
            RT.PlatformDevice:           "platform_device",
            RT.SensorModel:              "sensor_model",
            RT.SensorDevice:             "sensor_device",
            
            RT.MarineFacility:           "marine_facility",
            RT.Site:                     "site",
            RT.LogicalPlatform:          "logical_platform",
            RT.LogicalInstrument:        "logical_instrument",

            RT.DataProduct:              "data_product",
            RT.DataProducer:             "data_producer",

            }            

    # generate the service call for a given association operaton
    #  like assign_instrument_model_to_instrument_device
    def call_for_association(self, subj_type, pred, obj_type):
        
        #TODO: any nonstandard assignment methods get detected here
        #if ("Site", "hasChildSite", "Site") == (subj_type, pred, obj_type):
        #    return "assign.... etc

        return "assign_%s_to_%s" % (self.lookup_pyname[subj_type], self.lookup_pyname[obj_type])
        

    # actually execute a service call with the given params
    def _do_service_call(self, service, method, post_data):
        url = "http://%s:%s/ion-service/%s/%s" % (self.host, self.port, service, method)
        service_gateway_call = requests.post(
            url,
            data={'payload': json.dumps(post_data)}
            )

        if service_gateway_call.status_code != 200:
            raise BadRequest("The service gateway returned the following error: %d" % service_gateway_call.status_code)

        # TODO: these are for debug. take them out.
        sys.stderr.write(str(url) + "\n")
        sys.stderr.write(str(post_data['serviceRequest']) + "\n")

        return json.loads(service_gateway_call.content)

    
    # process a csv full of associations.
    # TODO: NOT TESTED YET
    def _preload_associations(self, resource_ids, associations_reader):
        for row in associations_reader:
            row = [x[1] for x in row.iteritems()]
            if not 5 <= len(row) < 7: 
                #5 fields are necessary, if there are 6 we'll ignore it.  7 we assume error
                raise BadRequest("Wrong number of fields in associations CSV!")
            
            # 5 fields expected: InstrumentDevice,#1,hasModel,InstrumentModel,#32
            subj_type = x[0]
            subj_id   = x[1]
            pred      = x[2]
            obj_type  = x[3]
            obj_id    = x[4]

            #make sure types exist
            if not subj_type in resource_ids:
                raise BadRequest("Can't associate a '%s' because none were loaded" % subj_type)
            if not subj_id in resource_ids[subj_type]:
                raise BadRequest("Can't associate a '%s' with ID '%s' because it was never defined" % subj_type, subj_id)
            if not obj_type in resource_ids:
                raise BadRequest("Can't associate a '%s' because none were loaded" % obj_type)
            if not obj_id in resource_ids[obj_type]:
                raise BadRequest("Can't associate a '%s' with ID '%s' because it was never defined" % obj_type, obj_id)
            if not pred in PRED:
                raise BadRequest("Unknown association type '%s'" % pred)

            # generate the service method that we need
            associate_op = self.call_for_association(subj_type, pred, obj_type)
            
            py = self.lookup_pyname

            #build dict
            resource_type_params = {}
            for key, value in row.iteritems():
                resource_type_params[key] = value

            #build payload
            post_data = SERVICE_REQUEST_TEMPLATE
            post_data['serviceRequest']['serviceName'] = self.lookup_svc[subj_type]
            post_data['serviceRequest']['serviceOp'] = associate_op
            post_data['serviceRequest']['params'][py[subj_type]] = subj_id
            post_data['serviceRequest']['params'][py[obj_type]] = obj_id

            response = self._do_service_call(self.lookup_svc[subj_type], 
                                             associate_op,
                                             post_data)

            response

           

    def _preload_resources(self, resource_type, reader):
        if not resource_type in self.lookup_svc:
            raise NotImplementedError("'%s' is not a recognized resource type" % resource_type)
        
        ids = {}
        for row in reader:
            if not "ID" in row:
                raise BadRequest("ID field missing from first column of %s.csv" % resource_type)
            
            #store ID and delete it
            friendly_id = row["ID"]
            del row["ID"]            

            #load the rest of the params into a dict
            resource_type_params = {}
            for key, value in row.iteritems():
                resource_type_params[key] = value

            # generate the service method we need
            resource_py = self.lookup_pyname[resource_type]
            create_op = "create_%s" % resource_py

            #special syntax for ionobjects: [resource_type, dict]
            post_data = SERVICE_REQUEST_TEMPLATE
            post_data['serviceRequest']['serviceName'] = self.lookup_svc[resource_type]
            post_data['serviceRequest']['serviceOp'] = create_op
            post_data['serviceRequest']['params'][resource_py] = [resource_type, resource_type_params]

            # make the call 
            response = self._do_service_call(self.lookup_svc[resource_type], 
                                             create_op,
                                             post_data)

            sys.stderr.write(str(response) + "\n")

            # TODO: properly parse this!!
            ids[friendly_id] = response

        return ids

