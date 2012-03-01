
import os
import sys
import csv

from pyon.public import RT, PRED #, LCS
from pyon.core.exception import BadRequest #, NotFound

import requests, json
#from functools import wraps

TAG_FIELD = "Scenarios"
TAG_DELIM = " " 


class PreloadCSV(object):
    
    def __init__(self, hostname, port, log_obj):
        """
        need hostname and port of service gateway
        """
        self.host = hostname
        self.port = port
        self.log = log_obj

        #cache 2 dicts
        self.lookup_svc = self._get_svc_by_resource()
        self.lookup_pyname = self._get_python_by_yaml()


    # anything ending in _csv is a path.  ending in _csvs means list of paths
    def preload(self, 
                stream_definition_csv, 
                simple_resource_csvs, 
                data_product_csv,
                data_process_csv,
                ingestion_configuration_csv,
                associations_csv, 
                tag=None):

        resource_ids = {}

        resource_ids = self.preload_stream_definitions(resource_ids, stream_definition_csv, tag)
        resource_ids = self.preload_simple_resources(resource_ids, simple_resource_csvs, tag)
        resource_ids = self.preload_data_products(resource_ids, data_product_csv, tag)
        resource_ids = self.preload_data_processes(resource_ids, data_process_csv, tag)
        resource_ids = self.preload_ingestion_configurations(resource_ids, ingestion_configuration_csv, tag)
        resource_ids = self.preload_associations(resource_ids, associations_csv, tag)


    def preload_stream_definitions(self, resource_ids, stream_defintion_csv, tag):
        return resource_ids

    def preload_data_products(self, resource_ids, data_product_csv, tag):
        return resource_ids

    def preload_data_processes(self, resource_ids, data_process_csv, tag):
        return resource_ids

    def preload_ingestion_configurations(self, resource_ids, ingestion_configuration_csv, tag):
        return resource_ids


    # to import a set of simple resources (non-special-cases)
    def preload_simple_resources(self, resource_ids, csv_files, tag):

        for c in csv_files:
            b = os.path.basename(c).split(".")[0]
            #sys.stderr.write("%s\n" % b)

            with open(c, "rb") as csvfile:
                reader = self._get_csv_reader(csvfile)
                resource_ids[b] = self._preload_resources(b, reader, tag)
                
        return resource_ids
                                   

    # to import associations (depends on previously-created resources)
    def preload_associations(self, resource_ids, associations_csv_path, tag):
        with open(associations_csv_path, "rb") as csvfile:
            associations_reader = self._get_csv_reader(csvfile)
            self._preload_associations(resource_ids, associations_reader, tag)

        return resource_ids




    
    # process a csv full of associations.    # TODO: NOT TESTED YET
    def _preload_associations(self, resource_ids, associations_reader, desired_tag):
        for row in associations_reader:
            valuesonly = []
            for f in associations_reader.fieldnames:
                valuesonly.append(row[f])

            row = valuesonly
            if not 5 <= len(row) < 7: 
                #5 fields are necessary, if there are 6 we'll ignore it.  7 we assume error
                raise BadRequest("Wrong number of fields in associations CSV!")
            
            # 5 fields expected: InstrumentDevice,#1,hasModel,InstrumentModel,#32
            #sys.stderr.write("\n\n%s\n\n" % str(row))
            subj_type = row[0]
            subj_id   = row[1]
            pred      = row[2]
            obj_type  = row[3]
            obj_id    = row[4]

            #make sure types exist
            if not subj_type in resource_ids:
                raise BadRequest("Can't associate a '%s' because none were loaded" % subj_type)
            if not subj_id in resource_ids[subj_type]:
                raise BadRequest("Can't associate a '%s' with ID '%s' because it was never defined" % (subj_type, subj_id))
            if not obj_type in resource_ids:
                raise BadRequest("Can't associate a '%s' because none were loaded" % obj_type)
            if not obj_id in resource_ids[obj_type]:
                raise BadRequest("Can't associate a '%s' with ID '%s' because it was never defined" % (obj_type, obj_id))
            if not pred in PRED:
                raise BadRequest("Unknown association type '%s'" % pred)

            # with tags, it's possible that the user is not loading all resources.
            # in that case, the resource ID will be None
            if desired_tag:
                if not resource_ids[subj_type][subj_id]:
                    continue
                if not resource_ids[obj_type][obj_id]:
                    continue

            # generate the service method that we need
            associate_op = self.call_for_association(subj_type, pred, obj_type)
            
            py = self.lookup_pyname


            #build payload
            post_data = self._service_request_template()
            post_data['serviceRequest']['serviceName'] = self.lookup_svc[subj_type]
            post_data['serviceRequest']['serviceOp'] = associate_op
            post_data['serviceRequest']['params']["%s_id" % py[subj_type]] = resource_ids[subj_type][subj_id]
            post_data['serviceRequest']['params']["%s_id" % py[obj_type]] = resource_ids[obj_type][obj_id]

            response = self._do_service_call(self.lookup_svc[subj_type], 
                                             associate_op,
                                             post_data)

            response

           

    def _preload_resources(self, resource_type, reader, desired_tag):
        if not resource_type in self.lookup_svc:
            raise NotImplementedError("'%s' is not a recognized resource type" % resource_type)
        
        ids = {}
        for row in reader:
            if not "ID" in row:
                raise BadRequest("ID field missing from first column of %s.csv" % resource_type)
            
            #store ID and delete it
            friendly_id = row["ID"]
            del row["ID"]            

            # filtering by scenario

            if TAG_FIELD in row:
                self.log.debug("TAG_FIELD found in row")
                # no desired tag means all
                if desired_tag:
                    self.log.debug("Matching tag field with desired='%s', candidates='%s'" % 
                                   (desired_tag, row[TAG_FIELD]))
                    #tags are delimited, so split it up
                    tags = row[TAG_FIELD].split(TAG_DELIM)
                    
                    if not desired_tag in tags:
                        # record the id as having been defined, but don't insert it
                        ids[friendly_id] = None
                        continue

                #delete this field
                del row[TAG_FIELD]
                    

            #load the rest of the params into a dict
            resource_type_params = {}
            for key, value in row.iteritems():
                resource_type_params[key] = value

            # generate the service method we need
            resource_py = self.lookup_pyname[resource_type]
            create_op = "create_%s" % resource_py

            #special syntax for ionobjects: [resource_type, dict]
            post_data = self._service_request_template()
            post_data['serviceRequest']['serviceName'] = self.lookup_svc[resource_type]
            post_data['serviceRequest']['serviceOp'] = create_op
            post_data['serviceRequest']['params'][resource_py] = [resource_type, resource_type_params]


            # make the call 
            response = self._do_service_call(self.lookup_svc[resource_type], 
                                             create_op,
                                             post_data)


            ids[friendly_id] = response

        return ids




    def _get_csv_reader(self, csvfile):
            #determine type of csv
            dialect = csv.Sniffer().sniff(csvfile.read(1024))
            csvfile.seek(0)
            return csv.DictReader(csvfile, dialect=dialect)



    # get the service responsible for a given resource
    def _get_svc_by_resource(self):
        return {
            RT.InstrumentAgent:          "instrument_management",
            RT.InstrumentAgentInstance:  "instrument_management",
            RT.InstrumentModel:          "instrument_management",
            RT.InstrumentDevice:         "instrument_management",
            RT.PlatformAgent:            "instrument_management",
            RT.PlatformAgentInstance:    "instrument_management",
            RT.PlatformModel:            "instrument_management",
            RT.PlatformDevice:           "instrument_management",
            RT.SensorModel:              "instrument_management",
            RT.SensorDevice:             "instrument_management",
            
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

        return "assign_%s_to_%s" % (self.lookup_pyname[obj_type], self.lookup_pyname[subj_type])
        

    # actually execute a service call with the given params
    def _do_service_call(self, service, method, post_data):
        url = "http://%s:%s/ion-service/%s/%s" % (self.host, self.port, service, method)
        service_gateway_call = requests.post(
            url,
            data={'payload': json.dumps(post_data)}
            )

        if service_gateway_call.status_code != 200:
            raise BadRequest("The service gateway returned the following error: %d" % service_gateway_call.status_code)

        # debug lines
        #sys.stderr.write(str(url) + "\n")
        #sys.stderr.write(str(post_data['serviceRequest']) + "\n")

        resp = json.loads(service_gateway_call.content)

        #sys.stderr.write(str(resp) + "\n")

        if "GatewayResponse" not in resp["data"]:
            if "GatewayError" in resp["data"]:
                raise BadRequest("%s: %s" % (resp["data"]["GatewayError"]["Exception"], 
                                             resp["data"]["GatewayError"]["Message"]))
            else:
                raise BadRequest("Unknown error object: %s" % str(resp))


        return resp["data"]["GatewayResponse"]

    def _service_request_template(self):
        return {
            'serviceRequest': {
                'serviceName': '', 
                'serviceOp': '',
                'params': {
                    #'object': [] # Ex. [BankObject, {'name': '...'}] 
                    }
                }
            }


