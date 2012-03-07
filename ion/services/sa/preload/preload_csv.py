
import os
import sys
import csv

from pyon.public import RT, PRED #, LCS
from pyon.core.exception import BadRequest #, NotFound
from pyon.core.bootstrap import IonObject 

import requests, json
#from functools import wraps

TAG_FIELD = "Scenarios"
TAG_DELIM = " " 

SC_MODULE = "StreamContainer_module"
SC_METHOD = "StreamContainer_method"

DP_STREAMDEF = "StreamDefinitionId"
DP_KEYWORDS_DELIM = ";"

PR_DEF = "data_process_definition_id"
PR_INPUT = "input_data_product_id"
PR_OUTPUT = "output_data_product_id"

class PreloadCSV(object):
    
    def __init__(self, hostname, port, pubsub_client, dataproduct_client, log_obj):
        """
        need hostname and port of service gateway
        """
        self.host = hostname
        self.port = port
        self.pubsub_client = pubsub_client #todo: hack working around pubsub+service GW probs
        self.dataproduct_client = dataproduct_client
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

        self.log.info("Got these resources: \n%s\n" % str(resource_ids))

    def _get_id(self, row, resource_type, resource_ids):
        if not "ID" in row:
            raise BadRequest("ID not defined in %s row" % resource_type)

        friendly_id = row["ID"]
        
        if "" == friendly_id:
            raise BadRequest("ID field was blank")
        
        if friendly_id in resource_ids[resource_type]:
            raise BadRequest("Duplicate ID '%s' in %s CSV file" % (friendly_id, resource_type))

        return friendly_id
        
        

    def preload_stream_definitions(self, resource_ids, stream_definition_csv, tag):

        with open(stream_definition_csv, "rb") as csvfile:
            reader = self._get_csv_reader(csvfile)

            resource_ids[RT.StreamDefinition] = {}
            for row in reader:
                for x in [SC_MODULE, SC_METHOD, "name", "description"]:
                    if not x in row:
                        raise BadRequest("%s not defined for StreamContainer row" % x)

                friendly_id = self._get_id(row, RT.StreamDefinition, resource_ids)

                row, tag_matched = self._check_tag(row, tag)
                if not tag_matched:
                    resource_ids[RT.StreamDefinition][friendly_id] = None
                    continue

                # get module and function by name specified in strings
                module = __import__(row[SC_MODULE], fromlist=[SC_METHOD])
                creator_func = getattr(module, row[SC_METHOD])

                container = creator_func()
                
                if "service gateway disabled because container can't be jsonified":
                    response = self.pubsub_client.create_stream_definition(container=container,
                                                                           name=row["name"],
                                                                           description=row["description"])
                    resource_ids[RT.StreamDefinition][friendly_id] = response
                else:
                    #build payload
                    post_data = self._service_request_template()
                    post_data['serviceRequest']['serviceName'] = "pubsub_management"
                    post_data['serviceRequest']['serviceOp'] = "create_stream_definition"
                    post_data['serviceRequest']['params']["container"] = container
                    post_data['serviceRequest']['params']["name"] = row["name"]
                    post_data['serviceRequest']['params']["description"] = row["description"]

                    response = self._do_service_call("pubsub_management",
                                                     "create_stream_definition",
                                                     post_data)

                    resource_ids[RT.StreamDefinition][friendly_id] = response
                    
        return resource_ids



    def preload_data_products(self, resource_ids, data_product_csv, tag):
        with open(data_product_csv, "rb") as csvfile:
            reader = self._get_csv_reader(csvfile)

            resource_ids[RT.DataProduct] = {}
            stream_def_id = None
            for row in reader:
                #pick up stream def and delete from row
                if DP_STREAMDEF in row:
                    if "" != row[DP_STREAMDEF]:
                        stream_def_id = row[DP_STREAMDEF]
                    del row[DP_STREAMDEF]

                friendly_id = self._get_id(row, RT.StreamDefinition, resource_ids)
                del row["ID"]
                
                # process tagging
                row, tag_matched = self._check_tag(row, tag)
                if not tag_matched:
                    resource_ids[RT.DataProduct][friendly_id] = None
                    continue

                #keywords is a dict
                if "keywords" in row:
                    k = {}
                    for v in row["keywords"].split(DP_KEYWORDS_DELIM):
                        k["fixme_%d" % len(k)] = v
                    row["keywords"] = k
                
                #load the rest of the params into a dict
                resource_type_params = {}
                iobj = IonObject(RT.DataProduct)
                for key, value in row.iteritems():
                    resource_type_params[key] = value
                    setattr(iobj, key, value)

                if "service gateway disabled because 'keywords' has a problem":
                    response = self.dataproduct_client.create_data_product(data_product=iobj)
                    resource_ids[RT.DataProduct][friendly_id] = response
                else:
                    #build payload
                    post_data = self._service_request_template()
                    post_data['serviceRequest']['serviceName'] = "data_product_management"
                    post_data['serviceRequest']['serviceOp'] = "create_data_product"
                    post_data['serviceRequest']['params']["data_product"] = [RT.DataProduct, resource_type_params]
                    if stream_def_id:
                        post_data['serviceRequest']['params']["stream_definition_id"] = stream_def_id

                    #self.log.debug(str(post_data))

                    response = self._do_service_call("data_product_management",
                                                     "create_data_product",
                                                     post_data)

                    resource_ids[RT.DataProduct][friendly_id] = response



        return resource_ids

    def preload_data_processes(self, resource_ids, data_process_csv, tag):

        with open(data_process_csv, "rb") as csvfile:
            reader = self._get_csv_reader(csvfile)

            resource_ids[RT.DataProcess] = {}
            for row in reader:
                for x in [PR_DEF, PR_INPUT]:
                    if not x in row:
                        raise BadRequest("%s not defined for DataProcess row" % x)

                friendly_id = self._get_id(row, RT.DataProcess, resource_ids)

                row, tag_matched = self._check_tag(row, tag)
                if not tag_matched:
                    resource_ids[RT.DataProcess][friendly_id] = None
                    continue

                # get any matching data products
                out_products = {}
                for f in sorted(row.keys()):
                    if f[:len(PR_OUTPUT)] == PR_OUTPUT:
                        v = row[f]
                        if "" != v:
                            self.log.debug("fake id for %s is %s" % (f, v))
                            real_id = resource_ids[RT.DataProduct][v]
                            out_products[f] = real_id

                        
                if None in out_products.values():
                    continue

                if False: #"service gateway disabled because container can't be jsonified":
                    #response = self.pubsub_client.create_stream_definition(container=container,
                    #                                                       name=row["name"],
                    #                                                       description=row["description"])
                    #resource_ids[RT.DataProcess][friendly_id] = response
                    pass
                else:
                    #build payload
                    post_data = self._service_request_template()
                    post_data['serviceRequest']['serviceName'] = "data_process_management"
                    post_data['serviceRequest']['serviceOp'] = "create_data_process"
                    post_data['serviceRequest']['params']["data_process_definition_id"] = resource_ids[RT.DataProcessDefinition][row[PR_DEF]]
                    post_data['serviceRequest']['params']["in_data_product_id"] = resource_ids[RT.DataProduct][row[PR_INPUT]]
                    post_data['serviceRequest']['params']["out_data_products"] = out_products

                    
                    self.log.debug("posting this:\n%s\n" % str(post_data))
                    response = self._do_service_call("data_process_management", 
                                                     "create_data_process",
                                                     post_data)


                    resource_ids[RT.DataProcess][friendly_id] = response
                    
        return resource_ids

    def preload_ingestion_configurations(self, resource_ids, ingestion_configuration_csv, tag):
        return resource_ids


    # to import a set of simple resources (non-special-cases)
    def preload_simple_resources(self, resource_ids, csv_files, tag):

        for c in csv_files:
            b = os.path.basename(c).split(".")[0]
            self.log.debug("%s\n" % b)

            with open(c, "rb") as csvfile:
                reader = self._get_csv_reader(csvfile)
                resource_ids[b] = self._preload_resources(b, reader, tag)
                
        return resource_ids
                                   

    # to import associations (depends on previously-created resources)
    def preload_associations(self, resource_ids, associations_csv_path, desired_tag):
        with open(associations_csv_path, "rb") as csvfile:
            associations_reader = self._get_csv_reader(csvfile)

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

                response # to avoid pyflake error

        return resource_ids

    def _check_tag(self, row, desired_tag):
        tag_matched = True

        if TAG_FIELD in row:
            #self.log.debug("TAG_FIELD '%s' found in row" % TAG_FIELD)
            # no desired tag means all
            if desired_tag:
                #self.log.debug("Matching tag field with desired='%s', candidates='%s'" % 
                #               (desired_tag, row[TAG_FIELD]))
                #tags are delimited, so split it up
                tags = row[TAG_FIELD].split(TAG_DELIM)

                if not desired_tag in tags:
                    # record the id as having been defined, but don't insert it
                    tag_matched = False

            #delete this field
            del row[TAG_FIELD]

        return row, tag_matched

    def _preload_resources(self, resource_type, reader, desired_tag, row_prep_fn=None):
        if not resource_type in self.lookup_svc:
            raise NotImplementedError("preloader doesn't have a service defined for '%s' resources" % resource_type)
        
        ids = {}
        resource_ids = {}
        resource_ids[resource_type] = ids
        for row in reader:

            friendly_id = self._get_id(row, resource_type, resource_ids)
            del row["ID"]

            #process the row if necessary
            if row_prep_fn:
                row = row_prep_fn(row)
                
            row, tag_matched = self._check_tag(row, desired_tag)
            if not tag_matched:
                ids[friendly_id] = None
                continue

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
            if not resource_type in self.lookup_pyname:
                raise NotImplementedError("The python name of a '%s' resource was not defined" % resource_type)
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

            RT.DataProcessDefinition:    "data_process_management",

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

            RT.DataProcessDefinition:    "data_process_definition",

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
                msg = "%s: %s" % (resp["data"]["GatewayError"]["Exception"], 
                                  resp["data"]["GatewayError"]["Message"])
                self.log.debug("gateway error! %s" % msg)
                self.log.debug("input was %s" % str(post_data))
                raise BadRequest(msg)
            else:
                msg = "Unknown error object: %s" % str(resp)
                self.log.debug("gateway error! %s" % msg)
                self.log.debug("input was %s" % str(post_data))
                raise BadRequest(msg)


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


