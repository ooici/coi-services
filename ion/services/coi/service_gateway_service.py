#!/usr/bin/env python



__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import inspect, collections, ast, simplejson, json
from flask import Flask, request
from gevent.wsgi import WSGIServer

from pyon.public import IonObject, Container, ProcessRPCClient
from pyon.core.exception import NotFound, Inconsistent
from pyon.core.registry import get_message_class_in_parm_type, getextends

from interface.services.coi.iservice_gateway_service import BaseServiceGatewayService
from interface.services.coi.iresource_registry_service import IResourceRegistryService, ResourceRegistryServiceProcessClient

#Initialize the flask app
app = Flask(__name__)

#Retain a module level reference to the service class for use with Process RPC calls below
service_gateway_instance = None

DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 5000

#This class is used to manage the WSGI/Flask server as an ION process - and as a process endpoint for ION RPC calls
class ServiceGatewayService(BaseServiceGatewayService):

    """
	The Service Gateway Service is the service that uses a gevent web server and Flask to bridge HTTP requests to AMQP RPC ION process service calls.
	
    """

    def on_init(self):

        #defaults
        self.http_server = None
        self.server_hostname = DEFAULT_WEB_SERVER_HOSTNAME
        self.server_port = DEFAULT_WEB_SERVER_PORT
        self.web_server_enabled = True
        self.logging = None

        #retain a pointer to this object for use in ProcessRPC calls
        global service_gateway_instance
        service_gateway_instance = self

        #get configuration settings if specified
        if 'web_server' in self.CFG:
            web_server_cfg = self.CFG['web_server']
            if web_server_cfg is not None:
                if 'hostname' in web_server_cfg:
                    self.server_hostname = web_server_cfg['hostname']
                if 'port' in web_server_cfg:
                    self.server_port = web_server_cfg['port']
                if 'enabled' in web_server_cfg:
                    self.web_server_enabled = web_server_cfg['enabled']
                if 'log' in web_server_cfg:
                    self.logging = web_server_cfg['log']

        #need to figure out how to redirect HTTP logging to a file
        if self.web_server_enabled:
            self.start_service(self.server_hostname,self.server_port)

    def on_quit(self):
        self.stop_service()


    def start_service(self, hostname=DEFAULT_WEB_SERVER_HOSTNAME, port=DEFAULT_WEB_SERVER_PORT):
        """Responsible for starting the gevent based web server."""

        if self.http_server is not None:
            self.stop_service()

        self.http_server = WSGIServer((hostname, port), app, log=self.logging)
        self.http_server.start()

        return True

    def stop_service(self):
        """Responsible for stopping the gevent based web server."""
        if self.http_server is not None:
            self.http_server.stop()
        return True

        

# This is a service operation for handling generic service operation calls with arguments passed as query string parameters; like this:
# http://hostname:port/ion-service/resource_registry/find_resources?restype=BankAccount&id_only=False
# Shows how to handle POST requests with the parameters passed in posted JSON object, though has both since it is easier to use a browser URL GET than a POST by hand.

# An example post of json data using wget
# wget -o out.txt --post-data 'payload={"serviceRequest": { "serviceName": "resource_registry", "serviceOp": "find_resources",
# "params": { "restype": "BankAccount", "lcstate": "", "name": "", "id_only": true } } }' http://localhost:5000/ion-service/resource_registry/find_resources
#
# Probably should make this service smarter to respond to the mime type in the request data ( ie. json vs text )
#
@app.route('/ion-service/<service_name>/<operation>', methods=['GET','POST'])
def process_gateway_request(service_name, operation):


    try:
        #Retrieve service definition
        from pyon.core.bootstrap import service_registry
        # MM: Note: service_registry can do more now
        target_service = service_registry.get_service_base(service_name)

        if not target_service:
            raise NotFound("Target service name not found in the URL")

        if operation == '':
            raise NotFound("Service operation not specified in the URL")


        #Find the concrete client class for making the RPC calls.
        target_client = None
        for name, cls in inspect.getmembers(inspect.getmodule(target_service),inspect.isclass):
            if issubclass(cls, ProcessRPCClient) and not name.endswith('ProcessRPCClient'):
                target_client = cls
                break

        if not target_client:
            raise NotFound("Service operation not correctly specified in the URL")

        jsonParms = None
        if request.method == "POST":
            payload = request.form['payload']
            #debug only
            #payload = '{"serviceRequest": { "serviceName": "resource_registry", "serviceOp": "find_resources", "params": { "restype": "BankAccount", "lcstate": "", "name": "", "id_only": false } } }'
            jsonParms = json.loads(payload)

            if jsonParms['serviceRequest']['serviceName'] != target_service.name:
                 raise Inconsistent("Target service name in the JSON request (%s) does not match service name in URL (%s)" % (str(jsonParms['serviceRequest']['serviceName']), target_service.name ) )

            if jsonParms['serviceRequest']['serviceOp'] != operation:
                 raise Inconsistent("Target service operation in the JSON request (%s) does not match service name in URL (%s)" % ( str(jsonParms['serviceRequest']['serviceOp']), operation ) )


        #Build parameter list - operation parameters must be in the proper order. Should replace this once unordered method invocation is
        # allowed in the container, but good enough for demonstration purposes.
        parm_list = {}
        method_args = inspect.getargspec(getattr(target_client,operation))
        for arg in method_args[0]:
            if arg == 'self' or arg == 'headers': continue # skip self and headers from being set

            if not jsonParms:
                if request.args.has_key(arg):
                    parm_type = get_message_class_in_parm_type(service_name, operation, arg)
                    if parm_type == 'str':
                        parm_list[arg] = convert_unicode(request.args[arg])
                    else:
                        parm_list[arg] = ast.literal_eval(convert_unicode(request.args[arg]))
            else:
                if jsonParms['serviceRequest']['params'].has_key(arg):
                    if isinstance(jsonParms['serviceRequest']['params'][arg], list):  #This if handles ION objects as a 2 element list: [Object Type, { field1: val1, ...}]
                        #TODO - Potentially remove these converisons whenever ION objects support unicode
                        # UNICODE strings are not supported with ION objects
                        ion_object_name = convert_unicode(jsonParms['serviceRequest']['params'][arg][0])
                        object_parms = convert_unicode(jsonParms['serviceRequest']['params'][arg][1])

                        new_obj = IonObject(ion_object_name)
                        #Iterate over the parameters to add to object; have to do this instead
                        #of passing a dict to get around restrictions in object creation on setting _id, _rev params
                        for parm in object_parms:
                            set_object_field(new_obj, parm, object_parms.get(parm))

                        new_obj._validate() # verify that all of the object fields were set with proper types
                        parm_list[arg] = new_obj
                    else:  # The else branch is for simple types ( non-ION objects )
                        parm_list[arg] = convert_unicode(jsonParms['serviceRequest']['params'][arg])

        client = target_client(node=Container.instance.node, process=service_gateway_instance)
        methodToCall = getattr(client, operation)
        result = methodToCall(**parm_list)

    except Exception, e:
        result =  "Error: %s" % e.message

    return json_response(result)

#Private implementation of standard flask jsonify to specify the use of an encoder to walk ION objects
def json_response(response_data):

    return app.response_class(simplejson.dumps({'data': response_data}, default=ion_object_encoder,
        indent=None if request.is_xhr else 2), mimetype='application/json')


#Use this function internally to recursively set sub object field values
def set_object_field(obj, field, field_val):
    if isinstance(field_val,dict):
        sub_obj = getattr(obj,field)
        for sub_field in field_val:
            set_object_field(sub_obj, sub_field, field_val.get(sub_field))
    else:
        setattr(obj, field, field_val)

#Used by json encoder
def ion_object_encoder(obj):
    return obj.__dict__

#Used to recursively convert unicode in JSON structures into proper data structures
def convert_unicode(data):
    if isinstance(data, unicode):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert_unicode, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert_unicode, data))
    else:
        return data

# This service method returns the list of registered resource objects sorted alphabetically. Optional query
# string parameter will filter by extended type  i.e. type=InformationResource. All registered objects
# will be returned if not filtered
#
# Examples:
# http://hostname:port/ion-service/list_resource_types
# http://hostname:port/ion-service/list_resource_types?type=InformationResource
# http://hostname:port/ion-service/list_resource_types?type=TaskableResource
#
@app.route('/ion-service/list_resource_types', methods=['GET','POST'])
def list_resource_types():


    try:
        #Look to see if a specific resource type has been specified - if not default to all
        if request.args.has_key('type'):
            resultSet = set(getextends(request.args['type'])) if getextends(request.args['type']) is not None else set()
        else:
            type_list = getextends('Resource')
            resultSet = set(type_list)

        ret_list = []
        for res in sorted(resultSet):
            ret_list.append(res)

        return json_response(ret_list)

    except Exception, e:
        ret =  "Error: %s" % e.message
        return json_response(ret)


#Returns a json object for a specified resource type with all default values.
@app.route('/ion-service/resource_type_schema/<resource_type>')
def get_resource_schema(resource_type):


    try:
        #ION Objects are not registered as UNICODE names
        ion_object_name = convert_unicode(resource_type)
        ret_obj = IonObject(ion_object_name, {})

        # If it's an op input param or response message object.
        # Walk param list instantiating any params that were marked None as default.
        if hasattr(ret_obj, "_svc_name"):
            schema = ret_obj._schema
            for field in ret_obj._schema:
                if schema[field]["default"] is None:
                    try:
                        value = IonObject(schema[field]["type"], {})
                    except NotFound:
                        # TODO
                        # Some other non-IonObject type.  Just use None as default for now.
                        value = None
                    setattr(ret_obj, field, value)

    except Exception, e:
        ret_obj =  "Error: %s" % e.message

    return json_response(ret_obj)


#More RESTfull examples...should probably not use but here for example reference

#This example calls the resource registry with an id passed in as part of the URL
#http://hostname:port/ion-service/resource/c1b6fa6aadbd4eb696a9407a39adbdc8
@app.route('/ion-service/rest/resource/<resource_id>')
def get_resource(resource_id):

    ret = None
    client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
    if resource_id != '':
        try:
            #Database object IDs are not unicode
            result = client.read(convert_unicode(resource_id))
            if not result:
                raise NotFound("No resource found for id: %s " % resource_id)

        except Exception, e:
            result =  "Error: %s" % e

    return json_response(result)


#Example operation to return a list of resources of a specific type like
#http://hostname:port/ion-service/find_resources/BankAccount
@app.route('/ion-service/rest/find_resources/<resource_type>')
def list_resources_by_type(resource_type):

    ret = None

    client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
    try:
        #Resource Types are not in unicode
        res_list,_ = client.find_resources(restype=convert_unicode(resource_type) )
        result = []
        for res in res_list:
            result.append(res)

    except Exception, e:
        result =  "Error: %s" % e

    return json_response(result)

#Example restful call to a client function for another service like
#http://hostname:port/ion-service/run_bank_client
@app.route('/ion-service/run_bank_client')
def create_accounts():
    from examples.bank.bank_client import run_client
    run_client(Container.instance, process=service_gateway_instance)
    return list_resources_by_type("BankAccount")




