#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from flask import Flask, request, jsonify
from gevent.wsgi import WSGIServer
import inspect, json, simplejson, collections

from pyon.public import AT, RT, IonObject, Container, ProcessRPCClient
from pyon.core.exception import NotFound, Inconsistent

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



        #probably need to specify the host name and port in configuration file and need to figure out how to redirect HTTP logging to a file
        self.start_service(self.server_hostname,self.server_port)

    def on_quit(self):
        self.stop_service()


    def start_service(self, hostname=DEFAULT_WEB_SERVER_HOSTNAME, port=DEFAULT_WEB_SERVER_PORT):
        """Responsible for starting the gevent based web server."""

        if self.http_server != None:
            self.stop_service()

        self.http_server = WSGIServer((hostname, port), app)
        self.http_server.start()

        return True

    def stop_service(self):
        """Responsible for stopping the gevent based web server."""
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


    #Retrieve service definition
    from pyon.service import service
    target_service = service.get_service_by_name(service_name)

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

    try:

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
            if arg == 'self': continue # skip self

            if not jsonParms:
                if request.args.has_key(arg):
                    parm_list[arg] = convert_unicode(request.args[arg])  # should be fixed to convert to proper type when necessary; ie "True" -> True
            else:
                if jsonParms['serviceRequest']['params'].has_key(arg):
                    if isinstance(jsonParms['serviceRequest']['params'][arg], list):
                        # For some reason, UNICODE strings are not supported with ION objects
                        ion_object_name = convert_unicode(jsonParms['serviceRequest']['params'][arg][0])
                        object_parms = convert_unicode(jsonParms['serviceRequest']['params'][arg][1])

                        parm_list[arg] = IonObject(ion_object_name, object_parms)
                    else:
                        parm_list[arg] = convert_unicode(jsonParms['serviceRequest']['params'][arg])

        client = target_client(node=Container.instance.node, process=service_gateway_instance)
        methodToCall = getattr(client, operation)
        result = methodToCall(**parm_list)


        ret = simplejson.dumps(result, default=ion_object_encoder)

    except Exception, e:
        ret =  "Error: %s" % e.message


    return jsonify(data=ret)

def ion_object_encoder(obj):
    return obj.__dict__

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


    resultSet = set()
    from pyon.core.object import IonObjectRegistry
    base_type_list = IonObjectRegistry.extended_objects

    #Look to see if a specific resource type has been specified - if not default to all
    if request.args.has_key('type'):
        resultSet = set(base_type_list.get(request.args['type'])) if base_type_list.get(request.args['type']) is not None else set()
    else:
        for res in base_type_list:
            ext_types = base_type_list.get(res)
            for res2 in ext_types:
                resultSet.add(res2)

    ret_list = []
    for res in sorted(resultSet):
        ret_list.append(res)

    return jsonify(data=ret_list)


#More RESTfull examples...should probably not use but here for example reference

#This example calls the resource registry with an id passed in as part of the URL
#http://hostname:port/ion-service/resource/c1b6fa6aadbd4eb696a9407a39adbdc8
@app.route('/ion-service/rest/resource/<resource_id>')
def get_resource(resource_id):

    ret = None
    client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
    if resource_id != '':
        try:
            result = client.read(resource_id)
            if not result:
                raise NotFound("No resource found for id: %s " % resource_id)

            ret = simplejson.dumps(result, default=ion_object_encoder)

        except Exception, e:
            ret =  "Error: %s" % e

    return jsonify(data=ret)


#Example operation to return a list of resources of a specific type like
#http://hostname:port/ion-service/list_resources/BankAccount
@app.route('/ion-service/rest/list_resources/<resource_type>')
def list_resources_by_type(resource_type):

    ret = None

    client = ResourceRegistryServiceProcessClient(node=Container.instance.node, process=service_gateway_instance)
    try:
        res_list,_ = client.find_resources(restype=resource_type )
        result = []
        for res in res_list:
            result.append(res)

        ret = simplejson.dumps(result, default=ion_object_encoder)
    except Exception, e:
        ret =  "Error: %s" % e

    return jsonify(data=ret)

#Example restful call to a client function for another service like
#http://hostname:port/ion-service/run_bank_client
@app.route('/ion-service/run_bank_client')
def create_accounts():
    from examples.bank.bank_client import run_client
    run_client(Container.instance, process=service_gateway_instance)
    return list_resources_by_type("BankAccount")




