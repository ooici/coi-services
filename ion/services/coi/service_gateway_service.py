#!/usr/bin/env python

from interface.services.coi.iresource_registry_service import IResourceRegistryService

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from flask import Flask, request
from gevent.wsgi import WSGIServer
import inspect, json

from pyon.core.exception import NotFound, Inconsistent
from pyon.container.cc import Container
from pyon.net.endpoint import ProcessRPCClient

from interface.services.coi.iservice_gateway_service import BaseServiceGatewayService

#Initialize the flask app
app = Flask(__name__)

#Retain a module level reference to the service class for use with Process RPC calls below
service_gateway_instance = None

DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 5000

#This class is used to manage the WSGI/Flask server as an ION process - and as a process endpoint for ION RPC calls
class ServiceGatewayService(BaseServiceGatewayService):

   # running_container = None
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
        parm_list = []
        method_args = inspect.getargspec(getattr(target_service,operation))
        for arg in method_args[0]:
            if arg == 'self': continue # skip self

            if not jsonParms:
                if request.args.has_key(arg):
                    parm_list.append(request.args[arg])  # should be fixed to convert to proper type when necessary; ie "True" -> True
                else:
                    parm_list.append('')  # should be fixed to pass the actual default value for the parameter
            else:
                parm_list.append(jsonParms['serviceRequest']['params'][arg])

        client = ProcessRPCClient(node=Container.instance.node, name=service_name, iface=target_service, process=service_gateway_instance)

        result = client.request({'method': operation, 'args': parm_list})
        ret = str(result)

    except Exception, e:
        ret =  "Error: %s" % e.message


    #Returns a string but this should probably be recoded to return json ( and json mime type
    return ret



#More RESTfull examples...should probably not use but here for example reference

#This example calls the resource registry with an id passed in as part of the URL
#http://hostname:port/ion-service/rest/get/c1b6fa6aadbd4eb696a9407a39adbdc8
@app.route('/ion-service/rest/get/<resource_id>')
def get_resource(resource_id):
    ret = "get" + str(resource_id)

    client = ProcessRPCClient(node=Container.instance.node, name='resource_registry', iface=IResourceRegistryService, process=service_gateway_instance)
    if resource_id != '':
        try:
            res = client.read(resource_id)
            if not res:
                raise NotFound("No resource found for id: %s " % resource_id)
            ret = str(res)

        except Exception, e:
            ret =  "Error: %s" % e

    return ret

#Example restful call to a client function for another service like
#http://hostname:port/ion-service/rest/create
@app.route('/ion-service/rest/create')
def create_accounts():
    from examples.bank.bank_client import run_client
    try:
        run_client(Container.instance, process=service_gateway_instance)
        return list_resources("BankAccount")

    except Exception, e:
        ret =  "Error: %s" % e

#Example operation to return a list of resources of a specific type like
#http://hostname:port/ion-service/rest/list/BankAccount
@app.route('/ion-service/rest/list/<resource_type>')
def list_resources(resource_type):

    ret = "list"

    client = ProcessRPCClient(node=Container.instance.node, name='resource_registry', iface=IResourceRegistryService, process=service_gateway_instance)

    try:
        reslist,_ = client.find_resources(restype=resource_type)
        str_list = []
        for res in reslist:
            str_list.append(str(res) + "<BR>")

        ret = ''.join(str_list)
    except Exception, e:
        ret =  "Error: %s" % e


    return ret



