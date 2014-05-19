#!/usr/bin/env python

__author__ = 'Michael Meisinger'
__license__ = 'Apache 2.0'

import collections, traceback, datetime, time, yaml
import flask, ast, pprint
from flask import Flask, request, abort, session, render_template
from gevent.wsgi import WSGIServer
import json

from pyon.core.exception import NotFound, Inconsistent, BadRequest, Unauthorized
from pyon.core.object import IonObjectBase
from pyon.core.registry import getextends, model_classes
from pyon.public import Container, StandaloneProcess, log, PRED, RT, IonObject, CFG
from pyon.util.containers import named_any
from ion.core.interaction_observer import InteractionObserver

from interface import objects


#Initialize the flask app
app = Flask(__name__, template_folder="static/templates")

DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 8080

containerui_instance = None

standard_types = ['str', 'int', 'bool', 'float', 'list', 'dict']
standard_resattrs = ['name', 'description', 'lcstate', 'availability', 'ts_created', 'ts_updated', 'alt_ids']
EDIT_IGNORE_FIELDS = ['rid', 'restype', 'lcstate', 'availability', 'ts_created', 'ts_updated']
EDIT_IGNORE_TYPES = ['list', 'dict', 'bool']
standard_eventattrs = ['origin', 'ts_created', 'description']
date_fieldnames = ['ts_created', 'ts_updated']


class ContainerUI(StandaloneProcess):
    """
    A simple Web UI to introspect the container and the ION datastores.
    """
    def on_init(self):

        self.http_server = None
        self.server_hostname = DEFAULT_WEB_SERVER_HOSTNAME
        self.server_port = self.CFG.get_safe('container.flask_webapp.port', DEFAULT_WEB_SERVER_PORT)
        self.web_server_enabled = True
        self.logging = None
        self.interaction_observer = None
        app.secret_key = self.__class__.__name__   # Enables sessions (for mscweb)

        #retain a pointer to this object for use in ProcessRPC calls
        global containerui_instance
        containerui_instance = self

        #Start the gevent web server unless disabled
        if self.web_server_enabled:
            self.start_service(self.server_hostname, self.server_port)

    def on_quit(self):
        if self.interaction_observer and self.interaction_observer.started:
            self.interaction_observer.stop()
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


# ----------------------------------------------------------------------------------------

@app.route('/', methods=['GET', 'POST'])
def process_index():
    try:
        from pyon.public import CFG
        from pyon.core.bootstrap import get_sys_name
        default_ds_server = CFG.get_safe("container.datastore.default_server", "postgresql")
        fragments = [
            "<h1>Welcome to Container Management UI</h1>",
            "<p><ul>",
            "<li><a href='/restypes'><b>Browse Resource Registry and Resource Objects</b></a>",
            "<ul>",
            "<li>Org: <a href='/list/Org'>Org</a>, <a href='/list/UserRole'>UserRole</a></li>",
            "<li>Users: <a href='/list/UserInfo'>UserInfo</a>, <a href='/list/ActorIdentity'>ActorIdentity</a>, <a href='/list/UserCredentials'>UserCredentials</a></li>",
            "<li>Observatory: <a href='/list/Observatory'>Observatory</a>, <a href='/list/Subsite'>Subsite</a>, <a href='/list/Deployment'>Deployment</a></li>",
            "<li>Platforms: <a href='/list/PlatformDevice'>PlatformDevice</a>, <a href='/list/PlatformSite'>PlatformSite</a>, <a href='/list/PlatformModel'>PlatformModel</a>, <a href='/list/PlatformAgent'>PlatformAgent</a>, <a href='/list/PlatformAgentInstance'>PlatformAgentInstance</a></li>",
            "<li>Instruments: <a href='/list/InstrumentDevice'>InstrumentDevice</a>, <a href='/list/InstrumentSite'>InstrumentSite</a>, <a href='/list/InstrumentModel'>InstrumentModel</a>, <a href='/list/InstrumentAgent'>InstrumentAgent</a>, <a href='/list/InstrumentAgentInstance'>InstrumentAgentInstance</a></li>",
            "<li>External Data: <a href='/list/ExternalDataset'>ExternalDataset</a>, <a href='/list/ExternalDataProvider'>ExternalDataProvider</a>, <a href='/list/ExternalDatasetModel'>ExternalDatasetModel</a>, <a href='/list/ExternalDatasetAgent'>ExternalDatasetAgent</a>, <a href='/list/ExternalDatasetAgentInstance'>ExternalDatasetAgentInstance</a></li>",
            "<li>Data: <a href='/list/DataProduct'>DataProduct</a>, <a href='/list/Dataset'>Dataset</a></li>",
            "<li>Coverage: <a href='/list/ParameterContext'>ParameterContext</a>, <a href='/list/ParameterDictionary'>ParameterDictionary</a>, <a href='/list/ParameterFunction'>ParameterFunction</a>, <a href='/list/StreamDefinition'>StreamDefinition</a></li>",
            "<li>Streaming: <a href='/list/DataProcessDefinition'>DataProcessDefinition</a>, <a href='/list/DataProcess'>DataProcess</a>, <a href='/list/DataProducer'>DataProducer</a>, <a href='/list/Stream'>Stream</a>, <a href='/list/Subscription'>Subscription</a></li>",
            "<li>Execution: <a href='/list/ProcessDefinition'>ProcessDefinition</a>, <a href='/list/Process'>Process</a>, <a href='/list/Service'>Service</a>, <a href='/list/ServiceDefinition'>ServiceDefinition</a>, <a href='/list/CapabilityContainer'>CapabilityContainer</a></li>",
            "<li>Exchange: <a href='/list/ExchangeSpace'>ExchangeSpace</a>, <a href='/list/ExchangePoint'>ExchangePoint</a>, <a href='/list/ExchangeName'>ExchangeName</a>, <a href='/list/ExchangeBroker'>ExchangeBroker</a></li>",
            "<li>Governance: <a href='/list/Commitment'>Commitment</a>, <a href='/list/Negotiation'>Negotiation</a>, <a href='/list/Policy'>Policy</a></li>",
            "</ul></li>",
            "<li><a href='/events'><b>Browse Events</b></a></li>",
            "<li><a href='/viewobj'><b>View Objects</b></a></li>",
            "<li><a href='/viewstate'><b>View Process State</b></a></li>",
            "<li><a href='/dir'><b>Browse ION Directory</b></a></li>",
            #"<li><a href='/mscweb'><b>Show system messages (MSCWeb)</b></a>",
            #"<ul>",
            #"<li><a href='/mscaction/stop'>Stop system message recording</a></li>",
            #"</ul></li>",
            "<li><a href='http://localhost:3000'><b>ION Web UI (if running)</b></a></li>",
            "<li><a href='http://" + CFG.get_safe("server.amqp.host") + ":55672/'><b>RabbitMQ Management UI V2.x (if running)</b></a></li>",
            "<li><a href='http://localhost:15672/'><b>RabbitMQ Management UI V3.x (if running)</b></a></li>",
            "<li><a href='http://localhost:9001/'><b>Supervisord UI (if running)</b></a></li>",
            "</ul></p>",
            "<h2>System and Container Properties</h2>",
            "<p><table>",
            "<tr><th>Property</th><th>Value</th></tr>",
            "<tr><td>system.name</td><td>%s</td></tr>" % get_sys_name(),
            "<tr><td>Broker</td><td>%s</td></tr>" % "%s:%s" % (CFG.server.amqp.host, CFG.server.amqp.port),
            "<tr><td>Datastore</td><td>%s</td></tr>" % "%s:%s" % (CFG.get_safe("server.%s.host" % default_ds_server), CFG.get_safe("server.%s.port" % default_ds_server)),
            "<tr><td>Container ID</td><td>%s</td></tr>" % Container.instance.id,
            "<tr><td>Read-Only</td><td>%s</td></tr>" % is_read_only(),
            "</table></p>",

            ]
        content = "\n".join(fragments)
        return build_page(content)

    except Exception, e:
        return build_error_page(traceback.format_exc())

# ----------------------------------------------------------------------------------------

@app.route('/restypes', methods=['GET','POST'])
def process_list_resource_types():
    try:
        type_list = set(getextends('Resource'))
        fragments = [
            build_standard_menu(),
            "<h1>List of Resource Types</h1>",
            "<p>",
        ]

        for restype in sorted(type_list):
            fragments.append("<a href='/list/%s'>%s</a>, " % (restype, restype))

        fragments.append("</p>")

        content = "\n".join(fragments)
        return build_page(content)

    except Exception, e:
        return build_error_page(traceback.format_exc())

# ----------------------------------------------------------------------------------------

@app.route('/alt/<namespace>/<alt_id>', methods=['GET'])
def process_alt_ids(namespace, alt_id):
    try:
        res_list, _ = Container.instance.resource_registry.find_resources_ext(alt_id_ns=namespace.encode('ascii'), alt_id=alt_id.encode('ascii'))
        fragments = [
            build_standard_menu(),
            "<h1>List of Matching Resources: %s</h1>" % alt_id,
            "<p>",
            "<table>",
        ]
        fragments.extend([
            "<thead>",
            "<tr>",
            "<th>ID</th>",
            "<th>Name</th>",
            "<th>Description</th>",
            "<th>Resource Type</th>",
            "<th>alt_ids</th>",
            "</tr>",
            "</thead>"])
        fragments.append("<tbody>")

        for res in res_list:
            fragments.append("<tr>")
            fragments.extend(build_table_alt_row(res))
            fragments.append("</tr>")
        fragments.append("</tbody>")

        fragments.append("</table></p>")
        fragments.append("<p>Number of resources: %s</p>" % len(res_list))

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception:
        return build_error_page(traceback.format_exc())
        
# ----------------------------------------------------------------------------------------

@app.route('/list/<resource_type>', methods=['GET','POST'])
def process_list_resources(resource_type):
    try:
        restype = str(resource_type)
        with_details = get_arg("details", "off") == "on"

        res_list,_ = Container.instance.resource_registry.find_resources(restype=restype)

        fragments = [
            build_standard_menu(),
            "<h1>List of '%s' Resources</h1>" % restype,
            build_command("Hide details" if with_details else "Show details", "/list/%s?details=%s" % (
                restype, "off" if with_details else "on")),
            build_command("New %s" % restype, "/new/%s" % restype),
            build_res_extends(restype),
            "<p>",
            "<table>",
            "<tr>"
        ]

        fragments.extend(build_table_header(restype))
        fragments.append("</tr>")

        for res in res_list:
            fragments.append("<tr>")
            fragments.extend(build_table_row(res, details=with_details))
            fragments.append("</tr>")

        fragments.append("</table></p>")
        fragments.append("<p>Number of resources: %s</p>" % len(res_list))

        content = "\n".join(fragments)
        return build_page(content)

    #except NotFound:
    #    return flask.redirect("/")
    except Exception, e:
        return build_error_page(traceback.format_exc())

def build_res_extends(restype):
    fragments = [
        "<p><i>Extends:</i> ",
    ]
    extendslist = [parent.__name__ for parent in _get_object_class(restype).__mro__ if parent.__name__ not in ['IonObjectBase','object']]
    for extend in extendslist:
        if extend != restype:
            fragments.append(build_link(extend, "/list/%s" % extend))
            fragments.append(", ")

    fragments.append("<br><i>Extended by:</i> ")
    for extends in sorted(getextends(restype)):
        if extends != restype:
            fragments.append(build_link(extends, "/list/%s" % extends))
            fragments.append(", ")

    fragments.append("</p>")

    return "".join(fragments)

def build_table_header(objtype):
    schema = _get_object_class(objtype)._schema
    fragments = []
    fragments.append("<th>ID</th>")
    for field in standard_resattrs:
        if field in schema:
            fragments.append("<th>%s</th>" % (field))
    for field in sorted(schema.keys()):
        if field not in standard_resattrs:
            fragments.append("<th>%s</th>" % (field))
    return fragments

def build_table_alt_row(obj):
    fragments = []
    fragments.extend([
        "<td><a href='/view/%s'>%s</a></td>" % (obj._id,obj._id),
        "<td>%s</td>" % obj.name,
        "<td>%s</td>" % obj.description,
        "<td><a href='/list/%s'>%s</a></td>" % (obj._get_type(),obj._get_type()),
        "<td>%s</td>" % obj.alt_ids])
    return fragments

def build_table_row(obj, details=True):
    schema = obj._schema
    fragments = []
    fragments.append("<td><a href='/view/%s'>%s</a></td>" % (obj._id,obj._id))
    for field in standard_resattrs:
        if field in schema:
            value = get_formatted_value(getattr(obj, field), fieldname=field, fieldtype=schema[field]["type"], details=True)
            fragments.append("<td>%s</td>" % (value))
    for field in sorted(schema.keys()):
        if field not in standard_resattrs:
            value = get_formatted_value(getattr(obj, field), fieldname=field, fieldtype=schema[field]["type"], brief=True, details=details)
            fragments.append("<td>%s</td>" % (value))
    return fragments

# ----------------------------------------------------------------------------------------

@app.route('/view/<resource_id>', methods=['GET','POST'])
def process_view_resource(resource_id):
    try:
        resid = str(resource_id)
        res = Container.instance.resource_registry.read(resid)
        restype = res._get_type()

        fragments = [
            build_standard_menu(),
            "<h1>View %s '%s'</h1>" % (build_type_link(restype), res.name),
            build_commands(resid, restype),
            "<h2>Fields</h2>",
            "<p>",
            "<table>",
            "<tr><th>Field</th><th>Type</th><th>Value</th></tr>"
            ]

        fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td>" % ("type", "str", restype))
        fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td>" % ("_id", "str", res._id))
        fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td>" % ("_rev", "str", res._rev))
        fragments.extend(build_nested_obj(res, ""))
        fragments.append("</p></table>")
        fragments.extend(build_associations(res._id))
        fragments.append("<h2>Recent Events</h2>")

        events_list = Container.instance.event_repository.find_events(origin=resid,
                        descending=True, limit=50)

        fragments.extend(build_events_table(events_list))
        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_error_page(traceback.format_exc())

def build_nested_obj(obj, prefix, edit=False):
    fragments = []
    schema = obj._schema
    for field in standard_resattrs:
        if field in schema:
            value = get_formatted_value(getattr(obj, field), fieldname=field, fieldtype=schema[field]["type"])
            if edit and field not in EDIT_IGNORE_FIELDS:
                fragments.append("<tr><td>%s%s</td><td>%s</td><td><input type='text' name='%s%s' value='%s' size='60'/></td>" % (prefix, field, schema[field]["type"], prefix, field, getattr(obj, field)))
            else:
                fragments.append("<tr><td>%s%s</td><td>%s</td><td>%s</td>" % (prefix, field, schema[field]["type"], value))
    for field in sorted(schema.keys()):
        if field not in standard_resattrs:
            value = getattr(obj, field)
            if schema[field]["type"] in model_classes or isinstance(value, IonObjectBase):
                value_type = value._get_type() if value else "None"
                # Nested object case
                fragments.append("<tr><td>%s%s</td><td>%s</td><td>%s</td>" % (prefix, field, schema[field]["type"], "[%s]" % value_type))
                if value:
                    fragments.extend(build_nested_obj(value, "%s%s." % (prefix,field), edit=edit))
            else:
                value = get_formatted_value(value, fieldname=field, fieldtype=schema[field]["type"])
                if edit and field not in EDIT_IGNORE_FIELDS and schema[field]["type"] not in EDIT_IGNORE_TYPES:
                    fragments.append("<tr><td>%s%s</td><td>%s</td><td><input type='text' name='%s%s' value='%s' size='60'/></td>" % (prefix, field, schema[field]["type"], prefix, field, getattr(obj, field)))
                else:
                    fragments.append("<tr><td>%s%s</td><td>%s</td><td>%s</td>" % (prefix, field, schema[field]["type"], value))
    return fragments

def build_associations(resid):
    fragments = list()

    fragments.append("<h2>Associations</h2>")
    fragments.append("<div id='chart'></div>")
    if CFG.get_safe('container.containerui.association_graph', True):
        #----------- Build the visual using javascript --------------#
        fragments.append("<script type='text/javascript' src='http://mbostock.github.com/d3/d3.v2.js'></script>   ")
        fragments.append("<script type='text/javascript' src='/static/tree-interactive.js'></script>")
        fragments.append("<script type='text/javascript'>build(\"%s\");</script>" % resid)
    #------------------------------------------------------------#
    fragments.append("<h3>FROM</h3>")
    fragments.append("<p><table>")
    fragments.append("<tr><th>Subject Type</th><th>Subject Name</th><th>Subject ID</th><th>Predicate</th><th>Command</th></tr>")
    obj_list, assoc_list = Container.instance.resource_registry.find_subjects(object=resid, id_only=False)
    iter_list = sorted(zip(obj_list, assoc_list), key=lambda x: [x[1].p, x[0].type_, x[0].name])
    for obj, assoc in iter_list:
        fragments.append("<tr>")
        fragments.append("<td>%s</td><td>%s&nbsp;</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
            build_type_link(obj.type_), obj.name, build_link(assoc.s, "/view/%s" % assoc.s),
            build_link(assoc.p, "/assoc?predicate=%s" % assoc.p),
            build_link("Delete", "/cmd/deleteassoc?rid=%s" % assoc._id, "return confirm('Are you sure to delete association?');")))

    fragments.append("</table></p>")
    fragments.append("<h3>TO</h3>")
    obj_list, assoc_list = Container.instance.resource_registry.find_objects(subject=resid, id_only=False)

    fragments.append("<p><table>")
    fragments.append("<tr><th>Object Type</th><th>Object Name</th><th>Object ID</th><th>Predicate</th><th>Command</th></tr>")

    iter_list = sorted(zip(obj_list, assoc_list), key=lambda x: [x[1].p, x[0].type_, x[0].name])
    for obj, assoc in iter_list:
        fragments.append("<tr>")
        fragments.append("<td>%s</td><td>%s&nbsp;</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
            build_type_link(obj.type_), obj.name, build_link(assoc.o, "/view/%s" % assoc.o),
            build_link(assoc.p, "/assoc?predicate=%s" % assoc.p),
            build_link("Delete", "/cmd/deleteassoc?rid=%s" % assoc._id, "return confirm('Are you sure to delete association?');")))

    fragments.append("</table></p>")
    return fragments

def build_commands(resource_id, restype):
    if is_read_only():
        return ""

    fragments = ["<h2>Commands</h2>"]

    fragments.append(build_command("Edit", "/edit/%s" % resource_id))

    fragments.append(build_command("Delete", "/cmd/delete?rid=%s" % resource_id, confirm="Are you sure to delete resource?"))

    options = [(p, p) for p in sorted(PRED)]
    args = [('select', 'pred', options), ('input', 'rid2', 45)]
    fragments.append(build_command("Associate from subject", "/cmd/createassoc?rid=%s&dir=from" % resource_id, args))
    fragments.append(build_command("Associate to object", "/cmd/createassoc?rid=%s&dir=to" % resource_id, args))

    from pyon.ion.resource import CommonResourceLifeCycleSM
    event_list = CommonResourceLifeCycleSM.MAT_EVENTS + CommonResourceLifeCycleSM.AVAIL_EVENTS
    options = zip(event_list, event_list)
    args = [('select', 'lcevent', options)]
    fragments.append(build_command("Execute Lifecycle Event", "/cmd/execute_lcs?rid=%s" % resource_id, args))

    state_list = CommonResourceLifeCycleSM.MATURITY + CommonResourceLifeCycleSM.AVAILABILITY
    options = zip(state_list, state_list)
    args = [('select', 'lcstate', options)]
    fragments.append(build_command("Change Lifecycle State", "/cmd/set_lcs?rid=%s" % resource_id, args))


    if restype == "InstrumentAgentInstance" or restype == "PlatformAgentInstance" or restype == "ExternalDatasetAgentInstance":
        fragments.append(build_command("Start Agent", "/cmd/start_agent?rid=%s" % resource_id))
        fragments.append(build_command("Stop Agent", "/cmd/stop_agent?rid=%s" % resource_id))

    elif restype == "InstrumentDevice":
        fragments.append(build_command("Start Agent", "/cmd/start_agent?rid=%s" % resource_id))
        fragments.append(build_command("Stop Agent", "/cmd/stop_agent?rid=%s" % resource_id))

        options = [('initialize', 'RESOURCE_AGENT_EVENT_INITIALIZE'),
                   ('go_active', 'RESOURCE_AGENT_EVENT_GO_ACTIVE'),
                   ('run', 'RESOURCE_AGENT_EVENT_RUN'),
                   ('acquire_sample', 'DRIVER_EVENT_ACQUIRE_SAMPLE'),
                   ('start_autosample', 'DRIVER_EVENT_START_AUTOSAMPLE'),
                   ('stop_autosample', 'DRIVER_EVENT_STOP_AUTOSAMPLE'),
                   ('go_direct_access', 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS'),
                   ('go_command', 'RESOURCE_AGENT_EVENT_GO_COMMAND'),
                   ('go_inactive', 'RESOURCE_AGENT_EVENT_GO_INACTIVE'),
                   ('reset', 'RESOURCE_AGENT_EVENT_RESET'),
                  ]

        args = [('select', 'agentcmd', options)]
        fragments.append(build_command("Agent Command", "/cmd/agent_execute?rid=%s" % resource_id, args))

    elif restype == "PlatformDevice":
        fragments.append(build_command("Start Agent", "/cmd/start_agent?rid=%s" % resource_id))
        fragments.append(build_command("Stop Agent", "/cmd/stop_agent?rid=%s" % resource_id))

        options = [('initialize', 'RESOURCE_AGENT_EVENT_INITIALIZE'),
                   ('go_active', 'RESOURCE_AGENT_EVENT_GO_ACTIVE'),
                   ('run', 'RESOURCE_AGENT_EVENT_RUN'),
                   ('acquire_sample', 'DRIVER_EVENT_ACQUIRE_SAMPLE'),
                   ('start_autosample', 'DRIVER_EVENT_START_AUTOSAMPLE'),
                   ('stop_autosample', 'DRIVER_EVENT_STOP_AUTOSAMPLE'),
                   ('go_direct_access', 'RESOURCE_AGENT_EVENT_GO_DIRECT_ACCESS'),
                   ('go_command', 'RESOURCE_AGENT_EVENT_GO_COMMAND'),
                   ('go_inactive', 'RESOURCE_AGENT_EVENT_GO_INACTIVE'),
                   ('reset', 'RESOURCE_AGENT_EVENT_RESET'),
                  ]
        args = [('select', 'agentcmd', options)]
        fragments.append(build_command("Agent Command", "/cmd/agent_execute?rid=%s" % resource_id, args))

    elif restype == "DataProcess":
        fragments.append(build_command("Start Process", "/cmd/start_process?rid=%s" % resource_id))
        fragments.append(build_command("Stop Process", "/cmd/stop_process?rid=%s" % resource_id))

    elif restype == "DataProduct":
        fragments.append(build_command("Latest Ingest", "/cmd/last_granule?rid=%s" % resource_id))

    elif restype == "Deployment":
        pass
#        res_list,_ = Container.instance.resource_registry.find_resources(RT.InstrumentSite, id_only=False)
#        if res_list:
#            options = [(res.name, res._id) for res in res_list]
#            args = [('select','deploy',options)]
#            fragments.append(build_command("Set Deployment", "/cmd/deploy?rid=%s" % resource_id, args))
#
#        res_list,_ = Container.instance.resource_registry.find_objects(resource_id, PRED.hasDeployment, RT.InstrumentSite, id_only=False)
#        if res_list:
#            options = [(res.name, res._id) for res in res_list]
#            args = [('select','deploy_prim',options)]
#            fragments.append(build_command("Deploy Primary", "/cmd/deploy_prim?rid=%s" % resource_id, args))

    if restype in ["Org", "Observatory", "Subsite", "PlatformSite", "InstrumentSite", "PlatformDevice", "InstrumentDevice"]:
        fragments.append(build_command("Show Sites and Status", "/cmd/sites?rid=%s" % resource_id))


    fragments.append("</table>")
    return "".join(fragments)

def build_command(text, link, args=None, confirm=None):
    fragments = []
    if args:
        arg_params = "'%s'" % link
        for arg in args:
            arg_type, arg_name, arg_more = arg
            arg_params += ",'%s','%s'" % (arg_name, arg_name)
        func_name = "linkto"
        if len(args) > 1:
            func_name += str(len(args))
        fragments.append("<div><a href='#' onclick=\"return %s(%s);\">%s</a> " % (func_name, arg_params, text))
        for arg in args:
            arg_type, arg_name, arg_more = arg
            if arg_type == "select":
                fragments.append("<select id='%s'>" % arg_name)
                for oname, oval in arg_more:
                    fragments.append("<option value='%s'>%s</option>" % (oval, oname))
                fragments.append("</select>")
            elif arg_type == "input":
                fragments.append("<input id='%s' type='text' size='%s'>" % (arg_name, arg_more or 30))
                fragments.append("</input>")
        fragments.append("</div>")
    else:
        if confirm:
            confirm = "return confirm('%s');" % confirm
        fragments.append("<div>%s</div>" % build_link(text, link, confirm))
    return "".join(fragments)

# ----------------------------------------------------------------------------------------

@app.route('/cmd/<cmd>', methods=['GET','POST'])
def process_command(cmd):
    try:
        cmd = str(cmd)
        resource_id = get_arg('rid')

        if is_read_only():
            raise Unauthorized("Cannot %s %s in read-only mode!" % (cmd, resource_id))

        res_obj = None
        if resource_id != "NEW" and cmd not in {'deleteassoc'}:
            res_obj = Container.instance.resource_registry.read(resource_id)

        func_name = "_process_cmd_%s" % cmd
        cmd_func = globals().get(func_name, None)
        if not cmd_func:
            raise Exception("Command %s unknown" % (cmd))

        result = cmd_func(resource_id, res_obj)

        fragments = [
            build_standard_menu(),
            "<h1>Command %s result</h1>" % cmd,
            "<p><pre>%s</pre></p>" % result,
            "<p>%s</p>" % build_link("Back to Resource Page", "/view/%s" % resource_id),
        ]

        content = "\n".join(fragments)
        return build_page(content)

    except Exception, e:
        return build_error_page(traceback.format_exc())

def _process_cmd_update(resource_id, res_obj=None):
    if resource_id == "NEW":
        restype = get_arg("restype")
        res_obj = IonObject(restype)

    schema = res_obj._schema
    set_fields = []

    for field,value in request.values.iteritems():
        value = str(value)
        nested_fields = field.split('.')
        local_field = nested_fields[0]
        if field in EDIT_IGNORE_FIELDS or local_field not in schema:
            continue
        if len(nested_fields) > 1:
            obj = res_obj
            skip_field = False
            for sub_field in nested_fields:
                local_obj = getattr(obj, sub_field, None)
                if skip_field or local_obj is None:
                    skip_field = True
                    continue
                elif isinstance(local_obj, IonObjectBase):
                    obj = local_obj
                else:
                    value = get_typed_value(value, obj._schema[sub_field])
                    setattr(obj, sub_field, value)
                    set_fields.append(field)
                    skip_field = True

        elif schema[field]['type'] in EDIT_IGNORE_TYPES:
            pass
        else:
            value = get_typed_value(value, res_obj._schema[field])
            setattr(res_obj, field, value)
            set_fields.append(field)

    #res_obj._validate()

    if resource_id == "NEW":
        Container.instance.resource_registry.create(res_obj)
    else:
        Container.instance.resource_registry.update(res_obj)

    return "OK. Set fields:\n%s" % pprint.pformat(sorted(set_fields))

def _process_cmd_createassoc(resource_id, res_obj=None):
    pred = get_arg("pred", None)
    direction = get_arg("dir", None)
    if not all([pred, direction]):
        raise BadRequest("Must provide all arguments")
    rid2 = get_arg("rid2", None)
    if not rid2:
        raise BadRequest("Must provide target resource id")
    rid2_obj = Container.instance.resource_registry.read(rid2)
    if direction == "from":
        Container.instance.resource_registry.create_association(rid2, pred, resource_id)
    elif direction == "to":
        Container.instance.resource_registry.create_association(resource_id, pred, rid2)
    return "OK"

def _process_cmd_delete(resource_id, res_obj=None):
    Container.instance.resource_registry.delete(resource_id)
    return "OK"

def _process_cmd_deleteassoc(resource_id, res_obj=None):
    Container.instance.resource_registry.delete_association(resource_id)
    return "OK"

def _process_cmd_set_lcs(resource_id, res_obj=None):
    lcstate = get_arg('lcstate')
    Container.instance.resource_registry.set_lifecycle_state(resource_id, lcstate)
    return "OK"

def _process_cmd_execute_lcs(resource_id, res_obj=None):
    lcevent = get_arg('lcevent')
    new_state = Container.instance.resource_registry.execute_lifecycle_transition(resource_id, lcevent)
    return "OK. New state: %s" % new_state

def _process_cmd_start_agent(resource_id, res_obj=None):
    agent_type = "instrument"
    if res_obj.type_ == "InstrumentDevice":
        iai_ids,_ = Container.instance.resource_registry.find_objects(resource_id, PRED.hasAgentInstance, RT.InstrumentAgentInstance, id_only=True)
        if iai_ids:
            resource_id = iai_ids[0]
        else:
            iai_ids,_ = Container.instance.resource_registry.find_objects(resource_id, PRED.hasAgentInstance, RT.ExternalDatasetAgentInstance, id_only=True)
            if iai_ids:
                resource_id = iai_ids[0]
            else:
                return "InstrumentAgentInstance/ExternalDatasetAgentInstance for InstrumentDevice %s not found" % resource_id
    elif res_obj.type_ == "PlatformDevice":
        agent_type = "platform"
        pai_ids,_ = Container.instance.resource_registry.find_objects(resource_id, PRED.hasAgentInstance, RT.PlatformAgentInstance, id_only=True)
        if pai_ids:
            resource_id = pai_ids[0]
        else:
            pai_ids,_ = Container.instance.resource_registry.find_objects(resource_id, PRED.hasAgentInstance, RT.ExternalDatasetAgentInstance, id_only=True)
            if pai_ids:
                resource_id = pai_ids[0]
            else:
                return "PlatformAgentInstance/ExternalDatasetAgentInstance for PlatformDevice %s not found" % resource_id
    elif res_obj.type_ == "PlatformAgentInstance":
        agent_type = "platform"

    from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
    ims_cl = InstrumentManagementServiceClient()
    if agent_type == "instrument":
        ims_cl.start_instrument_agent_instance(resource_id)
    elif agent_type == "platform":
        ims_cl.start_platform_agent_instance(resource_id)
    else:
        return "Unknown agent type"
    return "OK"

def _process_cmd_stop_agent(resource_id, res_obj=None):
    agent_type = "instrument"
    if res_obj._get_type() == "InstrumentDevice":
        iai_ids,_ = Container.instance.resource_registry.find_objects(resource_id, PRED.hasAgentInstance, RT.InstrumentAgentInstance, id_only=True)
        if iai_ids:
            resource_id = iai_ids[0]
        else:
            iai_ids,_ = Container.instance.resource_registry.find_objects(resource_id, PRED.hasAgentInstance, RT.ExternalDatasetAgentInstance, id_only=True)
            if iai_ids:
                resource_id = iai_ids[0]
            else:
                return "InstrumentAgentInstance/ExternalDatasetAgentInstance for InstrumentDevice %s not found" % resource_id
    elif res_obj._get_type() == "PlatformDevice":
        agent_type = "platform"
        pai_ids,_ = Container.instance.resource_registry.find_objects(resource_id, PRED.hasAgentInstance, RT.PlatformAgentInstance, id_only=True)
        if pai_ids:
            resource_id = pai_ids[0]
        else:
            pai_ids,_ = Container.instance.resource_registry.find_objects(resource_id, PRED.hasAgentInstance, RT.ExternalDatasetAgentInstance, id_only=True)
            if pai_ids:
                resource_id = pai_ids[0]
            else:
                return "PlatformAgentInstance/ExternalDatasetAgentInstance for PlatformDevice %s not found" % resource_id
    elif res_obj._get_type() == "PlatformAgentInstance":
        agent_type = "platform"

    from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
    ims_cl = InstrumentManagementServiceClient()
    if agent_type == "instrument":
        ims_cl.stop_instrument_agent_instance(resource_id)
    elif agent_type == "platform":
        ims_cl.stop_platform_agent_instance(resource_id)
    else:
        return "Unknown agent type"
    return "OK"

def _process_cmd_start_process(resource_id, res_obj=None):
    from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
    dpms_cl = DataProcessManagementServiceClient()
    dpms_cl.activate_data_process(resource_id)
    return "OK"

def _process_cmd_stop_process(resource_id, res_obj=None):
    from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
    dpms_cl = DataProcessManagementServiceClient()
    dpms_cl.deactivate_data_process(resource_id)
    return "OK"

def _process_cmd_agent_execute(resource_id, res_obj=None):
    agent_cmd = get_arg('agentcmd')
    from pyon.agent.agent import ResourceAgentClient
    from interface.objects import AgentCommand
    rac = ResourceAgentClient(process=containerui_instance, resource_id=resource_id)
    ac = AgentCommand(command=agent_cmd)
    if agent_cmd.startswith("RESOURCE"):
        res = rac.execute_agent(ac)
    else:
        res = rac.execute_resource(ac)
    res_dict = get_value_dict(res)
    res_str = get_formatted_value(res_dict, fieldtype="dict")
    return res_str

def _process_cmd_deploy(resource_id, res_obj=None):
    li_id = get_arg('deploy')
    from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
    ims_cl = InstrumentManagementServiceClient()
    ims_cl.deploy_instrument_device_to_logical_instrument(resource_id, li_id)
    return "OK"

def _process_cmd_deploy_prim(resource_id, res_obj=None):
    li_id = get_arg('deploy_prim')
    from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
    ims_cl = InstrumentManagementServiceClient()
    ims_cl.deploy_as_primary_instrument_device_to_logical_instrument(resource_id, li_id)
    return "OK"

def _process_cmd_undeploy_prim(resource_id, res_obj=None):
    li_id = get_arg('undeploy_prim')
    from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
    ims_cl = InstrumentManagementServiceClient()
    ims_cl.undeploy_primary_instrument_device_from_logical_instrument(resource_id, li_id)
    return "OK"

def _process_cmd_sites(resource_id, res_obj=None):
    from ion.services.sa.observatory.observatory_util import ObservatoryUtil
    from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
    from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
    from ion.processes.event.device_state import DeviceStateManager
    RR2 = EnhancedResourceRegistryClient(ResourceRegistryServiceClient())
    outil = ObservatoryUtil(container=Container.instance, enhanced_rr=RR2, device_status_mgr=DeviceStateManager())
    statuses = outil.get_status_roll_ups(resource_id, include_structure=True)
    fragments = [
        "</pre><h3>Org, Site and Device Status</h3>",
        ]

    if '_system' in statuses:
        extra = statuses['_system']
        child_sites, ancestors, devices = extra.get('sites', {}), extra.get('ancestors', {}), extra.get('devices', {})
        root_id = outil.get_site_root(resource_id, ancestors=ancestors) if ancestors else resource_id

        fragments.append("<p><table>")
        fragments.append("<tr><th>Resource</th><th>Type</th><th>AGG</th><th>PWR</th><th>COMM</th><th>DATA</th><th>LOC</th></tr>")
        device_info = {}
        if devices:
            dev_id_list = [dev for dev in devices.keys()]
            if dev_id_list:
                dev_list = Container.instance.resource_registry.read_mult(dev_id_list)
                device_info = dict(zip([res._id for res in dev_list], dev_list))
        elif ancestors:
            dev_id_list = [anc[1] for anc_list in ancestors.values() if anc_list is not None for anc in anc_list]
            dev_id_list.append(resource_id)
            dev_list = Container.instance.resource_registry.read_mult(dev_id_list)
            device_info = dict(zip([res._id for res in dev_list], dev_list))

        def stat(status, stype):
            stat = status.get(stype, 1)
            stat_str = ['BAD', '--', "<span style='color:green'>OK</span>","<span style='color:orange'>WARN</span>","<span style='color:red'>ERROR</span>"]
            return stat_str[stat]

        def status_table(parent_id, level, recurse=True):
            fragments.append("<tr>")
            par_detail = child_sites.get(parent_id, None) or device_info.get(parent_id, None)
            par_status = statuses.get(parent_id, {})
            entryname = "&nbsp;"*level + build_link(par_detail.name if par_detail else parent_id, "/view/%s" % parent_id)
            if parent_id == resource_id:
                entryname = "<b>" + entryname + "</b>"
            if par_detail and par_detail.type_.endswith("Device"):
                entryname = "<i>" + entryname + "</i>"
            fragments.append("<td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>" % (
                entryname,
                par_detail._get_type() if par_detail else "?",
                stat(par_status, 'agg'), stat(par_status, 'power'), stat(par_status, 'comms'), stat(par_status, 'data'), stat(par_status, 'loc')))
            fragments.append("</tr>")
            if recurse:
                devs = devices.get(parent_id, None) or []
                for dev in devs:
                    status_table(dev[1], level+1, recurse=False)

                ch_ids = ancestors.get(parent_id, None) or []
                for ch_id in ch_ids:
                    if type(ch_id) in (list, tuple):
                        ch_id = ch_id[1]  # TODO: Check why content type is different site/device
                    status_table(ch_id, level+1)

        status_table(root_id, 0)
        fragments.append("</table></p>")
        fragments.append("<pre>%s</pre>" % (pprint.pformat(statuses))),

    else:
        fragments.append("<pre>%s</pre>" % (pprint.pformat(statuses))),

    fragments.append("<pre>")
    content = "\n".join(fragments)
    return content

# ----------------------------------------------------------------------------------------

@app.route('/edit/<resource_id>', methods=['GET','POST'])
def process_edit_resource(resource_id):
    try:
        resid = str(resource_id)
        res = Container.instance.resource_registry.read(resid)
        restype = res._get_type()

        fragments = [
            build_standard_menu(),
            "<h1>Edit %s '%s'</h1>" % (build_type_link(restype), res.name),
            "<form name='edit' action='/cmd/update?rid=%s' method='post'>" % resid,
        ]
        fragments.extend(build_editable_resource(res, is_new=False))
        fragments.append("<p><input type='reset'/> <input type='submit' value='Save'/></p>")
        fragments.append("</form>")
        fragments.append("<p>%s</p>" % build_link("Back to Resource Page", "/view/%s" % resid)),

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_error_page(traceback.format_exc())

@app.route('/new/<restype>', methods=['GET','POST'])
def process_new_resource(restype):
    try:
        restype = str(restype)
        res = IonObject(restype)
        res._id = "NEW"
        for k,v in request.args.iteritems():
            if '.' in k:
                key = None
                obj = res
                attrs = k.split('.')
                while len(attrs):
                    key = attrs.pop(0)
                    if not len(attrs):
                        if hasattr(obj,key):
                            setattr(obj,key,v)
                            break
                    if hasattr(obj,key):
                        obj = getattr(obj,key)
                    else:
                        break

            elif hasattr(res,k):
                setattr(res,k,v)

        fragments = [
            build_standard_menu(),
            "<h1>Create New %s</h1>" % (build_type_link(restype)),
            "<form name='edit' action='/cmd/update?rid=NEW&restype=%s' method='post'>" % restype,
        ]
        fragments.extend(build_editable_resource(res, is_new=True))
        fragments.append("<p><input type='reset'/> <input type='submit' value='Create'/></p>")
        fragments.append("</form>")
        fragments.append("<p>%s</p>" % build_link("Back to List Page", "/list/%s" % restype)),

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_error_page(traceback.format_exc())

def build_editable_resource(res, is_new=False):
    restype = res._get_type()
    resid = res._id

    fragments = [
        "<p><table>",
        "<tr><th>Field</th><th>Type</th><th>Value</th></tr>"
    ]

    fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td>" % ("type", "str", restype))
    fragments.extend(build_nested_obj(res, "", edit=True))
    fragments.append("</p></table>")

    return fragments

# ----------------------------------------------------------------------------------------

@app.route('/assoc', methods=['GET','POST'])
def process_assoc_list():
    try:
        predicate = get_arg('predicate')

        assoc_list = Container.instance.resource_registry.find_associations(predicate=predicate, id_only=False)

        fragments = [
            build_standard_menu(),
            "<h1>List of Associations</h1>",
            "<p>Restrictions: predicate=%s</p>" % (predicate),
            "<p>",
            "<table>",
            "<tr><th>Subject</th><th>Subject type</th><th>Predicate</th><th>Object ID</th><th>Object type</th></tr>"
        ]

        for assoc in assoc_list:
            fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                build_link(assoc.s, "/view/%s" % assoc.s), build_type_link(assoc.st), assoc.p, build_link(assoc.o, "/view/%s" % assoc.o), build_type_link(assoc.ot)))

        fragments.append("</table></p>")

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_error_page(traceback.format_exc())

# ----------------------------------------------------------------------------------------

@app.route('/dir', methods=['GET','POST'], defaults={'path':'~'})
@app.route('/dir/<path>', methods=['GET','POST'])
def process_dir_path(path):
    try:
        #path = convert_unicode(path)
        path = str(path)
        path = path.replace("~", "/")

        fragments = [
            build_standard_menu(),
            "<h1>Directory %s</h1>" % (build_dir_path(path)),
            "<h2>Attributes</h2>",
            "<p><table><tr><th>Name</th><th>Value</th></tr>"
        ]

        entry = Container.instance.directory.lookup(path)
        if entry:
            for attr in sorted(entry.keys()):
                attval = entry[attr]
                fragments.append("<tr><td>%s</td><td>%s</td></tr>" % (attr, attval))
        fragments.append("</table></p>")

        fragments.append("</p><h2>Child Entries</h2><p><table><tr><th>Key</th><th>Timestamp</th><th>Attributes</th></tr>")
        de_list = Container.instance.directory.find_child_entries(path)
        for de in de_list:
            if '/' in de.parent:
                org, parent = de.parent.split("/", 1)
                parent = "/"+parent
            else:
                parent = ""
            fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                build_dir_link(parent,de.key), get_formatted_value(de.ts_updated, fieldname="ts_updated"), get_formatted_value(get_value_dict(de.attributes), fieldtype="dict")))

        fragments.append("</table></p>")

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_error_page(traceback.format_exc())

def build_dir_path(path):
    if path.startswith('/'):
        path = path[1:]
    levels = path.split("/")
    fragments = []
    parent = ""
    for level in levels:
        fragments.append(build_dir_link(parent,level))
        fragments.append("/")
        parent = "%s/%s" % (parent, level)
    return "".join(fragments)

def build_dir_link(parent, key):
    if parent == "/":
        path = "/%s" % (key)
    else:
        path = "%s/%s" % (parent, key)
    path = path.replace("/","~")
    return build_link(key, "/dir/%s" % path)

# ----------------------------------------------------------------------------------------

@app.route('/events', methods=['GET','POST'])
def process_events():
    try:
        event_type = request.args.get('event_type', None)
        origin = request.args.get('origin', None)
        limit = int(request.args.get('limit', 100))
        descending = request.args.get('descending', True)
        skip = int(request.args.get('skip', 0))

        events_list = Container.instance.event_repository.find_events(event_type=event_type, origin=origin,
                                     descending=descending, limit=limit, skip=skip)

        fragments = [
            build_standard_menu(),
            "<h1>List of Events</h1>",
            "Restrictions: event_type=%s, origin=%s, limit=%s, descending=%s, skip=%s" % (event_type, origin, limit, descending, skip),
        ]

        fragments.extend(build_events_table(events_list))

        if len(events_list) >= limit:
            fragments.append("<p>%s</p>" % build_link("Next page", "/events?skip=%s" % (skip + limit)))

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_error_page(traceback.format_exc())

def build_events_table(events_list):
    fragments = [
        "<p><table>",
        "<tr><th>Timestamp</th><th>Event type</th><th>Sub-type</th><th>Origin</th><th>Origin type</th><th>Other Attributes</th><th>Description</th></tr>"
    ]

    ignore_fields=["base_types", "origin", "description", "ts_created", "sub_type", "origin_type", "_rev", "_id"]
    for event_id, event_key, event in events_list:
        fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
            get_formatted_value(event.ts_created, fieldname="ts_created", time_millis=True),
            build_link(event._get_type(), "/events?event_type=%s" % event._get_type()),
            event.sub_type or "&nbsp;",
            build_link(event.origin, "/view/%s" % event.origin),
            event.origin_type or "&nbsp;",
            get_formatted_value(get_value_dict(event, ignore_fields=ignore_fields), fieldtype="dict"),
            event.description  or "&nbsp;"))

    fragments.append("</table></p>")

    return fragments

# ----------------------------------------------------------------------------------------

@app.route('/viewobj', methods=['GET','POST'])
def process_view_objects():
    try:
        obj_filter = get_arg('filter')
        obj_id = get_arg('object_id')
        args_filter = [('input', 'filter', 45)]
        args_view = [('input', 'object_id', 45)]
        fragments = [
            build_standard_menu(),
            "<h1>View Objects</h1>",
            "<h2>Filter</h2>",
            build_link("All Objects", "/viewobj?filter=*"),
            build_command("Object ID pattern", "/viewobj?dummy=1", args_filter),
            build_command("Object ID", "/viewobj?dummy=1", args_view),
        ]
        if obj_filter:
            fragments.append("<h2>Object List</h2>")
            fragments.append("<p>")

            obj_ids = Container.instance.object_store.obj_store.list_objects()
            fragments.append("<p><table>")
            fragments.append("<tr><th>Object ID</th></tr>")
            for oid in obj_ids:
                if obj_filter == "*" or obj_filter in oid:
                    fragments.append("<tr>")
                    fragments.append("<td>%s</td></tr>" % (build_link(oid, "/viewobj?object_id=%s" % oid)))

            fragments.append("</table></p>")
            fragments.append("</p>")

        if obj_id:
            fragments.append("<h2>Object Details</h2>")
            fragments.append("<p><pre>")
            obj = Container.instance.object_store.read_doc(obj_id)
            value = yaml.dump(obj, default_flow_style=False)
            fragments.append(value)
            fragments.append("</pre></p>")

        content = "\n".join(fragments)
        return build_page(content)

    except Exception as e:
        return build_error_page(traceback.format_exc())

# ----------------------------------------------------------------------------------------

@app.route('/viewstate', methods=['GET','POST'])
def process_view_state():
    try:
        state_id = get_arg('state_id')
        args_view = [('input', 'state_id', 45)]
        fragments = [
            build_standard_menu(),
            "<h1>View State</h1>",
            build_command("State ID", "/viewstate?dummy=1", args_view),
        ]
        if state_id:
            fragments.append("<h2>State Details</h2>")
            fragments.append("<p><pre>")
            obj_state, obj = Container.instance.state_repository.get_state(state_id)
            if obj:
                value = yaml.dump(obj_state, default_flow_style=False)
                fragments.append(value)
            fragments.append("</pre></p>")

        content = "\n".join(fragments)
        return build_page(content)

    except Exception as e:
        return build_error_page(traceback.format_exc())

# ----------------------------------------------------------------------------------------

@app.route('/map', methods=['GET'])
def process_map():
    '''
    Map!
    '''
    try:
        content = [
            "        <script type='text/javascript' src='http://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js'> </script>",
            "        <script type='text/javascript' src='https://maps.googleapis.com/maps/api/js?sensor=false'></script>",
            "<div id='map_canvas'></div>",
            "<script type='text/javascript' src='/static/gmap.js'></script>",
        ]
        content = "\n".join(content)
        return build_page(content)
    except Exception as e:
        return build_error_page(traceback.format_exc())


# ----------------------------------------------------------------------------------------

@app.route('/tree/<resid>', methods=['GET'])
def process_tree(resid):
    '''
    Creates a tree-like JSON string to be parsed by visual clients such as the D3 Framework
    @param resid Resource id
    @return An HTTP Response containing the JSON string (Content-Type: application/json)
    '''
    from flask import make_response, Response
    from ion.services.dm.utility.resource_tree import build
    try:
        resp = make_response(Response(),200)
        data = build(resid).to_j()
        resp.data = data
        resp.headers['Content-Type'] = 'application/json'
        resp.headers['Content-Length'] = len(data)
        return resp
    except Exception as e:
        return build_error_page(traceback.format_exc())

# ----------------------------------------------------------------------------------------

def is_read_only():
    return CFG.get_safe('container.containerui.read_only', False)

def build_type_link(restype):
    return build_link(restype, "/list/%s" % restype)

def build_link(text, link, onclick=None):
    if onclick:
        return "<a href='%s' onclick=\"%s\">%s</a>" % (link, onclick, text)
    else:
        return "<a href='%s'>%s</a>" % (link, text)

def build_standard_menu():
    return "<p><a href='/'>[Home]</a></p>"

def build_error_page(msg):
    fragments = [
        build_standard_menu(),
        "<h1>Error</h1>",
        "<p><pre>%s</pre></p>" % msg,
    ]
    content = "\n".join(fragments)
    return build_page(content)

def build_simple_page(content):
    return build_page("<p><pre>" + content + "</pre></p>")

def build_page(content, title=""):
    fragments = [
        "<!doctype html>",
        "<html><head>",
        "<link type='text/css' rel='stylesheet' href='/static/default.css' />"
        "<link type='text/css' rel='stylesheet' href='/static/demo.css' />",
        "<script type='text/javascript'>",
        "function linkto(href, arg_name, arg_id) {",
        "var aval = document.getElementById(arg_id).value;",
        "href = href + '&' + arg_name + '=' + aval;",
        "window.location.href = href;",
        "return true;",
        "}",
        "function linkto2(href, arg_name, arg_id, arg_name2, arg_id2) {",
        "var aval = document.getElementById(arg_id).value;",
        "var aval2 = document.getElementById(arg_id2).value;",
        "href = href + '&' + arg_name + '=' + aval + '&' + arg_name2 + '=' + aval2;",
        "window.location.href = href;",
        "return true;",
        "}",
        "</script></head>"
        "<body>",
        content,
        "</body></html>"
    ]
    return "\n".join(fragments)

def get_arg(arg_name, default=None):
    aval = request.values.get(arg_name, None)
    return str(aval) if aval else default

def convert_unicode(data):
    """
    Used to recursively convert unicode in JSON structures into proper data structures
    """
    if isinstance(data, unicode):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert_unicode, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert_unicode, data))
    else:
        return data

obj_classes = {}

def _get_object_class(objtype):
    if objtype in obj_classes:
        return obj_classes[objtype]
    obj_class = named_any("interface.objects.%s" % objtype)
    obj_classes[objtype] = obj_class
    return obj_class

def get_typed_value(value, schema_entry=None, targettype=None):
    targettype = targettype or schema_entry["type"]
    if targettype is 'str':
        return str(value)
    elif targettype is 'bool':
        lvalue = value.lower()
        if lvalue == 'true':
            return True
        elif lvalue == 'false' or lvalue == '':
            return False
        else:
            raise BadRequest("Value %s is no bool" % value)
    elif targettype is 'simplelist':
        if value.startswith('[') and value.endswith(']'):
            value = value[1:len(value)-1].strip()
        return list(value.split(','))
    elif schema_entry and 'enum_type' in schema_entry:
        enum_clzz = getattr(objects, schema_entry['enum_type'])
        if type(value) is str and value in enum_clzz._value_map:
            return enum_clzz._value_map[value]
        else:
            return int(value)
    else:
        return ast.literal_eval(value)


def get_value_dict(obj, ignore_fields=None):
    ignore_fields = ignore_fields or []
    if isinstance(obj, IonObjectBase):
        obj_dict = obj.__dict__
    else:
        obj_dict = obj
    val_dict = {}
    for k,val in obj_dict.iteritems():
        if k in ignore_fields:
            continue
        if isinstance(val, IonObjectBase):
            vdict = get_value_dict(val)
            val_dict[k] = vdict
        else:
            val_dict[k] = val
    return val_dict

def get_formatted_value(value, fieldname=None, fieldtype=None, fieldschema=None, brief=False, time_millis=False,
                        is_root=True, details=True):
    if not fieldtype and fieldschema:
        fieldtype = fieldschema['type']
    if isinstance(value, IonObjectBase):
        if brief:
            value = "[%s]" % value.type_
    elif fieldtype in ("list", "dict"):
        if details:
            value = yaml.dump(value, default_flow_style=False)
            value = value.replace("\n", "<br>")
            if value.endswith("<br>"):
                value = value[:-4]
            if is_root:
                value = "<span class='preform'>%s</span>" % value
        else:
            value = "..."
    elif fieldschema and 'enum_type' in fieldschema:
        enum_clzz = getattr(objects, fieldschema['enum_type'])
        return enum_clzz._str_map[int(value)]
    elif fieldname:
        if fieldname in date_fieldnames:
            try:
                value = get_datetime(value, time_millis)
            except Exception:
                pass
    if value == "":
        return "&nbsp;"
    return value


def get_datetime(ts, time_millis=False):
    tsf = float(ts) / 1000
    dt = datetime.datetime.fromtimestamp(time.mktime(time.localtime(tsf)))
    dts = str(dt)
    if time_millis:
        dts += "." + ts[-3:]
    return dts


@app.route('/mscaction/<action>', methods=['GET', 'POST'])
def mscaction(action):
    if action:
        action = str(action)
    if action == "start":
        if not containerui_instance.interaction_observer:
            containerui_instance.interaction_observer = InteractionObserver()
            containerui_instance.interaction_observer.start()
        return flask.redirect("/mscweb")
    elif action == "stop":
        if containerui_instance.interaction_observer and containerui_instance.interaction_observer.started:
            containerui_instance.interaction_observer.stop()
            return build_page("Message recording stopped")
        else:
            return build_page("Message recording was not enabled!")
    else:
        return flask.redirect("/")

@app.route('/mscweb', methods=['GET', 'POST'])
def mscweb():
    if not containerui_instance.interaction_observer:
        containerui_instance.interaction_observer = InteractionObserver()
        containerui_instance.interaction_observer.start()

    if 'last_data' not in session:
        last_data = {'last_index': 0}
        session['last_data'] = last_data

    return render_template('mschart.html')

@app.route('/data')
def data():
    # get associated last request
    last_data = session['last_data']
    use_idx = last_data['last_index']

    if use_idx is not None and containerui_instance.interaction_observer:

        # get open conversations if any saved in the session
        response_msgs = last_data.get('response_msgs', {})

        # get data out of io
        rawdata = containerui_instance.interaction_observer.msg_log[use_idx:]
        mscdata, response_msgs = containerui_instance.interaction_observer._get_data(rawdata, response_msgs)

        #store open conversations
        last_data['response_msgs'] = response_msgs
        # store last index
        last_data['last_index'] = len(containerui_instance.interaction_observer.msg_log)
        session['last_data'] = last_data

        # jsonize this and return it
        return json.dumps(mscdata)

    else:
        # may have a timestamp here from the user
        raise Exception("no can do chief")
