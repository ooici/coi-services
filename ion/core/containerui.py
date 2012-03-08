#!/usr/bin/env python

__author__ = 'Michael Meisinger'
__license__ = 'Apache 2.0'

import collections, traceback
import flask
from flask import Flask, request, abort
from gevent.wsgi import WSGIServer

from pyon.public import Container, StandaloneProcess, log
from pyon.core.exception import NotFound, Inconsistent, BadRequest
from pyon.core.registry import getextends

from pyon.util.containers import named_any


#Initialize the flask app
app = Flask(__name__)

DEFAULT_WEB_SERVER_HOSTNAME = ""
DEFAULT_WEB_SERVER_PORT = 8080

containerui_instance = None

class ContainerUI(StandaloneProcess):
    """
    A simple Web UI to introspect the container and the ION datastores.
    """
    def on_init(self):
        #defaults
        self.http_server = None
        self.server_hostname = DEFAULT_WEB_SERVER_HOSTNAME
        self.server_port = DEFAULT_WEB_SERVER_PORT
        self.web_server_enabled = True
        self.logging = None

        #retain a pointer to this object for use in ProcessRPC calls
        global containerui_instance
        containerui_instance = self

        #Start the gevent web server unless disabled
        if self.web_server_enabled:
            self.start_service(self.server_hostname, self.server_port)

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

# ----------------------------------------------------------------------------------------

@app.route('/', methods=['GET','POST'])
def process_index():
    try:
        from pyon.public import CFG, sys_name
        fragments = [
            "<h1>Welcome to ContainerUI</h1>",
            "<p><ul>",
            "<li><a href='/restypes'><b>Browse Resource Registry and Resource Objects</b></a></li>",
            "<li><a href='/dir'><b>Browse ION Directory</b></a></li>",
            "<li><a href='/events'><b>Browse Events</b></a></li>",
            "<li><a href='http://localhost:5984/_utils'><b>CouchDB Futon UI (if running)</b></a></li>",
            "</ul></p>",
            "<h2>Container and System Properties</h2>",
            "<p><table border='1' cellspacing='0'>",
            "<tr><th>Property</th><th>Value</th></tr>",
            "<tr><td>Container ID</td><td>%s</td></tr>" % Container.instance.id,
            "<tr><td>Sys_name</td><td>%s</td></tr>" % sys_name,
            "<tr><td>Broker</td><td>%s</td></tr>" % "%s:%s" % (CFG.server.amqp.host, CFG.server.amqp.port),
            "<tr><td>Datastore</td><td>%s</td></tr>" % "%s:%s" % (CFG.server.couchdb.host, CFG.server.couchdb.port),
            "</table></p>"
            ]
        content = "\n".join(fragments)
        return build_page(content)

    except Exception, e:
        return build_simple_page("Error: %s" % traceback.format_exc())

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
        return build_simple_page("Error: %s" % traceback.format_exc())

# ----------------------------------------------------------------------------------------

standard_resattrs = ['name', 'description', 'lcstate', 'ts_created', 'ts_updated']

@app.route('/list/<resource_type>', methods=['GET','POST'])
def process_list_resources(resource_type):
    try:
        restype = convert_unicode(resource_type)
        res_list,_ = Container.instance.resource_registry.find_resources(restype=restype)


        fragments = [
            build_standard_menu(),
            "<h1>List of '%s' Resources</h1>" % restype,
            build_res_extends(restype),
            "<p>",
            "<table border='1' cellspacing='0'>",
            "<tr>"
        ]

        fragments.extend(build_table_header(restype))
        #fragments.append("<th>Associations</th>")
        fragments.append("</tr>")

        for res in res_list:
            fragments.append("<tr>")
            fragments.extend(build_table_row(res))
            #fragments.append("<td>")
            #fragments.extend(build_associations(res._id))
            #fragments.append("</td>")
            fragments.append("</tr>")

        fragments.append("</table></p>")

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_simple_page("Error: %s" % traceback.format_exc())

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

def build_table_row(obj):
    schema = obj._schema
    fragments = []
    fragments.append("<td><a href='/view/%s'>%s</a></td>" % (obj._id,obj._id))
    for field in standard_resattrs:
        if field in schema:
            fragments.append("<td>%s&nbsp;</td>" % (getattr(obj, field)))
    for field in sorted(schema.keys()):
        if field not in standard_resattrs:
            fragments.append("<td>%s&nbsp;</td>" % (getattr(obj, field)))
    return fragments

# ----------------------------------------------------------------------------------------

@app.route('/view/<resource_id>', methods=['GET','POST'])
def process_view_resource(resource_id):
    try:
        resid = convert_unicode(resource_id)
        res = Container.instance.resource_registry.read(resid)
        restype = res._get_type()

        fragments = [
            build_standard_menu(),
            "<h1>View %s '%s'</h1>" % (build_type_link(restype), res.name),
            "<h2>Fields</h2>",
            "<p>",
            "<table border='1' cellspacing='0'>",
            "<tr><th>Field</th><th>Type</th><th>Value</th></tr>"
            ]

        schema = res._schema
        fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td>" % ("type", "str", restype))
        fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td>" % ("_id", "str", res._id))
        fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td>" % ("_rev", "str", res._rev))


        for field in standard_resattrs:
            if field in schema:
                fragments.append("<tr><td>%s</td><td>%s</td><td>%s&nbsp;</td>" % (field, schema[field]["type"], getattr(res, field)))
        for field in sorted(schema.keys()):
            if field not in standard_resattrs:
                fragments.append("<tr><td>%s</td><td>%s</td><td>%s&nbsp;</td>" % (field, schema[field]["type"], getattr(res, field)))

        fragments.append("</p></table>")

        fragments.extend(build_associations(res._id))

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_simple_page("Error: %s" % traceback.format_exc())

def build_associations(resid):
    fragments = []
    fragments.append("<h2>Associations</h2>")
    fragments.append("<h3>FROM</h3>")
    fragments.append("<p><table border='1' cellspacing='0'>")
    fragments.append("<tr><th>Type</th><th>Name</th><th>ID</th><th>Predicate</th></tr>")
    obj_list, assoc_list = Container.instance.resource_registry.find_objects(subject=resid, id_only=False)
    for obj,assoc in zip(obj_list,assoc_list):
        fragments.append("<tr>")
        fragments.append("<td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                build_type_link(obj._get_type()), obj.name, build_link(assoc.o, "/view/%s" % assoc.o), build_link(assoc.p, "/assoc?predicate=%s" % assoc.p)))

    fragments.append("</table></p>")
    fragments.append("<h3>TO</h3>")
    fragments.append("<p><table border='1' cellspacing='0'>")
    fragments.append("<tr><th>Predicate</th><th>Type</th><th>Name</th><th>ID</th></tr>")

    obj_list, assoc_list = Container.instance.resource_registry.find_subjects(object=resid, id_only=False)
    for obj,assoc in zip(obj_list,assoc_list):
        fragments.append("<tr>")
        fragments.append("<td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                build_link(assoc.p, "/assoc?predicate=%s" % assoc.p), build_type_link(obj._get_type()), obj.name, build_link(assoc.s, "/view/%s" % assoc.s)))

    fragments.append("</table></p>")
    return fragments

# ----------------------------------------------------------------------------------------

@app.route('/assoc', methods=['GET','POST'])
def process_assoc_list():
    try:
        predicate = request.args.get('predicate', None)

        assoc_list = Container.instance.resource_registry.find_associations(predicate=predicate, id_only=False)

        fragments = [
            build_standard_menu(),
            "<h1>List of Associations</h1>",
            "Restrictions: predicate=%s" % (predicate),
            "<p>",
            "<table border='1' cellspacing='0'>",
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
        return build_simple_page("Error: %s" % traceback.format_exc())

# ----------------------------------------------------------------------------------------

@app.route('/dir', methods=['GET','POST'], defaults={'path':'~'})
@app.route('/dir/<path>', methods=['GET','POST'])
def process_dir_path(path):
    try:
        #path = convert_unicode(path)
        path = str(path)
        path = path.replace("~", "/")
        de_list = Container.instance.directory.find_child_entries(path)
        entry = Container.instance.directory.lookup(path)
        fragments = [
            build_standard_menu(),
            "<h1>Directory %s</h1>" % (build_dir_path(path)),
            "<h2>Attributes</h2>",
            "<p><table border='1' cellspacing='0'><tr><th>Name</th><th>Value</th></tr>"
        ]

        for attr in sorted(entry.keys()):
            attval = entry[attr]
            fragments.append("<tr><td>%s</td><td>%s</td></tr>" % (attr, attval))
        fragments.append("</table></p>")

        fragments.append("</p><h2>Child Entries</h2><p><table border='1' cellspacing='0'><tr><th>Key</th><th>Timestamp</th><th>Attributes</th></tr>")
        for de in de_list:
            if '/' in de.parent:
                org, parent = de.parent.split("/", 1)
                parent = "/"+parent
            else:
                parent = ""
            fragments.append("<tr><td>%s</td><td>%s</td><td>%s&nbsp;</td></tr>" % (build_dir_link(parent,de.key), "&nbsp;", str(de.attributes)))

        fragments.append("</table></p>")

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_simple_page("Error: %s" % traceback.format_exc())

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
    path = "%s/%s" % (parent, key)
    path = path.replace("/","~")
    return build_link(key, "/dir/%s" % path)

# ----------------------------------------------------------------------------------------

standard_eventattrs = ['origin', 'ts_created', 'description']

@app.route('/events', methods=['GET','POST'])
def process_events():
    try:
        event_type = request.args.get('event_type', "Event")
        origin = request.args.get('origin', None)
        limit = request.args.get('limit', 100)
        descending = request.args.get('descending', True)

        events_list = Container.instance.event_repository.find_events(event_type=event_type, origin=origin,
                                     reverse_order=descending, max_results=limit)

        fragments = [
            build_standard_menu(),
            "<h1>List of Events</h1>",
            "Restrictions: event_type=%s, origin=%s, limit=%s, descending=%s" % (event_type, origin, limit, descending),
            "<p>",
            "<table border='1' cellspacing='0'>",
            "<tr><th>Timestamp</th><th>Event type</th><th>Origin</th><th>Description</th><th>Attributes</th></tr>"
        ]

        for event in events_list:
            attrs = ""
            fragments.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                        event._get_type(), event.origin, event.ts_created, event.description, attrs))

        fragments.append("</table></p>")

        content = "\n".join(fragments)
        return build_page(content)

    except NotFound:
        return flask.redirect("/")
    except Exception, e:
        return build_simple_page("Error: %s" % traceback.format_exc())

# ----------------------------------------------------------------------------------------

def build_type_link(restype):
    return build_link(restype, "/list/%s" % restype)

def build_link(text, link):
    return "<a href='%s'>%s</a>" % (link, text)

def build_standard_menu():
    return "<p><a href='/'>[Home]</a></p>"

def build_simple_page(content):
    return build_page("<p><pre>" + content + "</pre></p>")

def build_page(content, title=""):
    fragments = [
        "<html><head></head><body>",
        content,
        "</body></html>"
    ]
    return "\n".join(fragments)

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

