#!/usr/bin/env python

from pyon.ion.process import SimpleProcess
from pyon.util.file_sys import FileSystem
from logging import getLogger
from pydap.wsgi.file import make_app
from gevent.wsgi import WSGIServer
from pyon.util.log import log
from traceback import print_exc

class LightweightPyDAP(SimpleProcess):
    def on_start(self):
        try:
            SimpleProcess.on_start(self)
            self.pydap_host = self.CFG.get_safe('container.pydap_gateway.web_server.host', 'localhost')
            self.pydap_port = self.CFG.get_safe('container.pydap_gateway.web_server.port', '8001')

            self.pydap_data_path = self.CFG.get_safe('server.pydap.data_path', 'RESOURCE:ext/pydap')

            self.pydap_data_path = FileSystem.get_extended_url(self.pydap_data_path)

            self.app = make_app(None, self.pydap_data_path, 'ion/core/static/templates/')
            self.log = getLogger('pydap')
            self.log.write = self.log.info
            self.server = WSGIServer((self.pydap_host, int(self.pydap_port)), self.app, log=self.log)
            self.server.start()
        except: 
            log.exception('Unable to start PyDAP server')
            raise


    def on_quit(self):
        self.server.stop()
        super(LightweightPyDAP,self).on_quit()

