#!/usr/bin/env python

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

"""
TELNET server class
"""

from gevent import socket
from pyon.core.exception import ServerError
from pyon.util.log import log
import gevent
import uuid
	
class TelnetServer(object):

	server_socket = None
	port = None
	ip_address = None
	parent_input_callback = None
	username = None
	token = None
	fileobj = None
	parent_requested_close = False
	TELNET_PROMPT = 'ION telnet>'
	PORT_RANGE_LOWER = 8000
	PORT_RANGE_UPPER = 8010


	def write(self, text):
		log.debug("TelnetServer.write(): text = " + str(text))
		if self.fileobj:
			self.fileobj.write(text)
			self.fileobj.flush()
		else:
			log.warning("TelnetServer.write(): no connection yet, can not write text")			

	
	def writeline(self, text):
		"""Send a packet with line ending."""
		self.write(text+chr(10))


	def authorized(self, token):
		if token == self.token:
			return True
		else:
			log.debug("entered token =" + token + ", expected token =" + self.token)
			return False
	

	def close_connection(self):
		if not self.parent_requested_close:
			# indicate to parent that connection has been lost
			self.parent_input_callback(-1)
		else:		
			try:
				self.server_socket.shutdown(socket.SHUT_RDWR)
			except Exception as ex:
				# can happen if telnet client closes session first
				log.debug("TelnetServer.close_connection(): exception caught for socket shutdown:" + str(ex))
			self.server_socket.close()
	

	def exit_handler (self, reason):
		self.close_connection()
		log.debug("TelnetServer.handler(): stopping, " + reason)
	

	def handler(self):
		"The actual service to which the user has connected."
		log.debug("TelnetServer.handler(): starting")
		
		self.fileobj = self.connection_socket.makefile()
		username = None
		token = None
		self.write("Username: ")
		try:
			username = self.fileobj.readline().rstrip('\n\r')
		except EOFError:
			self.exit_handler("lost connection")
			return
		if username == '':
			self.exit_handler("lost connection")
			return
		self.write("token: ")
		try:
			token = self.fileobj.readline().rstrip('\n\r')
		except EOFError:
			self.exit_handler("lost connection")
			return
		if not self.authorized(token):
			log.debug("login failed")
			self.writeline("login failed")
			self.exit_handler("login failed")
			return
		self.writeline("connected")   # let telnet client user know they are connected
		while True:
			#self.write(self.TELNET_PROMPT)
			try:
				input_line = self.fileobj.readline()
			except EOFError:
				self.exit_handler("lost connection")
				break
			if input_line == '':
				self.exit_handler("lost connection")
				break
			log.debug("rcvd: " + input_line)
			log.debug("len=" + str(len(input_line)))
			self.parent_input_callback(input_line.rstrip('\n\r'))
			

	def server_greenlet(self):
		log.debug("TelnetServer.server_greenlet(): started")
		self.connection_socket = None
		try:
			self.server_socket.listen(1)
			self.connection_socket, address = self.server_socket.accept()
		except Exception as ex:
			log.info("TelnetServer.server_greenlet(): exception caught <%s>" %str(ex))
			if not self.parent_requested_close:
				# indicate to parent that connection has been lost
				self.parent_input_callback(-1)
		else:
			self.handler()
		log.debug("TelnetServer.server_greenlet(): stopping")
		

	def __init__(self, input_callback=None, ip_address=None):
		log.debug("TelnetServer.__init__(): IP address = %s" %ip_address)

		# save callback if specified
		if not input_callback:
			log.warning("TelnetServer.__init__(): callback not specified")
			raise ServerError("TelnetServer.__init__(): callback not specified")
		self.parent_input_callback = input_callback
		
		# save ip address if specified
		if not ip_address:
			log.warning("TelnetServer.__init__(): IP address not specified")
			raise ServerError("TelnetServer.__init__(): IP address not specified")
		self.ip_address = ip_address
		
		# search for available port
		self.port = self.PORT_RANGE_LOWER
		self.server_socket = socket.socket()
		self.server_socket.allow_reuse_address = True
		while True:
			try:
				log.debug("trying to bind to port %s on %s" %(str(self.port), self.ip_address))
				self.server_socket.bind((self.ip_address, self.port))
				break
			except Exception as ex:
				log.debug("exception caught for socket bind:" + str(ex))
				self.port = self.port + 1
				if self.port > self.PORT_RANGE_UPPER:
					log.warning("TelnetServer.__init__(): no available ports for server")
					raise ServerError("TelnetServer.__init__(): no available ports")
					return

		# create token
		self.token = str(uuid.uuid4()).upper()
		
		log.debug("TelnetServer.__init__(): starting server greenlet")
		self.server = gevent.spawn(self.server_greenlet)
		

	def get_connection_info(self):
		return self.port, self.token


	def stop(self):
		log.debug("TelnetServer.stop()")
		self.parent_requested_close = True
		if self.connection_socket:
			# telnet connection has been made
			try:
				self.connection_socket.shutdown(socket.SHUT_RDWR)
			except Exception as ex:
				# can happen if telnet client closes session first
				log.debug("TelnetServer.stop(): exception caught for socket shutdown:" + str(ex))
			gevent.sleep(.2)
			self.server.kill()
		else:
			self.server_socket.close()
		del self.server_socket
			

	def send(self, data):
		# send data from parent to telnet server process to forward to client
		log.debug("TelnetServer.send(): data = " + str(data))
		self.write(data)
		
