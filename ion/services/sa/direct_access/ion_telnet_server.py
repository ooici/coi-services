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
	
class TelnetServer(object):

	server_socket = None
	port = None
	ip_address = None
	parent_input_callback = None
	username = None
	password = None
	fileobj = None
	parent_requested_close = False
	TELNET_PROMPT = 'ION telnet>'
	PORT_RANGE_LOWER = 8000
	PORT_RANGE_UPPER = 8010

	def write(self, text):
		self.fileobj.write(text)
		self.fileobj.flush()

	
	def writeline(self, text):
		"""Send a packet with line ending."""
		self.write(text+chr(10))

	def authorized(self, username, password):
		if username == self.username and password == self.password:
			return True
		else:
			log.debug("un=" + username + " pw=" + password + " sun=" + self.username + " spw=" + self.password)
			return False
	
	def close_connection(self):
		if not self.parent_requested_close:
			self.parent_input_callback(-1)
		else:		
			try:
				self.server_socket.shutdown(socket.SHUT_RDWR)
			except Exception as ex:
				# can happen if telnet client closes session first
				log.debug("exception caught for socket shutdown:" + str(ex))
			self.server_socket.close()
	
	def exit_handler (self, reason):
		self.close_connection()
		log.debug("TelnetServer.handler(): stopping, " + reason)
	
	def handler(self, new_socket, address):
		"The actual service to which the user has connected."
		log.debug("TelnetServer.handler(): starting")
		
		self.fileobj = new_socket.makefile()
		username = None
		password = None
		self.write("Username: ")
		try:
			username = self.fileobj.readline().rstrip('\n\r')
		except EOFError:
			self.exit_handler("lost connection")
			return
		if username == '':
			self.exit_handler("lost connection")
			return
		self.write("Password: ")
		try:
			password = self.fileobj.readline().rstrip('\n\r')
		except EOFError:
			self.exit_handler("lost connection")
			return
		if not self.authorized(username, password):
			log.debug("login failed")
			self.writeline("login failed")
			self.exit_handler("login failed")
			return
		while True:
			self.write(self.TELNET_PROMPT)
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
		self.server_socket.listen(1)
		new_socket, address = self.server_socket.accept()
		self.handler(new_socket, address)
		log.debug("TelnetServer.server_greenlet(): stopping")
		
	def __init__(self, input_callback=None):
		log.debug("TelnetServer.__init__()")
		if not input_callback:
			log.warning("TelnetServer.__init__(): callback not specified")
			raise ServerError("callback not specified")
		self.parent_input_callback = input_callback
		
		# TODO: get username and password dynamically
		self.username = 'admin'
		self.password = '123'
	
		# TODO: get ip_address & port number dynamically
		# TODO: ensure that port is not already in use
		self.port = self.PORT_RANGE_LOWER
		self.ip_address = 'localhost'
		#self.ip_address = '67.58.49.202'
			
		# create telnet server object and start the server process
		self.server_socket = socket.socket()
		self.server_socket.allow_reuse_address = True
		while True:
			try:
				self.server_socket.bind((self.ip_address, self.port))
				break
			except:
				self.port = self.port + 1
				log.debug("trying to bind to port " + str(self.port))
				if self.port > self.PORT_RANGE_UPPER:
					log.warning("TelnetServer.server_greenlet(): no available ports for server")
					self.close_connection()
					return
		gevent.spawn(self.server_greenlet)
		
	def get_connection_info(self):
		return self.ip_address, self.port, self.username, self.password

	def stop(self):
		log.debug("TelnetServer.stop()")
		self.parent_requested_close = True
		try:
			self.server_socket.shutdown(socket.SHUT_RDWR)
		except Exception as ex:
			# can happen if telnet client closes session first
			log.debug("exception caught for socket shutdown:" + str(ex))
			return
		gevent.sleep(.1)
		self.server_socket.close()
		del self.server_socket
			
	def send(self, data):
		# send data from parent to telnet server process to forward to client
		log.debug("TelnetServer.write(): data = " + str(data))
		self.write(data)
		
