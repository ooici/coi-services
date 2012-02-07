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
	parentInputCallback = None
	username = None
	password = None
	fileobj = None
	parentRequestedClose = False
	telnetPrompt = 'ION telnet>'

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
	
	def exitHandler (self, reason):
		if not self.parentRequestedClose:
			self.parentInputCallback(-1)
		else:		
			try:
				self.server_socket.shutdown(socket.SHUT_RDWR)
			except Exception as ex:
				# can happen if telnet client closes session first
				log.debug("exception caught for socket shutdown:" + str(ex))
			self.server_socket.close()
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
			self.exitHandler("lost connection")
			return
		if username == '':
			self.exitHandler("lost connection")
			return
		self.write("Password: ")
		try:
			password = self.fileobj.readline().rstrip('\n\r')
		except EOFError:
			self.exitHandler("lost connection")
			return
		if not self.authorized(username, password):
			log.debug("login failed")
			self.writeline("login failed")
			self.exitHandler("login failed")
			return
		while True:
			self.write(self.telnetPrompt)
			try:
				inputLine = self.fileobj.readline()
			except EOFError:
				self.exitHandler("lost connection")
				break
			if inputLine == '':
				self.exitHandler("lost connection")
				break
			log.debug("rcvd: " + inputLine)
			log.debug("len=" + str(len(inputLine)))
			self.parentInputCallback(inputLine)
			
	def serverGreenlet(self):
		log.debug("TelnetServer.serverGreenlet(): started")
		self.server_socket = socket.socket()
		self.server_socket.allow_reuse_address = True
		self.server_socket.bind((self.ip_address, self.port))
		self.server_socket.listen(1)
		new_socket, address = self.server_socket.accept()
		self.handler(new_socket, address)
		
	def __init__(self, inputCallback=None):
		log.debug("TelnetServer.__init__()")
		if not inputCallback:
			log.warning("TelnetServer.__init__(): callback not specified")
			raise ServerError("callback not specified")
		self.parentInputCallback = inputCallback
		
		# TODO: get username and password dynamically
		self.username = 'admin'
		self.password = '123'
	
		# TODO: get ip_address & port number dynamically
		# TODO: ensure that port is not already in use
		self.port = 8000
		self.ip_address = 'localhost'
		#self.ip_address = '67.58.49.202'
			
		# create telnet server object and start the server process
		gevent.spawn(self.serverGreenlet)
		
	def getConnectionInfo(self):
		return self.ip_address, self.port, self.username, self.password

	def stop(self):
		log.debug("TelnetServer.stop()")
		self.parentRequestedClose = True
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
		
