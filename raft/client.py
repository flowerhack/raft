"""
Client-side raft code
"""

import json
import socket
import sys

class Client(object):
	TCP_IP = '127.0.0.1'
	TCP_PORT = 55534
	BUFFER_SIZE = 1024

	# TODO put this in a stdin loop

	message = {}
	message['sender'] = 'CLIENT'

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((TCP_IP, TCP_PORT))
	print "Please input commands."
	while True:
		line = sys.stdin.readline()
		message['message'] = line
		s.send(json.dumps(message))
		print "Message sent!"
		data = s.recv(BUFFER_SIZE)
	s.close()
	print "Received data:", data