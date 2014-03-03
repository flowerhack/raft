"""
Logging code for raft
"""

class Log(object):
	def __init__(self):
		self.records = []

	def log(message):
		self.records.append(message)