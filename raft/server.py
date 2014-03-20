"""
Server-side raft code
"""

import raftlog
import config
import logging as syslog
import json
import time
import asyncio
import functools
import os
import signal
import pdb
import sys

import socket
from random import choice, randint

STATUSES = ['LEADER', 'FOLLOWER', 'CANDIDATE']

MESSAGE_TYPES = ['AppendEntries', 'AppendEntriesCommitted', 'AcceptAppendEntries', 'RequestVote', 'GiveVote', 'DenyVote', 'Heartbeat']

# Flow:
# handle_request -> process_[type]_request -> [broadcast,send]_request

class Server(object):
    def __init__(self, loop):
        self.id = 1  # TODO Replace with UUID
        self.status = 'FOLLOWER'  # TODO Change! Loading as 'LEADER' only for test purposes!
        self.term = 1 # Persist to stable storage
        self.raftlog = raftlog.Log() # Persist to stable storage
        self.voted_for = self.id # Persist to stable storage
        self.leader_id = None
        self.active = True
        self.election_timeout = randint(100,500)/1000
        self.start_time = time.clock()
        self.vote_count = 0
        self.valid_rpc_received = False
        self.loop = loop
        self.init_timeouts()

# -------------- Handling and Processing Requests --------------

    @asyncio.coroutine
    def handle_request(self, message):
        """ Handles all incoming requests """
        # Parse out information sent
        # TODO

        sender = message['sender']

        # Perform appropriate action based on sender type
        if sender == 'CLIENT':
            response = yield from self.process_client_request(message)
        elif sender == 'CANDIDATE':
            response = yield from self.process_candidate_request(message)
        elif sender == 'LEADER':
            response = yield from self.process_leader_request(message)
        elif sender == 'FOLLOWER':
            response = yield from self.process_follower_request(message)
        else:
            response = "Error"

        return response

    def process_follower_request(self, sender, message):
        """ Handle requests issued by followers. """
        if message['type'] == 'GiveVote':
            self.vote_count += 1
            if self.vote_count >= config.NEEDED_FOR_CONSENSUS:
                self.become_leader()
        if message['type'] == 'DenyVote':
            pass
            # Do the DenyVote protocol.
        if message['type'] == 'AcceptAppendEntries':
            if self.status == 'LEADER':
                # See if we can commit yet.  If we can...
                self.perform_action(message)
                # Do we mark it as committed?
                self.broadcast_request('AppendEntriesCommitted')

    def process_client_request(self, message):
        """ Handle requests issued by clients. """
        if self.status == 'LEADER':
            self.raftlog.append(message)
            self.broadcast_request('AppendEntries', message)
        else:
            # All client requsts are handled by the leader.
            self.forward_request(sender, message, leader_id)

    def process_leader_request(self, message):
        """ Handle requests issued by leaders. """
        # If we're receiving a valid leader request, elections are done.
        # TODO: must check leader's validity before becoming follower!
        if self.status != 'FOLLOWER':
            self.status = 'FOLLOWER'

        if message.type == 'AppendEntries':
            if self.consistency_check(message):
                self.raftlog.append(message)
                return True
            else:
                return False

    # ish
    def process_candidate_request(self, message):
        """ Handle requests issued by candidates. """
        # if self.status = 'LEADER'
        if message['term'] > self.term:
            self.term = message['term']
        if message['term'] == self.term:
            if voted_for is None or voted_for == self.id:
                if message['log'] >= self.raftlog:
                    vote_granted = True
                    election_timeout.reset()

        return vote_granted, self.term

# -------------- Utility Functions --------------

    def become_leader(self):
        self.nextIndex = self.raftlog.last.index + 1
        self.status = 'LEADER'
        self.broadcast_request('Heartbeat')

    def start_new_election(self):
        """ Start a new election and announce self as a candidate. """
        print('Starting new election')  # DEBUGMSG
        self.term = self.term + 1
        self.state = 'CANDIDATE'
        self.vote = self.id
        self.broadcast_request('RequestVote')

    def check_timeouts(self):
        """ Check to see if a new election needs to be called. """
        print('Checking timeouts')  # DEBUGMSG
        if not self.valid_rpc_received:
            self.start_new_election()
        else:
            self.election_timeout = randint(100,500)
            self.loop.call_later(self.election_timeout, self.check_timeouts)

    # This function may not be necessary.
    def init_timeouts(self):
        print('Initializing timeouts')  # DEBUGMSG
        self.loop.call_later(self.election_timeout, self.check_timeouts)

    def consistency_check(self):
        """
        Before a follower performs an AppendEntries call,
        check to see if the leader is valid.
        """
        # If the leader has a higher term, update our own term.
        if message['term'] > self.term:
            self.term = message['term']
            self.election_timeout.reset()
            # TODO fix these two lines
            if prev_log_index.term != prev_log_term:
                return "failed"
            else:
                # If existing entries conflict with new entries,
                # delete all existing entries starting with first conflicting entry
                # Append any new entries not already in log
                # Advance state machine with newly committed entries
                pass

    def craft_message(self, msg_type, data):
        message = {}
        message['type'] = msg_type
        if msg_type == 'AppendEntries':
            message['term'] = self.term
            message['leader_id'] = self.id  # Only leaders will send AppendEntries
            message['prev_log_index'] = self.raftlog.HOWHANDLE
            message['prev_log_term'] = self.raftlog.HOWHANDLE
            message['entries'] = data
            message['commit_index'] = WHATDO

        if msg_type == 'RequestVote':
            message['candidate_id'] = self.id  # Candidates send their own RequestVotes
            message['term'] = self.term
            try:
                message['last_log_index'] = self.raftlog[-1][0]
            except:
                message['last_log_index'] = None
            try:
                message['last_log_term'] = self.raftlog[-1][1]
            except:
                message['last_log_term'] = None

        if msg_type == 'Heartbeat':
            message['sender_id'] = self.id

        if msg_type == 'AppendEntriesCommitted':
            pass

        if msg_type == 'AcceptAppendEntries':
            pass

        if msg_type == 'RequestVote':
            pass

        if msg_type == 'DenyVote':
            pass

        return message

# -------------- Sending, Broadcasting Requests --------------

    def broadcast_request(self, msg_type, data=None):
        """ Broadcrast a msg_type request to all servers """
        for server in config.SERVER_IDS:
            if not ((server[0] == TCP_IP) and (server[1] == TCP_PORT)):
                asyncio.Task(self.send_request(server, msg_type, data))

    @asyncio.coroutine
    def send_request(self, target, msg_type, data=None):
        """ Send a msg_type request to target server """
        # TODO handle the case where the connection is refused! ConnectionRefusedError
        try:
            reader, writer = yield from asyncio.open_connection(target[0], target[1])
            while True:
                line = yield from reader.readline()
                if not line:
                    break
        except ConnectionRefusedError:
            # TODO: handle the case where we never get a response back;
            # do we need to explicitly declare servers "dead"?
            pass

    def forward_request(self, data):
        """ Forward a request to the leader """
        pass

    def eof_received(self):
        pass

# -------------- TCP listener --------------
    # I should probably abstract out the protocol at some point.

    def connection_made(self, transport):
        print('Connection received')
        self.transport = transport

    def data_received(self, data):
        print('Data received')
        response = yield from self.handle_request(data)

        self.transport.write(response)

        self.transport.close()

    def connection_lost(self, exec):
        pass

TCP_IP = str(sys.argv[1])
TCP_PORT = int(sys.argv[2])

loop = asyncio.get_event_loop()
coro = loop.create_server(functools.partial(Server, loop=loop), TCP_IP, TCP_PORT)
server = loop.run_until_complete(coro)
print('Serving requests')
try:
    loop.run_forever()
except KeyboardInterrupt:
    print("exit")
finally:
    server.close()
    loop.close()