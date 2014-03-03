"""
Server-side raft code
"""

import raftlog
import config
import logging as syslog
import json
import time

import socket
from random import choice, randint

STATUSES = ['LEADER', 'FOLLOWER', 'CANDIDATE']

MESSAGE_TYPES = ['AppendEntries', 'AppendEntriesCommitted', 'AcceptAppendEntries', 'RequestVote', 'GiveVote', 'DenyVote', 'Heartbeat']

# Flow:
# handle_request -> process_[type]_request -> [broadcast,send]_request

class Server(object):
    def __init__(self):
        self.id = 1  # TODO Replace with UUID
        self.status = 'FOLLOWER'  # TODO Change! Loading as 'LEADER' only for test purposes!
        self.term = 1 # Persist to stable storage
        self.raftlog = raftlog.Log() # Persist to stable storage
        self.voted_for = self.id # Persist to stable storage
        self.leader_id = None
        self.active = True
        self.election_timeout = randint(100,500)
        self.start_time = time.clock()
        self.vote_count = 0
        self.valid_rpc_received = False

# -------------- Handling and Processing Requests --------------

    def handle_request(self, message):
        """ Handles all incoming requests """
        # Parse out information sent
        # TODO

        sender = message['sender']

        # Perform appropriate action based on sender type
        if sender == 'CLIENT':
            self.process_client_request(message)
        elif sender == 'CANDIDATE':
            self.process_candidate_request(message)
        elif sender == 'LEADER':
            self.process_leader_request(message)
        elif sender == 'FOLLOWER':
            self.process_follower_request(message)

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
        self.term = self.term + 1
        self.state = 'CANDIDATE'
        self.vote = self.id
        self.broadcast_request('RequestVote')

    def check_timeouts(self):
        if (time.clock() - self.start_time) > self.election_timeout:
            if not (self.valid_rpc_received or self.vote_granted): 
                self.start_new_election()

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
            message['last_log_index'] = self.raftlog.HOWHANDLE
            message['last_log_term'] = self.raftlog.HOWHANDLE

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

    def broadcast_request(self, type):
        for server in config.SERVER_IDS:
            pass

    def send_request(self, msg_type, data):
        # Open up a socket
        # Send to socket
        json(dumps(self.craft_message(msg_type, data)))
        # Close socket

    def forward_request(self, data):
        pass

# -------------- TCP listener --------------
    # I should probably abstract out the protocol at some point.

    def run(self):
        print "yo"
        TCP_IP = '127.0.0.1'
        TCP_PORT = 55534
        BUFFER_SIZE = 1024

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # This line is for testing only! TODO remove!
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((TCP_IP, TCP_PORT))
        s.listen(True)

        conn, addr = s.accept()

        # Busyloop that listens to incoming requests.
        while (self.active):
            print "Running..."
            data = conn.recv(BUFFER_SIZE)
            if data:
                print "Requst received"
                response = self.handle_request(json.loads(data))
                conn.send(response)
            else:
                self.check_timeouts()
        conn.close()

server = Server()
server.run()
