import socket
import threading
import numpy as np
import time
import os
import random

# The IP used in connection
LOCALHOST = '127.0.0.1'
BUFFER_SIZE = 1024

# Setting the seed ensures that the port matrix is always identical
np.random.seed(1234)

# The number of nodes to simulate
n_nodes = 6

# Scale beteen 0 and this for the waiting time before one becomes a candidate, a scale of 3 means a waiting time of maximum 3 seconds.
candidate_waiting_scale = 3

# Create ports matrix, this is deterministic and ensures that senders/receivers always know what port to choose
# Ports are accessed by index [sender, receiver]
ports = np.random.choice(np.arange(8400, 8800), size=(n_nodes, n_nodes), replace=False)

# If one of these commands is send/received, shutdown the server
shutdown_commands = ["cc", "quit", "close"]

class Receiver(threading.Thread):
    """
    A receiver is one of two parts in the communication link between nodes. A receiver receives messages from only one specified sender
    and passes it on to the parent server (or node). Per other server, a receiver is created dedicated to only that other server.
    """
    def __init__(self, server, sender_node_name, port):
        threading.Thread.__init__(self)
        self.server = server
        self.sender_node_name = sender_node_name
        self.port = port
        self.running = True

    def run(self):
        receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ip_address = LOCALHOST
        receiver_socket.bind((ip_address, self.port))

        receiver_socket.listen(10)
        connection, _ = receiver_socket.accept()
        while self.running:
            received_message =  connection.recv(BUFFER_SIZE)
            if received_message:
                received_message = received_message.decode("utf-8")
                self.server.received_messages.append((received_message, self.sender_node_name))
                self.server.log.append((received_message, self.sender_node_name))
                if received_message in shutdown_commands:
                    connection.close()
                    self.running = False
                    break
                received_message = None

class Sender(threading.Thread):
    def __init__(self, server, receiver_node_name, receiver_port, receiver_ip=LOCALHOST):
        threading.Thread.__init__(self)
        self.server = server
        self.receiver_node_name = receiver_node_name
        self.receiver_port = receiver_port
        self.receiver_ip = receiver_ip
        self.message2send = None
        self.running = True
        
    def run(self):
        sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # print(F"Sender from {self.server.server_id} to {self.receiver_node_name} via {self.receiver_ip}:{self.receiver_port}")
        sender_socket.connect((self.receiver_ip, self.receiver_port))
        while self.running:
            if self.message2send != None:
                # print("Sending from", self.server.server_id, "to", self.receiver_node_name,"message", self.message2send)
                sender_socket.sendall(bytes(self.message2send, "utf-8"))
                if self.message2send in shutdown_commands:
                    self.running = False
                    break
                self.message2send = None


class Server(threading.Thread):
    def __init__(self, server_id):
        threading.Thread.__init__(self)
        self.running = True
        self.connected = False
        self.connected_servers = []

        self.ports = ports
        self.server_id = server_id
        self.received_messages = []
        self.log = []

        self.deadline = None

        self.state = "connecting" # ["connecting", "waiting", "candidate", "voted", "leader", "follower"]
        self.leader = None
        self.term = 0
        self.voted_term = 0

        self.senders = {}
        self.receivers = {}
        self.other_servers = []
        for i in range(n_nodes):
            if i != self.server_id:
                self.senders[i] = Sender(self, i, ports[self.server_id, i])
                self.receivers[i] = Receiver(self, i, ports[i, self.server_id])
                self.other_servers.append(i)

    def run(self):
        while self.running:
            if not self.connected:
                self.connecting_run()

            elif self.state == "waiting":
                self.waiting_run()

            elif self.state == "voted":
                self.voted_run()

            elif self.state == "candidate":
                self.candidate_run()

            elif self.state == "follower":
                self.follower_run()

            elif self.state == "leader":
                self.leader_run()

            
            else:
                # This should never happen
                break

    def connecting_run(self):
        # Send message to other nodes
        self.broadcast("acknowledge connection")

        # Receive from other nodes
        while len(self.connected_servers) < (n_nodes - 1):
            if len(self.received_messages) != 0:
                msg = self.received_messages.pop(0)
                msg, send_id = msg[0], msg[1]
                if msg == "acknowledge connection":
                    self.connected_servers.append(send_id)

        # Fully connected
        self.state = "waiting"
        self.connected = True

    def waiting_run(self):
        self.term = max(self.term, self.voted_term)
        self.voted_term = self.term

        waiting_time = random.random() * candidate_waiting_scale
        self.deadline = time.time() + waiting_time
        print(self.server_id, "waiting for", waiting_time)
        while self.state == "waiting":
            # Check for others becoming candidate, this check works better this way than the othe way around
            # as the other way around will result in many candidates on slow local systems
            if len(self.received_messages) != 0:
                msg = self.received_messages.pop(0)
                msg, send_id = msg[0], msg[1]
                if "vote for me as leader" in msg:
                    split_msg = msg.split(sep=" ")
                    if self.voted_term < int(split_msg[-1]):
                        self.voted_term = int(split_msg[-1])
                        # Someone else is a candidate
                        print(self.server_id, "voted for", send_id)
                        self.send("yes, I vote for you", send_id)
                        self.state = "voted"
                
                # Keep this as backup in case the leader gets elected but we miss it
                elif msg == "I have been elected as leader":
                    self.state = "follower"
                    self.leader = send_id            
            else:
                if time.time() > self.deadline:
                    # No longer waiting, become candidate
                    self.state = "candidate"


    def voted_run(self):
        while self.state == "voted":
            if len(self.received_messages) != 0:
                msg = self.received_messages.pop(0)
                msg, send_id = msg[0], msg[1]
                if "vote for me as leader" in msg:
                    split_msg = msg.split(sep=" ")
                    if self.voted_term < int(split_msg[-1]):
                        self.voted_term = int(split_msg[-1])
                        print(self.server_id, "voted for", send_id)
                        self.send("yes, I vote for you", send_id)
                    else:
                        print(self.server_id, "refused vote for", send_id)
                        self.send("no, I voted for someone else", send_id)
                elif msg == "I have been elected as leader":
                    self.state = "follower"
                    self.leader = send_id


    def candidate_run(self):
        self.term += 1
        print(self.server_id, "has become candidate")
        self.votes = {
            "y" : 1,
            "n" : 0
        }
        self.broadcast(F"vote for me as leader {self.term}")
        while self.state == "candidate":
            if len(self.received_messages) != 0:
                msg = self.received_messages.pop(0)
                msg, send_id = msg[0], msg[1]
                if msg == "yes, I vote for you":
                    self.votes["y"] += 1
                elif msg == "no, I voted for someone else":
                    self.votes["n"] += 1
                
                elif "vote for me as leader" in msg:
                    split_msg = msg.split(sep=" ")
                    if self.term < int(split_msg[-1]):
                        self.voted_term = int(split_msg[-1])
                        print(self.server_id, "voted for", send_id)
                        self.send("yes, I vote for you", send_id)
                        self.state = "voted"

                    else:
                        print(self.server_id, "refused vote for", send_id)
                        self.send("no, I voted for someone else", send_id)

                elif msg == "I have been elected as leader":
                    self.state = "follower"
                    self.leader = send_id
                    break        

            if self.votes["y"] > n_nodes / 2:
                print(self.server_id, "became leader")
                # Majority votes yes, you have been elected
                self.state = "leader"

            elif self.votes["n"] > n_nodes / 2:
                print(self.server_id, "failed to become leader")
                # Majority votes no, become waiting again
                self.state = "waiting"

        
    def leader_run(self):
        self.broadcast("I have been elected as leader")
        print(self.server_id, "is leader, sending shutdown in 3 seconds")
        self.leader = self.server_id
        for i in range (3):
            # Sending multiple leader reminders for any stragglers stuck in candidate/waiting/failed elections
            time.sleep(1)
            self.broadcast("I have been elected as leader")

        self.broadcast("cc")
        self.running = False
    
    def follower_run(self):
        print(self.server_id, "is follower, waiting for shutdown")
        while self.running:
            if len(self.received_messages) != 0:
                msg = self.received_messages.pop(0)
                msg, send_id = msg[0], msg[1]
                if msg in shutdown_commands:
                    # self.close_connections()
                    self.running = False


    def send(self, message2send, receiving_server_id):
        try:
            self.senders[receiving_server_id].message2send = message2send
        except:
            pass

    def broadcast(self, message2broadcast):
        try:
            for i in self.other_servers:
                self.senders[i].message2send = message2broadcast
        except:
            print("Something went wrong")
            pass

    def start_listening(self):
        for i in self.other_servers:
            self.receivers[i].start()

    def start_senders(self):
        for i in self.other_servers:
            self.senders[i].start()

    def close_connections(self):
        self.running = False
        for i in self.other_servers:
            self.broadcast(shutdown_commands[0])
            self.receivers[i].running = False
            self.senders[i].running = False

# Print port matrix
print(ports)

servers = []
# Create the different servers
for s in range(n_nodes):
    servers.append(Server(s))

# Open receiver channels
for s in servers:
    s.start_listening()

# Open sender channels
for s in servers:
    s.start_senders()

# Start the internal servers' logic
for s in servers:
    s.start()

# Run wait for 40 seconds before getting results
time.sleep(40)
print("\n\n")
for s in servers:
    print(s.server_id, "logs", s.log)
    print(s.server_id, "has leader", s.leader, "\n")
    s.close_connections()

# Force close the threads
os._exit(0)
