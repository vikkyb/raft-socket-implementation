# THIS SCRIPT ONLY WORKS WITH PYTHON 2.7.5!!!

import socket
import threading
import numpy as np
import time
import os
import random
import argparse

LOCALHOST = '127.0.0.1'
BUFFER_SIZE = 1024
DAS5HOST = "10.141.0."
np.random.seed(1234)
candidate_waiting_scale = 3


parser = argparse.ArgumentParser(description="Start RAFT client")
parser.add_argument("reserved_nodes")

args = parser.parse_args()

reserved_nodes = args.reserved_nodes.split()
print(reserved_nodes)

n_nodes = len(reserved_nodes)
ports = np.random.choice(np.arange(8400, 8800), size=(n_nodes, n_nodes), replace=False)

local_name = socket.gethostname()
local_ip_address = socket.gethostbyname(local_name)

local_name_index = reserved_nodes.index(local_name)

# ports = np.random.randint(8400, 8800, (n_nodes, n_nodes))
# ports are accessed by [sender, receiver]

shutdown_commands = ["cc", "quit", "close"]

class Receiver(threading.Thread):
    def __init__(self, server, sender_node_name, port):
        threading.Thread.__init__(self)
        self.server = server
        self.sender_node_name = sender_node_name
        self.port = port
        self.running = True

    def run(self):
        receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname) 
        # ip_address = LOCALHOST
        print("\nOpening receiver on server", self.server.server_id, "with ip and port", ip_address, self.port)
        receiver_socket.bind((ip_address, self.port))
        # print(F"Receiver from {self.sender_node_name} to {self.server.server_id} via {ip_address}:{self.port}")


        receiver_socket.listen(10)
        connection, _ = receiver_socket.accept()
        while self.running:
            received_message =  connection.recv(BUFFER_SIZE)
            if received_message:
                received_message = received_message.decode("utf-8")
                # print("Message received", received_message)
                self.server.received_messages.append((received_message, self.sender_node_name))
                self.server.log.append((received_message, self.sender_node_name))
                if received_message in shutdown_commands:
                    connection.close()
                    self.running = False
                    self.server.running = False
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
        print("Opening sender on server", self.server.server_id, "to ip and port", self.receiver_ip, self.receiver_port)

        # print(F"Sender from {self.server.server_id} to {self.receiver_node_name} via {self.receiver_ip}:{self.receiver_port}")
        sender_socket.connect((self.receiver_ip, self.receiver_port))
        while self.running:
            if self.message2send != None:
                # print("Sending from", self.server.server_id, "to", self.receiver_node_name,"message", self.message2send)
                sender_socket.sendall(bytes(self.message2send.encode("utf-8")))
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
                ip_addition = str(int(reserved_nodes[i][-2:]))
                self.senders[i] = Sender(self, i, ports[self.server_id, i], receiver_ip=str(DAS5HOST + ip_addition))
                self.receivers[i] = Receiver(self, i, ports[i, self.server_id])
                self.other_servers.append(i)
        # print(F"I am {self.server_id} and the others are {self.other_servers}")

    def run(self):
        while self.running:
            if not self.connected:
                self.connecting_run()
                # print(self.server_id, "fully connected", self.state, self.connected_servers)
                # Create connections
                # at some point fully connected and become waiting

            elif self.state == "waiting":
                # Wait until deadline is passed
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
                # if len(self.received_messages) != 0:
                #     msg = self.received_messages.pop(0)
                #     print("received", msg, "on server", self.server_id)
                #     if msg[0] in shutdown_commands:
                #         self.running = False
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
        print(self.server_id, "fully connected")
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
        self.broadcast("vote for me as leader " + str(self.term))
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

node_server = Server(local_name_index)
node_server.start_listening()
print("This is node", local_name, "with address", local_ip_address, "and index", local_name_index)
blank = raw_input("Only continue once all nodes are running")
node_server.start_senders()
node_server.start()
time.sleep(10)
print("Node", local_name, "has leader", node_server.leader)
print("Closing process in 5 seconds")

# print(ports)

# servers = []
# for s in range(n_nodes):
#     servers.append(Server(s))

# for s in servers:
#     s.start_listening()

# for s in servers:
#     s.start_senders()

# for s in servers:
#     s.start()

# # time.sleep(5)
# # servers[0].broadcast("Hello, I am server 0")
# # time.sleep(5)
# # servers[0].broadcast("Hello again, still server 0")
# # time.sleep(5)

time.sleep(5)
# print("\n\n")
# for s in servers:
#     print(s.server_id, "logs", s.log)
#     print(s.server_id, "has leader", s.leader, "\n")
#     s.close_connections()

os._exit(0)
