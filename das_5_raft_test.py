# THIS SCRIPT ONLY WORKS WITH PYTHON 2.7.5!!!
# The code was originally written for python 3.7 but the DAS-5 server only has a python3 version that does not include numpy and I need numpy
# Some print statements are therefore a bit weird or are printed as weird tuples, sorry for that! :(
import socket
import threading
import numpy as np
import time
import os
import random
import argparse

# Localhost is not used
LOCALHOST = '127.0.0.1'
BUFFER_SIZE = 1024

# DAS-5 local address
DAS5HOST = "10.141.0."

# Setting the seed ensures that the port matrix is always identical
np.random.seed(1234)

# Scale beteen 0 and this for the waiting time before one becomes a candidate, a scale of 3 means a waiting time of maximum 3 seconds.
candidate_waiting_scale = 3

parser = argparse.ArgumentParser(description="Start RAFT client")
parser.add_argument("reserved_nodes")

args = parser.parse_args()

# Identify reserved nodes
reserved_nodes = args.reserved_nodes.split()

# Create ports matrix, this is deterministic and ensures that senders/receivers always know what port to choose
n_nodes = len(reserved_nodes)
ports = np.random.choice(np.arange(8400, 8800), size=(n_nodes, n_nodes), replace=False)

# Get this node's ip address and node name
local_name = socket.gethostname()
local_ip_address = socket.gethostbyname(local_name)
local_name_index = reserved_nodes.index(local_name)

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
        # Set IP address to this server's IP and get the assigned port
        receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname) 

        receiver_socket.bind((ip_address, self.port))

        # Start listening for the sender of the other server
        receiver_socket.listen(10)

        # Accept the incoming connection
        connection, _ = receiver_socket.accept()

        while self.running:
            # Loop such that messages can always be received
            received_message =  connection.recv(BUFFER_SIZE)

            if received_message:
                # Message is received, decode message
                received_message = received_message.decode("utf-8")

                # Add message to log and to the server's inbox. 
                # The log is not the same as a RAFT log, but serves as a storage medium for all received messages
                self.server.received_messages.append((received_message, self.sender_node_name))
                self.server.log.append((received_message, self.sender_node_name))
                
                if received_message in shutdown_commands:
                    # We have received a message to shut down the server
                    connection.close()
                    self.running = False
                    self.server.running = False
                    break

                # Reset the received_message
                received_message = None

class Sender(threading.Thread):
    """
    A sender is one of two parts in the communication link between nodes. A sender sends messages the parent server supplied to only one
    specified receiver. Per other server, there is one dedicated sender that only sends to that other server
    """
    def __init__(self, server, receiver_node_name, receiver_port, receiver_ip=LOCALHOST):
        threading.Thread.__init__(self)
        self.server = server
        self.receiver_node_name = receiver_node_name

        # Receiver information such as port and IP
        self.receiver_port = receiver_port
        self.receiver_ip = receiver_ip

        self.running = True

        # If this variable is set to something other than None, it is encoded and transmitted
        self.message2send = None
        
    def run(self):
        sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Create the connection to the target receiver

        sender_socket.connect((self.receiver_ip, self.receiver_port))

        while self.running:
            # Loop such that messages can always be send
            if self.message2send != None:
                # Send the message that has to be send
                sender_socket.sendall(bytes(self.message2send.encode("utf-8")))
                if self.message2send in shutdown_commands:
                    # If a shutdown command is send, also close the connection on this side and exit the loop
                    self.running = False
                    break

                # Reset message2send
                self.message2send = None


class Server(threading.Thread):
    """
    This is the main server object, of which there should be one per node (in a distrbuted setting) or multiple on a local system.
    Servers have senders and receivers that are bound to other server such that there is a two way connection between itself and all other servers.
    There are different states the servers can be in, this is explained in the sections on states below. 
    These states encode what role/phase the server is currently in for the RAFT leader election.
    """
    def __init__(self, server_id):
        threading.Thread.__init__(self)
        self.running = True
        self.connected = False # Only true once it has connected to all other servers
        self.connected_servers = []

        # The port matrix used to find what ports should be used for what server.
        self.ports = ports
        self.server_id = server_id

        # This servers as an unread inbox, once a message is put in here and dealt with, it is removed from the list
        self.received_messages = [] 

        # This is not a RAFT log, but serves as a medium to store all messages received during the election.
        self.log = []

        self.deadline = None

        # All possible states the server can be in
        self.state = "connecting" # ["connecting", "waiting", "candidate", "voted", "leader", "follower"]
        self.leader = None # What server is leader
        self.term = 0 # The term this server has as a candidate
        self.voted_term = 0 # The highest term this server voted for 

        # These dictionaries contain the senders and receivers
        self.senders = {}
        self.receivers = {}

        # A list of other server IDs, used in the for loop below
        self.other_servers = []
        
        for i in range(n_nodes):
            if i != self.server_id:
                ip_addition = str(int(reserved_nodes[i][-2:])) # Determines what number should come after the DAS 5 local address

                # Create sender and receiver for each other server
                self.senders[i] = Sender(self, i, ports[self.server_id, i], receiver_ip=str(DAS5HOST + ip_addition))
                self.receivers[i] = Receiver(self, i, ports[i, self.server_id])
                self.other_servers.append(i)

    def run(self):
        while self.running:
            if not self.connected:
                # Create connections to other servers
                self.connecting_run()

            elif self.state == "waiting":
                # Wait until the server receives a vote request or has waited long enough to become a candidate
                self.waiting_run()

            elif self.state == "voted":
                # The server has voted and will reject other candidates with nonhigher terms
                self.voted_run()

            elif self.state == "candidate":
                # The server is a candidate and will ask for votes from other servers
                self.candidate_run()

            elif self.state == "follower":
                # The server has been designated a follower, someone else has been elected
                self.follower_run()

            elif self.state == "leader":
                # The server has been elected as leader and commands other servers to become followers
                self.leader_run()
            
            else:
                # We should never arrive at this point, if so break
                break

    def connecting_run(self):
        # Send message to other nodes to acknowledge the connection
        self.broadcast("acknowledge connection")

        # Receive acknowledgement from other nodes, only continue when all nodes responded
        while len(self.connected_servers) < (n_nodes - 1):
            if len(self.received_messages) != 0:
                msg = self.received_messages.pop(0)
                msg, send_id = msg[0], msg[1]
                if msg == "acknowledge connection":
                    # Process acknowledgement
                    self.connected_servers.append(send_id)

        # This point should online arrive once fully connected
        print(self.server_id, "fully connected")
        self.state = "waiting" # Transition from connecting to waiting
        self.connected = True # Fully connected


    def waiting_run(self):
        self.term = max(self.term, self.voted_term) # Update your term just in case you become candidate
        self.voted_term = self.term

        # Wait for random time
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
                    # Someone requested your vote
                    split_msg = msg.split()
                    if self.voted_term < int(split_msg[-1]):
                        # Only vote for higher terms than you have voted for previously
                        self.voted_term = int(split_msg[-1]) # Update highest voted term
                        print(self.server_id, "voted for", send_id)
                        
                        # Send vote yes
                        self.send("yes, I vote for you", send_id)

                        # Update state to voted
                        self.state = "voted"
                
                # Keep this as backup in case the leader gets elected but we miss it
                elif msg == "I have been elected as leader":
                    self.state = "follower"
                    self.leader = send_id            
            else:
                if time.time() > self.deadline:
                    # We have waited for too long, time to become a candidate
                    self.state = "candidate"


    def voted_run(self):
        while self.state == "voted":
            # Loop until a leader is elected or until a request of higher term is send
            if len(self.received_messages) != 0:
                msg = self.received_messages.pop(0)
                msg, send_id = msg[0], msg[1]
                if "vote for me as leader" in msg:
                    split_msg = msg.split()

                    if self.voted_term < int(split_msg[-1]):
                        # Only vote yes if term is higher
                        self.voted_term = int(split_msg[-1])
                        print(self.server_id, "voted for", send_id)
                        self.send("yes, I vote for you", send_id)

                    else:
                        # Else vote no
                        print(self.server_id, "refused vote for", send_id)
                        self.send("no, I voted for someone else", send_id)

                elif msg == "I have been elected as leader":
                    # Someone became leader and commanded this server becomes follower
                    self.state = "follower"
                    self.leader = send_id


    def candidate_run(self):
        # Increment term
        self.term += 1
        print(self.server_id, "has become candidate")

        # Keep track of votes
        self.votes = {
            "y" : 1,
            "n" : 0
        }
        self.broadcast("vote for me as leader " + str(self.term))
        while self.state == "candidate":
            if len(self.received_messages) != 0:
                msg = self.received_messages.pop(0)
                msg, send_id = msg[0], msg[1]

                # Process votes
                if msg == "yes, I vote for you":
                    self.votes["y"] += 1
                elif msg == "no, I voted for someone else":
                    self.votes["n"] += 1
                
                # Reply to other candidates, only votes yes for higher terms and no for others
                elif "vote for me as leader" in msg:
                    split_msg = msg.split()
                    if self.term < int(split_msg[-1]):
                        self.voted_term = int(split_msg[-1])
                        print(self.server_id, "voted for", send_id)
                        self.send("yes, I vote for you", send_id)
                        # We are no longer a candidate and will move to the voted state
                        self.state = "voted"

                    else:
                        print(self.server_id, "refused vote for", send_id)
                        self.send("no, I voted for someone else", send_id)

                elif msg == "I have been elected as leader":
                    # Someone became leader and commanded this server becomes follower
                    self.state = "follower"
                    self.leader = send_id
                    break        

            if self.votes["y"] > n_nodes / 2:
                print(self.server_id, "became leader")
                # Majority votes yes, you have been elected
                self.state = "leader"

            elif self.votes["n"] > n_nodes / 2:
                print(self.server_id, "failed to become leader")
                # Majority votes no, no need to continue asking for votes, become waiting again
                self.state = "waiting"

        
    def leader_run(self):
        # Send to all other nodes that you have been elected
        self.broadcast("I have been elected as leader")
        print(self.server_id, "is leader, sending shutdown in 3 seconds")
        self.leader = self.server_id
        for i in range (3):
            # Sending multiple leader reminders for any stragglers stuck in candidate/waiting/failed elections
            time.sleep(1)
            self.broadcast("I have been elected as leader")

        # Send shutdown command
        self.broadcast("cc")
        self.running = False
    
    def follower_run(self):
        # Server has become a follower and is waiting for the shutdown command
        print(self.server_id, "is follower, waiting for shutdown")
        while self.running:
            if len(self.received_messages) != 0:
                msg = self.received_messages.pop(0)
                msg, send_id = msg[0], msg[1]
                if msg in shutdown_commands:
                    # self.close_connections()
                    self.running = False


    def send(self, message2send, receiving_server_id):
        # Message sending function, sends message2send to the server with number receiving_server_id
        try:
            self.senders[receiving_server_id].message2send = message2send
        except:
            pass

    def broadcast(self, message2broadcast):
        # Broadcast message2broadcast to all other servers
        try:
            for i in self.other_servers:
                self.senders[i].message2send = message2broadcast
        except:
            pass

    def start_listening(self):
        # Open the listening channels on the receivers, this should be done before the sending channels are opened
        for i in self.other_servers:
            self.receivers[i].start()

    def start_senders(self):
        # Open the sending channels on the senders, this should be done after the receiving channels are opened
        for i in self.other_servers:
            self.senders[i].start()

    def close_connections(self):
        # Close all open connections and send shutdown commands to all
        self.running = False
        for i in self.other_servers:
            self.broadcast(shutdown_commands[0])
            self.receivers[i].running = False
            self.senders[i].running = False

# Create a single server instance for the DAS-5 node we are currently on
node_server = Server(local_name_index)

# Open the receiving channels
node_server.start_listening()

# Print for double checking
print("This is node", local_name, "with address", local_ip_address, "and index", local_name_index)

# This statement halts the process until you have the receiver channels opened on all nodes
blank = raw_input("Only continue once all nodes are running")

# Start the election!
node_server.start_senders()
node_server.start()
time.sleep(10)
# Print results after 10 seconds
print("Node", local_name, "has leader", node_server.leader, "and voted term", node_server.voted_term, "and term", node_server.term)
print("Closing process in 5 seconds")

# Shutdown in 5 seconds, ensures that no threads continue running in the background eating away memory or taking up ports
time.sleep(5)
# Forceful shutdown
os._exit(0)