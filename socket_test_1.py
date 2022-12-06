import socket
import threading
import numpy as np
import time
import os

LOCALHOST = '127.0.0.1'
BUFFER_SIZE = 1024

np.random.seed(1234)
n_nodes = 6
ports = np.random.choice(np.arange(8400, 8800), size=(n_nodes, n_nodes), replace=False)
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
        # hostname = socket.gethostname()
        # ip_address = socket.gethostbyname(hostname) 
        ip_address = LOCALHOST
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
                print("Sending from", self.server.server_id, "to", self.receiver_node_name,"message", self.message2send)
                sender_socket.sendall(bytes(self.message2send, "utf-8"))
                if self.message2send in shutdown_commands:
                    self.running = False
                    break
                self.message2send = None


class Server(threading.Thread):
    def __init__(self, server_id):
        threading.Thread.__init__(self)
        self.running = True
        self.ports = ports
        self.server_id = server_id
        self.received_messages = []

        self.senders = {}
        self.receivers = {}
        self.other_servers = []
        for i in range(n_nodes):
            if i != self.server_id:
                self.senders[i] = Sender(self, i, ports[self.server_id, i])
                self.receivers[i] = Receiver(self, i, ports[i, self.server_id])
                self.other_servers.append(i)
        # print(F"I am {self.server_id} and the others are {self.other_servers}")


    def run(self):
        while self.running:
            if len(self.received_messages) != 0:
                msg = self.received_messages.pop(0)
                print("received", msg, "on server", self.server_id)
                if msg[0] in shutdown_commands:
                    self.running = False
                    break

        # if self.server_id == 0:
        #     time.sleep(3)
        #     self.broadcast("quit")

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

print(ports)

servers = []
for s in range(n_nodes):
    servers.append(Server(s))

for s in servers:
    s.start_listening()

for s in servers:
    s.start_senders()

for s in servers:
    s.start()

time.sleep(5)
servers[0].broadcast("Hello, I am server 0")
time.sleep(5)
servers[0].broadcast("Hello again, still server 0")
time.sleep(5)

for s in servers:
    print(s.server_id, "received", s.received_messages)
    s.close_connections()

os._exit(0)
