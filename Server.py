import socket
import threading
import pickle  # Python object serialization
import traceback  # Stacktrace in case of exceptions
import Message_Types
import struct
import Vector_Clock
import time


class Server(threading.Thread):

    HOST = socket.gethostname()  # get server host name
    IP_ADDRESS = socket.gethostbyname(HOST)  # get server ip address by host name
    TCP_PORT = 10000
    BROADCAST_PORT = 12000
    HEARTBEAT_PORT = 14000
    MULTICAST_PORT = 16000
    MULTICAST_PORT_SERVERS = 18000
    ACK_PORT = 20000
    SERVER_JOIN_PORT = 22000
    MULTICAST_GROUP = "224.1.1.1"  # multicast group address
    MULTICAST_TTL = 2
    BROADCAST_ADDRESS = "255.255.255.255"  # broadcast address
    LEADER_ADDRESS = None
    started_servers = []  # list of started servers

    def __init__(self):
        threading.Thread.__init__(self)  # inherits all threading methods
        self.lock = threading.Lock()
        self.contacted_connections = {}  # for heartbeat
        self.received_ack_from_connections = {}  # for heartbeat
        self.listen_to_replication_thread = None
        self.number_of_join_replies_received = 0
        self.leader_address = 0
        self.is_leader = False
        self.connected_clients = {}
        self.client_connections = {}
        self.server_connections = {}
        self.message_history = []
        self.server_address = self.IP_ADDRESS
        self.server_port = self.TCP_PORT
        self.broadcast_port = self.BROADCAST_PORT
        self.heartbeat_port = self.HEARTBEAT_PORT
        self.multicast_port = self.MULTICAST_PORT
        self.multicast_port_servers = self.MULTICAST_PORT_SERVERS
        self.server_join_port = self.SERVER_JOIN_PORT
        self.multicast_group = self.MULTICAST_GROUP
        self.multicast_ttl = self.MULTICAST_TTL
        self.ack_port = self.ACK_PORT
        self.new_client_clock = Vector_Clock.Vector_Clock(None)
        self.message_queue = {}
        self.UDP_thread = threading.Thread(target=self.create_BROADCAST_socket)
        self.MULTICAST_thread = threading.Thread(target=self.create_MULTICAST_socket)
        self.MULTICAST_thread.start()
        self.ACK_thread = threading.Thread(target=self.create_ACK_socket)
        self.ACK_thread.start()
        self.create_SERVER_JOIN_socket()
        self.create_LE_ring_socket()
        self.check_for_dead_connections_thread = threading.Thread(target=self.check_for_dead_connections)
        self.check_for_dead_connections_thread.start()
        self.UDP_thread.start()
        self.join_network()
        self.create_HEARTBEAT_socket()
        self.receive_heartbeat_thread = threading.Thread(target=self.receive_heartbeat)
        self.receive_heartbeat_thread.start()
        self.send_heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.send_heartbeat_thread.start()
        self.ACK_thread.join()

    # Creates a broadcast socket for dynamic discovery
    def create_BROADCAST_socket(self):
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.broadcast_socket.bind((self.server_address, self.broadcast_port))
        self.receive_broadcast_messages()

    # Creates a tcp socket for sending/receiving messages to/from the clients
    def create_TCP_socket(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.server_address, self.server_port))
        Server.started_servers.append((self.server_address, self.server_port))  # adding started server to list
        self.server_socket.listen(3)
        self.listen_to_connecting_clients()

    # Creates a multicast socket for sending group messages to the clients
    def create_MULTICAST_socket(self):
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)  # For specific network configuration

    # Creates a heartbock socket for crash fault tolerance
    def create_HEARTBEAT_socket(self):
        self.heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # Set a timeout so the socket does not block indefinitely when trying to receive data.
        # self.heartbeat_socket.settimeout(60)
        # Set the time-to-live for messages to 1 so they do not go past the local network segment.
        ttl = struct.pack('b', 1)
        self.heartbeat_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        self.heartbeat_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind to the server address
        server_address = ('', 14000)
        self.heartbeat_socket.bind(('', self.heartbeat_port))
        # Tell the operating system to add the socket to the multicast group on all interfaces.
        group = socket.inet_aton(self.multicast_group)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        self.heartbeat_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    # Creates a socket for join messages
    def create_SERVER_JOIN_socket(self):
        self.server_join_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # Set a timeout so the socket does not block indefinitely when trying to receive data.
        # self.SERVER_JOIN_socket.settimeout(60)
        # Set the time-to-live for messages to 1 so they do not go past the local network segment.
        ttl = struct.pack('b', 1)
        self.server_join_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
        self.server_join_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind to the server address
        server_address = ('', 22000)
        self.server_join_socket.bind(('', self.server_join_port))
        # Tell the operating system to add the socket to the multicast group on all interfaces.
        group = socket.inet_aton(self.multicast_group)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        self.server_join_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    # Creates an acknowledgement socket for receiving ack messages
    def create_ACK_socket(self):
        self.ACK_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ACK_socket.bind((self.server_address, self.ack_port))
        self.receive_acknowledgement()

    # Receives broadcast messages for dynamic discovery
    def receive_broadcast_messages(self):
        print("[WAITING FOR INCOMING BROADCAST ...]")
        try:
            while True:
                data, addr = self.broadcast_socket.recvfrom(1024)
                print("[BROADCAST RECEIVED FROM]: ", addr[0])
                message_header = data[0:3].decode()
                message_data = data[3:1024]
                self.handle_connecting_participants(addr, message_header, message_data)
        except:
            traceback.print_exc()
            self.broadcast_socket.close()

    # Answers client on broadcast with empty message for dynamic discovery
    def answer_client(self, addr):
        self.broadcast_socket.sendto(str.encode(""), addr)

    # Starts a thread for receiving messages for every new connected client
    def listen_to_connecting_clients(self):
        try:
            while True:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self.receive_message, args=(conn, addr)).start()
        except:
            conn.close()

    # Receives client messages
    def receive_message(self, conn, addr):  # Bearbeitet seperat jeden client in einem Thread
        self.send_remote_address(conn, addr)  # used for client initialization
        self.receive_client_name(conn, addr)  # used for client initialization
        if addr not in self.new_client_clock.vector_clock_dictionary:
            self.new_client_clock.add_participant_to_clock(addr)  # adds client to vector clock for new client initialization
        self.new_client_clock.print_clock()
        # self.send_chat_history(conn)  # not in use
        time.sleep(0.2)
        self.send_vc_to_client(conn)  # used for client initialization
        self.send_list_of_clients()  # used for client initialization
        try:
            while True:
                message = conn.recv(1024)
                message_header = message[0:3].decode()
                message_data = message[3:1024]
                if message_header == Message_Types.MessageTypes.SEND_TO_ALL:  # group message
                    self.new_client_clock.increase_clock(addr)
                    self.new_client_clock.print_clock()
                    if len(self.connected_clients) != 1:  # if just one client connected don't add messages to queue (buffer)
                        self.message_queue[self.connected_clients.get(addr).encode() + "/".encode() + message_data] = {}
                        for client in self.connected_clients.keys():
                            if client == addr:
                                self.message_queue[self.connected_clients.get(addr).encode() + "/".encode() + message_data][client] = True
                            else:
                                self.message_queue[self.connected_clients.get(addr).encode() + "/".encode() + message_data][client] = False

                        print("[MESSAGE QUEUE AT RECEIVING]: ", self.message_queue)
                    self.send_message_to_all(addr, (self.connected_clients.get(addr).encode() + "/".encode() + message_data))

                    # for message history
                    # sender = str(self.connected_clients.get(addr))
                    # self.add_message_to_history(sender, message_data.decode())

                else:  # private message
                    message = message.decode()
                    message = message.split('/')
                    dict_value = message[0]
                    for name in self.client_connections.values():
                        if name == dict_value:
                            target_conn = list(self.client_connections.keys())[
                                list(self.client_connections.values()).index(name)]
                    for name in self.connected_clients.values():
                        if name == dict_value:
                            target_addr = list(self.connected_clients.keys())[
                                list(self.connected_clients.values()).index(name)]
                    sender = str(self.connected_clients.get(addr))
                    message_with_origin = sender + '/' + message[1]
                    self.send_message_to_one(conn, addr, target_conn, target_addr, message_with_origin)  # foward private message to specific client

        except:
            conn.close()
            print("[CLIENT || IP: " + str(addr[0]) + " | PORT: " + str(addr[1]) + " | NAME: " + str(self.connected_clients.get(addr)) + "] DISCONNECTED")
            if addr in self.connected_clients:
                self.connected_clients.pop(addr)
                self.client_connections.pop(conn)
                self.send_list_of_clients()
                self.new_client_clock.vector_clock_dictionary.pop(addr)
            print("[CONNECTED CLIENTS]: " + str(self.connected_clients))

    # Adds messages to message history - not in use
    def add_message_to_history(self, sender, message_data):
        if len(self.message_history) < 10:  # currently up to 10 messages are stored in the chat history
            self.message_history.append(sender + ": " + message_data)
        else:
            self.message_history.pop(0)
            self.message_history.append(sender + ": " + message_data)

    # Sends private message via tcp to specific client
    def send_message_to_one(self, origin_conn, origin_addr, target_conn, target_addr, message_data):
        try:
            target_conn.send(message_data.encode())
            print("[CLIENT || IP: " + str(origin_addr[0]) + " | PORT: " + str(origin_addr[1]) + " | NAME: " + str(self.connected_clients.get(origin_addr)) + "] SEND MESSAGE TO " +
                  "[CLIENT || IP: " + str(target_addr[0]) + " | PORT: " + str(target_addr[1]) + " | NAME: " + str(self.connected_clients.get(target_addr)) + "]")
        except:
            origin_conn.close()
            print("[CLIENT || IP: " + str(origin_addr[0]) + " | PORT: " + str(origin_addr[1]) + " | NAME: " + str(self.connected_clients.get(origin_addr)) + "] DISCONNECTED")
            if origin_addr in self.connected_clients:
                self.connected_clients.pop(origin_addr)
                self.client_connections.pop(origin_conn)
                self.send_list_of_clients()
                self.new_client_clock.vector_clock_dictionary.pop(origin_addr)
            print("[CONNECTED CLIENTS]: " + str(self.connected_clients))

    # Sends group message via multicast to all clients
    def send_message_to_all(self, addr, message_data):
        self.multicast_socket.sendto(message_data, (self.multicast_group, self.multicast_port))
        print("[CLIENT || IP: " + str(addr[0]) + " | PORT: " + str(addr[1]) + " | NAME: " + str(self.connected_clients.get(addr)) + "] SEND GROUP MESSAGE")

    # Sends messages to all replicas for replication purposes
    def send_message_to_all_replicas(self, message_data):
        self.multicast_socket.sendto(message_data, (self.multicast_group, self.multicast_port_servers))

    # Receives entered name of the client
    def receive_client_name(self, conn, addr):
        try:
            while True:
                CLIENT_NAME = conn.recv(1024)
                new_list = []  # helper variable
                counter = 0  # helper variable
                for key, value in self.connected_clients.items():
                    if CLIENT_NAME.decode() == value:
                        to_del_list = list(self.connected_clients.keys())[list(self.connected_clients.values()).index(value)]
                        new_list.append(to_del_list)
                        counter = counter + 1
                        self.new_client_clock.vector_clock_dictionary[addr] = self.new_client_clock.vector_clock_dictionary.pop(to_del_list)

                if counter > 0:  # using helper variable to update client key on replication
                    for item in new_list:
                        self.connected_clients[addr] = self.connected_clients.pop(item)

                self.connected_clients[addr] = CLIENT_NAME.decode()
                self.client_connections[conn] = CLIENT_NAME.decode()
                print("[CLIENT || IP: " + str(addr[0]) + " | PORT: " + str(addr[1]) + " | NAME: " + str(self.connected_clients.get(addr)) + "] CONNECTED")
                print("[CONNECTED CLIENTS]: " + str(self.connected_clients))
                break
        except:
            traceback.print_exc()
            conn.close()

    # Sends list of all connected clients to clients
    def send_list_of_clients(self):
        if self.is_leader:
            message_header = Message_Types.MessageTypes.CLIENT_LIST.encode()
            message_data = pickle.dumps(self.connected_clients)  # pickle.dumps = object serialization in python
            full_message = message_header + message_data
            for conn in self.client_connections:
                conn.send(full_message)
            print("[CLIENT LIST SENT]")

    # Sends chat history to new client - not in use
    def send_chat_history(self, conn):
        if len(self.message_history):
            message_header = Message_Types.MessageTypes.REPLICATION_CHAT_HISTORY.encode()
            message_data = pickle.dumps(self.message_history)
            full_message = message_header + message_data
            conn.send(full_message)
        else:
            print("There is no message history yet to push.")

    # Receives ack messages for sent group messages
    def receive_acknowledgement(self):
        try:
            while True:
                message, address = self.ACK_socket.recvfrom(1024)
                split_message = message.split(b'/')

                acknowledgement_type = split_message[0].decode()
                initial_sender = split_message[1]
                receiver_name = split_message[2].decode()
                message_text = split_message[3]
                vector_clock = split_message[4]
                key_message = initial_sender + b'/' + message_text + b'/' + vector_clock

                origin_key = None

                for key, value in self.connected_clients.items():
                    if value == receiver_name:
                        origin_key = key

                if acknowledgement_type == "P-ACK":  # checks for positive ack
                    if key_message in self.message_queue:
                        self.message_queue[key_message][origin_key] = True
                        if all(value == True for value in self.message_queue[key_message].values()):  # check if all clients acknowledged the message
                            self.message_queue.pop(key_message)  # delete the message
                        print("[MESSAGE QUEUE AFTER ACK-MESSAGE]: ", self.message_queue)

                elif acknowledgement_type == "N-ACK":  # checks for negative ack and resends all messages from buffer
                    if key_message in self.message_queue:
                        self.message_queue[key_message][origin_key] = True
                        if all(value == True for value in self.message_queue[key_message].values()):  # check if all clients acknowledged the message
                            self.message_queue.pop(key_message)  # delete the message
                        print("[MESSAGE QUEUE AFTER ACK-MESSAGE]: ", self.message_queue)

                        for group_message in self.message_queue:
                            self.multicast_socket.sendto(group_message, (self.multicast_group, self.multicast_port))
                            print("[BUFFER MESSAGE SENT]")

        except:
            traceback.print_exc()
            pass

    # Sends remote client address to client
    def send_remote_address(self, connection, remote_address):
        remote_address = pickle.dumps(remote_address)
        connection.send(remote_address)

    # Sends heartbeat messages to other servers
    def send_heartbeat(self):
        message = "Alive?"
        starttime = time.time()
        condition = True
        while condition:
            try:
                contacted_servers = []
                time.sleep(1.0 - ((time.time() - starttime) % 1.0))
                for server in self.server_connections.keys():
                    if server != self.IP_ADDRESS:
                        sent = self.heartbeat_socket.sendto(message.encode(), (server, self.heartbeat_port))
                        contacted_servers.append(server)
                        self.lock.acquire()
                        current_time = time.time()
                        self.contacted_connections[server] = current_time
                        self.lock.release()
            except:
                pass

    # Receives heartbeat messages from other servers
    def receive_heartbeat(self):
        # Receive/respond loop
        condition = True
        while condition:
            try:
                data, address = self.heartbeat_socket.recvfrom(1024)
                if address[0] == self.IP_ADDRESS:
                    continue

                decoded_data = data.decode()
                if decoded_data == "Alive?":
                    msg = 'ack'
                    self.heartbeat_socket.sendto(msg.encode(), address)
                elif decoded_data == "ack":
                    current_time = time.time()
                    self.lock.acquire()
                    self.received_ack_from_connections[address[0]] = current_time
                    self.lock.release()
            except:
                pass

    # Checks for crashed servers
    def check_for_dead_connections(self):
        condition = True
        starttime = time.time()
        while condition:
            try:
                servers_to_del = []
                counter = 0
                time.sleep(2.0 - ((time.time() - starttime) % 2.0))
                for server in self.contacted_connections.keys():
                    time_value = self.contacted_connections.get(server)
                    if len(self.received_ack_from_connections) > 0:  # race condition can happen when acks are not delivered and this dict is empty
                        time_delta = time_value - self.received_ack_from_connections.get(server)
                    else:
                        time_delta = 2
                    if time_delta > 5:
                        servers_to_del.append(server)
                        counter = counter + 1
                        break

                if counter > 0:
                    for server in servers_to_del:
                        if server == self.leader_address:
                            self.server_connections.pop(server)
                            self.contacted_connections.pop(server)
                            print("[TIMEOUT FOR LEADER - STARTING LEADER ELECTION]")
                            self.start_leader_election()
                            break
                        else:
                            print("[TIMEOUT FOR SERVER]: " + server)
                            self.server_connections.pop(server)
                            self.contacted_connections.pop(server)

            except:
                pass

    # Send vector clock to new connected clients for initialization
    def send_vc_to_client(self, conn):
        if self.is_leader:
            message_header = Message_Types.MessageTypes.VC_PARTICIPANT.encode()
            message_data = pickle.dumps(self.new_client_clock)  # pickle.dumps = object serialization in python
            full_message = message_header + message_data
            conn.send(full_message)
            print("[VECTOR CLOCK SENT TO CLIENT]")

    # Handles incoming tcp messages based on their header types
    def handle_message_type(self, conn, addr, message_header, message_data):
        if message_header == Message_Types.MessageTypes.SEND_TO_ALL:
            self.send_message_to_all((self.connected_clients.get(addr).encode() + "*/*".encode() + message_data))
        elif message_header == Message_Types.MessageTypes.REPLICATION_CLIENT_LIST and self.is_leader == False:
            print("[RECEIVED MESSAGE TO UPDATE CLIENT LIST]")
            self.replica_receive_client_list(message_data)
        elif message_header == Message_Types.MessageTypes.REPLICATION_CLIENT_CONN_LIST:
            print("[RECEIVED MESSAGE TO UPDATE CLIENT CONNECTIONS]")
            self.replica_receive_client_connections(message_data)
        elif message_header == Message_Types.MessageTypes.REPLICATION_SERVER_LIST:
            print("[RECEIVED MESSAGE TO UPDATE SERVER LIST]")
            self.replica_receive_server_list(message_data)
        elif message_header == Message_Types.MessageTypes.REPLICATION_CHAT_HISTORY:  # not in use
            print("[RECEIVED MESSAGE TO UPDATE CHAT HISTORY]")
            self.replica_receive_chat_history(message_data)
        elif message_header == Message_Types.MessageTypes.REPLICATION_SERVER_VC:
            print("[RECEIVED MESSAGE TO UPDATE VECTOR CLOCK]")
            self.replica_receive_vector_clock(message_data)
        else:
            pass

    # Server joins and waits for reply on the network
    def join_network(self):
        time.sleep(1)
        message_header = Message_Types.MessageTypes.JOIN_NETWORK_SERVER.encode()
        message_data = str(self.is_leader).encode()
        full_message = message_header + message_data
        self.broadcast_socket.sendto(full_message, (self.BROADCAST_ADDRESS, self.broadcast_port))
        self.check_for_other_servers_on_join_thread = threading.Thread(target=self.check_for_other_servers_on_join)
        self.check_for_other_servers_on_join_thread.start()
        self.receive_join_reply_thread = threading.Thread(target=self.receive_join_reply)
        self.receive_join_reply_thread.start()

    # Handles new connections based on their header
    def handle_connecting_participants(self, addr, header, data):
        if header == Message_Types.MessageTypes.JOIN_NETWORK_SERVER:  # new server
            self.accept_joining_server(data, addr)
        elif header == Message_Types.MessageTypes.JOIN_NETWORK_CLIENT:  # new client
            self.accept_joining_client(data, addr)
        else:
            pass

    # Accepting new server in the network
    def accept_joining_server(self, data, addr):
        message_data = data.decode()  # -> boolean expression here; True or False
        self.server_connections[addr[0]] = message_data
        if addr[0] == self.IP_ADDRESS:
            print("[SERVER WITH IP: " + addr[0] + " (YOURSELF) ACCEPTED IN THE NETWORK]")
        else:
            print("[SERVER WITH IP: " + addr[0] + " ACCEPTED IN THE NETWORK]")
        reply_message = str(self.is_leader).encode()
        self.broadcast_socket.sendto(reply_message, (addr[0], self.server_join_port))

    # Receives server replies after sending join messages
    def receive_join_reply(self):
        while True:  # Receive/respond loop
            try:
                data, address = self.server_join_socket.recvfrom(1024)
                if address[0] == self.IP_ADDRESS:
                    continue

                decoded_data = data.decode()

                if decoded_data == 'True':
                    print(str("[RECEIVED REPLY FROM " + address[0]) + " WHO IS THE LEADER -> THIS SERVER WILL START IN REPLICA STATE]")
                    self.leader_address = address[0]
                    self.number_of_join_replies_received = self.number_of_join_replies_received + 1
                    self.start_replica_functions()

                elif decoded_data == 'False':
                    self.number_of_join_replies_received = self.number_of_join_replies_received + 1
            except:
                time.sleep(5)
                print("[SOMETHING WENT WRONG WHEN RECEIVING JOIN REPLIES FROM OTHER SERVER]")

    # Checking for leader state in the network
    def check_for_other_servers_on_join(self):
        starttime = time.time()
        while True:
            try:
                time.sleep(1.0 - ((time.time() - starttime) % 1.0))
                time_delta = time.time() - starttime
                if self.number_of_join_replies_received >= 1 and self.leader_address == 0:  # for the case that two servers are started and none of them is leader
                    print("[MULTIPLE SERVERS ARE ONLINE BUT NONE IS LEADER - STARTING LEADER ELECTION]")
                    self.listen_to_replication_thread = threading.Thread(target=self.listen_to_replica_messages)
                    self.listen_to_replication_thread.start()
                    self.start_leader_election()
                    break
                if time_delta > 2.1 and self.number_of_join_replies_received == 0:  # means that there is only this Server online
                    print("[NOT HEARD FROM OTHER SERVER - STARTING LEADER ELECTION]")
                    self.start_leader_election()
                    break
                if self.leader_address != 0:  # means that this server received a reply from the leader
                    break

            except:
                traceback.print_exc()
                self.check_for_other_servers_on_join_thread.join()
                print("[SOMETHING WENT WRONG WHEN CHECKING FOR LEADER IN THE NETWORK]")

    # Accepting new client in the network
    def accept_joining_client(self, data, addr):
        if self.is_leader:
            try:
                message_data = data.decode()
                self.answer_client(addr)
                print("[CLIENT " + str(addr[0]) + " SUCCESSFULLY ACCEPTED IN THE NETWORK]")
            except:
                print("[SOMETHING WENT WRONG DURING CLIENT JOIN]")
        else:
            print("[RECEIVED JOIN MESSAGE FROM CLIENT " + str(addr) + " - REPLICA DISCARDS THIS MESSAGE]")

    # Replicates data from leader server to replica servers
    def replicate_data_to_servers(self):
        start_time = time.time()
        while True:
            try:
                if len(self.server_connections.keys()) > 1:
                    print("[REPLICATING DATA...]")
                    self.replicate_list_of_clients()
                    time.sleep(0.1)
                    self.replicate_vector_clock()
                    time.sleep(0.1)
                    self.replicate_list_of_servers()
                    time.sleep(0.1)
                    # self.replicate_message_history()  # not in use
                    time.sleep(5.0 - ((time.time() - start_time) % 5.0))  # replicating every x seconds
            except:
                traceback.print_exc()
                print("[SOMETHING WENT WRONG DURING REPLICATION]")

    # Leader server replicates vector clock to replicas
    def replicate_vector_clock(self):
        message_header = Message_Types.MessageTypes.REPLICATION_SERVER_VC.encode()
        message_data = pickle.dumps(self.new_client_clock)
        full_message = message_header + message_data
        self.multicast_socket.sendto(full_message, (self.multicast_group, self.multicast_port_servers))
        # print("[Vector Clock replicated.]")

    # Leader server replicates list of clients to replicas
    def replicate_list_of_clients(self):
        message_header = Message_Types.MessageTypes.REPLICATION_CLIENT_LIST.encode()
        message_data = pickle.dumps(self.connected_clients)
        full_message = message_header + message_data
        self.multicast_socket.sendto(full_message, (self.multicast_group, self.multicast_port_servers))
        # print("[Client list replicated.]")

    # Leader server replicates client connections to replicas - not in use
    def replicate_client_connections(self):
        message_header = Message_Types.MessageTypes.REPLICATION_CLIENT_LIST.encode()

    # Leader server replicates list of servers to replicas
    def replicate_list_of_servers(self):
        message_header = Message_Types.MessageTypes.REPLICATION_SERVER_LIST.encode()
        message_data = pickle.dumps(self.server_connections)
        full_message = message_header + message_data
        self.multicast_socket.sendto(full_message, (self.multicast_group, self.multicast_port_servers))
        # print("[Server list replicated.]")

    # Leader server replicates message history to replicas - not in use
    def replicate_message_history(self):
        message_header = Message_Types.MessageTypes.REPLICATION_CHAT_HISTORY.encode()
        message_data = pickle.dumps(self.message_history)
        full_message = message_header + message_data
        self.multicast_socket.sendto(full_message, (self.multicast_group, self.multicast_port_servers))
        # print("[Chat history replicated.]")

    # Replicas listen to leader server replication messages
    def listen_to_replica_messages(self):
        self.multicast_socket.bind(('', self.multicast_port_servers))
        mreq = struct.pack('4sl', socket.inet_aton(self.multicast_group), socket.INADDR_ANY)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        try:
            while not self.is_leader:
                message, server = self.multicast_socket.recvfrom(1024)
                # print("Received replica message from " + server[0])
                message_header = message[0:3].decode()
                message_data = message[3:1024]
                self.handle_message_type('', server, message_header, message_data)
        except:
            print("[SOMETHING WENT WRONG WHILE RECEIVING REPLICA MESSAGE]")

    # Replica receives replicated client list from leader server
    def replica_receive_client_list(self, message_data):
        try:
            self.connected_clients = pickle.loads(message_data)
            # print("Clients in the network:")
            # print(self.connected_clients)
        except:
            print("[SOMETHING WENT WRONG WHILE RECEIVING CLIENT LIST]")

    # Replica receives replicated vector clock from leader server
    def replica_receive_vector_clock(self, message_data):
        try:
            self.new_client_clock = pickle.loads(message_data)
            # print(self.new_client_clock.vector_clock_dictionary)
        except:
            # traceback.print_exc()
            print("[SOMETHING WENT WRONG WHILE RECEIVING VECTOR CLOCK]")

    # Replica receives replicated client connections from leader server - not in use
    def replica_receive_client_connections(self, message_data):
        try:
            # self.client_connections = dill.loads(message_data)
            # print("Client connections:")
            # print(self.client_connections)
            pass
        except:
            print("[SOMETHING WENT WRONG WHILE RECEIVING CLIENT CONNECTIONS]")

    # Replica receives replicated server list from leader server
    def replica_receive_server_list(self, message_data):
        try:
            self.server_connections = pickle.loads(message_data)
            # print("Servers in the network:")
            # print(self.server_connections)
        except:
            print("[SOMETHING WENT WRONG WHILE RECEIVING SERVER LIST]")

    # Replica receives replicated chat history from leader server - not in use
    def replica_receive_chat_history(self, message_data):
        try:
            self.message_history = pickle.loads(message_data)
            # print("Received chat history:")
            # print(self.message_history)
        except:
            print("[SOMETHING WENT WRONG WHILE RECEIVING CHAT HISTORY]")

    # Replicas start replication functions
    def start_replica_functions(self):
        print("[STARTING REPLICA FUNCTIONS]")
        self.listen_to_replication_thread = threading.Thread(target=self.listen_to_replica_messages)
        self.listen_to_replication_thread.start()

    # Ends replica functions (when role changed to leader) - not in use
    def end_replica_functions(self):
        try:
            if self.listen_to_replication_thread is not None:
                self.listen_to_replication_thread.join()
        except:
            traceback.print_exc()

    # Starts leader functions
    def start_leader_functions(self):
        print("[STARTING LEADER FUNCTIONS]")
        self.replication_thread = threading.Thread(target=self.replicate_data_to_servers)
        self.replication_thread.start()
        self.TCP_thread = threading.Thread(target=self.create_TCP_socket)
        self.TCP_thread.start()

    # Creates a ring socket for the leader election
    def create_LE_ring_socket(self):
        self.LE_my_uid = self.IP_ADDRESS  # IP-Address as UID
        self.LE_ring_port = 15005
        self.LE_ring_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.LE_ring_socket.bind((self.LE_my_uid, self.LE_ring_port))

    # Starts the leader election process
    def start_leader_election(self):
        if len(self.server_connections) == 0:
            return
        # if len(self.server_connections) == 1:
        #     self.is_leader = True
        #     print('NEW LEADER ELECTED | NEW LEADER-IP: {}'.format(self.IP_ADDRESS))
        #     self.leader_address = self.IP_ADDRESS
        #     self.server_connections[self.IP_ADDRESS] = True
        #     self.start_leader_functions()
        else:
            # Variables for leader election
            self.LE_leader_uid = ''
            self.LE_participant = False
            self.LE_members = form_ring(self.server_connections)

            print('[NODE IS UP AND RUNNING AT {}:{}]'.format(self.LE_my_uid, self.LE_ring_port))
            self.lcr_thread = threading.Thread(target=self.start_lcr)
            self.lcr_thread.start()
            self.send_election_message()

    # Starts lcr algorithm for leader election - listening for messages
    def start_lcr(self):
        print('[WAITING TO RECEIVE ELECTION MESSAGES]')
        print('[AVAILABLE SERVERS ARE: ' + str(self.server_connections) + ']')
        lcr_counter = 0  # helper variable
        new_leader = False  # helper variable
        while True:
            data, address = self.LE_ring_socket.recvfrom(1024)
            election_message = pickle.loads(data)

            if election_message['isLeader']:  # forward elected leader message to left neighbour
                if lcr_counter < 1:
                    print('{} isLeader, FROM: {}'.format(election_message, address[0]))
                    self.LE_leader_uid = election_message['pid']
                    self.LE_participant = False
                    lcr_counter += 1
                    self.LE_ring_socket.sendto(pickle.dumps(election_message), (get_neighbour(self.LE_members, self.LE_my_uid), self.LE_ring_port))
                    if new_leader:
                        self.leader_address = self.LE_leader_uid
                        break
                else:
                    break

            if not election_message['isLeader'] and election_message['pid'] < self.LE_my_uid and not self.LE_participant:  # send new election message with my id to left neighbour
                print('{} < , FROM: {}'.format(election_message, address[0]))
                new_election_message = {
                    "pid": self.LE_my_uid,
                    "isLeader": False
                }
                self.LE_participant = True
                self.LE_ring_socket.sendto(pickle.dumps(new_election_message), (get_neighbour(self.LE_members, self.LE_my_uid), self.LE_ring_port))

            elif not election_message['isLeader'] and election_message['pid'] > self.LE_my_uid:  # send received election message to left neighbour
                print('{} > , FROM: {}'.format(election_message, address[0]))
                self.LE_participant = True
                self.LE_ring_socket.sendto(pickle.dumps(election_message), (get_neighbour(self.LE_members, self.LE_my_uid), self.LE_ring_port))

            elif not election_message['isLeader'] and election_message['pid'] == self.LE_my_uid:  # send new leader elected message to left neighbour
                print('{} == , FROM: {}'.format(election_message, address[0]))
                self.LE_leader_uid = self.LE_my_uid
                new_election_message = {
                    "pid": self.LE_my_uid,
                    "isLeader": True
                }
                self.LE_participant = False
                new_leader = True
                self.LE_ring_socket.sendto(pickle.dumps(new_election_message), (get_neighbour(self.LE_members, self.LE_my_uid), self.LE_ring_port))

        if self.LE_my_uid == self.LE_leader_uid:  # I am the leader now
            try:
                self.is_leader = True
                self.LEADER_ADDRESS = self.LE_leader_uid
                self.leader_address = self.LE_leader_uid
                self.lock.acquire()
                self.server_connections[self.LE_leader_uid] = True
                self.lock.release()
                # self.end_replica_functions() - not in use
                self.start_leader_functions()
            except:
                traceback.print_exc()
        else:  # I am a replica now
            try:
                self.is_leader = False
                self.LEADER_ADDRESS = self.LE_leader_uid
                self.leader_address = self.LE_leader_uid
                self.server_connections[self.LE_leader_uid] = True
            except:
                traceback.print_exc()
        print('[NEW LEADER ELECTED | NEW LEADER-IP: {}]'.format(self.LE_leader_uid))

    # Sends initial leader election message into the ring
    def send_election_message(self):
        new_election_message = {
            "pid": self.LE_my_uid,
            "isLeader": False
        }
        self.LE_participant = True
        self.LE_ring_socket.sendto(pickle.dumps(new_election_message), (get_neighbour(self.LE_members, self.LE_my_uid), self.LE_ring_port))


# Forms a ring out of the passed parameter
def form_ring(members):
    sorted_binary_ring = sorted([socket.inet_aton(member) for member in members])
    sorted_ip_ring = [socket.inet_ntoa(node) for node in sorted_binary_ring]
    return sorted_ip_ring

# Gets the next neighbour in the ring of the passed node
def get_neighbour(ring, current_node_ip, direction='left'):
    current_node_index = ring.index(current_node_ip) if current_node_ip in ring else -1
    if current_node_index != -1:
        if direction == 'left':
            if current_node_index + 1 == len(ring):
                return ring[0]
            else:
                return ring[current_node_index + 1]
        else:
            if current_node_index == 0:
                return ring[len(ring) - 1]
            else:
                return ring[current_node_index - 1]
    else:
        return None


if __name__ == "__main__":
    s = Server()
