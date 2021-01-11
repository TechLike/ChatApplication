import socket
import threading
import pickle
import Message_Types
import Vector_Clock
import struct
import time
import traceback


class Client:
    HOST = socket.gethostname()  # client host name
    IP_ADDRESS = socket.gethostbyname(HOST)  # get access to the client ip
    BROADCAST_ADDRESS = "255.255.255.255"
    TCP_PORT = 10000
    BROADCAST_PORT = 12000
    MULTICAST_PORT = 16000
    ACK_PORT = 20000
    MULTICAST_GROUP = '224.1.1.1'
    IS_ALL_GROUPS = True
    CLIENT_NAME = None

    def __init__(self):
        self.run_threads = True
        self.client_address = self.IP_ADDRESS
        self.server_address = None
        self.server_port = self.TCP_PORT
        self.broadcast_port = self.BROADCAST_PORT
        self.ack_port = self.ACK_PORT
        self.multicast_port = self.MULTICAST_PORT
        self.multicast_group = self.MULTICAST_GROUP
        self.client_list = {}
        self.delivery_queue = []
        self.hold_back_queue = []
        self.create_ACK_socket()
        self.create_UDP_socket()
        self.thread_pool = []
        self.create_TCP_socket((self.server_address, self.server_port))
        self.create_MULTICAST_socket()

    # Creates an udp socket to send broadcasts for dynamic discovery
    def create_UDP_socket(self):
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, True)
        self.broadcast_socket.settimeout(5)
        self.send_broadcast()
        threading.Thread(target=self.receive_reply_on_broadcast()).start()

    # Creates an udp socket to send acknowledgement messages for causal ordered reliable multicast
    def create_ACK_socket(self):
        self.ack_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ack_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Creates a tcp socket to send messages to the server
    def create_TCP_socket(self, server_address):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.server_address, self.server_port))
        self.receive_own_address()
        self.send_name()
        threading.Thread(target=self.receive_message).start()
        threading.Thread(target=self.send_message).start()

    # Creates a multicast socket to receive multicast messages from the server
    def create_MULTICAST_socket(self):
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 0)  # For specific network configuration
        if self.IS_ALL_GROUPS:
            self.multicast_socket.bind(('', self.multicast_port))
        else:
            self.multicast_socket.bind((self.multicast_group, self.multicast_port))
        mreq = struct.pack('4sl', socket.inet_aton(self.multicast_group), socket.INADDR_ANY)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        threading.Thread(target=self.receive_group_message).start()

    # Sends a broadcast message for dynamic discovery
    def send_broadcast(self):
        print("\n[BROADCAST SENT]")
        message_header = Message_Types.MessageTypes.JOIN_NETWORK_CLIENT.encode()
        message_data = str.encode("")
        full_message = message_header + message_data
        self.broadcast_socket.sendto(full_message, (self.BROADCAST_ADDRESS, self.broadcast_port))

    # Receives the address tuple of client ip and port
    def receive_own_address(self):
        try:
            while True:
                self.own_address = self.client_socket.recv(1024)
                self.own_address = pickle.loads(self.own_address)
                self.vector_clock = Vector_Clock.Vector_Clock((self.own_address))
                # print("\n[MY_TIMESTAMP]: ", self.vector_clock.vector_clock_dictionary)
                break
        except:
            print("\n[FAILED TO RECEIVE OWN_ADDRESS]")

    # Receives a reply from the server on sent broadcast
    def receive_reply_on_broadcast(self):
        while self.server_address == None:
            try:
                data, addr = self.broadcast_socket.recvfrom(1024)
                if addr[0] != 0:
                    self.server_address = addr[0]  # Receiving ip address from server to establish a connection on the predefined port
                else:
                    self.send_broadcast()
            except:
                self.send_broadcast()

    # Receives a multicast group message
    def receive_group_message(self):
        try:
            while True:
                raw_message = self.multicast_socket.recv(1024)
                message = raw_message.split(b'/')
                origin = message[0].decode()
                message_text = message[1].decode()

                if origin == self.CLIENT_NAME:  # if I am the sender ignore the message
                    continue

                self.hold_back_queue.append(message)  # first put incoming message in hold back queue
                origin_vector_clock = pickle.loads(message[2])  # vector clock of the sender

                origin_key = None
                for key, name in self.client_list.items():  # getting ip address of sender out of client list via name
                    if name == origin:
                        origin_key = key

                requirement = False
                for key in self.vector_clock.vector_clock_dictionary.keys():  # check if timestamps except of sender timestamp are smaller or equal than my vector clock timestamp
                    if key != origin_key:
                        if origin_vector_clock[key] <= self.vector_clock.vector_clock_dictionary[key]:
                            # print("Requirements fulfilled!")
                            requirement = True
                        else:
                            # print("Requirements not fulfilled!")
                            requirement = False
                            break

                self.hold_back_queue.reverse()
                list = []  # helper variable to delete messages from hold back queue
                counter = 0  # helper variable
                for queue_message in self.hold_back_queue:  # iterating the hold back queue messages
                    complete_clock = pickle.loads(queue_message[2])  # vector clock from hold back queue messages
                    if complete_clock[origin_key] == self.vector_clock.vector_clock_dictionary[origin_key] + 1 and requirement == True:  # causal ordering requirement that sender timestamp is exactly 1 higher than out timestamp for sender

                        value = queue_message.copy()  # solving reference issues
                        list.append(value)  # adding queue message to helper list
                        counter = counter + 1  # increasing helper variable
                        self.delivery_queue.append(value)  # adding message to delivery queue
                        self.vector_clock.vector_clock_dictionary[origin_key] = self.vector_clock.vector_clock_dictionary[origin_key] + 1  # increasing timestamp from sender by 1

                        positive_acknowledgement = "P-ACK".encode()
                        acknowledgement_data = value.copy()  # solving reference issues
                        acknowledgement_data[0] = self.CLIENT_NAME.encode()  # adding receiver name to acknowledge message
                        acknowledgement_data = b'/'.join(acknowledgement_data)
                        ack_message = positive_acknowledgement + "/".encode() + origin.encode() + "/".encode() + acknowledgement_data
                        self.ack_socket.sendto(ack_message, (self.server_address, self.ack_port))  # send positive ack message to server

                        for q_message in self.delivery_queue:
                            q_origin = q_message[0].decode()
                            q_message_text = q_message[1].decode()
                            print("\n[GROUP MESSAGE RECEIVED FROM | " + q_origin + "]: " + q_message_text)  # delivering message
                            print("\n[MY CLOCK AFTER RECEIVING]:", self.vector_clock.vector_clock_dictionary)
                            self.delivery_queue.remove(q_message)

                    elif complete_clock[origin_key] <= self.vector_clock.vector_clock_dictionary[origin_key] and requirement == True:  # causal ordering requirement that message message was already received
                        # print("MESSAGE ALREADY RECEIVED")
                        if message in self.hold_back_queue:
                            self.hold_back_queue.remove(message)  # deleting message from hold back queue

                    elif complete_clock[origin_key] > self.vector_clock.vector_clock_dictionary[origin_key] + 1 or requirement == False:  # causal ordering requirement that messages are missing
                        # print("MESSAGE LOST")
                        negative_acknowledgement = "N-ACK".encode()
                        bad_message = queue_message.copy()  # solving reference issues
                        bad_message[0] = self.CLIENT_NAME.encode()  # adding receiver name to acknowledge message
                        bad_message = b'/'.join(queue_message)
                        ack_message = negative_acknowledgement + "/".encode() + origin.encode() + "/".encode() + bad_message
                        self.ack_socket.sendto(ack_message, (self.server_address, self.ack_port))  # send negative ack message to server

                if counter != 0:  # checking helper variable
                    for m in list:  # iterating helper list
                        self.hold_back_queue.remove(m)  # removing delivered messages from hold back queue
        except:
            self.multicast_socket.close()

    # Sends entered name to server
    def send_name(self):
        while self.CLIENT_NAME == None:
            self.CLIENT_NAME = input("\n[INPUT YOUR NAME]: ")
            if self.CLIENT_NAME in self.client_list.values():
                print("\n[THIS NAME IS ALREADY TAKEN]")
            self.client_socket.send(self.CLIENT_NAME.encode())

    # Receives different tcp messages with different header types
    def receive_message(self):
        try:
            while True:
                if self.server_address:
                    message = self.client_socket.recv(1024)
                    if message:
                        message_header = message[0:3].decode()
                        message_data = message[3:1024]
                        self.handle_TCP_message_type(message_header, message_data, message)
                    else:
                        continue
                else:
                    break
        except:
            self.client_socket.close()
            self.server_address = None
            self.reestablish_server_connection()

    # Sends a tcp message for group or private chat
    def send_message(self):
        try:
            while self.run_threads:
                destination = input('\n[TO SEND PRIVATE MESSAGE INSERT USER NAME ELSE WILL BE SEND AS GROUP MESSAGE]: ')
                if destination not in self.client_list.values():
                    destination = Message_Types.MessageTypes.SEND_TO_ALL
                    message_data = input('\n[MESSAGE INPUT]: ')
                    self.vector_clock.increase_clock(self.own_address)  # increasing clock if group message
                    print('\n[VECTOR CLOCK AFTER SENDING]: ', self.vector_clock.vector_clock_dictionary)
                    vector_clock = self.vector_clock.vector_clock_dictionary
                    message = destination.encode() + message_data.encode() + "/".encode() + pickle.dumps(vector_clock)
                    self.client_socket.send(message)
                    time.sleep(0.5)
                else:
                    message_data = input('\n[MESSAGE INPUT]: ')
                    message = destination + "/" + message_data
                    self.client_socket.send(message.encode())
                    time.sleep(0.5)
                if not self.run_threads:
                    break
        except:
            return

    # Receiving latest vector clock from server for new connected clients
    def receive_vc_from_server(self, data):
        self.vector_clock = pickle.loads(data)
        self.vector_clock.vector_clock_dictionary = dict(sorted(self.vector_clock.vector_clock_dictionary.items()))  # sorting vector clock for better visualization in console

    # Receiving list of all client connected to the server
    def receive_list_of_clients(self, message_data):
        self.client_list = pickle.loads(message_data)

    # Receiving chat history - not in use
    def receive_chat_history(self, message_data):
        chat_history = pickle.loads(message_data)
        print("***** last public chat messages *****")
        for element in chat_history:
            print(element)
        print("***** end of chat history *****")

    # Handles incoming tcp messages based on their header
    def handle_TCP_message_type(self, message_header, message_data, message):
        if message_header == Message_Types.MessageTypes.VC_PARTICIPANT:
            self.receive_vc_from_server(message_data)

        elif message_header == Message_Types.MessageTypes.CLIENT_LIST:
            self.receive_list_of_clients(message_data)

            for user in self.client_list.keys():
                if user not in self.vector_clock.vector_clock_dictionary:  # adding user timestamp to clock
                    self.vector_clock.add_participant_to_clock(user)

            clocks_to_delete = []  # helper variable
            for user in self.vector_clock.vector_clock_dictionary:  # removing deleted user from vector clock
                if user not in self.client_list:
                    clocks_to_delete.append(user)

            for clock in clocks_to_delete:  # delete vector clock on user leaving event
                self.vector_clock.vector_clock_dictionary.pop(clock)

            user_string = ""  # variable for all available users in the chat
            for user in self.client_list.values():
                user_string += " | " + str(user) + " | "
            print("\n[USERS ONLINE]: ", user_string)
            self.vector_clock.print_clock()
        elif message_header == Message_Types.MessageTypes.REPLICATION_CHAT_HISTORY:
            self.receive_chat_history(message_data)
        else:
            message = message.decode()
            message = message.split('/')
            print("\n[PRIVATE MESSAGE RECEIVED FROM " + message[0] + "]: " + message[1])

    # Reestablishing connection to new leader server
    def reestablish_server_connection(self):
        print("\n[REESTABLISHING SERVER CONNECTION]")
        start_time = time.time()
        while self.server_address == None:
            try:
                time.sleep(3.0 - ((time.time() - start_time) % 3.0))
                self.create_UDP_socket()
            except:
                traceback.print_exc()
        print("\n[CONNECTION TO NETWORK REESTABLISHED]")
        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            time.sleep(1)
            self.client_socket.connect((self.server_address, self.server_port))
            self.receive_own_address()
            self.client_socket.send(self.CLIENT_NAME.encode())
            threading.Thread(target=self.receive_message).start()
        except:
            traceback.print_exc()


if __name__ == "__main__":
    c = Client()
