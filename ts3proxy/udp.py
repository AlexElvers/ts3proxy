import asyncio
import time
import traceback

from .blacklist import Blacklist


class UdpRelayServer(asyncio.DatagramProtocol):
    """
    The server protocol waits for data from real TeamSpeak clients.
    """

    def __init__(self, relay):
        self.relay = relay
        self.transport = None

    def connection_made(self, transport):
        print("UDP server started")
        self.transport = transport

    def datagram_received(self, data, addr):
        print("received data from", addr, ":", data)
        self.relay.handle_data_from_client(data, addr)

    def sendto(self, data, addr):
        """
        Send data to the TeamSpeak client.
        """
        self.transport.sendto(data, addr)


class UdpRelayClient(asyncio.DatagramProtocol):
    """
    The client protocol sends data to the real TeamSpeak server.
    """

    def __init__(self, relay, addr):
        self.relay = relay
        self.addr = addr  # client address
        self.last_seen = time.time()
        self.transport = None

    def connection_made(self, transport):
        print("connected to UDP server")
        self.transport = transport

    def datagram_received(self, data, addr):
        # relay data to TeamSpeak client
        self.relay.socket.sendto(data, self.addr)

    def connection_lost(self, exc):
        print("disconnected from UDP server")

    def send(self, data):
        """
        Send data to the TeamSpeak server.
        """
        self.last_seen = time.time()
        self.transport.sendto(data)


class UdpRelay:
    """
    Relay for UDP communication of TeamSpeak 3
    """

    def __init__(self, logging, statistics,
                 relay_address="0.0.0.0", relay_port=9987,
                 remote_address="127.0.0.1", remote_port=9987,
                 blacklist_file="blacklist.txt", whitelist_file="whitelist.txt"):
        self.relay_address = relay_address
        self.relay_port = relay_port
        self.remote_address = remote_address
        self.remote_port = remote_port
        self.blacklist = Blacklist(blacklist_file, whitelist_file)
        self.logging = logging
        self.statistics = statistics
        self.server = None
        self.clients = {}  # address -> UdpRelayClient
        self.scheduler_handle = None

    @classmethod
    def create_from_config(cls, logging, statistics, relay_config):
        return cls(
            logging=logging,
            statistics=statistics,
            relay_address=relay_config['relayAddress'],
            relay_port=relay_config['relayPort'],
            remote_address=relay_config['remoteAddress'],
            remote_port=relay_config['remotePort'],
            blacklist_file=relay_config['blacklist'],
            whitelist_file=relay_config['whitelist'],
        )

    async def start(self):
        """
        Start server protocol.
        """
        loop = asyncio.get_event_loop()
        transport, self.server = await loop.create_datagram_endpoint(
            lambda: UdpRelayServer(self),
            local_addr=(self.relay_address, self.relay_port),
        )
        self.scheduler_handle = loop.call_soon(self.scheduler)

    def close(self):
        """
        Close server and client protocols.
        """
        self.scheduler_handle.cancel()
        self.server.transport.close()
        for client in self.clients.values():
            self.disconnect_client(client)

    def disconnect_client(self, client):
        client.transport.close()
        del self.clients[client.addr]
        self.statistics.remove_user(client.addr)

    def disconnect_inactive_clients(self):
        """
        Disconnect inactive clients.
        """
        for addr, client in self.clients.items():
            if client.last_seen <= time.time() - 2:
                self.logging.debug('disconnected: {}'.format(addr))
                self.disconnect_client(addr)

    def scheduler(self):
        print("schedule")
        self.disconnect_inactive_clients()
        self.scheduler_handle = asyncio.get_event_loop().call_later(1, self.scheduler)

    def handle_data_from_client(self, data, addr):
        # check if the client is denied by our blacklist
        if self.blacklist.check(addr[0]):
            # if it's a new and unknown client
            if addr not in self.clients:
                if not self.statistics.user_limit_reached():
                    self.logging.debug('connection from: {}'.format(addr))
                    loop = asyncio.get_event_loop()
                    client_protocol = UdpRelayClient(self, addr)
                    self.clients[addr] = client_protocol
                    loop.create_task(loop.create_datagram_endpoint(
                        lambda: client_protocol,
                        remote_addr=(self.remote_address, self.remote_port),
                    ))
                    self.statistics.add_user(addr)
                else:
                    self.logging.info('connection from {} not allowed. user limit ({}) reached.'.format(
                        addr[0], self.statistics.max_users))
            # send data to ts3 server
            if addr in self.clients:
                self.clients[addr].send(data, (self.remote_address, self.remote_port))
        else:
            self.logging.info('connection from {} not allowed. blacklisted.'.format(addr[0]))
            if addr in self.clients:
                self.disconnect_client(self.clients[addr])
