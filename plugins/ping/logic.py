import logging
import socket as _socket
import struct
import time

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    _d_size = struct.calcsize('d')
    PING_TIMES = 3

    '''
    Utilities for ICMP socket.

    For the socket usage: https://lkml.org/lkml/2011/5/10/389
    For the packet structure: https://bitbucket.org/delroth/python-ping
    
    based on https://github.com/lilydjwg/winterpy/blob/master/pylib/icmplib.py

    !!!!  sudo sysctl -w net.ipv4.ping_group_range="0 65536"
    !!!!   to run as no-root
    '''

    def __init__(self, options):
        self.count = options.get('count', '5')
        self.address = options['host']

    @staticmethod
    def pack_packet(seq, payload):
        # Header is type (8), code (8), checksum (16), id (16), sequence (16)
        # The checksum is always recomputed by the kernel, and the id is the port number
        header = struct.pack('bbHHh', 8, 0, 0, 0, seq)  # ICMP_ECHO_REQUEST = 8
        return header + payload

    @staticmethod
    def parse_packet(data):
        type, code, checksum, packet_id, sequence = struct.unpack('bbHHh', data[:8])
        return sequence, data[8:]

    def pack_packet_with_time(self, seq, packetsize=56):
        padding = (packetsize - self._d_size) * b'Q'
        timeinfo = struct.pack('d', time.time())
        return self.pack_packet(seq, timeinfo + padding)

    def parse_packet_with_time(self, data):
        seq, payload = self.parse_packet(data)
        t = struct.unpack('d', payload[:self._d_size])[0]
        return seq, t

    def ping_once(self):
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM, _socket.IPPROTO_ICMP)
        s.settimeout(5)
        s.sendto(self.pack_packet_with_time(1), (_socket.gethostbyname(self.address), 0))
        packet, peer = s.recvfrom(1024)
        _, t = self.parse_packet_with_time(packet)
        log.debug(f'ping once {self.address}: {time.time() - t} sec')
        return time.time() - t

    def ping(self):
        try:
            total_delay = 0
            for n in range(self.PING_TIMES):
                total_delay += self.ping_once() * 1000
                log.debug(f'{n:} total delay={total_delay}')
            return {'ping': total_delay // self.PING_TIMES}
        except (_socket.gaierror, _socket.timeout, ConnectionRefusedError, ConnectionResetError, TimeoutError) as err:
            log.exception(err)
            return {'ping': -1}
        except Exception as err:
            log.exception(err)
            raise
