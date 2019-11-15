import logging
import time

import dns.resolver

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.myResolver = dns.resolver.Resolver()
        self.myResolver.retry_servfail = False
        self.myResolver.timeout = int(options.get('timeout', 5))
        self.myResolver.lifetime = int(options.get('timeout', 5))
        self.myResolver.port = int(options.get('port', 53) or 53)
        self.myResolver.nameservers = [options.get('dns_serv')]
        self.domain = options.get('domain')
        self.rec_type = options.get('rec_type')

    def connect(self):
        try:
            myAnswers = self.myResolver.query(self.domain, self.rec_type)
            for rdata in myAnswers:
                log.debug(rdata)
        except dns.exception.Timeout as e:
            raise ConnectionError(e)

    def resolver(self):
        time_start = time.time()
        self.connect()
        conn_time = time.time() - time_start
        conn_time = round(conn_time, 6)
        log.debug('connection time: %d' % conn_time)

        return conn_time
