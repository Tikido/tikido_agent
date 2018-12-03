import redis
import logging
import time

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.host = options.get('host')
        self.port = int(options.get('port', 1433))
        self.db = options.get('db', '')
        self.timeout = options.get('timeout', 3)

    def connect(self):
        try:
            self._redis = redis.Redis(host=self.host, port=self.port, socket_timeout=self.timeout, socket_connect_timeout=self.timeout, socket_keepalive=False)
            log.debug('connected to redis @ {}:{}'.format(self.host, self.port))
        except Exception as err:
            raise ConnectionError

    def ping(self):
        self.connect()
        self._redis.ping()
        return True

    def status(self):
        time_start = time.time()
        self.connect()
        info = self._redis.info()

        time_end = time.time()
        response = int(1000*(time_end - time_start))

        memory = int(info.get('used_memory', 0)) # in MB
        users = int(info.get('connected_clients', 0))
        payload = {
            'users': users,
            'memory': memory,
            'response': response
        }

        return payload

    def database_keys_count(self):
        self.connect()
        info = self._redis.info()
        return {el: info[el]['keys'] for el in info if el == self.db}

    def list_databases(self):
        self.connect()
        info = self._redis.info()

        return {el:el for el in info if el.startswith('db')}
