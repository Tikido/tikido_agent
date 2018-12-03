import requests
from requests import auth
import time
import logging

import pystalkd.Beanstalkd

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.host = options.get('host')
        self.port = int(options.get('port'))
        self.tube_mask = options.get('tube', '')
        self.timeout = options.get('timeout', 5)
        self.conn = self._connect()

    def _connect(self):
        try:
            conn = pystalkd.Beanstalkd.Connection(self.host, self.port, parse_yaml=True, connect_timeout=self.timeout)
            if conn:
                return conn
        except Exception as e:
            log.exception(e)
            return ConnectionError

    def status(self):
        try:
            if self.conn:
                res = self.conn.stats()

                return {'current_connections': res['current-connections'],
                        'current_jobs_ready': res['current-jobs-ready'],
                        'job_timeouts': res['job-timeouts'],
                        'rusage_stime': res['rusage-stime'],
                        'rusage_utime': res['rusage-utime']}
        except Exception:
            raise ConnectionError

    def status_tube(self):
        try:
            current_jobs_ready = 0
            if self.conn:
                for tube in self.conn.tubes():
                    if self.tube_mask in tube:
                        current_jobs_ready += self.conn.stats_tube(tube)['current-jobs-ready']
                return {'current_jobs_ready': current_jobs_ready}
        except Exception:
            raise ConnectionError

    def uptime(self):
        try:
            if self.conn:
                res = self.conn.stats()['uptime']
                return res
        except Exception:
            raise ConnectionError
