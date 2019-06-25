import requests
from requests import auth
from urllib import parse
import logging
from ..agent_core import IAgentCore

log = logging.getLogger(__name__)


class Logic(IAgentCore):
    def __init__(self, options):
        self.url = options.get('url')
        self.auth_type = options.get('auth_type', 'direct')
        self.username = options.get('username')
        self.password = options.get('password')
        self.timeout = options.get('timeout', 5)
        super(Logic, self).__init__(options)

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def status(self):
        try:
            if not self.url.lower().endswith('/server-status') or not self.url.lower().endswith('/server-status?json'):
                self.url = parse.urljoin(self.url, "server-status?json")
            if self.auth_type == "direct":
                res = requests.get(self.url, timeout=self.timeout)
            else:
                res = requests.get(self.url, auth=auth.HTTPBasicAuth(self.username, self.password), timeout=self.timeout)
            res.raise_for_status()

            values = dict()
            values['bytes_per_sec'] = res.json()['TrafficAverage5s']
            values['requests_per_sec'] = res.json()['RequestAverage5s']
            values['busy_servers'] = res.json()['BusyServers']/(res.json()['BusyServers'] + res.json()['IdleServers']) * 100 // 1
            values['uptime'] = res.json()['Uptime']

            return values
        except (requests.exceptions.ConnectionError, requests.exceptions.MissingSchema, requests.exceptions.HTTPError) as err:
            raise ConnectionError(err)

    def uptime(self):
        try:
            if not self.url.lower().endswith('/server-status') or not self.url.lower().endswith('/server-status?json'):
                self.url = parse.urljoin(self.url, "server-status?json")
            if self.auth_type == "direct":
                res = requests.get(self.url, timeout=self.timeout)
            else:
                res = requests.get(self.url, auth=auth.HTTPBasicAuth(self.username, self.password), timeout=self.timeout)
            res.raise_for_status()

            return res.json()['Uptime']
        except (requests.exceptions.ConnectionError, requests.exceptions.MissingSchema, requests.exceptions.HTTPError) as err:
            raise ConnectionError(err)
