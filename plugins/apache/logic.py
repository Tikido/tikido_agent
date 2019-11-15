import requests
from requests import auth
import time
import logging

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.url = options.get('url')
        self.auth_type = options.get('auth_type', 'direct')
        self.username = options.get('username')
        self.password = options.get('password')
        self.timeout = options.get('timeout', 5)

    def ping(self):
        try:
            start = time.time()
            if self.auth_type == "direct":
                res = requests.get(self.url, timeout=self.timeout)
            else:
                res = requests.get(self.url, auth=auth.HTTPBasicAuth(self.username, self.password), timeout=self.timeout)
            res.raise_for_status()
            end = time.time()
            ping_res = (end - start) * 1000
            return ping_res
        except (requests.exceptions.ConnectionError, requests.exceptions.MissingSchema):
            raise ConnectionError

    def status(self):
        try:
            if not self.url.lower().endswith('/server-status') or not self.url.lower().endswith('/server-status?auto'):
                self.url = f"{self.url}/server-status?auto"
            if self.auth_type == "direct":
                res = requests.get(self.url, timeout=self.timeout)
            else:
                res = requests.get(self.url, auth=auth.HTTPBasicAuth(self.username, self.password), timeout=self.timeout)
            res.raise_for_status()
            text_status = res.text.split('\n')
            text_status.pop(0)
            text_status.pop(-1)
            raw_values = {i.split(':', maxsplit=1)[0].strip(): i.split(':', maxsplit=1)[1].strip() for i in text_status if ':' in i}
            raw_values['MaxWorkers'] = len(raw_values['Scoreboard'])

            values = dict()
            values['BusyWorkers'] = int(raw_values['BusyWorkers']) / len(raw_values['Scoreboard']) * 100 // 1
            values['BytesPerSec'] = float(raw_values['BytesPerSec'])

            if 'ServerMPM' in raw_values and raw_values['ServerMPM'] != 'WinNT':  # in Windows load average not available
                values['Load1'] = float(raw_values['Load1'])
                values['Load15'] = float(raw_values['Load15'])
            else:
                values['Load1'] = 0
                values['Load15'] = 0

            return values
        except (requests.exceptions.ConnectionError, requests.exceptions.MissingSchema, requests.exceptions.HTTPError) as e:
            raise ConnectionError(e)
