import logging
import time
import requests
log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.url = options.get('url')
        self.verify = options.get('verify', False)
        self.timeout = options.get('timeout', 10)

    def ping(self):
        start = time.time()
        res = requests.get(self.url, timeout=self.timeout, verify=self.verify)
        res.raise_for_status()
        end = time.time()
        ping_res = (end - start) * 1000 // 1  # f"{end-start:10.4f} sec"  # return text value or original in microseconds
        return ping_res

    def status(self):
        res = requests.get(self.url, timeout=self.timeout, verify=self.verify)
        res.raise_for_status()
        response = res.text.split()
        resp = dict(active_connection=int(response[2]),
                    # resp['accepts'] = int(response[7])
                    # resp['handled'] = int(response[8])
                    # resp['request'] = int(response[9])
                    reading=int(response[11]),
                    writing=int(response[13]),
                    waiting=int(response[15]),
                    request_per_connection=round(int(response[9]) / int(response[8]), 3),  # f"{int(response[9]) / int(response[8]):10.4f}",
                    unhandled=int(response[7]) - int(response[8]))
        return resp

    def unhandled(self):
        res = requests.get(self.url, timeout=self.timeout, verify=self.verify)
        res.raise_for_status()
        response = res.text.split()
        accepts = int(response[7])
        handled = int(response[8])
        unhandled = accepts - handled
        return unhandled

    def status_plus(self):
        res = requests.get(self.url, timeout=self.timeout, verify=self.verify)
        res.raise_for_status()
        status_plus = res.json()
        return status_plus

