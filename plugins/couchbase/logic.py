import logging
import requests
from requests.auth import HTTPBasicAuth
from urllib import parse
from ..agent_core import IAgentCore

log = logging.getLogger(__name__)


class Logic(IAgentCore):
    def __init__(self, options):
        self.url: str = options.get('url', '')
        self.user: str = options.get('user', '')
        self.password: str = options.get('password', '')
        self.timeout: int = options.get('timeout', 5)
        super().__init__(options)

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def status(self):
        login_details = None

        if self.user:
            login_details = HTTPBasicAuth(self.user, self.password)

        try:
            res = requests.get(parse.urljoin(self.url, 'pools/nodes/'), timeout=self.timeout, auth=login_details)
            res.raise_for_status()

            memory_used = round(res.json()['storageTotals']['ram']['used'] / res.json()['storageTotals']['ram']['total'] * 100, 2)
            hdd_used = res.json()['storageTotals']['hdd']['usedByData']

            return {'memory_used': memory_used, 'hdd_used': hdd_used}
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as err:
            raise ConnectionError(err)
        except Exception as err:
            raise ReferenceError(err)

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def nodes(self):
        login_details = None

        if self.user:
            login_details = HTTPBasicAuth(self.user, self.password)

        try:
            res = requests.get(parse.urljoin(self.url, 'pools/nodes/'), timeout=self.timeout, auth=login_details)
            res.raise_for_status()

            for node in res.json()['nodes']:
                if node['hostname'] == self.node:
                    memory_used = round(res.json()['storageTotals']['ram']['used'] / res.json()['storageTotals']['ram']['total'] * 100, 2)
                    hdd_used = res.json()['storageTotals']['hdd']['usedByData']
                    cpu_used = round(node['systemStats']['cpu_utilization_rate'], 2)

                    # logic to check failover, must be deeply specified
                    if node['clusterMembership'] != 'active':
                        _status = -1
                    else:
                        _status = True

                    return {'memory_used': memory_used, 'hdd_used': hdd_used, 'cpu_used': cpu_used, 'status': _status}

            return {'error': 'node not found'}
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as err:
            raise ConnectionError(err)
        except Exception as err:
            raise ReferenceError(err)

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def list_nodes(self):
        login_details = None

        if self.user:
            login_details = HTTPBasicAuth(self.user, self.password)

        res = requests.get(parse.urljoin(self.url, 'pools/nodes/'), timeout=self.timeout, auth=login_details)
        res.raise_for_status()

        values = {node['hostname']: node['hostname'] for node in res.json()['nodes']}

        return values
