import logging
import requests
from requests.auth import HTTPBasicAuth
from urllib import parse
from ..agent_core import IAgentCore

log = logging.getLogger(__name__)


class Logic(IAgentCore):
    def __init__(self, options):
        self.url: str = options.get('url') + ';csv'
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
            res = requests.get(self.url, timeout=self.timeout, auth=login_details)
            res.raise_for_status()

            raw_values = {section.split(',')[:2][0] + ',' + section.split(',')[:2][1]: section for section in res.text.splitlines()[1:]}

            values = raw_values[self.section].split(',')

            if 'open' in values[17].lower() or 'up' in values[17].lower():
                current_status = 1
            else:
                current_status = 0

            return {'bytes_in': int(values[8]), 'bytes_out': int(values[9]), 'response_error': int(values[11]), 'current_status': current_status}
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as err:
            raise ConnectionError(err)

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def sections(self):
        login_details = None

        if self.user:
            login_details = HTTPBasicAuth(self.user, self.password)

        res = requests.get(self.url, timeout=self.timeout, auth=login_details)
        res.raise_for_status()

        values = {section.split(',')[:2][0] + ',' + section.split(',')[:2][1]: section for section in res.text.splitlines()[1:]}

        return values.keys()
