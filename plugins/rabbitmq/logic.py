import logging
import requests
from requests.auth import HTTPBasicAuth
from urllib import parse
from ..agent_core import IAgentCore

log = logging.getLogger(__name__)


class Logic(IAgentCore):
    def __init__(self, options):
        self.url: str = options.get('url')
        self.user: str = options.get('user', '')
        self.password: str = options.get('password', '')
        self.timeout: int = options.get('timeout', 5)
        self.queue_name: str = options.get('queue_name', '')
        super().__init__(options)

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def status(self):
        login_details = None

        if self.user:
            login_details = HTTPBasicAuth(self.user, self.password)

        try:
            res = requests.get(parse.urljoin(self.url, 'api/overview'), timeout=self.timeout, auth=login_details)
            res.raise_for_status()

            overview = res.json()

            return {'publish_rate': overview['message_stats']['publish_details']['rate'],
                    'deliver_rate': overview['message_stats']['deliver_get_details']['rate'],
                    'messages_rate': overview['queue_totals']['messages_details']['rate'],
                    'connections': overview['object_totals']['connections']}
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as err:
            raise ConnectionError(err)

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def queue_status(self):
        login_details = None

        if self.user:
            login_details = HTTPBasicAuth(self.user, self.password)

        try:
            res = requests.get(parse.urljoin(self.url, 'api/queues/' + self.queue_name), timeout=self.timeout, auth=login_details)
            res.raise_for_status()

            return {'messages_count': res.json()['messages']}
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as err:
            raise ConnectionError(err)

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def list_queues(self):
        login_details = None

        if self.user:
            login_details = HTTPBasicAuth(self.user, self.password)

        try:
            res = requests.get(parse.urljoin(self.url, 'api/queues'), timeout=self.timeout, auth=login_details)
            res.raise_for_status()

            return {f"{'%2f' if queue['vhost'] == '/' else queue['vhost']}/{queue['name']}": queue['name'] for queue in res.json()}
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as err:
            return self.widget_err(err_type='ConnectionError', message=err)
        except Exception as err:
            return self.widget_err(err_type=type(err), message=err)
