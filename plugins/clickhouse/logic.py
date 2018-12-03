import logging

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore

import requests


class Logic(IAgentCore):
    def __init__(self, options):
        self.proto, self.host, self.port = options.get('proto', 'http'), options.get('host'), options.get('port') or 8173
        self.username, self.passw = options.get('username'), options.get('passw')
        self.http_auth = f"{self.username}:{self.passw}@" if self.username else ''
        self.url = f"{self.proto}://{self.http_auth}{self.host}:{self.port}/?database=default"

    def connect(self):
        req = requests.get(f"{self.url}")
        req.raise_for_status()

    def _get_metrics(self, fetch_col_descriptions=False):
        # for the popular subset of metrics see also https://www.instana.com/blog/instana-announces-ai-powered-monitoring-for-clickhouse/

        cols = '*' if fetch_col_descriptions else 'metric, value'
        req = requests.get(f'''{self.url}&query=Select {cols} from system.metrics
          where metric in ('TCPConnection','HTTPConnection','DelayedInserts','MemoryTracking','Query','Read','Write') format JSON''',
                           timeout=3)
        req.raise_for_status()
        metrics = req.json()['data']
        # [{'description': 'Number of executing queries', 'metric': 'Query', 'value': '1'},
        #  {'description': 'Number of executing background merges', 'metric': 'Merge', 'value': '0'},

        req = requests.get(f'''{self.url}&query=Select * from system.asynchronous_metrics 
          where metric in ('MaxPartCountForPartition','ReplicasMaxAbsoluteDelay','ReplicasSumQueueSize','Uptime') format JSON''',
                           timeout=3)
        req.raise_for_status()
        async_metrics = req.json()['data']
        # [{'metric': 'jemalloc.background_thread.run_interval', 'value': 0},
        #  {'metric': 'jemalloc.background_thread.num_runs', 'value': 0},

        cols = ', description' if fetch_col_descriptions else ''
        req = requests.get(f'''{self.url}&query=Select event as metric, value{cols} from system.events 
          where metric in ('InsertedBytes','InsertedRows','InsertQuery','MergedRows','MergedUncompressedBytes','ReadCompressedBytes',
          'SelectedParts','SelectQuery') format JSON''',
                           timeout=3)
        req.raise_for_status()
        events = req.json()['data']
        # [{'event': 'FileOpen', 'value': 45824293, 'description': 'Number of files opened.'},
        #  {...

        return metrics, async_metrics, events

    def health(self):
        metrics, async_metrics, events = self._get_metrics()
        return {metr['metric']: metr['value'] for metr in metrics + async_metrics}
