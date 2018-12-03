from elasticsearch import Elasticsearch, exceptions
import logging
import time

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.host_port = options.get('host_port')
        self.index = options.get('index', '')
        self.doc_type = options.get('doc_type', '')
        self.sort_field = options.get('sort_field', '')
        self.src = options.get('src', '').split('|')[0]
        self.timestamp = options.get('timestamp', '')
        self.index_wildcard = options.get('index_wildcard', '*')
        self.node = options.get('node', '')

    def connect(self):
        try:
            self.elastic = Elasticsearch(self.host_port, timeout=5, request_timeout=5, retries=False, max_retries=0)
            res = self.elastic.ping()
            if not res:
                raise ConnectionError
        except Exception as e:
            raise ConnectionError

    def ping(self):
        try:
            delay = time.time()
            self.connect()

            return round((time.time() - delay) * 1000)
        except exceptions.ConnectionError:
            return -1

    def status_cluster(self):
        self.connect()
        cluster_state = self.elastic.cluster.stats()['nodes']

        fs_free_percent = cluster_state['fs']['available_in_bytes'] / cluster_state['fs']['total_in_bytes'] * 100 // 1
        os_mem_free_percent = cluster_state['os']['mem']['free_percent']
        process_cpu_percent = cluster_state['process']['cpu']['percent']

        res = dict()
        res['fs_free_percent'] = fs_free_percent
        res['os_mem_free_percent'] = os_mem_free_percent
        res['process_cpu_percent'] = process_cpu_percent
        return res

    def search_direct(self):
        self.connect()
        res = self.elastic.search(index=self.index, doc_type=self.doc_type, version=True, size=1, body={"sort": {self.sort_field: {"order": "desc"}}})

        return res['hits']['hits'][0]['_source'][self.sort_field]

    def search_source(self):
        self.connect()
        res = self.elastic.search(index=self.index, doc_type=self.doc_type, version=True, size=1, _source=",".join((self.src, self.sort_field)),
                                  body={"query": {"bool": {"filter": {"range": {self.sort_field: {"gt": self.timestamp}}}}},
                                        "sort": {self.sort_field: {"order": "asc"}}})

        return res

    def count(self):
        self.connect()
        res = self.elastic.count(index=self.index, doc_type=self.doc_type)
        return res['count']

    def indices(self):
        self.connect()
        res = self.elastic.indices.get(index=self.index_wildcard)
        return res

    def doctypes(self):
        self.connect()
        res = self.elastic.indices.get(index=self.index)
        return res

    def status_nodes(self):
        self.connect()
        nodes_stats = self.elastic.nodes.stats(self.node)['nodes']
        nodes = dict()
        nodes['fs_free_percent'] = nodes_stats[self.node]['fs']['total']['available_in_bytes'] / nodes_stats[self.node]['fs']['total']['total_in_bytes'] * 100 // 1
        nodes['os_mem_free_percent'] = nodes_stats[self.node]['os']['mem']['free_percent']
        nodes['process_cpu_percent'] = nodes_stats[self.node]['process']['cpu']['percent']

        return nodes

    def nodes_list(self):
        self.connect()
        res = self.elastic.nodes.stats()['nodes']
        return {el: res[el]['name'] for el in res}