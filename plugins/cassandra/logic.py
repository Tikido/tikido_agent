import jmxquery
import logging
from time import time

from ..agent_core import IAgentCore

log = logging.getLogger(__name__)


class Logic(IAgentCore):
    def __init__(self, options):
        self.host = options.get('host')
        self.port = options.get('port')
        self.user = options.get('user', '')
        self.password = options.get('password', '')
        self.key_space = options.get('keyspace', '')
        self.jmx_conn = None

        super(Logic, self).__init__(options)

    def connect(self):
        try:
            self.jmx_conn = jmxquery.JMXConnection(f"service:jmx:rmi:///jndi/rmi://{self.host}:{self.port}/jmxrmi",
                                                   jmx_username=self.user, jmx_password=self.password)
            if self.user:
                query = [jmxquery.JMXQuery("org.apache.cassandra.metrics:type=Keyspace")]
                self.jmx_conn.query(query)
            else:
                self.jmx_conn = jmxquery.JMXConnection(f"service:jmx:rmi:///jndi/rmi://{self.host}:{self.port}/jmxrmi")
                query = [jmxquery.JMXQuery("org.apache.cassandra.metrics:type=Keyspace")]
                self.jmx_conn.query(query)
        except Exception:
            raise ConnectionError

    def metrics(self):
        self.connect()
        metrics = dict()

        query = [jmxquery.JMXQuery("org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency")]
        res = self.jmx_conn.query(query)
        metrics['read_one_minute_rate'] = round([_.value for _ in res if _.attribute == 'OneMinuteRate'][0], 6)
        metrics['read_75th_percentile'] = round([_.value for _ in res if _.attribute == '75thPercentile'][0], 6)
        metrics['read_99th_percentile'] = round([_.value for _ in res if _.attribute == '99thPercentile'][0], 6)

        query = [jmxquery.JMXQuery("org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency")]
        res = self.jmx_conn.query(query)
        metrics['write_one_minute_rate'] = round([_.value for _ in res if _.attribute == 'OneMinuteRate'][0], 6)
        metrics['write_75th_percentile'] = round([_.value for _ in res if _.attribute == '75thPercentile'][0], 6)
        metrics['write_99th_percentile'] = round([_.value for _ in res if _.attribute == '99thPercentile'][0], 6)

        query = [jmxquery.JMXQuery("org.apache.cassandra.metrics:type=DroppedMessage,scope=READ,name=Dropped/Count")]
        res = self.jmx_conn.query(query)
        metrics['dropped_count'] = res[0].value

        metrics['timestamp'] = time()

        return metrics

    def status(self):
        self.connect()
        status = dict()

        query = [jmxquery.JMXQuery("java.lang:type=Memory/HeapMemoryUsage")]
        res = self.jmx_conn.query(query)
        status['heap_memory_usage'] = [_.value for _ in res if _.attributeKey == 'used'][0]

        query = [jmxquery.JMXQuery("java.lang:type=Threading/ThreadCount")]
        res = self.jmx_conn.query(query)
        status['thread_count'] = res[0].value

        query = [jmxquery.JMXQuery("java.lang:type=ClassLoading/LoadedClassCount")]
        res = self.jmx_conn.query(query)
        status['loaded_class_count'] = res[0].value

        query = [jmxquery.JMXQuery("java.lang:type=OperatingSystem/ProcessCpuLoad")]
        res = self.jmx_conn.query(query)
        status['process_cpu_load'] = round(res[0].value, 4)

        return status
