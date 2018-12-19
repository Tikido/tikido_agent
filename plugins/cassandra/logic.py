import jmxquery
import logging
from time import time
log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.host = options.get('host')
        self.port = options.get('port')
        self.user = options.get('user', '')
        self.password = options.get('password', '')
        self.keyspace = options.get('keyspace', '')
        self.jmxconn = None

    def connect(self):
        try:
            if self.user:
                self.jmxconn = jmxquery.JMXConnection(f"service:jmx:rmi:///jndi/rmi://{self.host}:{self.port}/jmxrmi", jmx_username=self.user, jmx_password=self.password)
                jmxQuery = [jmxquery.JMXQuery("org.apache.cassandra.metrics:type=Keyspace")]
                res = self.jmxconn.query(jmxQuery)
            else:
                self.jmxconn = jmxquery.JMXConnection(f"service:jmx:rmi:///jndi/rmi://{self.host}:{self.port}/jmxrmi")
                jmxQuery = [jmxquery.JMXQuery("org.apache.cassandra.metrics:type=Keyspace")]
                res = self.jmxconn.query(jmxQuery)
        except Exception:
            raise ConnectionError

    def metrics(self):
        self.connect()
        metrics = dict()

        Query = [jmxquery.JMXQuery("org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency")]
        res = self.jmxconn.query(Query)
        metrics['read_one_minute_rate'] = round([_.value for _ in res if _.attribute == 'OneMinuteRate'][0], 6)
        metrics['read_75th_percentile'] = round([_.value for _ in res if _.attribute == '75thPercentile'][0], 6)
        metrics['read_99th_percentile'] = round([_.value for _ in res if _.attribute == '99thPercentile'][0], 6)

        Query = [jmxquery.JMXQuery("org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency")]
        res = self.jmxconn.query(Query)
        metrics['write_one_minute_rate'] = round([_.value for _ in res if _.attribute == 'OneMinuteRate'][0], 6)
        metrics['write_75th_percentile'] = round([_.value for _ in res if _.attribute == '75thPercentile'][0], 6)
        metrics['write_99th_percentile'] = round([_.value for _ in res if _.attribute == '99thPercentile'][0], 6)

        Query = [jmxquery.JMXQuery("org.apache.cassandra.metrics:type=DroppedMessage,scope=READ,name=Dropped/Count")]
        res = self.jmxconn.query(Query)
        metrics['dropped_count'] = res[0].value

        metrics['timestamp'] = time()

        return metrics

    def status(self):
        self.connect()
        status = dict()

        query = [jmxquery.JMXQuery("java.lang:type=Memory/HeapMemoryUsage")]
        res = self.jmxconn.query(query)
        status['heap_memory_usage'] = [_.value for _ in res if _.attributeKey == 'used'][0]

        query = [jmxquery.JMXQuery("java.lang:type=Threading/ThreadCount")]
        res = self.jmxconn.query(query)
        status['thread_count'] = res[0].value

        query = [jmxquery.JMXQuery("java.lang:type=ClassLoading/LoadedClassCount")]
        res = self.jmxconn.query(query)
        status['loaded_class_count'] = res[0].value

        query = [jmxquery.JMXQuery("java.lang:type=OperatingSystem/ProcessCpuLoad")]
        res = self.jmxconn.query(query)
        status['process_cpu_load'] = round(res[0].value, 4)

        return status
