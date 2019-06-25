import jmxquery
import logging

from ..agent_core import IAgentCore

log = logging.getLogger(__name__)


class Logic(IAgentCore):
    def __init__(self, options):
        self.host = options.get('host')
        self.port = options.get('port')
        self.user = options.get('user', '')
        self.password = options.get('password', '')
        self.topic_name = options.get('topic_name', '')
        self.jmx_conn = None

        super(Logic, self).__init__(options)

    def connect(self):
        try:
            self.jmx_conn = jmxquery.JMXConnection(f"service:jmx:rmi:///jndi/rmi://{self.host}:{self.port}/jmxrmi",
                                                   jmx_username=self.user, jmx_password=self.password)
            if self.user:
                query = [jmxquery.JMXQuery("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec")]
                self.jmx_conn.query(query)
            else:
                self.jmx_conn = jmxquery.JMXConnection(f"service:jmx:rmi:///jndi/rmi://{self.host}:{self.port}/jmxrmi")
                query = [jmxquery.JMXQuery("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec")]
                self.jmx_conn.query(query)
        except Exception:
            raise ConnectionError

    def metrics(self):
        self.connect()
        metrics = dict()

        topic = f',topic={self.topic_name}' if self.topic_name else ''

        query = [jmxquery.JMXQuery(f"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec{topic}")]
        res = self.jmx_conn.query(query)
        metrics['bytes_in'] = round([_.value for _ in res if _.attribute == 'OneMinuteRate'][0], 4)

        query = [jmxquery.JMXQuery(f"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec{topic}")]
        res = self.jmx_conn.query(query)
        metrics['bytes_out'] = round([_.value for _ in res if _.attribute == 'OneMinuteRate'][0], 4)

        query = [jmxquery.JMXQuery(f"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec{topic}")]
        res = self.jmx_conn.query(query)
        metrics['messages_in'] = round([_.value for _ in res if _.attribute == 'OneMinuteRate'][0], 4)

        return metrics
