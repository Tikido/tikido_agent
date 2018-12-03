import mysql.connector
from mysql.connector import errors
import logging
log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.host = options.get('host')
        self.port = int(options.get('port', 3306))
        self.user = options.get('user_name')
        self.password = options.get('password')
        self.timeout = options.get('timeout', 3)
        self.database = options.get('database', 'mysql')
        self.connect()

    def connect(self):
        try:
            mysql_options = {"host": self.host, "port": self.port, "user": self.user, "password": self.password,
                             'connection_timeout': self.timeout}
            self.server = mysql.connector.connect(**mysql_options)
            self.server.database = self.database
            self.cursor = self.server.cursor()
            self.cursor.execute(f"USE {self.database}")
        except mysql.connector.errors.DatabaseError as err:
            raise ConnectionError

    def execute(self, execute_str):
        self.cursor.execute(execute_str)
        res = self.cursor.fetchall()
        return res

    def status(self):
        res = self.execute("SELECT * FROM performance_schema.global_variables WHERE variable_name IN ('max_connections')")
        variables = {'variables.' + name: value for name, value in res}

        res = self.execute(
                "SELECT * FROM performance_schema.global_status WHERE variable_name IN ('Threads_connected', 'Uptime', 'Bytes_received', 'Bytes_sent')")
        status = {'status.' + name: value for name, value in res}
        variables.update(status)
        return variables
