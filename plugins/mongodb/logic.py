import pymongo
import logging
import time

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.host = options.get('host', '127.0.0.1')
        self.port = int(options.get('port', 27017))
        self.authdb = options.get('authdb', '')
        self.user = options.get('user', None)
        self.password = options.get('password', None)
        self.database = options.get('database', None)

    def connect(self):
        self.mongo = pymongo.MongoClient(host=self.host, port=self.port)
        log.debug('connected to mongodb @ {}:{}'.format(self.host, self.port))

        if self.user and self.password:  # authentificate to database
            db = self.mongo[self.authdb]
            db.authenticate(self.user, password=self.password)
            log.debug('authenticate to mongodb @ {}:{}'.format(self.host, self.port))

    def test_connect(self):
        time_start = time.time()
        self.connect()

        info = self.mongo.server_info()
        log.debug(info)

        conn_time = time.time() - time_start
        conn_time = round(conn_time, 0)
        log.debug('connection time: %d' % conn_time)

        return True

    def _get_server_status_info(self):
        self.connect()
        res = self.mongo.admin.command("serverStatus")
        return res

    def test_connections(self):
        res = self._get_server_status_info()

        current = float(res['connections']['current'])
        available = float(res['connections']['available'])
        used_percent = int(float(current / (available + current)) * 100)

        return {"used_percent": used_percent}

    def test_memory(self):
        res = self._get_server_status_info()

        mem_resident = float(res['mem']['resident']) / 1024.0
        mem_virtual = float(res['mem']['virtual']) / 1024.0
        mem_mapped = float(res['mem']['mapped']) / 1024.0
        mem_mapped_journal = float(res['mem']['mappedWithJournal']) / 1024.0

        return {"mem_resident": mem_resident, "mem_virtual": mem_virtual, "mem_mapped": mem_mapped, "mem_mapped_journal": mem_mapped_journal}

    def test_flushing(self):
        res = self._get_server_status_info()
        if res.get('backgroundFlushing'):
            flush_average = float(res['backgroundFlushing']['average_ms'])
            flush_time = float(res['backgroundFlushing']['last_ms'])

            return {"flush_average": flush_average, "flush_time": flush_time}
        else:
            return {}

    def test_database_size(self):
        if self.database:  # specified db
            self.connect()
            res = self.mongo[self.database].command('dbstats')
            return res['storageSize'] / 1024 / 1024
        else:
            self.connect()
            db_list = self.mongo.admin.command('listDatabases')
            total_size = 0
            for db in db_list['databases']:
                res = self.mongo[db['name']].command('dbstats')
                total_size += res['storageSize'] / 1024 / 1024
            return total_size

    def list_databases(self):
        self.connect()
        db_list = self.mongo.admin.command('listDatabases')
        return {e['name']: e['name'] for e in db_list['databases']}
