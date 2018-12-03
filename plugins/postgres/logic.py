import psycopg2
import logging
import time

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


# https://github.com/spotify/postgresql-metrics
class Logic(IAgentCore):
    def __init__(self, options):
        self.host = options.get('host', '127.0.0.1')
        self.port = int(options.get('port', 5432))
        self.username = options.get('username', None)
        self.password = options.get('password', None)
        self.database = options.get('database', '')
        self.timeout = int(options.get('timeout', 5))

    def connect(self):
        self.server = psycopg2.connect(user=self.username, password=self.password, host=self.host, port=self.port, database=self.database,
                                       connect_timeout=self.timeout)
        self.cursor = self.server.cursor()
        log.debug('connected to postgresql @ {}:{}'.format(self.host, self.port))

    def test_connect(self):
        time_start = time.time()
        self.connect()
        log.debug('connection time: %d' % round(time.time() - time_start, 0))
        return True

    def _execute(self, sql):
        self.connect()
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        return res

    def get_client_connections(self):
        res = self._execute("select (select setting from pg_settings where name='max_connections'),count(*) FROM pg_stat_activity")
        if res:
            return int(res[0][1]) / int(res[0][0]) * 100
        else:
            return {}

    def get_database_disk_usage(self):
        res = self._execute('SELECT pg_database_size(datname) FROM pg_database WHERE datname = current_database()')
        if res:
            return {'database_size': float(res[0][0] / 1024 / 1024)}
        else:
            return {}

    def get_transaction_rate(self):
        res = self._execute('SELECT xact_commit + xact_rollback, xact_rollback FROM pg_stat_database WHERE datname = current_database()')
        if res:
            return {"transactions": res[0][0], "rollbacks": res[0][1]}
        else:
            return {}

    def get_heap_hit_ratio(self):
        res = self._execute('SELECT sum(heap_blks_read), sum(heap_blks_hit) FROM pg_statio_user_tables')
        if res and res[0][0] != 0:
            return {'heap_ratio': float(res[0][1] / (res[0][0] + res[0][1])) * 100.0}
        else:
            return {'heap_ratio': 0}

    def get_lock_stats(self):
        res = self._execute('SELECT count(*) from pg_locks')  # 'SELECT locktype, granted, count(*) FROM pg_locks GROUP BY locktype, granted'
        if res:
            return {'lock_count': res[0][0]}
        else:
            return {}

    def get_longest_transaction(self):
        res = self._execute('SELECT now()-xact_start FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY xact_start ASC LIMIT 1')
        if res:
            return {'seconds': int(res[0][0].total_seconds())}
        else:
            return {}

    def get_replications_stats(self):
        res = self._execute('SELECT * FROM pg_stat_replication')
        #            'SELECT client_addr, pg_xlog_location_diff(pg_current_xlog_location(), replay_location) AS bytes_diff FROM public.pg_stat_replication'
        return res or {}

    def get_index_hit_ratio(self):
        res = self._execute(
            'SELECT relname, idx_scan/float4(idx_scan+seq_scan) FROM pg_stat_user_tables where idx_scan <> 0')
        if res:
            return {e[0]: e[1] for e in res}
        else:
            return {}

    def list_databases(self):
        res = self._execute('select datname from pg_database where datallowconn is true')
        return {e[0]: e[0] for e in res}
