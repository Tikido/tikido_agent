import logging
import os

import cx_Oracle

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    SYSTEM_METRICS = {
        'User Rollbacks Per Sec': 'user_rollbacks',
        'Total Sorts Per User Call': 'sorts_per_user_call',
        'Rows Per Sort': 'rows_per_sort',
        'Disk Sort Per Sec': 'disk_sorts',
        'Memory Sorts Ratio': 'memory_sorts_ratio',
        'Buffer Cache Hit Ratio': 'buffer_cachehit_ratio',
        'Cursor Cache Hit Ratio': 'cursor_cachehit_ratio',
        'Host CPU Utilization (%)': 'host_cpu_utilization_percent',
        'Library Cache Hit Ratio': 'library_cachehit_ratio',
        'Shared Pool Free %': 'shared_pool_free',
        'Physical Reads Per Sec': 'physical_reads',
        'Physical Writes Per Sec': 'physical_writes',
        'Enqueue Timeouts Per Sec': 'enqueue_timeouts',
        'GC CR Block Received Per Second': 'gc_cr_block_received',
        'Global Cache Blocks Corrupted': 'cache_blocks_corrupt',
        'Global Cache Blocks Lost': 'cache_blocks_lost',
        'Logons Per Sec': 'logons_per_sec',
        'Average Active Sessions': 'active_sessions',
        'Long Table Scans Per Sec': 'long_table_scans',
        'SQL Service Response Time': 'service_response_time',
        'Database Wait Time Ratio': 'database_wait_time_ratio',
        'Session Limit %': 'session_limit_usage',
        'Session Count': 'session_count',
        'Temp Space Used': 'temp_space_used'
        }

    def __init__(self, options):

        if 'schema_table' in options:
            self.schema, self.table_name = options.get('schema_table').split('.')

        self.conn = None
        self.cursor = None

        super().__init__(options)

    def connect(self):
        try:
            self.conn = cx_Oracle.connect(f'{self.username}/{self.password}@{self.host}:{self.port or 1521}/{self.sid}',
                                          encoding="UTF-8", nencoding="UTF-8")
            self.cursor = self.conn.cursor()
        except cx_Oracle.Error as err:
            log.debug(err)
            raise ConnectionError(err)

    def disconnect(self):
        self.conn.close()

    def version(self):
        self.connect()
        return self.conn.version

    @staticmethod
    def client_version():
        return cx_Oracle.clientversion()

    def health(self):
        self.connect()
        res = self._get_health_metrics()
        self.disconnect()
        return res

    # -----------------------------------------------------------------------------------------------------
    def _get_health_metrics(self):
        out = {}
        # sys metrics
        self.cursor.execute('''SELECT metric_name, round(value, 6), begin_time FROM GV$SYSMETRIC  ORDER BY begin_time''')
        for metric_name, metric_val, begin_time in self.cursor.fetchall():
            if metric_name in self.SYSTEM_METRICS:
                out[self.SYSTEM_METRICS[metric_name]] = metric_val

        # tablespaces metrics
        self.cursor.execute('''SELECT TABLESPACE_NAME, SUM(BYTES), SUM(MAXBYTES) FROM SYS.DBA_DATA_FILES GROUP BY TABLESPACE_NAME''')
        for tablespace_name, bytes, maxbytes in self.cursor.fetchall():
            if bytes is None:
                # offline = True
                used = 0
            else:
                # offline = False
                used = float(bytes)

            size = 0 if maxbytes is None else float(maxbytes)

            if used >= size:
                in_use_percent = 100
            elif not used or not size:
                in_use_percent = 0
            else:
                in_use_percent = used / size * 100

            out["tablespace." + tablespace_name] = in_use_percent  # dict(bytes=bytes, maxbytes=maxbytes, offline=offline, in_use_percent=in_use,
            # used=used)

        return out

    def get_metrics(self):
        self.connect()
        self.cursor.execute('''SELECT TABLESPACE_NAME FROM SYS.DBA_DATA_FILES ''')
        tablespaces = self.cursor.fetchall()
        metrics = self._get_health_metrics()
        self.disconnect()

        return metrics, tablespaces

    def get_table_meta(self):
        self.connect()
        self.cursor.execute(f"""select column_name, data_type, data_length from 
            (SELECT column_name, data_type, data_length, 
            owner||'.'||table_name as schema_table
            from all_tab_columns)
            where  schema_table='{self.schema_table}'""")
        columns = [i[0].lower() for i in self.cursor.description]
        res = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        self.disconnect()
        return res

    def add_row(self, data2insert, placeholders):
        self.connect()
        log.debug(f"Insert into {self.schema_table} ({','.join(data2insert.keys())}) values ({placeholders}) %r", data2insert)

        os.environ["NLS_LANG"] = ".AL32UTF8"
        self.cursor.execute(f"Insert into {self.schema_table} ({','.join(data2insert.keys())}) values ({placeholders})", data2insert)
        self.conn.commit()
        self.disconnect()

    def get_max_pk(self):
        self.connect()
        self.cursor.execute("Select max({}) as max_pk from {}".format(self.pk, self.table))
        max_pk = self.cursor.fetchone()[0]
        self.disconnect()
        return max_pk

    def get_rows(self):
        self.connect()
        try:
            self.cursor.execute(f"Select * from {self.table} where {self.pk}>:pk order by {self.pk} FETCH NEXT 1 ROWS ONLY", pk=self.state.max_pk)
        except cx_Oracle.DatabaseError:
            self.cursor.execute(f"Select * from {self.table} where {self.pk}>:pk order by {self.pk}", pk=self.state.max_pk)

        columns = [i[0] for i in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def list_tables(self):
        self.connect()
        self.cursor.execute(
            f"""SELECT tc.owner|| '.' ||tc.table_name as name, tc.comments from all_tab_comments tc join all_tables t on tc.owner = t.owner and tc.table_name = t.table_name where t.dropped = 'NO' and t.tablespace_name not in ( 'SYS', 'SYSAUX')  and t.owner not in ('SYS', 'SYSTEM')""")
        res = self.cursor.fetchall()
        self.disconnect()
        return res

    def get_query_max_pk(self):
        self.connect()
        self.cursor.execute(f"Select max({self.pk}) as max_pk from ({self.query}) q")
        res = self.cursor.fetchone()[0]
        self.disconnect()
        return res

    def get_query_rows(self):
        self.connect()
        self.cursor.execute(f"Select * from ({self.query}) q where {self.pk}>:max_pk order by {self.pk}", max_pk=self.state.max_pk)
        columns = [i[0] for i in self.cursor.description]
        res = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        self.disconnect()
        return res

    def get_table_meta_with_column_types(self):
        self.connect()
        self.cursor.execute(
            f"""select column_name, data_type, char_length, nullable from all_tab_columns where table_name='{self.table_name}' and owner='{self.schema}'""")
        columns = [i[0].lower() for i in self.cursor.description]
        res = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        self.disconnect()
        return res

    def get_query_columns(self):
        self.connect()
        self.cursor.execute(self.query)
        res = [i[0] for i in self.cursor.description]
        self.disconnect()
        return res
