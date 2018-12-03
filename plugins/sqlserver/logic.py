import pymssql
import logging

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.host = options.get('host')
        self.port = int(options.get('port', 1433))
        self.username = options.get('username')
        self.password = options.get('password')
        self.timeout = options.get('timeout', 3)
        self.db = options.get('db', '')

    def connect(self):
        self.conn = pymssql.connect(server=self.host, user=self.username, password=self.password, database=self.db, port=self.port, as_dict=True, timeout=self.timeout, autocommit=True)
        self.cur = self.conn.cursor(as_dict=True)
        log.debug('connected to sqlserver @ {}:{}'.format(self.host, self.port))

    def health(self):
        out = {}
        self.connect()
        self.cur.execute('''SELECT *
     --[User Connections]           AS UserConnections
    FROM
       (SELECT
          counter_name,
          cntr_value
        FROM master..sysperfinfo
        WHERE object_name LIKE '%:Access Methods%' OR object_name LIKE '% Manager%' OR object_name LIKE '% Statistics%') P
     PIVOT (SUM(cntr_value) FOR [counter_name] IN (
        -- [Active Temp Tables],
        -- [Batch Requests/sec],
       --  [Buffer cache hit ratio],
        -- [Buffer cache hit ratio base],
        -- [Checkpoint pages/sec],
       --  [Free pages],
       --  [FreeSpace Scans/sec],
       --  [Full Scans/sec],
       --  [Index Searches/sec],
       --  [Lazy writes/sec],
       --  [Page life expectancy],
      --   [Page reads/sec],
       --  [Page Splits/sec],
      --   [Page writes/sec],
      --   [Probe Scans/sec],
       --  [Processes blocked],
       --  [Range Scans/sec],
       --  [Readahead pages/sec],
       --  [SQL Cache Memory (KB)],
       --  [SQL Compilations/sec],
         [Target pages],
        -- [Total pages],
         [User Connections]
     )) AS PVT''')
        for row in self.cur:
            out.update(row)

        # {'Target pages': 409600,
        # 'User Connections': 8}

        # print('-' * 50)

        self.cur.execute("""SET NOCOUNT ON
    DECLARE @tt TABLE(dbname VARCHAR(50), UnallocatedSpace BIGINT, Status INT)
    DECLARE @DBName VARCHAR(50)
    DECLARE @DBState INT
    DECLARE cursr INSENSITIVE CURSOR FOR SELECT
                   name,
                   state
                 FROM sys.databases
    OPEN cursr
    FETCH NEXT FROM cursr
    INTO @DBName, @DBState
    WHILE @@FETCH_STATUS = 0
      BEGIN
        IF (has_dbaccess(@DBName) = 1)
          INSERT @tt EXEC ('select dbname = ''' + @DBName + ''', UnallocatedSpace = (SELECT sum(total_pages) FROM [' + @DBName + '].sys.allocation_units) * 8192, Status= ''' + @DBState + '''')
        ELSE
          INSERT @tt VALUES (@DBName, 0, @DBState)
        FETCH NEXT FROM cursr
        INTO @DBName, @DBState
      END
    CLOSE cursr
    DEALLOCATE cursr
    SELECT
      --[Active Transactions]         AS ActiveTransactions,
      --[Backup/Restore Throughput/sec]       AS BackupRestoreThroughput,
      --[Bulk Copy Throughput/sec]    AS BulkCopyThroughput,
      ([Data File(s) Size (KB)] * 1024)     AS DataFilesSize,
    --  [DBCC Logical Scan Bytes/sec]         AS DBCCLogicalScanBytes,
     -- [Log Bytes Flushed/sec]       AS LogBytesFlushed,
     -- ([Log File(s) Size (KB)] * 1024)      AS LogFilesSize,
     -- ([Log File(s) Used Size (KB)] * 1024) AS LogFilesUsedSize,
     -- [Log Flushes/sec]             AS LogFlushes,
    --  [Log Flush Waits/sec]         AS LogFlushWaits,
     -- [Log Flush Wait Time]         AS LogFlushWaitTime,
     -- [Percent Log Used]            AS PercentLogUsed,
     -- [Repl. Trans. Rate]           AS ReplTransRate,
      --[Shrink Data Movement Bytes/sec]      AS ShrinkDataMovementBytes,
      (CASE WHEN [Data File(s) Size (KB)] IS NULL
        THEN NULL
       WHEN (([Data File(s) Size (KB)] * 1024) >= tt.UnallocatedSpace)
         THEN (([Data File(s) Size (KB)] * 1024) - tt.UnallocatedSpace)
       ELSE 0 END)          AS SpaceAvailable,
      tt.Status             AS Status,
     -- [Transactions/sec]            AS Transactions,
      RTRIM(tt.dbname)              AS instance_name
    FROM @tt AS tt LEFT JOIN
        (SELECT
           instance_name,
           counter_name,
           cntr_value
         FROM master..sysperfinfo
         WHERE object_name LIKE '%:Databases%') P
      PIVOT (SUM(cntr_value) FOR [counter_name] IN (
        --  [Active Transactions],
        --  [Backup/Restore Throughput/sec],
        --  [Bulk Copy Throughput/sec],
          [Data File(s) Size (KB)],
        --  [DBCC Logical Scan Bytes/sec],
        --  [Log Bytes Flushed/sec],
        --  [Log File(s) Size (KB)],
        --  [Log File(s) Used Size (KB)],
        --  [Log Flushes/sec],
       --   [Log Flush Waits/sec],
        --  [Log Flush Wait Time],
        --  [Percent Log Used],
        --  [Repl. Trans. Rate],
        --  [Shrink Data Movement Bytes/sec],
          [Transactions/sec])) AS PVT
        ON tt.dbname = PVT.instance_name

    where tt.dbname='{}'""".format(self.db))

        for row in self.cur:
            out.update(row)
            break
            # print(']')

        # {'DataFilesSize': 8388608,
        #  'SpaceAvailable': 5750784,
        #  'Status': 0,
        #  'instance_name': 'tiki'}
        # print()

        self.cur.execute("""SELECT
      --[Info Errors]    AS InfoErrors,  [Kill Connection Errors] AS KillConnectionErrors,  [User Errors]    AS UserErrors,  [AllocUnit],  [Application],  [Database],
      --[Extent],  [File],  [HoBT],  [Key],  [Metadata],  [Object],  [Page],  [RID],
      [_Total]         AS Total,
      RTRIM(counter_name)      AS counter_name
    FROM
        (SELECT
           instance_name,
           counter_name,
           cntr_value
         FROM master..sysperfinfo
         WHERE object_name LIKE '%:Locks%' OR object_name LIKE '%:SQL Errors%') P
      PIVOT (SUM(cntr_value) FOR [instance_name] IN (
        --  [DB Offline Errors],
         -- [Kill Connection Errors],
        --  [User Errors],
        --  [Info Errors],
          [_Total]
         -- [AllocUnit],
         -- [Application],
         -- [Database],
         -- [Extent],
         -- [File],
         -- [HoBT],
         -- [Key],
         -- [Metadata],
         -- [Object],
         -- [Page],
         -- [RID]
      )) AS PVT
    WHERE counter_name = 'Lock Wait Time (ms)'""")
        for row in self.cur:
            out[row['counter_name']] = row['Total']
            break
        # {'Total': 12014, 'counter_name': 'Lock Wait Time (ms)'}
        # print()

        self.cur.execute("""SELECT   -- object_name,
            counter_name, --instance_name,
            cntr_value --, cntr_type
    FROM
       Sys.dm_os_performance_counters
       where
       counter_name in ('Avg Disk Write IO (ms)', 'Avg Disk Read IO (ms)', 'Total Server Memory (KB)', 'Used memory (KB)') and (instance_name in ('internal', ''))""")
        for row in self.cur:
            out[row['counter_name'].strip()] = row['cntr_value']
        # print('=' * 30)

        #         '''SELECT
        #     object_name, counter_name, instance_name, cntr_value, cntr_type
        # FROM
        #     sys.dm_os_performance_counters'''
        #
        #         '''SELECT
        #           COUNT(*)
        #         FROM
        #           master..sysprocesses
        #         WHERE
        #           hostprocess IS NOT NULL AND program_name != 'JS Agent' '''

        out.pop('Status', None)
        out.pop('instance_name', None)

        out['used_memory_percent'] = (out['Used memory (KB)'] / out['Total Server Memory (KB)'] * 100)

        # {'Avg Disk Read IO (ms)': 307,
        #  'Avg Disk Write IO (ms)': 137,

        #  'DataFilesSize': 8388608,
        #  'SpaceAvailable': 5750784,

        #  'Lock Wait Time (ms)': 12014,

        #  'Total Server Memory (KB)': 258992, # http://www.sqlshack.com/sql-server-memory-performance-metrics-part-2-available-bytes-total-server-target-server-memory/
        #  'Used memory (KB)': 128176,
        #  'Target pages': 409600, The Target Server Memory (KB) value shows how much memory SQL Server needs to for best performance

        #  'User Connections': 8}
        return out
