import logging
import os
import psutil
import fnmatch

log = logging.getLogger(__name__)
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):
        self.disk = options.get('disk', '.')
        self.partition = options.get('partition', '/')
        self.directory = options.get('directory', '.')
        self.file_name = options.get('file_name', '')
        self.recursive = int(options.get('recursive', False))
        self.search_pattern = options.get('search_pattern', '*')
        self.return_bytes = int(options.get('return_bytes', 0))
        self.size = int(options.get('file_size', 0))

    def cpu_info(self):
        res = psutil.cpu_percent()
        return res

    def directory_count_files(self):
        count = 0
        for root, dirs, files in os.walk(self.directory):
            for file in files:
                if fnmatch.fnmatch(file, self.search_pattern):
                    count += 1
            if not self.recursive:
                break
        return count

    def directory_total_size(self):
        size = 0
        for root, dirs, files in os.walk(self.directory):
            for file in files:
                path = os.path.join(root, file)
                if os.path.exists(path):
                    try:
                        size += os.path.getsize(path)
                    except Exception as err:
                        print(err)
            if not self.recursive:
                break
        return size

    def disk_io(self):
        res = psutil.disk_io_counters()
        return {'read_count': res.read_count, 'write_count': res.write_count}

    def disk_usage(self):
        res = psutil.disk_usage(self.partition)
        return res[3]

    def file_creation(self):
        path = os.path.join(self.directory, self.file_name)
        if os.path.exists(path):
            stat = os.stat(path)
            return stat.st_ctime
        else:
            raise FileNotFoundError(f'File {path} not found')

    def file_last_modification(self):
        path = os.path.join(self.directory, self.file_name)
        if os.path.exists(path):
            stat = os.stat(path)
            return stat.st_mtime
        else:
            raise FileNotFoundError(f'File {path} not found')

    def file_size(self):
        path = os.path.join(self.directory, self.file_name)
        if os.path.exists(path):
            stat = os.stat(path)
            data = ''
            if self.size and self.return_bytes:
                with open(path, 'rb') as file:
                    file.seek(self.size)
                    data = file.read(self.return_bytes).decode('utf-8')
            return {'file_size': stat.st_size, 'data': data}
        else:
            raise FileNotFoundError(path)

    def memory_info(self):
        res = psutil.virtual_memory()
        return res.percent

    def network_io(self):
        res = psutil.net_io_counters()
        return {'bytes_recv': res.bytes_recv, 'bytes_sent': res.bytes_sent}

    def uptime_info(self):
        res = int(psutil.boot_time())
        return res

    def list_partitions(self):
        return {e.mountpoint: e.device for e in psutil.disk_partitions(all) if
                e.fstype in ['ext4', 'vfat', 'fuseblk', 'NTFS']}  # fuseblk == ntfs
