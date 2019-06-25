# -*- coding: utf-8 -*-
import logging

log = logging.getLogger(__name__)

import winrm
from ..agent_core import IAgentCore


class Logic(IAgentCore):
    def __init__(self, options):

        super().__init__(options)

        try:
            self.sess = winrm.Session(self.host + (':' + self.port if self.port else ''),
                                      auth=(self.winuser, self.winuserpass),
                                      transport='ssl' if self.ssl == '1' else '',
                                      server_cert_validation='ignore')
            # rslt = self.sess.run_cmd('whoami')
            # if rslt.status_code == 0:
            #     log.info(rslt.std_out.decode('utf8'))
            # else:
            #     log.warning(rslt.std_err.decode('utf8'))
        except Exception as e:
            log.exception('cannot connect to Windows host - %s' % str(e))
            raise self.PluginException('cannot connect to Windows host - %s' % str(e))

    def whoami(self):  # web
        rslt = self.sess.run_cmd('whoami')
        if rslt.status_code == 0:
            log.debug(rslt.std_out.decode('utf8'))
            return dict(result='success', value='success',
                        vars={}
                        )
        else:
            log.warning(rslt.std_err.decode('utf8'))
            return dict(result='error', value=rslt.std_err.decode('utf8'), vars={})

    def _get_ram(self):
        ps_script = """$RAM = WmiObject Win32_ComputerSystem
        $MB = 1048576
        "Installed Memory: " + [int]($RAM.TotalPhysicalMemory /$MB) + " MB" """

        return self.sess.run_ps(ps_script)

    def _get_cpu(self):
        # TODO more at https://www.datadoghq.com/blog/monitoring-windows-server-2012/
        #         ps_script = """$ProcessorPercentage = (Get-WmiObject Win32_PerfFormattedData_PerfOS_Processor  -filter "Name='_Total'").PercentProcessorTime
        # Write-Output "$ProcessorPercentage" """
        ps_script = """(Get-WmiObject Win32_PerfFormattedData_PerfOS_Processor -filter "Name='_Total'").PercentProcessorTime"""
        # print(self.sess.run_ps( """Get-WmiObject -Query "Select InterruptsPersec from Win32_PerfFormattedData_PerfOS_Processor where Name='_Total'" """).std_out.decode('utf8'))
        return int(self.sess.run_ps(ps_script).std_out)  # .decode('utf8')

    def _get_perf_metrics(self):
        ps_script = """$ProcessorPercentage = (Get-WmiObject Win32_PerfFormattedData_PerfOS_Processor  -filter "Name='_Total'").PercentProcessorTime
        
        $AvailableMBytes = (Get-WmiObject Win32_PerfFormattedData_PerfOS_Memory).AvailableMBytes
        
        $PercentFreeSpaceDiskC = (Get-WmiObject Win32_PerfFormattedData_PerfDisk_LogicalDisk -filter "Name='C:'").PercentFreeSpace
        $PercentFreeSpaceDiskD = (Get-WmiObject Win32_PerfFormattedData_PerfDisk_LogicalDisk -filter "Name='D:'").PercentFreeSpace
        
        $DiskTransfersPersecDiskC = (Get-WmiObject Win32_PerfFormattedData_PerfDisk_LogicalDisk -filter "Name='C:'").DiskTransfersPersec
        $DiskTransfersPersecDiskD = (Get-WmiObject Win32_PerfFormattedData_PerfDisk_LogicalDisk -filter "Name='D:'").DiskTransfersPersec
                
        $NetBytesTotalPersec = (Get-WmiObject Win32_PerfFormattedData_Tcpip_NetworkInterface )[0].BytesTotalPersec
        
        Write-Output "$ProcessorPercentage" '|' $AvailableMBytes '|'  $PercentFreeSpaceDiskC '|'  $PercentFreeSpaceDiskD '|'  $DiskTransfersPersecDiskC '|'  $DiskTransfersPersecDiskD  '|' $NetBytesTotalPersec"""
        # print(self.sess.run_ps( """Get-WmiObject -Query "Select InterruptsPersec from Win32_PerfFormattedData_PerfOS_Processor where Name='_Total'" """).std_out.decode('utf8'))
        return [int(x) if x else None for x in (_.strip() for _ in self.sess.run_ps(ps_script).std_out.decode('ascii').split('|'))]

    def service(self):  # monitor and report if service is NOT running
        # (get-service "themes").Status

        # WMIC Service WHERE "Name = 'SericeName'" GET Started
        # or WMIC Service WHERE "Name = 'ServiceName'" GET ProcessId (ProcessId will be zero if service isn't started)

        # for non-English Windozws? TODO
        #
        # call wmic /locale:ms_409 service where (name="wsearch") get state /value | findstr State=Running
        # if %ErrorLevel% EQU 0 (
        #     echo Running
        # ) else (
        #     echo Not running
        # )
        data = self.sess.run_cmd("sc", ('query', self.svc)).std_out.decode('ascii').strip().split('\r\n')
        is_running = 'RUNNING' in data[2].split(': ')[1]
        return is_running

    def run_cmd(self, command, command_args):
        result = self.sess.run_cmd(command, args=command_args)
        return dict(std_out=result.std_out.decode('utf8'), std_err=result.std_err.decode('utf8'), status_code=result.status_code)

    def run_cmd_stdout(self, command, command_args):
        return self.sess.run_cmd(command, args=command_args).std_out.decode('utf8')

    def run_ps(self, script):
        result = self.sess.run_ps(script)
        return dict(std_out=result.std_out.decode('utf8'), std_err=result.std_err.decode('utf8'), status_code=result.status_code)


if __name__ == '__main__':
    logging.getLogger('').setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('\n%(levelname)-8s %(name)-12s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    c = Core(
        {'host': '10.0.0.157', 'ssl': '1', 'winuser': 'root', 'port': '',
         'winuserpass': 'root'}
        )
    c.pre_action()
    c.pp(c.list_services())
    0 / 0

    # print(c.sess.run_cmd('whoami').std_out.decode('ascii'))
    # log.warning('querying....')
    # c.pp(c.list_event_logs())
    #
    # 0/0

    # s = winrm.Session('10.0.0.130', auth=('root', 'root'))
    # s = winrm.Session('10.211.55.7', auth=('root', 'root'))
    s = winrm.Session('10.0.0.157', auth=('root', 'root'), transport='ssl', server_cert_validation='ignore')
    # r = s.run_cmd('ipconfig', ['/all'])
    # print(r.status_code)
    #
    # print(r.std_out.decode('utf8'))
    # print()
    # print(str(r.std_err))
    ########
    r = s.run_cmd('whoami')

    print(r.std_out.decode('utf8'))

    while 1:
        cmd = input('PS> ')
        command_args = cmd.split(' ')
        print((command_args[0], command_args[1:]))
        r = s.run_cmd(command_args[0], command_args[1:])
        print(r.status_code)
        print(r.std_out.decode('utf8'))
