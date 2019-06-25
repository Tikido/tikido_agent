meta = dict(
    name='winrm',
)


# https://support.microsoft.com/en-us/help/2019527/how-to-configure-winrm-for-https

# @ windows host run:


####Enable WinRM over HTTP and HTTPS with self-signed certificate (includes firewall rules):
## from powershell:
#    Invoke-Expression ((New-Object System.Net.Webclient).DownloadString('https://raw.githubusercontent.com/ansible/ansible/devel/examples/scripts/ConfigureRemotingForAnsible.ps1'))


# or


# winrm quickconfig -transport:https

# winrm set winrm/config/service/Auth '@{Basic="true"}'
# winrm set winrm/config/service '@{AllowUnencrypted="true"}'
# winrm set winrm/config/winrs '@{MaxMemoryPerShellMB="1024"}'
# ??? Set-WSManInstance WinRM/Config/Client -ValueSet @{TrustedHosts="*"}


# ports 5985 / 5986-SSL

# useful commands: https://blogs.technet.microsoft.com/jonjor/2009/01/09/winrm-windows-remote-management-troubleshooting/

##### SSL SELF-SIGNED CERT (for fresh windozes) http://www.joseph-streeter.com/?p=1086
# New-SelfSignedCertificate -DnsName comp-name.domain.tdl -CertStoreLocation Cert:\LocalMachine\My
# (aka)    $Cert = New-SelfSignedCertificate -CertstoreLocation Cert:\LocalMachine\My -DnsName "myHost"
# winrm create winrm/config/listener?Address=*+Transport=HTTPS '@{Hostname="comp-name.domain.tdl";CertificateThumbprint="65C6C9F1B062FE48E53687AA226F6FF1655AFBCC";port="5986"}'
# New-NetFirewallRule -DisplayName "Windows Remote Management (HTTPS-In)" -Name "Windows Remote Management (HTTPS-In)" -Profile Any -LocalPort 5986 -Protocol TCP

### check it
# winrs -r:https://HOSTNAME:5986 -u:user_name -p:password hostname
# Invoke-Command -ComputerName HOSTNAME -Port 5986 -Credential (Get-Credential) -UseSSL -SessionOption (New-PSSessionOption -SkipCACheck -SkipCNCheck) -ScriptBlock { Write-Host "Hello from $($env:ComputerName)" }






#             https://msdn.microsoft.com/en-us/powershell/reference/5.1/microsoft.powershell.management/get-eventlog
# Event Logs  http://www.windowsnetworking.com/kbase/WindowsTips/WindowsServer2008/AdminTips/Admin/Managingeventlogsfromthecommandline.html
#        Win CORE doesn't support powershell - so use
#           Wevtutil qe Security /rd:true /f:text /q:     *\[Security\[(EventID=4780)\]\]
