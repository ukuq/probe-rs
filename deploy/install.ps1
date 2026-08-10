#Requires -Version 5.1
#Requires -RunAsAdministrator

<#
.SYNOPSIS
Installs probe-rs as a SYSTEM startup task with an interactive tray companion.

.EXAMPLE
.\install.ps1
.\install.ps1 install -BinaryPath C:\path\to\probe-rs.exe
.\install.ps1 status
.\install.ps1 uninstall
.\install.ps1 uninstall -Purge
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("install", "uninstall", "start", "stop", "status")]
    [string]$Action = "install",

    [string]$BinaryPath = (Join-Path $PSScriptRoot "..\target\release\probe-rs.exe"),

    [switch]$NoStart,

    [switch]$Purge
)

$ErrorActionPreference = "Stop"
$TaskName = "probe-rs"
$InstallDir = Join-Path ([Environment]::GetFolderPath("ProgramFiles")) "probe-rs"
$DataDir = Join-Path ([Environment]::GetFolderPath("CommonApplicationData")) "probe-rs"
$InstalledBinary = Join-Path $InstallDir "probe-rs.exe"
$InstalledTrayBinary = Join-Path $InstallDir "probe-rs-tray.exe"
$ConfigPath = Join-Path $DataDir "config.toml"
$NetStaticPath = Join-Path $DataDir "net_static.json"
$ExampleConfig = Join-Path $PSScriptRoot "..\config.example.toml"
$TrayShortcut = Join-Path ([Environment]::GetFolderPath("CommonStartup")) "probe-rs-tray.lnk"
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

if ($Purge -and $Action -ne "uninstall") {
    throw "-Purge can only be used with uninstall."
}
if ($NoStart -and $Action -ne "install") {
    throw "-NoStart can only be used with install."
}

function Get-ProbeTask {
    Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
}

function Test-PlaceholderConfig {
    param([string]$Path)

    $text = [IO.File]::ReadAllText($Path)
    return $text -match '(?m)^server_id\s*=\s*"cf-server-uuid"\s*$' -or
        $text -match '(?m)^worker_url\s*=\s*"https://monitor\.example\.com/update"\s*$' -or
        $text -match '(?m)^worker_url\s*=\s*"https://komari\.example\.com"\s*$' -or
        $text -match '(?m)^worker_url\s*=\s*"https://monitor\.example\.com/report"\s*$'
}

function Set-ProtectedPathAcl {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [bool]$IsContainer
    )

    $acl = Get-Acl -LiteralPath $Path
    $acl.SetAccessRuleProtection($true, $false)
    foreach ($rule in @($acl.Access)) {
        [void]$acl.RemoveAccessRuleSpecific($rule)
    }

    $inheritance = [Security.AccessControl.InheritanceFlags]::None
    if ($IsContainer) {
        $inheritance = [Security.AccessControl.InheritanceFlags]::ContainerInherit -bor `
            [Security.AccessControl.InheritanceFlags]::ObjectInherit
    }
    foreach ($sidValue in @('S-1-5-18', 'S-1-5-32-544')) {
        $sid = New-Object Security.Principal.SecurityIdentifier($sidValue)
        $rule = New-Object Security.AccessControl.FileSystemAccessRule(
            $sid,
            [Security.AccessControl.FileSystemRights]::FullControl,
            $inheritance,
            [Security.AccessControl.PropagationFlags]::None,
            [Security.AccessControl.AccessControlType]::Allow
        )
        [void]$acl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $Path -AclObject $acl
}

function Protect-DataDirectory {
    # Config contains the reporting secret. Directories need inheritable rules,
    # while files need rules that apply to the file itself. Applying (OI)(CI)
    # recursively with icacls can leave an existing file with an empty DACL.
    Set-ProtectedPathAcl -Path $DataDir -IsContainer $true
    Get-ChildItem -LiteralPath $DataDir -Recurse -Force | ForEach-Object {
        Set-ProtectedPathAcl -Path $_.FullName -IsContainer $_.PSIsContainer
    }
}

function Get-TrayProcesses {
    $processes = @(
        Get-CimInstance Win32_Process -Filter "Name='probe-rs.exe'" -ErrorAction SilentlyContinue
        Get-CimInstance Win32_Process -Filter "Name='probe-rs-tray.exe'" -ErrorAction SilentlyContinue
    )
    @($processes | Where-Object {
            $_.ExecutablePath -in @($InstalledBinary, $InstalledTrayBinary) -and
            $_.CommandLine -match '(?i)(?:^|\s|")--tray(?:\s|$|")'
        })
}

function Stop-Tray {
    foreach ($process in @(Get-TrayProcesses)) {
        Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Install-TrayStartup {
    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($TrayShortcut)
    $shortcut.TargetPath = $InstalledTrayBinary
    $shortcut.Arguments = "--tray"
    $shortcut.WorkingDirectory = $InstallDir
    $shortcut.IconLocation = $InstalledBinary
    $shortcut.Description = "probe-rs notification area companion"
    $shortcut.WindowStyle = 7
    $shortcut.Save()
}

function Start-Tray {
    if (-not (Test-Path -LiteralPath $InstalledTrayBinary -PathType Leaf)) {
        return
    }
    if (@(Get-TrayProcesses).Count -eq 0) {
        Start-Process `
            -FilePath $InstalledTrayBinary `
            -ArgumentList "--tray" `
            -WorkingDirectory $InstallDir `
            -WindowStyle Hidden
    }
}

function Write-InitialConfig {
    if (Test-Path -LiteralPath $ExampleConfig -PathType Leaf) {
        $content = [IO.File]::ReadAllText((Resolve-Path -LiteralPath $ExampleConfig).Path)
        $replacement = "net_static_path = '$NetStaticPath'"
        $content = [Text.RegularExpressions.Regex]::Replace(
            $content,
            '(?m)^net_static_path\s*=.*$',
            $replacement
        )
    }
    else {
        $content = @"
net_static_path = '$NetStaticPath'

[[reporters]]
id = "primary"
protocol = "probe"
server_id = "srv-01"
secret = "change-me"
worker_url = "https://monitor.example.com/report"
report_interval = 60
reset_day = 1
interfaces = []
disks = []
report_gpu = false
report_errors = true
report_self = false

[reporters.intervals]
collect = 10
ping = 30
slow = 60
gpu = 60
ip = 600
diskio = 10
"@
    }
    [IO.File]::WriteAllText($ConfigPath, $content, $Utf8NoBom)
}

function Install-Probe {
    $source = Resolve-Path -LiteralPath $BinaryPath -ErrorAction SilentlyContinue
    if (-not $source -or -not (Test-Path -LiteralPath $source.Path -PathType Leaf)) {
        throw "Binary not found: $BinaryPath. Run 'cargo build --release' first or pass -BinaryPath."
    }

    $existingTask = Get-ProbeTask
    if ($existingTask) {
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    }
    Stop-Tray

    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
    Protect-DataDirectory
    Copy-Item -LiteralPath $source.Path -Destination $InstalledBinary -Force
    # Keep the interactive tray in a separate image file. This prevents a
    # signed-in user's tray process from locking the agent during self-update.
    Copy-Item -LiteralPath $source.Path -Destination $InstalledTrayBinary -Force

    $newConfig = -not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)
    if ($newConfig) {
        Write-InitialConfig
    }
    else {
        Write-Host "Keeping existing config: $ConfigPath"
    }

    $taskAction = New-ScheduledTaskAction `
        -Execute $InstalledBinary `
        -Argument ('--config "{0}"' -f $ConfigPath) `
        -WorkingDirectory $InstallDir
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $principal = New-ScheduledTaskPrincipal `
        -UserId "SYSTEM" `
        -LogonType ServiceAccount `
        -RunLevel Highest
    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -RestartCount 999 `
        -RestartInterval (New-TimeSpan -Minutes 1) `
        -ExecutionTimeLimit ([TimeSpan]::Zero)
    $task = New-ScheduledTask `
        -Action $taskAction `
        -Trigger $trigger `
        -Principal $principal `
        -Settings $settings `
        -Description "probe-rs server monitoring agent"
    Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force | Out-Null
    Install-TrayStartup

    if ($NoStart) {
        Disable-ScheduledTask -TaskName $TaskName | Out-Null
        Write-Host "Installed without starting."
        return
    }

    if (Test-PlaceholderConfig $ConfigPath) {
        Disable-ScheduledTask -TaskName $TaskName | Out-Null
        Write-Host "Installed, but the sample config is still in use."
        Write-Host "Edit: $ConfigPath"
        Write-Host "Then run: .\deploy\install.ps1 start"
        return
    }

    Enable-ScheduledTask -TaskName $TaskName | Out-Null
    Start-ScheduledTask -TaskName $TaskName
    Start-Tray
    Write-Host "Installed and started. Check status with: .\deploy\install.ps1 status"
}

function Uninstall-Probe {
    $task = Get-ProbeTask
    if ($task) {
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }
    Stop-Tray
    if (Test-Path -LiteralPath $TrayShortcut) {
        Remove-Item -LiteralPath $TrayShortcut -Force
    }
    if (Test-Path -LiteralPath $InstalledBinary) {
        Remove-Item -LiteralPath $InstalledBinary -Force
    }
    if (Test-Path -LiteralPath $InstalledTrayBinary) {
        Remove-Item -LiteralPath $InstalledTrayBinary -Force
    }
    if ((Test-Path -LiteralPath $InstallDir) -and
        -not (Get-ChildItem -LiteralPath $InstallDir -Force | Select-Object -First 1)) {
        Remove-Item -LiteralPath $InstallDir -Force
    }

    if ($Purge) {
        if (Test-Path -LiteralPath $DataDir) {
            Remove-Item -LiteralPath $DataDir -Recurse -Force
        }
        Write-Host "Uninstalled and removed config/data from $DataDir"
    }
    else {
        Write-Host "Uninstalled. Config/data kept at $DataDir (use -Purge to remove them)."
    }
}

function Start-Probe {
    if (-not (Get-ProbeTask)) {
        throw "Scheduled task '$TaskName' is not installed."
    }
    if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
        throw "Config not found: $ConfigPath"
    }
    if (Test-PlaceholderConfig $ConfigPath) {
        throw "Edit the placeholder config before starting: $ConfigPath"
    }
    Protect-DataDirectory
    Enable-ScheduledTask -TaskName $TaskName | Out-Null
    Start-ScheduledTask -TaskName $TaskName
    Install-TrayStartup
    Start-Tray
    Write-Host "probe-rs started."
}

function Stop-Probe {
    if (-not (Get-ProbeTask)) {
        throw "Scheduled task '$TaskName' is not installed."
    }
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    Stop-Tray
    Write-Host "probe-rs stopped; it remains enabled for the next system startup."
}

function Show-ProbeStatus {
    $task = Get-ProbeTask
    if (-not $task) {
        Write-Host "probe-rs is not installed."
        return
    }
    $info = Get-ScheduledTaskInfo -TaskName $TaskName
    [PSCustomObject]@{
        TaskName       = $TaskName
        State          = $task.State
        Enabled        = $task.Settings.Enabled
        LastRunTime    = $info.LastRunTime
        LastTaskResult = $info.LastTaskResult
        NextRunTime    = $info.NextRunTime
        Binary         = $InstalledBinary
        TrayBinary     = $InstalledTrayBinary
        Config         = $ConfigPath
        TrayStartup    = $TrayShortcut
    } | Format-List
}

switch ($Action) {
    "install" { Install-Probe }
    "uninstall" { Uninstall-Probe }
    "start" { Start-Probe }
    "stop" { Stop-Probe }
    "status" { Show-ProbeStatus }
}
