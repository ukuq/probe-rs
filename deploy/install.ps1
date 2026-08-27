#Requires -Version 5.1

<#
.SYNOPSIS
Installs probe-rs either as a machine-wide SYSTEM task or for the current user.

.EXAMPLE
.\install.ps1
.\install.ps1 install -BinaryPath C:\path\to\probe-rs.exe
.\install.ps1 install -Scope Machine -BinaryPath C:\path\to\probe-rs.exe
.\install.ps1 status
.\install.ps1 uninstall -Scope Machine
.\install.ps1 uninstall -Purge
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("install", "uninstall", "start", "stop", "status")]
    [string]$Action = "install",

    [string]$BinaryPath,

    [ValidateSet("Machine", "User")]
    [string]$Scope = "User",

    [switch]$NoStart,

    [switch]$DebugLog,

    [switch]$Purge
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($BinaryPath)) {
    $BinaryPath = Join-Path $PSScriptRoot "..\target\release\probe-rs.exe"
}
$TaskName = "probe-rs"
$SystemSid = "S-1-5-18"
$TaskTriggerEvent = 0
$TaskCreateOrUpdate = 6
$TaskLogonServiceAccount = 5
$IsMachine = $Scope -eq "Machine"
if ($IsMachine) {
    $InstallDir = Join-Path ([Environment]::GetFolderPath("ProgramFiles")) "probe-rs"
    $DataDir = Join-Path ([Environment]::GetFolderPath("CommonApplicationData")) "probe-rs"
    $ConfigPath = Join-Path $DataDir "config.toml"
    $StartupDir = [Environment]::GetFolderPath("CommonStartup")
}
else {
    $InstallDir = Join-Path ([Environment]::GetFolderPath("LocalApplicationData")) "probe-rs"
    $DataDir = Join-Path $InstallDir "data"
    $ConfigPath = Join-Path $InstallDir "config.toml"
    $StartupDir = [Environment]::GetFolderPath("Startup")
}
$InstalledBinary = Join-Path $InstallDir "probe-rs.exe"
$InstalledTrayBinary = Join-Path $InstallDir "probe-rs-tray.exe"
$ExampleConfig = Join-Path $PSScriptRoot "..\config.example.toml"
$TrayShortcut = Join-Path $StartupDir "probe-rs-tray.lnk"
$AgentShortcut = Join-Path $StartupDir "probe-rs-agent.lnk"
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
$isAdministrator = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if ($IsMachine -and -not $isAdministrator) {
    throw "Machine scope requires an elevated administrator PowerShell. Use -Scope User for a non-admin installation."
}

if ($Purge -and $Action -ne "uninstall") {
    throw "-Purge can only be used with uninstall."
}
if ($NoStart -and $Action -ne "install") {
    throw "-NoStart can only be used with install."
}

function Get-ProbeTask {
    if (-not $IsMachine) {
        return $null
    }
    Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
}

function Add-ProbeResumeTrigger {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    # ScheduledTasks has no cmdlet for event triggers. Update the registered
    # definition through the Task Scheduler API while preserving its action,
    # principal, restart policy, startup trigger, and logon trigger.
    $subscription = @'
<QueryList>
  <Query Id="0" Path="System">
    <Select Path="System">*[System[Provider[@Name='Microsoft-Windows-Power-Troubleshooter'] and EventID=1]]</Select>
    <Select Path="System">*[System[Provider[@Name='Microsoft-Windows-Kernel-Power'] and EventID=107]]</Select>
  </Query>
</QueryList>
'@
    $scheduler = New-Object -ComObject "Schedule.Service"
    $scheduler.Connect()
    $rootFolder = $scheduler.GetFolder("\")
    $registeredTask = $rootFolder.GetTask($Name)
    $definition = $registeredTask.Definition
    $eventTrigger = $definition.Triggers.Create($TaskTriggerEvent)
    $eventTrigger.Id = "ResumeFromSleep"
    $eventTrigger.Enabled = $true
    $eventTrigger.Delay = "PT10S"
    $eventTrigger.Subscription = $subscription.Trim()

    [void]$rootFolder.RegisterTaskDefinition(
        $Name,
        $definition,
        $TaskCreateOrUpdate,
        $SystemSid,
        $null,
        $TaskLogonServiceAccount,
        $null
    )
}

function Test-PlaceholderConfig {
    param([string]$Path)

    $text = [IO.File]::ReadAllText($Path)
    return $text -match '(?m)^server_id\s*=\s*"(srv-01|cf-server-uuid)"\s*$' -or
        $text -match '(?m)^url\s*=\s*"https://monitor\.example\.com/update"\s*$' -or
        $text -match '(?m)^endpoint\s*=\s*"https://komari\.example\.com"\s*$' -or
        $text -match '(?m)^worker_url\s*=\s*"https://monitor\.example\.com/(report|update)"\s*$' -or
        $text -match '(?m)^worker_url\s*=\s*"https://komari\.example\.com"\s*$'
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
    if (-not $IsMachine) {
        return
    }
    # Config contains the reporting secret. Directories need inheritable rules,
    # while files need rules that apply to the file itself. Applying (OI)(CI)
    # recursively with icacls can leave an existing file with an empty DACL.
    Set-ProtectedPathAcl -Path $DataDir -IsContainer $true
    Get-ChildItem -LiteralPath $DataDir -Recurse -Force | ForEach-Object {
        Set-ProtectedPathAcl -Path $_.FullName -IsContainer $_.PSIsContainer
    }
}

function Get-TrayProcesses {
    @(
        Get-Process -Name "probe-rs-tray" -ErrorAction SilentlyContinue |
            Where-Object { $_.Path -eq $InstalledTrayBinary }
    )
}

function Get-AgentProcesses {
    @(
        Get-Process -Name "probe-rs" -ErrorAction SilentlyContinue |
            Where-Object { $_.Path -eq $InstalledBinary }
    )
}

function Stop-ProbeProcesses {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [object[]]$Processes,

        [Parameter(Mandatory = $true)]
        [string]$Kind
    )

    $processIds = @($Processes | ForEach-Object { $_.Id })
    foreach ($processId in $processIds) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }

    $deadline = [DateTime]::UtcNow.AddSeconds(5)
    do {
        $remaining = @(
            $processIds | Where-Object {
                Get-Process -Id $_ -ErrorAction SilentlyContinue
            }
        )
        if ($remaining.Count -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 100
    } while ([DateTime]::UtcNow -lt $deadline)

    throw "Timed out waiting for $Kind process(es) to stop: $($remaining -join ', ')"
}

function Stop-Tray {
    Stop-ProbeProcesses -Processes @(Get-TrayProcesses) -Kind "tray"
}

function Stop-Agent {
    Stop-ProbeProcesses -Processes @(Get-AgentProcesses) -Kind "agent"
}

function Get-AgentArguments {
    $arguments = ('--config "{0}"' -f $ConfigPath)
    if (-not $IsMachine) {
        $arguments = "--user-mode $arguments"
    }
    if ($DebugLog) {
        $arguments += ' --debug'
    }
    return $arguments
}

function Get-TrayArguments {
    $arguments = ('--tray --config "{0}"' -f $ConfigPath)
    if (-not $IsMachine) {
        $arguments = "--tray --user-mode --config `"$ConfigPath`""
    }
    return $arguments
}

function Set-StartupShortcut {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$TargetPath,

        [Parameter(Mandatory = $true)]
        [string]$Arguments,

        [Parameter(Mandatory = $true)]
        [string]$Description
    )

    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($Path)
    $shortcut.TargetPath = $TargetPath
    $shortcut.Arguments = $Arguments
    $shortcut.WorkingDirectory = $InstallDir
    $shortcut.IconLocation = $InstalledBinary
    $shortcut.Description = $Description
    $shortcut.WindowStyle = 7
    $shortcut.Save()
    $saved = $shell.CreateShortcut($Path)
    if ($saved.TargetPath -ne $TargetPath) {
        throw "Failed to create startup shortcut target: $Path -> $TargetPath"
    }
}

function Install-TrayStartup {
    Set-StartupShortcut `
        -Path $TrayShortcut `
        -TargetPath $InstalledTrayBinary `
        -Arguments (Get-TrayArguments) `
        -Description "probe-rs notification area companion ($Scope)"
}

function Install-AgentStartup {
    if ($IsMachine) {
        return
    }
    Set-StartupShortcut `
        -Path $AgentShortcut `
        -TargetPath $InstalledBinary `
        -Arguments (Get-AgentArguments) `
        -Description "probe-rs monitoring agent (User)"
}

function Remove-AgentStartup {
    if (Test-Path -LiteralPath $AgentShortcut) {
        Remove-Item -LiteralPath $AgentShortcut -Force
    }
}

function Start-Tray {
    if (-not (Test-Path -LiteralPath $InstalledTrayBinary -PathType Leaf)) {
        return
    }
    if (@(Get-TrayProcesses).Count -eq 0) {
        Start-Process `
            -FilePath $InstalledTrayBinary `
            -ArgumentList (Get-TrayArguments) `
            -WorkingDirectory $InstallDir `
            -WindowStyle Hidden
    }
}

function Start-UserAgent {
    if ($IsMachine -or @(Get-AgentProcesses).Count -gt 0) {
        return
    }
    Start-Process `
        -FilePath $InstalledBinary `
        -ArgumentList (Get-AgentArguments) `
        -WorkingDirectory $InstallDir `
        -WindowStyle Hidden
}

function Write-InitialConfig {
    if (Test-Path -LiteralPath $ExampleConfig -PathType Leaf) {
        $content = [IO.File]::ReadAllText((Resolve-Path -LiteralPath $ExampleConfig).Path)
        $replacement = "data_dir = '$DataDir'"
        $content = [Text.RegularExpressions.Regex]::Replace(
            $content,
            '(?m)^data_dir\s*=.*$',
            $replacement
        )
    }
    else {
        $content = @"
schema = 1

data_dir = '$DataDir'

[auto_update]
enabled = false
channel = "stable"
check_interval = 21600
proxys = []

[[reporters]]
id = "primary"

[reporters.probe]
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

[reporters.probe.intervals]
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
    if (-not $IsMachine) {
        Stop-Agent
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

    if ($IsMachine) {
        $taskAction = New-ScheduledTaskAction `
            -Execute $InstalledBinary `
            -Argument (Get-AgentArguments) `
            -WorkingDirectory $InstallDir
        $triggers = @(
            New-ScheduledTaskTrigger -AtStartup
            # No -User means any interactive user logon; the task still runs as SYSTEM.
            New-ScheduledTaskTrigger -AtLogOn
        )
        $taskPrincipal = New-ScheduledTaskPrincipal `
            -UserId $SystemSid `
            -LogonType ServiceAccount `
            -RunLevel Highest
        $settings = New-ScheduledTaskSettingsSet `
            -AllowStartIfOnBatteries `
            -Disable `
            -DontStopIfGoingOnBatteries `
            -MultipleInstances IgnoreNew `
            -StartWhenAvailable `
            -RestartCount 999 `
            -RestartInterval (New-TimeSpan -Minutes 1) `
            -ExecutionTimeLimit ([TimeSpan]::Zero)
        $task = New-ScheduledTask `
            -Action $taskAction `
            -Trigger $triggers `
            -Principal $taskPrincipal `
            -Settings $settings `
            -Description "probe-rs server monitoring agent"
        Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force | Out-Null
        Add-ProbeResumeTrigger -Name $TaskName
    }
    Install-TrayStartup

    if ($NoStart) {
        if ($IsMachine) {
            Disable-ScheduledTask -TaskName $TaskName | Out-Null
        }
        else {
            Remove-AgentStartup
        }
        Write-Host "Installed without starting."
        return
    }

    if (Test-PlaceholderConfig $ConfigPath) {
        if ($IsMachine) {
            Disable-ScheduledTask -TaskName $TaskName | Out-Null
        }
        else {
            Remove-AgentStartup
        }
        Write-Host "Installed, but the sample config is still in use."
        Write-Host "Edit: $ConfigPath"
        Write-Host "Then run: .\deploy\install.ps1 start -Scope $Scope"
        return
    }

    if ($IsMachine) {
        Enable-ScheduledTask -TaskName $TaskName | Out-Null
        Start-ScheduledTask -TaskName $TaskName
    }
    else {
        Install-AgentStartup
        Start-UserAgent
    }
    Start-Tray
    Write-Host "Installed and started ($Scope). Check status with: .\deploy\install.ps1 status -Scope $Scope"
}

function Uninstall-Probe {
    $task = Get-ProbeTask
    if ($task) {
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }
    if (-not $IsMachine) {
        Stop-Agent
    }
    Stop-Tray
    if (Test-Path -LiteralPath $TrayShortcut) {
        Remove-Item -LiteralPath $TrayShortcut -Force
    }
    Remove-AgentStartup
    if (Test-Path -LiteralPath $InstalledBinary) {
        Remove-Item -LiteralPath $InstalledBinary -Force
    }
    if (Test-Path -LiteralPath $InstalledTrayBinary) {
        Remove-Item -LiteralPath $InstalledTrayBinary -Force
    }
    if ($Purge) {
        if (-not $IsMachine -and (Test-Path -LiteralPath $InstallDir)) {
            Remove-Item -LiteralPath $InstallDir -Recurse -Force
        }
        elseif (Test-Path -LiteralPath $DataDir) {
            Remove-Item -LiteralPath $DataDir -Recurse -Force
        }
        Write-Host "Uninstalled and removed config/data ($Scope)."
    }
    else {
        Write-Host "Uninstalled ($Scope). Config/data kept (use -Purge to remove them)."
    }
    if ((Test-Path -LiteralPath $InstallDir) -and
        -not (Get-ChildItem -LiteralPath $InstallDir -Force | Select-Object -First 1)) {
        Remove-Item -LiteralPath $InstallDir -Force
    }
}

function Start-Probe {
    if ($IsMachine -and -not (Get-ProbeTask)) {
        throw "Scheduled task '$TaskName' is not installed."
    }
    if (-not $IsMachine -and -not (Test-Path -LiteralPath $InstalledBinary -PathType Leaf)) {
        throw "User installation not found: $InstalledBinary"
    }
    if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
        throw "Config not found: $ConfigPath"
    }
    if (Test-PlaceholderConfig $ConfigPath) {
        throw "Edit the placeholder config before starting: $ConfigPath"
    }
    Protect-DataDirectory
    if ($IsMachine) {
        Enable-ScheduledTask -TaskName $TaskName | Out-Null
        Start-ScheduledTask -TaskName $TaskName
    }
    else {
        Install-AgentStartup
        Start-UserAgent
    }
    Install-TrayStartup
    Start-Tray
    Write-Host "probe-rs started ($Scope)."
}

function Stop-Probe {
    if ($IsMachine -and -not (Get-ProbeTask)) {
        throw "Scheduled task '$TaskName' is not installed."
    }
    if ($IsMachine) {
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    }
    else {
        Stop-Agent
    }
    Stop-Tray
    Write-Host "probe-rs stopped ($Scope); it remains enabled for the next applicable startup."
}

function Show-ProbeStatus {
    if ($IsMachine) {
        $task = Get-ProbeTask
        if (-not $task) {
            Write-Host "probe-rs is not installed (Machine)."
            return
        }
        $info = Get-ScheduledTaskInfo -TaskName $TaskName
        $state = $task.State
        $enabled = $task.Settings.Enabled
        $lastRunTime = $info.LastRunTime
        $lastTaskResult = $info.LastTaskResult
        $nextRunTime = $info.NextRunTime
    }
    else {
        if (-not (Test-Path -LiteralPath $InstalledBinary -PathType Leaf)) {
            Write-Host "probe-rs is not installed (User)."
            return
        }
        $agentProcesses = @(Get-AgentProcesses)
        $state = if ($agentProcesses.Count -gt 0) { "Running" } else { "Stopped" }
        $enabled = Test-Path -LiteralPath $AgentShortcut
        $lastRunTime = $null
        $lastTaskResult = $null
        $nextRunTime = $null
    }
    [PSCustomObject]@{
        Scope          = $Scope
        TaskName       = if ($IsMachine) { $TaskName } else { $null }
        State          = $state
        Enabled        = $enabled
        LastRunTime    = $lastRunTime
        LastTaskResult = $lastTaskResult
        NextRunTime    = $nextRunTime
        Binary         = $InstalledBinary
        TrayBinary     = $InstalledTrayBinary
        Config         = $ConfigPath
        Data            = $DataDir
        AgentStartup    = if ($IsMachine) { $null } else { $AgentShortcut }
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
