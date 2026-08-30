#Requires -Version 5.1

<#
.SYNOPSIS
Installs or updates probe-rs in CF protocol mode on Windows.

.DESCRIPTION
This installer only manages probe-rs. It does not stop, migrate, or uninstall
the official cf-probe agent. User scope is the non-admin default; Machine scope
requires an elevated PowerShell and installs a SYSTEM scheduled task.

.EXAMPLE
.\cf-install.ps1 install -Id <UUID> -Secret <SECRET> -Url https://example.com/update

.EXAMPLE
.\cf-install.ps1 install -Scope Machine -Id <UUID> -Secret <SECRET> -Url https://example.com/update
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("install", "uninstall")]
    [string]$Action = "install",

    [ValidateSet("Machine", "User")]
    [string]$Scope = "User",

    [Alias("id")]
    [string]$ServerId,

    [string]$Secret,

    [Alias("url")]
    [string]$WorkerUrl,

    [Alias("collect_interval", "collect")]
    [ValidateRange(0, 2147483647)]
    [int]$CollectInterval = 0,

    [Alias("wss_report_interval", "wss-report-interval")]
    [ValidateRange(1, 5)]
    [int]$WssReportInterval = 2,

    [Alias("interval")]
    [ValidateRange(1, 2147483647)]
    [int]$ReportInterval = 60,

    [Alias("connection_mode", "connection-mode")]
    [ValidateSet("auto", "http")]
    [string]$ConnectionMode,

    [Alias("ping_mode", "ping-mode")]
    [ValidateSet("tcp", "icmp")]
    [string]$PingMode,

    [Alias("reset_day")]
    [ValidateRange(0, 31)]
    [int]$ResetDay = 1,

    [string]$Ct,
    [string]$Cu,
    [string]$Cm,
    [string]$Bd,

    [Alias("interface", "iface")]
    [string]$Interfaces,

    [Alias("auto_update", "auto-update")]
    [string]$AutoUpdate,

    [Alias("update_channel", "update-channel")]
    [ValidateSet("stable", "prerelease")]
    [string]$UpdateChannel,

    [Alias("update_repository", "update-repository")]
    [ValidatePattern('^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$')]
    [string]$UpdateRepository,

    [Alias("rx_correction")]
    [string]$RxCorrection,

    [Alias("tx_correction")]
    [string]$TxCorrection,

    [Alias("reporter_id", "reporter-id")]
    [ValidatePattern('^[A-Za-z0-9_.-]+$')]
    [string]$ReporterId = "cf",

    [Alias("no_start", "no-start")]
    [switch]$NoStart,

    [Alias("install_version", "install-version")]
    [string]$InstallVersion = "v0.1.4-beta.6",

    [Alias("install_ghproxy", "install-ghproxy")]
    [string]$InstallGhProxy,

    [Alias("bin")]
    [string]$BinarySource,

    [switch]$Purge
)

$ErrorActionPreference = "Stop"
$Installer = Join-Path $PSScriptRoot "install.ps1"
$detectedMachineTask = Get-ScheduledTask -TaskName "probe-rs" -ErrorAction SilentlyContinue
if (-not $PSBoundParameters.ContainsKey("Scope") -and $detectedMachineTask) {
    # Older cf-install.ps1 releases always used Machine scope. Preserve that
    # scope on an in-place update instead of starting a duplicate User agent.
    $Scope = "Machine"
    Write-Host "Existing Machine installation detected; keeping Machine scope."
}
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
$NetStaticPath = Join-Path $DataDir "net_static.json"
$AgentShortcut = Join-Path $StartupDir "probe-rs-agent.lnk"
$DefaultUpdateRepository = "ukuq/probe-rs"
$AssetName = "probe-rs-windows-x86_64.exe"

function ConvertTo-Boolean {
    param([string]$Value, [string]$Name)

    switch -Regex ($Value.Trim()) {
        '^(1|true|yes)$' { return $true }
        '^(0|false|no|)$' { return $false }
        default { throw "$Name must be 0 or 1." }
    }
}

function Get-ShortcutArguments {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return ""
    }
    try {
        $shell = New-Object -ComObject WScript.Shell
        return [string]$shell.CreateShortcut($Path).Arguments
    }
    catch {
        Write-Warning "Could not inspect existing startup shortcut: $($_.Exception.Message)"
        return ""
    }
}

function Get-UserAgentProcesses {
    if ($IsMachine) {
        return @()
    }
    @(
        Get-Process -Name "probe-rs" -ErrorAction SilentlyContinue |
            Where-Object { $_.Path -eq $InstalledBinary }
    )
}

if (-not (Test-Path -LiteralPath $Installer -PathType Leaf)) {
    throw "Windows installer not found: $Installer"
}
if ($IsMachine) {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Machine scope requires an elevated administrator PowerShell. Use -Scope User for a non-admin installation."
    }
}
if ($Purge -and $Action -ne "uninstall") {
    throw "-Purge can only be used with uninstall."
}
if ($Action -eq "uninstall") {
    & $Installer uninstall -Scope $Scope -Purge:$Purge
    exit
}

foreach ($requiredWhenPresent in @(
        @{ Parameter = "ServerId"; Flag = "-Id"; Value = $ServerId },
        @{ Parameter = "Secret"; Flag = "-Secret"; Value = $Secret },
        @{ Parameter = "WorkerUrl"; Flag = "-Url"; Value = $WorkerUrl }
    )) {
    if ($PSBoundParameters.ContainsKey($requiredWhenPresent.Parameter) -and
        [string]::IsNullOrWhiteSpace($requiredWhenPresent.Value)) {
        throw "$($requiredWhenPresent.Flag) must not be empty."
    }
}
if ($PSBoundParameters.ContainsKey("WorkerUrl")) {
    $parsedWorkerUrl = $null
    if (-not [Uri]::TryCreate($WorkerUrl, [UriKind]::Absolute, [ref]$parsedWorkerUrl) -or
        $parsedWorkerUrl.Scheme -notin @("http", "https")) {
        throw "-Url must be an absolute HTTP(S) URL."
    }
}
if ($PSBoundParameters.ContainsKey("InstallGhProxy")) {
    $parsedProxy = $null
    if ([string]::IsNullOrWhiteSpace($InstallGhProxy) -or
        -not [Uri]::TryCreate($InstallGhProxy, [UriKind]::Absolute, [ref]$parsedProxy) -or
        $parsedProxy.Scheme -notin @("http", "https") -or
        -not [string]::IsNullOrEmpty($parsedProxy.UserInfo) -or
        -not [string]::IsNullOrEmpty($parsedProxy.Query) -or
        -not [string]::IsNullOrEmpty($parsedProxy.Fragment)) {
        throw "-InstallGhProxy must be an absolute HTTP(S) URL without credentials, query, or fragment."
    }
}
if ($PSBoundParameters.ContainsKey("UpdateRepository")) {
    $repositoryParts = $UpdateRepository.Split('/')
    if ($repositoryParts.Count -ne 2 -or
        $repositoryParts[0] -in @('.', '..') -or
        $repositoryParts[1] -in @('.', '..')) {
        throw "-UpdateRepository must use owner/repo."
    }
}
$downloadRepository = if ($PSBoundParameters.ContainsKey("UpdateRepository")) {
    $UpdateRepository
}
else {
    $DefaultUpdateRepository
}
$GitHubRepo = "https://github.com/$downloadRepository"

$noStartEnabled = [bool]$NoStart
$autoUpdateEnabled = $null
if ($PSBoundParameters.ContainsKey("AutoUpdate")) {
    $autoUpdateEnabled = ConvertTo-Boolean $AutoUpdate "-AutoUpdate"
}

# CmdletBinding supplies the common -Debug switch. Reuse it as the agent's
# persistent debug-log option, and preserve an existing --debug task/shortcut
# setting when this installation does not explicitly mention -Debug.
$debugSpecified = $PSBoundParameters.ContainsKey("Debug")
$debugEnabled = $debugSpecified -and [bool]$PSBoundParameters["Debug"]
$existingTask = if ($IsMachine) {
    $detectedMachineTask
}
else {
    $null
}
if (-not $debugSpecified) {
    if ($existingTask -and $existingTask.Actions.Arguments -match '(?:^|\s)--debug(?:\s|$)') {
        $debugEnabled = $true
    }
    elseif (-not $IsMachine -and
        (Get-ShortcutArguments $AgentShortcut) -match '(?:^|\s)--debug(?:\s|$)') {
        $debugEnabled = $true
    }
}
$previousTaskEnabled = $false
$previousTaskRunning = $false
$previousUserEnabled = $false
$previousUserRunning = $false
$scopeStateNeedsRestore = $false

$temporaryBinary = $null
$temporaryChecksums = $null
try {
    $verifyReleaseAsset = $false
    $proxyBinarySource = $null
    $proxyChecksumSource = $null
    if ([string]::IsNullOrWhiteSpace($BinarySource)) {
        if ([string]::IsNullOrWhiteSpace($InstallVersion)) {
            throw "-InstallVersion must not be empty."
        }
        if ($InstallVersion -eq "latest") {
            $releaseBase = "$GitHubRepo/releases/latest/download"
        }
        else {
            $tag = if ($InstallVersion.StartsWith("v")) {
                $InstallVersion
            }
            else {
                "v$InstallVersion"
            }
            if ($tag -notmatch '^v[A-Za-z0-9._-]+$') {
                throw "Invalid -InstallVersion: $InstallVersion"
            }
            $releaseBase = "$GitHubRepo/releases/download/$tag"
        }
        $BinarySource = "$releaseBase/$AssetName"
        $checksumSource = "$releaseBase/SHA256SUMS"
        if (-not [string]::IsNullOrWhiteSpace($InstallGhProxy)) {
            $prefix = $InstallGhProxy.TrimEnd('/')
            $proxyBinarySource = "$prefix/$BinarySource"
            $proxyChecksumSource = "$prefix/$checksumSource"
        }
        $verifyReleaseAsset = $true
    }

    if (Test-Path -LiteralPath $BinarySource -PathType Leaf) {
        $resolvedBinary = (Resolve-Path -LiteralPath $BinarySource).Path
    }
    else {
        $binaryUri = $null
        if (-not [Uri]::TryCreate($BinarySource, [UriKind]::Absolute, [ref]$binaryUri) -or
            $binaryUri.Scheme -notin @("http", "https")) {
            throw "Binary source is neither a local file nor an HTTP(S) URL: $BinarySource"
        }
        $temporaryBinary = Join-Path ([IO.Path]::GetTempPath()) (
            "probe-rs-{0}.exe" -f [Guid]::NewGuid().ToString("N")
        )
        Write-Host "Downloading binary: $BinarySource"
        try {
            Invoke-WebRequest -Uri $binaryUri -OutFile $temporaryBinary -UseBasicParsing
        }
        catch {
            if ([string]::IsNullOrWhiteSpace($proxyBinarySource)) { throw }
            Write-Warning "Direct binary download failed; trying proxy $InstallGhProxy"
            Invoke-WebRequest -Uri $proxyBinarySource -OutFile $temporaryBinary -UseBasicParsing
        }
        $resolvedBinary = $temporaryBinary

        if ($verifyReleaseAsset) {
            $temporaryChecksums = Join-Path ([IO.Path]::GetTempPath()) (
                "probe-rs-sha-{0}.txt" -f [Guid]::NewGuid().ToString("N")
            )
            if (-not [string]::IsNullOrWhiteSpace($proxyChecksumSource)) {
                # Fetch the checksum directly from GitHub when possible: if the
                # proxy delivers both binary and checksum, a compromised proxy
                # can replace both and the check only catches transfer damage.
                try {
                    Invoke-WebRequest -Uri $checksumSource -OutFile $temporaryChecksums -UseBasicParsing
                }
                catch {
                    Write-Warning (
                        "Direct SHA256SUMS download failed; falling back to proxy " +
                        "(checksum then only detects transfer corruption, not origin)."
                    )
                    Invoke-WebRequest -Uri $proxyChecksumSource -OutFile $temporaryChecksums -UseBasicParsing
                }
            }
            else {
                Invoke-WebRequest -Uri $checksumSource -OutFile $temporaryChecksums -UseBasicParsing
            }
            $escapedAsset = [regex]::Escape($AssetName)
            $checksumText = [IO.File]::ReadAllText($temporaryChecksums)
            $match = [regex]::Match(
                $checksumText,
                "(?mi)^([a-f0-9]{64})\s+\*?$escapedAsset\s*$"
            )
            if (-not $match.Success) {
                throw "SHA256SUMS does not contain $AssetName."
            }
            $actualHash = (Get-FileHash -LiteralPath $resolvedBinary -Algorithm SHA256).Hash
            if ($actualHash -ne $match.Groups[1].Value) {
                throw "Binary checksum mismatch."
            }
        }
    }

    if (Get-Process -Name "cf-probe" -ErrorAction SilentlyContinue) {
        Write-Warning (
            "Official cf-probe is still running. Using the same credentials " +
            "will duplicate reports; this installer will not stop or remove it."
        )
    }

    # install.ps1 -NoStart stops the selected scope. From this point onward,
    # any configuration error must restore its previous enabled/running state.
    if ($IsMachine) {
        $taskBeforeInstall = Get-ScheduledTask -TaskName "probe-rs" -ErrorAction SilentlyContinue
        $previousTaskEnabled = $taskBeforeInstall -and [bool]$taskBeforeInstall.Settings.Enabled
        $previousTaskRunning = $taskBeforeInstall -and ([string]$taskBeforeInstall.State -eq "Running")
        $scopeStateNeedsRestore = [bool]$taskBeforeInstall
    }
    else {
        $previousUserEnabled = Test-Path -LiteralPath $AgentShortcut -PathType Leaf
        $previousUserRunning = @(Get-UserAgentProcesses).Count -gt 0
        $scopeStateNeedsRestore = $previousUserEnabled -or $previousUserRunning
    }
    & $Installer install -Scope $Scope -BinaryPath $resolvedBinary -NoStart -DebugLog:$debugEnabled

    $configureArgs = @(
        "configure-cf",
        "--config", $ConfigPath,
        "--net-static-path", $NetStaticPath,
        "--reporter-id", $ReporterId
    )
    if ($PSBoundParameters.ContainsKey("ServerId")) {
        $configureArgs += @("--server-id", $ServerId)
    }
    if ($PSBoundParameters.ContainsKey("Secret")) {
        $configureArgs += @("--secret", $Secret)
    }
    if ($PSBoundParameters.ContainsKey("WorkerUrl")) {
        $configureArgs += @("--url", $WorkerUrl)
    }
    if ($PSBoundParameters.ContainsKey("CollectInterval")) {
        $configureArgs += @("--collect", $CollectInterval.ToString())
    }
    if ($PSBoundParameters.ContainsKey("WssReportInterval")) {
        $configureArgs += @("--wss-report-interval", $WssReportInterval.ToString())
    }
    if ($PSBoundParameters.ContainsKey("ReportInterval")) {
        $configureArgs += @("--report-interval", $ReportInterval.ToString())
    }
    if ($PSBoundParameters.ContainsKey("ConnectionMode")) {
        $configureArgs += @("--connection-mode", $ConnectionMode.ToLowerInvariant())
    }
    if ($PSBoundParameters.ContainsKey("PingMode")) {
        $configureArgs += @("--ping-mode", $PingMode.ToLowerInvariant())
    }
    if ($PSBoundParameters.ContainsKey("ResetDay")) {
        $configureArgs += @("--reset-day", $ResetDay.ToString())
    }
    if ($PSBoundParameters.ContainsKey("Interfaces")) {
        $configureArgs += @("--interfaces", $Interfaces)
    }
    foreach ($probe in @(
            @{ Parameter = "Ct"; Flag = "--ct"; Value = $Ct },
            @{ Parameter = "Cu"; Flag = "--cu"; Value = $Cu },
            @{ Parameter = "Cm"; Flag = "--cm"; Value = $Cm },
            @{ Parameter = "Bd"; Flag = "--bd"; Value = $Bd }
        )) {
        if ($PSBoundParameters.ContainsKey($probe.Parameter)) {
            $configureArgs += @($probe.Flag, $probe.Value)
        }
    }
    if ($PSBoundParameters.ContainsKey("AutoUpdate")) {
        $configureArgs += @("--auto-update", $autoUpdateEnabled.ToString().ToLowerInvariant())
    }
    if ($PSBoundParameters.ContainsKey("UpdateRepository")) {
        $configureArgs += @("--update-repository", $UpdateRepository)
    }
    if ($PSBoundParameters.ContainsKey("UpdateChannel")) {
        $configureArgs += @("--update-channel", $UpdateChannel)
    }
    if ($PSBoundParameters.ContainsKey("InstallGhProxy")) {
        $configureArgs += @("--update-proxy", $InstallGhProxy)
    }

    $selectedOutput = @(& $InstalledBinary @configureArgs)
    if ($LASTEXITCODE -ne 0) {
        throw "CF configuration failed with exit code $LASTEXITCODE."
    }
    $selectedReporter = [string]($selectedOutput | Select-Object -Last 1)
    $selectedReporter = $selectedReporter.Trim()
    if ([string]::IsNullOrWhiteSpace($selectedReporter)) {
        throw "CF Reporter selection failed."
    }

    if ($PSBoundParameters.ContainsKey("RxCorrection") -or
        $PSBoundParameters.ContainsKey("TxCorrection")) {
        $correctionArgs = @(
            "set-traffic-correction",
            "--config", $ConfigPath,
            "--reporter-id", $selectedReporter
        )
        if ($PSBoundParameters.ContainsKey("RxCorrection")) {
            $correctionArgs += @("--rx-gib", $RxCorrection)
        }
        if ($PSBoundParameters.ContainsKey("TxCorrection")) {
            $correctionArgs += @("--tx-gib", $TxCorrection)
        }
        & $InstalledBinary @correctionArgs
        if ($LASTEXITCODE -ne 0) {
            throw "Traffic correction failed with exit code $LASTEXITCODE."
        }
        Write-Host "Applied local traffic correction to Reporter '$selectedReporter'."
    }

    if ($noStartEnabled) {
        Write-Host "CF mode installed without starting. Reporter: $selectedReporter"
    }
    else {
        & $Installer start -Scope $Scope -DebugLog:$debugEnabled
        Write-Host "CF mode installed and started. Reporter: $selectedReporter"
    }
    Write-Host "Config: $ConfigPath"
    $scopeStateNeedsRestore = $false
}
catch {
    $installError = $_
    if ($scopeStateNeedsRestore) {
        try {
            if ($IsMachine) {
                $restoredTask = Get-ScheduledTask -TaskName "probe-rs" -ErrorAction Stop
                if ($previousTaskRunning) {
                    Enable-ScheduledTask -TaskName "probe-rs" | Out-Null
                    $runningRestored = $false
                    for ($attempt = 0; $attempt -lt 15; $attempt++) {
                        Start-ScheduledTask -TaskName "probe-rs" -ErrorAction SilentlyContinue
                        Start-Sleep -Milliseconds 200
                        $restoredTask = Get-ScheduledTask -TaskName "probe-rs" -ErrorAction Stop
                        if ([string]$restoredTask.State -eq "Running") {
                            $runningRestored = $true
                            break
                        }
                    }
                    if (-not $runningRestored) {
                        throw "The probe-rs task did not return to the Running state."
                    }
                }
                else {
                    Stop-ScheduledTask -TaskName "probe-rs" -ErrorAction SilentlyContinue
                }
                if ($previousTaskEnabled) {
                    Enable-ScheduledTask -TaskName "probe-rs" | Out-Null
                }
                else {
                    Disable-ScheduledTask -TaskName "probe-rs" | Out-Null
                }
                Write-Warning "CF installation failed; restored the previous probe-rs Machine task state."
            }
            else {
                & $Installer start -Scope User -DebugLog:$debugEnabled
                if (-not $previousUserRunning) {
                    & $Installer stop -Scope User
                }
                if (-not $previousUserEnabled -and
                    (Test-Path -LiteralPath $AgentShortcut -PathType Leaf)) {
                    Remove-Item -LiteralPath $AgentShortcut -Force
                }
                Write-Warning "CF installation failed; restored the previous probe-rs User startup and running state."
            }
        }
        catch {
            Write-Warning "CF installation failed and the previous probe-rs $Scope state could not be restored: $($_.Exception.Message)"
        }
    }
    throw $installError
}
finally {
    foreach ($temporary in @($temporaryBinary, $temporaryChecksums)) {
        if ($temporary -and (Test-Path -LiteralPath $temporary)) {
            Remove-Item -LiteralPath $temporary -Force
        }
    }
}
