#Requires -Version 5.1
#Requires -RunAsAdministrator

<#
.SYNOPSIS
Installs or updates probe-rs in CF protocol mode on Windows.

.DESCRIPTION
This installer only manages probe-rs. It does not stop, migrate, or uninstall
the official cf-probe agent.
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet("install", "uninstall")]
    [string]$Action = "install",

    [Alias("id")]
    [string]$ServerId,

    [string]$Secret,

    [Alias("url")]
    [string]$WorkerUrl,

    [Alias("collect_interval", "collect")]
    [ValidateRange(0, 2147483647)]
    [int]$CollectInterval = 0,

    [Alias("interval")]
    [ValidateRange(1, 2147483647)]
    [int]$ReportInterval = 60,

    [Alias("reset_day")]
    [ValidateRange(0, 31)]
    [int]$ResetDay = 1,

    [string]$Ct,
    [string]$Cu,
    [string]$Cm,
    [string]$Bd,

    [Alias("interface", "interfaces", "iface")]
    [string]$Interfaces,

    [Alias("auto_update", "auto-update")]
    [string]$AutoUpdate,

    [Alias("update_channel", "update-channel")]
    [ValidateSet("stable", "prerelease")]
    [string]$UpdateChannel,

    [Alias("rx_correction")]
    [string]$RxCorrection,

    [Alias("tx_correction")]
    [string]$TxCorrection,

    [Alias("reporter_id", "reporter-id")]
    [ValidatePattern('^[A-Za-z0-9_.-]+$')]
    [string]$ReporterId,

    [Alias("replace_cf", "replace-cf")]
    [string]$ReplaceCf = "0",

    [Alias("no_start", "no-start")]
    [string]$NoStart = "0",

    [Alias("install_version", "install-version")]
    [string]$InstallVersion = "v0.1.3-beta.3",

    [Alias("install_ghproxy", "install-ghproxy")]
    [string]$InstallGhProxy,

    [Alias("bin")]
    [string]$BinarySource,

    [switch]$Purge
)

$ErrorActionPreference = "Stop"
$Installer = Join-Path $PSScriptRoot "install.ps1"
$InstallDir = Join-Path ([Environment]::GetFolderPath("ProgramFiles")) "probe-rs"
$InstalledBinary = Join-Path $InstallDir "probe-rs.exe"
$DataDir = Join-Path ([Environment]::GetFolderPath("CommonApplicationData")) "probe-rs"
$ConfigPath = Join-Path $DataDir "config.toml"
$NetStaticPath = Join-Path $DataDir "net_static.json"
$GitHubRepo = "https://github.com/ukuq/probe-rs"
$AssetName = "probe-rs-windows-x86_64.exe"

function ConvertTo-Boolean {
    param([string]$Value, [string]$Name)

    switch -Regex ($Value.Trim()) {
        '^(1|true|yes)$' { return $true }
        '^(0|false|no|)$' { return $false }
        default { throw "$Name must be 0 or 1." }
    }
}

if (-not (Test-Path -LiteralPath $Installer -PathType Leaf)) {
    throw "Windows installer not found: $Installer"
}
if ($Purge -and $Action -ne "uninstall") {
    throw "-Purge can only be used with uninstall."
}
if ($Action -eq "uninstall") {
    & $Installer uninstall -Purge:$Purge
    exit
}

if ([string]::IsNullOrWhiteSpace($ServerId)) { throw "-Id is required." }
if ([string]::IsNullOrWhiteSpace($Secret)) { throw "-Secret is required." }
if ([string]::IsNullOrWhiteSpace($WorkerUrl)) { throw "-Url is required." }
$parsedWorkerUrl = $null
if (-not [Uri]::TryCreate($WorkerUrl, [UriKind]::Absolute, [ref]$parsedWorkerUrl) -or
    $parsedWorkerUrl.Scheme -notin @("http", "https")) {
    throw "-Url must be an absolute HTTP(S) URL."
}

$replaceCfEnabled = ConvertTo-Boolean $ReplaceCf "-ReplaceCf"
$noStartEnabled = ConvertTo-Boolean $NoStart "-NoStart"
$autoUpdateEnabled = $null
if ($PSBoundParameters.ContainsKey("AutoUpdate")) {
    $autoUpdateEnabled = ConvertTo-Boolean $AutoUpdate "-AutoUpdate"
}

# CmdletBinding supplies the common -Debug switch. Reuse it as the agent's
# persistent debug-log option, and preserve an existing --debug task setting
# when this installation does not explicitly mention -Debug.
$debugSpecified = $PSBoundParameters.ContainsKey("Debug")
$debugEnabled = $debugSpecified -and [bool]$PSBoundParameters["Debug"]
$existingTask = Get-ScheduledTask -TaskName "probe-rs" -ErrorAction SilentlyContinue
if (-not $debugSpecified) {
    if ($existingTask -and $existingTask.Actions.Arguments -match '(?:^|\s)--debug(?:\s|$)') {
        $debugEnabled = $true
    }
}
$previousTaskEnabled = $false
$previousTaskRunning = $false
$taskStateNeedsRestore = $false

$temporaryBinary = $null
$temporaryChecksums = $null
try {
    $verifyReleaseAsset = $false
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
            $BinarySource = "$prefix/$BinarySource"
            $checksumSource = "$prefix/$checksumSource"
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
        Invoke-WebRequest -Uri $binaryUri -OutFile $temporaryBinary -UseBasicParsing
        $resolvedBinary = $temporaryBinary

        if ($verifyReleaseAsset) {
            $temporaryChecksums = Join-Path ([IO.Path]::GetTempPath()) (
                "probe-rs-sha-{0}.txt" -f [Guid]::NewGuid().ToString("N")
            )
            Invoke-WebRequest -Uri $checksumSource -OutFile $temporaryChecksums -UseBasicParsing
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

    # install.ps1 -NoStart stops and disables an existing task. From this
    # point onward, any configuration error must restore its previous state.
    $taskBeforeInstall = Get-ScheduledTask -TaskName "probe-rs" -ErrorAction SilentlyContinue
    $previousTaskEnabled = $taskBeforeInstall -and [bool]$taskBeforeInstall.Settings.Enabled
    $previousTaskRunning = $taskBeforeInstall -and ([string]$taskBeforeInstall.State -eq "Running")
    $taskStateNeedsRestore = [bool]$taskBeforeInstall
    & $Installer install -BinaryPath $resolvedBinary -NoStart -DebugLog:$debugEnabled

    $configureArgs = @(
        "configure-cf",
        "--config", $ConfigPath,
        "--net-static-path", $NetStaticPath,
        "--server-id", $ServerId,
        "--secret", $Secret,
        "--url", $WorkerUrl
    )
    if ($PSBoundParameters.ContainsKey("CollectInterval")) {
        $configureArgs += @("--collect", [Math]::Max(1, $CollectInterval).ToString())
    }
    if ($PSBoundParameters.ContainsKey("ReportInterval")) {
        $configureArgs += @("--report-interval", $ReportInterval.ToString())
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
    if ($PSBoundParameters.ContainsKey("UpdateChannel")) {
        $configureArgs += @("--update-channel", $UpdateChannel)
    }
    if ($PSBoundParameters.ContainsKey("ReporterId")) {
        $configureArgs += @("--reporter-id", $ReporterId)
    }
    if ($replaceCfEnabled) {
        $configureArgs += "--replace-cf"
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
        & $Installer start
        Write-Host "CF mode installed and started. Reporter: $selectedReporter"
    }
    Write-Host "Config: $ConfigPath"
    $taskStateNeedsRestore = $false
}
catch {
    $installError = $_
    if ($taskStateNeedsRestore) {
        try {
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
            Write-Warning "CF installation failed; restored the previous probe-rs task state."
        }
        catch {
            Write-Warning "CF installation failed and the previous probe-rs task state could not be restored: $($_.Exception.Message)"
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
