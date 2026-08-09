#Requires -Version 5.1
#Requires -RunAsAdministrator

<#
.SYNOPSIS
Installs probe-rs in CF-Server-Monitor protocol mode on Windows.

.EXAMPLE
.\cf-install.ps1 install -Id <UUID> -Secret <API_SECRET> `
    -Url https://worker.example.com/update -CollectInterval 0 -Interval 60

.EXAMPLE
.\cf-install.ps1 uninstall -Purge
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

    [Alias("collect_interval")]
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

    [Alias("bin")]
    [string]$BinarySource,

    [Alias("auto_update")]
    [string]$AutoUpdate,

    [Alias("rx_correction")]
    [string]$RxCorrection,

    [Alias("tx_correction")]
    [string]$TxCorrection,

    [Alias("reporter_id")]
    [ValidatePattern('^[A-Za-z0-9_.-]+$')]
    [string]$ReporterId = "primary",

    [switch]$Purge
)

$ErrorActionPreference = "Stop"
$Installer = Join-Path $PSScriptRoot "install.ps1"
$DataDir = Join-Path ([Environment]::GetFolderPath("CommonApplicationData")) "probe-rs"
$ConfigPath = Join-Path $DataDir "config.toml"
$NetStaticPath = Join-Path $DataDir "net_static.json"
$ReleaseUrl = "https://github.com/ukuq/probe-rs/releases/latest/download/probe-rs-windows-x86_64.exe"
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

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

if ([string]::IsNullOrWhiteSpace($ServerId)) {
    throw "-Id is required."
}
if ([string]::IsNullOrWhiteSpace($Secret)) {
    throw "-Secret is required."
}
if ([string]::IsNullOrWhiteSpace($WorkerUrl)) {
    throw "-Url is required."
}
$parsedWorkerUrl = $null
if (-not [Uri]::TryCreate($WorkerUrl, [UriKind]::Absolute, [ref]$parsedWorkerUrl) -or
    $parsedWorkerUrl.Scheme -notin @("http", "https")) {
    throw "-Url must be an absolute HTTP(S) URL."
}

foreach ($ignored in @("AutoUpdate", "RxCorrection", "TxCorrection")) {
    if ($PSBoundParameters.ContainsKey($ignored)) {
        Write-Host "Ignoring -$ignored; probe-rs handles updates/correction through its own protocol."
    }
}

function ConvertTo-TomlString {
    param([AllowEmptyString()][string]$Value)

    # A JSON string uses the same quoting needed by a TOML basic string for
    # quotes, backslashes and control characters.
    return ConvertTo-Json -InputObject $Value -Compress
}

function New-CfReporterBlock {
    $effectiveCollect = [Math]::Max(1, $CollectInterval)
    $lines = New-Object 'System.Collections.Generic.List[string]'
    $lines.Add("[[reporters]]")
    $lines.Add(("id = {0}" -f (ConvertTo-TomlString $ReporterId)))
    $lines.Add('protocol = "cf"')
    $lines.Add(("server_id = {0}" -f (ConvertTo-TomlString $ServerId)))
    $lines.Add(("secret = {0}" -f (ConvertTo-TomlString $Secret)))
    $lines.Add(("worker_url = {0}" -f (ConvertTo-TomlString $WorkerUrl)))
    $lines.Add('config_version = ""')
    $lines.Add("report_interval = $ReportInterval")
    $lines.Add("reset_day = $ResetDay")
    $lines.Add("interfaces = []")
    $lines.Add("disks = []")
    $lines.Add("report_gpu = true")
    $lines.Add("report_errors = true")
    $lines.Add("report_self = false")
    $lines.Add("")
    $lines.Add("[reporters.intervals]")
    $lines.Add("collect = $effectiveCollect")
    $lines.Add("ping = 30")
    $lines.Add("slow = 60")
    $lines.Add("gpu = 60")
    $lines.Add("ip = 600")
    $lines.Add("diskio = 10")

    foreach ($probe in @(
            @{ Name = "ct"; Target = $Ct },
            @{ Name = "cu"; Target = $Cu },
            @{ Name = "cm"; Target = $Cm },
            @{ Name = "bd"; Target = $Bd }
        )) {
        if (-not [string]::IsNullOrWhiteSpace($probe.Target)) {
            $lines.Add("")
            $lines.Add("[[reporters.pings]]")
            $lines.Add(("name = {0}" -f (ConvertTo-TomlString $probe.Name)))
            $lines.Add('type = "tcp"')
            $lines.Add(("target = {0}" -f (ConvertTo-TomlString $probe.Target)))
            $lines.Add("interval = 30")
        }
    }

    $lines.Add("")
    $lines.Add("[reporters.ext.cf]")
    $lines.Add("correction = true")
    $lines.Add("batch = true")
    return $lines -join [Environment]::NewLine
}

function Remove-ReporterBlock {
    param([string]$Text, [string]$Id)

    $blockPattern = '(?ms)^[ \t]*\[\[reporters\]\][ \t]*\r?\n(?<body>.*?)(?=^[ \t]*\[\[reporters\]\][ \t]*\r?$|\z)'
    return [regex]::Replace($Text, $blockPattern, {
            param($match)
            $escapedId = [regex]::Escape($Id)
            $idPattern = "(?m)^[ \t]*id[ \t]*=[ \t]*(?:`"$escapedId`"|'$escapedId')[ \t]*(?:#[^\r\n]*)?\r?$"
            if ([regex]::IsMatch($match.Groups['body'].Value, $idPattern)) { return '' }
            return $match.Value
        })
}

function Remove-SeededReporterBlocks {
    param([string]$Text)

    $blockPattern = '(?ms)^[ \t]*\[\[reporters\]\][ \t]*\r?\n(?<body>.*?)(?=^[ \t]*\[\[reporters\]\][ \t]*\r?$|\z)'
    return [regex]::Replace($Text, $blockPattern, {
            param($match)
            $body = $match.Groups['body'].Value
            $seeded = $body -match '(?m)^[ \t]*server_id[ \t]*=[ \t]*"cf-server-uuid"' -or
                $body -match '(?m)^[ \t]*worker_url[ \t]*=[ \t]*"https://monitor\.example\.com/update"' -or
                $body -match '(?m)^[ \t]*worker_url[ \t]*=[ \t]*"https://komari\.example\.com"' -or
                $body -match '(?m)^[ \t]*worker_url[ \t]*=[ \t]*"http://127\.0\.0\.1:8080/report"'
            if ($seeded) { return '' }
            return $match.Value
        })
}

function Write-CfConfig {
    param([bool]$PreserveExisting)

    if ($PreserveExisting -and (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
        $existing = [IO.File]::ReadAllText($ConfigPath)
        $existing = Remove-SeededReporterBlocks $existing
        $firstReporter = [regex]::Match($existing, '(?m)^[ \t]*\[\[reporters\]\][ \t]*\r?$')
        if ($firstReporter.Success) {
            $global = $existing.Substring(0, $firstReporter.Index)
            # The new root only contains net_static_path. Legacy root collector
            # fields are deliberately not migrated: write a fresh config below.
            if ($global -notmatch '(?m)^[ \t]*(server_id|enable_gpu)[ \t]*=' -and
                $global -notmatch '(?m)^[ \t]*\[\[?\s*(intervals|pings)') {
                $updated = Remove-ReporterBlock $existing $ReporterId
                $content = $updated.TrimEnd() + [Environment]::NewLine + [Environment]::NewLine +
                    (New-CfReporterBlock) + [Environment]::NewLine
                [IO.File]::WriteAllText($ConfigPath, $content, $Utf8NoBom)
                Write-Host "Preserved other Reporters and upserted CF Reporter '$ReporterId'."
                return
            }
        }
    }

    $lines = New-Object 'System.Collections.Generic.List[string]'
    $lines.Add(("net_static_path = {0}" -f (ConvertTo-TomlString $NetStaticPath)))
    $lines.Add("")
    $lines.Add((New-CfReporterBlock))
    [IO.File]::WriteAllLines($ConfigPath, $lines, $Utf8NoBom)
}

$temporaryBinary = $null
try {
    if ([string]::IsNullOrWhiteSpace($BinarySource)) {
        $BinarySource = $ReleaseUrl
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
    }

    # install.ps1 seeds config.example.toml on a clean host. Remember whether
    # the config predated this CF installation so those sample Reporters are
    # not mistaken for user-owned Reporters that should be preserved.
    $preserveExistingConfig = Test-Path -LiteralPath $ConfigPath -PathType Leaf
    & $Installer install -BinaryPath $resolvedBinary -NoStart
    Write-CfConfig -PreserveExisting $preserveExistingConfig
    & $Installer start
    Write-Host "CF mode installed. Config: $ConfigPath"
}
finally {
    if ($temporaryBinary -and (Test-Path -LiteralPath $temporaryBinary)) {
        Remove-Item -LiteralPath $temporaryBinary -Force
    }
}
