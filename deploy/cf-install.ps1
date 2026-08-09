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

function Write-CfConfig {
    $effectiveCollect = [Math]::Max(1, $CollectInterval)
    $lines = New-Object 'System.Collections.Generic.List[string]'
    $lines.Add(("server_id = {0}" -f (ConvertTo-TomlString $ServerId)))
    $lines.Add(("secret = {0}" -f (ConvertTo-TomlString $Secret)))
    $lines.Add(("worker_url = {0}" -f (ConvertTo-TomlString $WorkerUrl)))
    $lines.Add('protocol = "cf"')
    $lines.Add(("net_static_path = {0}" -f (ConvertTo-TomlString $NetStaticPath)))
    $lines.Add("reset_day = $ResetDay")
    $lines.Add('config_version = ""')
    $lines.Add("interfaces = []")
    $lines.Add("enable_gpu = true")
    $lines.Add("report_errors = true")
    $lines.Add("report_self = false")
    $lines.Add("")
    $lines.Add("[intervals]")
    $lines.Add("collect = $effectiveCollect")
    $lines.Add("report = $ReportInterval")
    $lines.Add("ping = 30")
    $lines.Add("slow = 60")
    $lines.Add("gpu = 60")
    $lines.Add("ip = 600")

    foreach ($probe in @(
            @{ Name = "ct"; Target = $Ct },
            @{ Name = "cu"; Target = $Cu },
            @{ Name = "cm"; Target = $Cm },
            @{ Name = "bd"; Target = $Bd }
        )) {
        if (-not [string]::IsNullOrWhiteSpace($probe.Target)) {
            $lines.Add("")
            $lines.Add("[[pings]]")
            $lines.Add(("name = {0}" -f (ConvertTo-TomlString $probe.Name)))
            $lines.Add(("target = {0}" -f (ConvertTo-TomlString $probe.Target)))
        }
    }

    $lines.Add("")
    $lines.Add("[ext.cf]")
    $lines.Add("correction = true")
    $lines.Add("batch = true")
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

    & $Installer install -BinaryPath $resolvedBinary -NoStart
    Write-CfConfig
    & $Installer start
    Write-Host "CF mode installed. Config: $ConfigPath"
}
finally {
    if ($temporaryBinary -and (Test-Path -LiteralPath $temporaryBinary)) {
        Remove-Item -LiteralPath $temporaryBinary -Force
    }
}
