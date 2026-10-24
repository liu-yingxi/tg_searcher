#requires -version 3.0
<#
.SYNOPSIS
    One-click launcher for the TG Searcher Telegram bot on Windows.

.DESCRIPTION
    Finds tg-searcher.exe, loads .env, validates searcher.yaml and starts the bot.
    Works both in the source checkout (target\release\tg-searcher.exe) and in the
    portable Windows folder produced by scripts\package-windows.ps1.

.PARAMETER Config
    Path to searcher.yaml. Defaults to .\searcher.yaml or .\config\searcher.yaml.

.PARAMETER Exe
    Path to tg-searcher.exe. Defaults to .\tg-searcher.exe, target\release, or dist\windows.

.PARAMETER Clear
    Clear existing text/image indexes before starting.

.PARAMETER Debug
    Enable debug logging.

.PARAMETER Check
    Validate the configuration and exit (no bot start).

.PARAMETER BotSmoke
    Verify the bot token against the Telegram Bot API and exit.

.PARAMETER Version
    Print the executable version and exit.

.PARAMETER Restart
    Restart automatically if the bot exits with a non-zero code.

.PARAMETER NoCheck
    Skip the pre-start configuration check.

.PARAMETER NoPause
    Do not wait for Enter when the script ends (for scheduled tasks/CI).

.PARAMETER Init
    Create missing searcher.yaml/.env from the samples and exit.
#>

param(
    [string]$Config = "",
    [string]$Exe = "",
    [switch]$Clear,
    [switch]$Debug,
    [switch]$Check,
    [switch]$BotSmoke,
    [switch]$Version,
    [switch]$Restart,
    [switch]$NoCheck,
    [switch]$NoPause,
    [switch]$Init
)

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
if (-not $Root) {
    $Root = Split-Path -Parent $MyInvocation.MyCommand.Path
}
Set-Location -LiteralPath $Root

function Pause-IfNeeded {
    param([int]$Code)
    if (-not $NoPause -and $Host.Name -eq "ConsoleHost") {
        try {
            Read-Host "Press Enter to exit" | Out-Null
        } catch {
            # Non-interactive stdin; ignore.
        }
    }
    exit $Code
}

function Fail {
    param([string]$Message)
    Write-Host ""
    Write-Host "ERROR: $Message" -ForegroundColor Red
    Pause-IfNeeded 1
}

function Find-Executable {
    $candidates = @()
    if ($Exe) {
        $candidates += $Exe
    } else {
        $candidates += @(
            (Join-Path $Root "tg-searcher.exe"),
            (Join-Path $Root "dist\windows\tg-searcher.exe"),
            (Join-Path $Root "target\release\tg-searcher.exe")
        )
    }
    $fallback = $null
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            $full = (Resolve-Path -LiteralPath $candidate).Path
            if (-not $fallback) {
                $fallback = $full
            }
            $directory = Split-Path -Parent $full
            if (Test-Path -Path (Join-Path $directory "opencv_world*.dll")) {
                return $full
            }
        }
    }
    return $fallback
}

function Find-Configuration {
    $candidates = @()
    if ($Config) {
        $candidates += $Config
    } else {
        $candidates += @(
            (Join-Path $Root "searcher.yaml"),
            (Join-Path $Root "config\searcher.yaml"),
            (Join-Path $Root "searcher.yml"),
            (Join-Path $Root "config\searcher.yml")
        )
    }
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    return $null
}

function Import-DotEnv {
    param([string]$Path)
    $count = 0
    foreach ($raw in (Get-Content -LiteralPath $Path -ErrorAction Stop)) {
        $line = $raw.Trim().TrimStart([char]0xFEFF)
        if (-not $line -or $line.StartsWith("#")) {
            continue
        }
        if ($line -match "^(?i:export)\s+") {
            $line = $line.Substring(7).TrimStart()
        }
        $separator = $line.IndexOf("=")
        if ($separator -le 0) {
            continue
        }
        $name = $line.Substring(0, $separator).Trim()
        $value = $line.Substring($separator + 1).Trim()
        if ($value.Length -ge 2) {
            $first = $value.Substring(0, 1)
            $last = $value.Substring($value.Length - 1, 1)
            if (($first -eq '"' -and $last -eq '"') -or ($first -eq "'" -and $last -eq "'")) {
                $value = $value.Substring(1, $value.Length - 2)
            }
        }
        if ($name -match "^[A-Za-z_][A-Za-z0-9_]*$") {
            # Values already present in the process environment win over .env.
            if (-not (Test-Path -Path "Env:$name")) {
                Set-Item -Path "Env:$name" -Value $value
                $count++
            }
        }
    }
    return $count
}

function Assert-OpenCvRuntime {
    param([string]$ExePath)
    $exeDir = Split-Path -Parent $ExePath
    $local = Get-ChildItem -LiteralPath $exeDir -Filter "opencv_world*.dll" -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -notmatch "d\.dll$" } |
        Select-Object -First 1
    if ($local) {
        return
    }
    foreach ($directory in ($env:Path -split ";")) {
        $directory = $directory.Trim().Trim('"')
        if (-not $directory -or -not (Test-Path -LiteralPath $directory -PathType Container)) {
            continue
        }
        $found = Get-ChildItem -LiteralPath $directory -Filter "opencv_world*.dll" -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -notmatch "d\.dll$" } |
            Select-Object -First 1
        if ($found) {
            return
        }
    }
    Fail "opencv_world*.dll was not found next to $ExePath or in PATH.`nUse the portable build (scripts\package-windows.cmd) or add the OpenCV bin directory to PATH."
}

function Assert-NotPlaceholder {
    param([string]$ConfigPath, [string]$EnvPath)
    $text = Get-Content -LiteralPath $ConfigPath -Raw -ErrorAction SilentlyContinue
    if (-not $text) {
        return
    }
    $problems = @()
    if ($text.Contains('${TG_API_ID}') -and $env:TG_API_ID -eq '123456') {
        $problems += 'TG_API_ID is still the sample value 123456'
    }
    if ($text.Contains('${TG_API_HASH}') -and ([string]::IsNullOrWhiteSpace($env:TG_API_HASH) -or $env:TG_API_HASH -eq 'replace_me')) {
        $problems += 'TG_API_HASH is empty or still replace_me'
    }
    if ($text.Contains('${TG_PHONE}') -and $env:TG_PHONE -eq '+15551234567') {
        $problems += 'TG_PHONE is still the sample value +15551234567'
    }
    if ($text.Contains('${TG_BOT_TOKEN}') -and ([string]::IsNullOrWhiteSpace($env:TG_BOT_TOKEN) -or $env:TG_BOT_TOKEN -eq 'replace_me')) {
        $problems += 'TG_BOT_TOKEN is empty or still replace_me'
    }
    if ($problems.Count -gt 0) {
        $where = "Set the real values as environment variables"
        if ($EnvPath -and (Test-Path -LiteralPath $EnvPath -PathType Leaf)) {
            $where = "Edit $EnvPath and replace the sample values"
        }
        $message = @(
            "The configuration still references sample credentials:"
            ($problems | ForEach-Object { "  - $_" })
            ""
            $where + ", then run start-bot.cmd again."
            ""
            "TG_API_ID / TG_API_HASH: create an application at https://my.telegram.org"
            "TG_BOT_TOKEN: create a bot with @BotFather"
            "TG_PHONE: the phone number of the userbot account, in international format"
        ) -join [Environment]::NewLine
        Fail $message
    }
}

# --- Resolve executable -------------------------------------------------
$exePath = Find-Executable
if (-not $exePath) {
    Fail "tg-searcher.exe not found. Build it with:`n  cargo build --release`nor create a portable folder with:`n  .\scripts\package-windows.cmd -OpenCvBin <opencv bin dir>"
}
$exePath = [System.IO.Path]::GetFullPath($exePath)
Assert-OpenCvRuntime $exePath

if ($Version) {
    Write-Host "Executable: $exePath"
    & $exePath --version
    Pause-IfNeeded $LASTEXITCODE
}

# --- Configuration and first-run bootstrap --------------------------------
$configPath = Find-Configuration
$configCreated = $false
$created = @()

if (-not $configPath -and $Config) {
    Fail "Configuration file not found: $Config"
}

if (-not $configPath) {
    $sample = Join-Path $Root "searcher.sample.yaml"
    if (Test-Path -LiteralPath $sample -PathType Leaf) {
        $target = Join-Path $Root "searcher.yaml"
        Copy-Item -LiteralPath $sample -Destination $target
        $configPath = $target
        $configCreated = $true
        $created += $target
    }
}

$envPath = Join-Path $Root ".env"
$envExample = Join-Path $Root ".env.example"
if (($Init -or $configCreated) -and -not (Test-Path -LiteralPath $envPath -PathType Leaf) -and (Test-Path -LiteralPath $envExample -PathType Leaf)) {
    Copy-Item -LiteralPath $envExample -Destination $envPath
    $created += $envPath
}

if (Test-Path -LiteralPath $envPath -PathType Leaf) {
    $loaded = Import-DotEnv $envPath
    if ($loaded -gt 0) {
        Write-Host "Loaded $loaded variable(s) from $envPath"
    }
}

if ($Init) {
    if ($created.Count -eq 0) {
        Write-Host "Nothing to initialise; configuration and .env already exist."
    } else {
        Write-Host ""
        Write-Host "Created:" -ForegroundColor Yellow
        foreach ($path in $created) {
            Write-Host "  $path"
        }
        Write-Host ""
        Write-Host "Edit these files (Telegram API credentials, bot token, admin id), then run start-bot.cmd again."
    }
    Pause-IfNeeded 0
}

if (-not $configPath) {
    Fail "searcher.yaml was not found. Pass -Config <path> or put searcher.sample.yaml next to this script."
}

if ($created.Count -gt 0) {
    Write-Host ""
    Write-Host "Created:" -ForegroundColor Yellow
    foreach ($path in $created) {
        Write-Host "  $path"
    }
    Write-Host ""
    Write-Host "Fill in the Telegram credentials/token first, then run start-bot.cmd again."
    Pause-IfNeeded 0
}

if (-not $BotSmoke) {
    Assert-NotPlaceholder $configPath $envPath
}

# --- Preflight check ------------------------------------------------------Write-Host "Executable: $exePath"
Write-Host "Configuration: $configPath"
Write-Host "Working directory: $Root"
Write-Host ""

if (-not $NoCheck -and -not $Check -and -not $BotSmoke) {
    Write-Host "Checking configuration..." -ForegroundColor Cyan
    & $exePath -f $configPath --check
    if ($LASTEXITCODE -ne 0) {
        Fail "Configuration check failed (exit code $LASTEXITCODE). Fix the reported problem and try again."
    }
    Write-Host ""
}

if ($Check) {
    & $exePath -f $configPath --check
    Pause-IfNeeded $LASTEXITCODE
}

if ($BotSmoke) {
    Write-Host "Checking bot token..." -ForegroundColor Cyan
    & $exePath -f $configPath --bot-smoke
    Pause-IfNeeded $LASTEXITCODE
}

# --- Run ------------------------------------------------------------------
$arguments = @("-f", $configPath)
if ($Clear) {
    $arguments += "-c"
}
if ($Debug) {
    $arguments += "--debug"
}

Write-Host "Starting TG Searcher bot. Press Ctrl+C to stop." -ForegroundColor Green
Write-Host ""

while ($true) {
    & $exePath @arguments
    $code = $LASTEXITCODE
    if (-not $Restart -or $code -eq 0) {
        Pause-IfNeeded $code
    }
    Write-Host ""
    Write-Host "Bot exited with code $code; restarting in 5 seconds (Ctrl+C to stop)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
}
