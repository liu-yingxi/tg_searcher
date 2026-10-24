# Build a portable Windows x64 folder (and zip) with all required DLLs.
#
# Usage (PowerShell, from the repository root):
#   .\scripts\package-windows.ps1 -OpenCvBin "C:\opencv\build\x64\vc16\bin"
#
# The official OpenCV Windows package ships only shared libraries, so the
# portable build bundles opencv_world*.dll and the MSVC runtime DLLs next to
# tg-searcher.exe. No OpenCV installation is required on the target machine.

param(
    [string]$OpenCvBin = "",
    [string]$OutDir = "dist\windows",
    [string]$ZipPath = "dist\tg-searcher-windows-x64.zip"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not $OpenCvBin) {
    if ($env:OPENCV_LINK_PATHS) {
        $lib = ($env:OPENCV_LINK_PATHS -split ';')[0]
        $candidate = Join-Path (Split-Path -Parent $lib) "bin"
        if (Test-Path $candidate) { $OpenCvBin = $candidate }
    }
}
if (-not $OpenCvBin -or -not (Test-Path $OpenCvBin)) {
    throw "OpenCV bin directory not found. Pass -OpenCvBin <path to ...\build\x64\vc16\bin>."
}

Write-Host "Building release binary..."
cargo build --release --locked
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
Copy-Item "target\release\tg-searcher.exe" $OutDir -Force
Copy-Item "searcher.sample.yaml" $OutDir -Force
Copy-Item ".env.example" $OutDir -Force
Copy-Item "start-bot.ps1" $OutDir -Force
Copy-Item "start-bot.cmd" $OutDir -Force

$opencvDll = Get-ChildItem $OpenCvBin -Filter "opencv_world*.dll" |
    Where-Object { $_.Name -notmatch 'd\.dll$' } |
    Select-Object -First 1
if (-not $opencvDll) { throw "opencv_world*.dll not found in $OpenCvBin" }
Copy-Item $opencvDll.FullName $OutDir -Force

# OpenCV is Apache-2.0 licensed; ship its notice with the redistributed DLL.
$opencvLicense = Join-Path $OpenCvBin "..\..\..\..\LICENSE.txt"
if (Test-Path $opencvLicense) {
    Copy-Item $opencvLicense (Join-Path $OutDir "LICENSE-OpenCV.txt") -Force
}

# The Rust binary and OpenCV link the dynamic MSVC runtime. Bundle the
# redistributable DLLs so the folder works on machines without the VC++ runtime.
$runtimeDlls = @(
    "msvcp140.dll",
    "msvcp140_1.dll",
    "msvcp140_2.dll",
    "vcruntime140.dll",
    "vcruntime140_1.dll",
    "concrt140.dll"
)
$runtimeSources = @()
if ($env:VCToolsRedistDir) {
    $runtimeSources += Get-ChildItem $env:VCToolsRedistDir -Recurse -Filter "msvcp140.dll" -ErrorAction SilentlyContinue |
        Select-Object -First 1 -ExpandProperty DirectoryName
}
$runtimeSources += "$env:WINDIR\System32"
foreach ($dll in $runtimeDlls) {
    $found = $null
    foreach ($source in $runtimeSources) {
        $candidate = Join-Path $source $dll
        if (Test-Path $candidate) { $found = $candidate; break }
    }
    if ($found) {
        Copy-Item $found $OutDir -Force
    } else {
        Write-Warning "MSVC runtime DLL not found: $dll (install the VC++ redistributable)"
    }
}

$readme = @'
TG Searcher portable build (Windows x64)

1. Run:
     .\start-bot.cmd

   On the first run it creates searcher.yaml and .env from the samples if they
   are missing, then asks you to edit them. After that it loads .env, validates
   the configuration and starts the bot. Use -Check to validate only, -Version
   to print the version, or -BotSmoke to test the token.

2. Or set the required environment variables (TG_API_ID, TG_API_HASH, TG_PHONE,
   TG_ADMIN_ID, TG_BOT_TOKEN), or write literal values into searcher.yaml.
   Variables written as ${VAR} are expanded at startup; missing variables are
   an error.

3. Manual start:
     .\tg-searcher.exe -f searcher.yaml

The first run is interactive: enter the Telegram login code (and 2FA password
if enabled). The session is stored under runtime_dir.

Bundled: opencv_world*.dll and the MSVC runtime DLLs. Python/OpenCV do not need
to be installed. If you prefer a single self-contained executable, OpenCV must
be built statically from source; see docs/build.md.
'@
Set-Content -Path (Join-Path $OutDir "README-portable.txt") -Value $readme -Encoding utf8

Write-Host "Creating $ZipPath ..."
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $ZipPath) | Out-Null
if (Test-Path $ZipPath) { Remove-Item $ZipPath -Force }
Compress-Archive -Path (Join-Path $OutDir "*") -DestinationPath $ZipPath -Force

Write-Host "Portable build ready:"
Get-ChildItem $OutDir | Select-Object Name, Length
Get-Item $ZipPath | Select-Object FullName, Length
