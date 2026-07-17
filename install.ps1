<#
.SYNOPSIS
Installs the FrankenSQLite command-line client on 64-bit Windows.

.DESCRIPTION
Downloads the requested signed FrankenSQLite release, verifies its SHA-256
checksum, validates its exact version and SQL execution, and atomically installs
fsqlite.exe. PowerShell 5.1 and newer are supported.

.PARAMETER Version
Exact release tag to install, such as v0.1.17. Defaults to the latest release.

.PARAMETER Destination
Directory that receives fsqlite.exe. Defaults to $HOME\.local\bin.

.PARAMETER OfflineArchive
Path to a previously downloaded windows_amd64 release ZIP. Requires Version and
Checksum unless NoVerify is explicitly selected.

.PARAMETER Checksum
Expected 64-character SHA-256 digest for OfflineArchive.

.PARAMETER FromSource
Build and install the exact fsqlite-cli crate version with Cargo instead of
downloading a release artifact. Cannot be combined with OfflineArchive.

.PARAMETER SkipPostVerify
Skip candidate and installed-binary version and SQL smoke tests.

.PARAMETER NoVerify
Skip archive checksum and manifest-signature verification. Unsafe.

.PARAMETER Quiet
Suppress non-error installer output.

.PARAMETER Proxy
HTTP or HTTPS proxy URI. Defaults to HTTPS_PROXY, then HTTP_PROXY.

.PARAMETER KeepTemp
Preserve the unique temporary directory for diagnostics.

.EXAMPLE
irm "https://raw.githubusercontent.com/Dicklesworthstone/frankensqlite/main/install.ps1?$([DateTime]::UtcNow.Ticks)" | iex

Installs the latest release with a cache-busting request.

.EXAMPLE
.\install.ps1 -Version v0.1.17 -Destination C:\Tools\FrankenSQLite

Installs an exact release to a custom directory.

.EXAMPLE
.\install.ps1 -Version v0.1.17 -OfflineArchive .\fsqlite-0.1.17-windows_amd64.zip -Checksum $sha256 -KeepTemp

Verifies and installs a previously downloaded archive without network access.
#>

[CmdletBinding()]
param(
    [string]$Version = $env:FSQLITE_VERSION,
    [string]$Destination = $(if ($env:FSQLITE_INSTALL_DIR) { $env:FSQLITE_INSTALL_DIR } else { Join-Path $HOME '.local\bin' }),
    [string]$OfflineArchive,
    [string]$Checksum,
    [switch]$FromSource,
    [switch]$SkipPostVerify,
    [switch]$NoVerify,
    [switch]$Quiet,
    [string]$Proxy = $(if ($env:HTTPS_PROXY) { $env:HTTPS_PROXY } else { $env:HTTP_PROXY }),
    [switch]$KeepTemp
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$OriginalSecurityProtocol = [Net.ServicePointManager]::SecurityProtocol
[Net.ServicePointManager]::SecurityProtocol = $OriginalSecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
$Owner = if ($env:FSQLITE_GITHUB_OWNER) { $env:FSQLITE_GITHUB_OWNER } else { 'Dicklesworthstone' }
$Repo = if ($env:FSQLITE_GITHUB_REPO) { $env:FSQLITE_GITHUB_REPO } else { 'frankensqlite' }
$MinisignPublicKey = 'RWTQoKUb0Ue4NsqTpPWnABCrIU0+m25zsMlbv6UcRClQ7jmRP3A7NmTB'
$TempDirectory = $null
$LockStream = $null
$StagedBinary = $null

function Write-Info([string]$Message) { if (-not $Quiet) { Write-Host "-> $Message" -ForegroundColor Cyan } }
function Write-Ok([string]$Message) { if (-not $Quiet) { Write-Host "OK $Message" -ForegroundColor Green } }
function Fail([string]$Message) { throw $Message }

function Invoke-Download([string]$Uri, [string]$OutFile) {
    $parameters = @{
        Uri = $Uri
        OutFile = $OutFile
        UseBasicParsing = $true
        MaximumRedirection = 5
        TimeoutSec = 30
    }
    if ($Proxy) { $parameters.Proxy = $Proxy }
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            Invoke-WebRequest @parameters
            return
        } catch {
            if ($attempt -eq 3) { throw }
            Start-Sleep -Seconds $attempt
        }
    }
}

function Resolve-Version {
    if ($script:Version) {
        if ($script:Version -match '^\d+\.\d+\.\d+([.-][0-9A-Za-z.-]+)?$') { $script:Version = "v$($script:Version)" }
        if ($script:Version -notmatch '^v\d+\.\d+\.\d+([.-][0-9A-Za-z.-]+)?$') {
            Fail "Invalid version '$script:Version'; expected vX.Y.Z"
        }
        return
    }
    if ($OfflineArchive) { Fail '-OfflineArchive requires -Version vX.Y.Z' }
    $uri = "https://api.github.com/repos/$Owner/$Repo/releases/latest"
    $parameters = @{ Uri = $uri; UseBasicParsing = $true; TimeoutSec = 15; Headers = @{ Accept = 'application/vnd.github+json' } }
    if ($Proxy) { $parameters.Proxy = $Proxy }
    try {
        $release = Invoke-RestMethod @parameters
        $script:Version = [string]$release.tag_name
    } catch {
        $redirectParameters = @{ Uri = "https://github.com/$Owner/$Repo/releases/latest"; UseBasicParsing = $true; TimeoutSec = 15; MaximumRedirection = 5 }
        if ($Proxy) { $redirectParameters.Proxy = $Proxy }
        $response = Invoke-WebRequest @redirectParameters
        $script:Version = [IO.Path]::GetFileName($response.BaseResponse.ResponseUri.AbsolutePath)
    }
    if ($script:Version -notmatch '^v\d+\.\d+\.\d+([.-][0-9A-Za-z.-]+)?$') {
        Fail "GitHub returned an invalid release tag: $script:Version"
    }
}

function Get-Sha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Test-Archive([string]$Path, [string]$Expected) {
    $actual = Get-Sha256 $Path
    if ($actual -ne $Expected.ToLowerInvariant()) {
        Fail "Checksum mismatch for $([IO.Path]::GetFileName($Path)): expected $Expected, got $actual"
    }
    Write-Ok 'SHA-256 verified'
}

function Stage-Binary([string]$Source) {
    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    $script:StagedBinary = Join-Path $Destination ".fsqlite.install.$PID.$([Guid]::NewGuid().ToString('N')).exe"
    Copy-Item -LiteralPath $Source -Destination $script:StagedBinary
}

function Promote-Binary {
    $target = Join-Path $Destination 'fsqlite.exe'
    Move-Item -LiteralPath $script:StagedBinary -Destination $target -Force
    $script:StagedBinary = $null
}

function Install-FromSource {
    if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) { Fail 'cargo is required for -FromSource' }
    $root = Join-Path $TempDirectory 'cargo-root'
    Write-Info "Building fsqlite-cli $Version from source"
    if ($Quiet) {
        $cargoLog = Join-Path $TempDirectory 'cargo-install.log'
        & cargo +nightly install fsqlite-cli --version "=$($Version.TrimStart('v'))" --locked --root $root --bin fsqlite *> $cargoLog
    } else {
        & cargo +nightly install fsqlite-cli --version "=$($Version.TrimStart('v'))" --locked --root $root --bin fsqlite
    }
    if ($LASTEXITCODE -ne 0) {
        if ($Quiet -and (Test-Path -LiteralPath $cargoLog)) {
            [Console]::Error.WriteLine((Get-Content -LiteralPath $cargoLog -Tail 40 | Out-String).TrimEnd())
            $script:KeepTemp = $true
            Fail "cargo install failed; full log preserved at $cargoLog"
        }
        Fail 'cargo install failed'
    }
    $candidate = Join-Path $root 'bin\fsqlite.exe'
    Stage-Binary $candidate
    if (-not $SkipPostVerify) { Test-FsqliteBinary $script:StagedBinary }
    Promote-Binary
}

function Invoke-BoundedFsqlite([string]$Binary, [string]$Arguments, [int]$TimeoutSeconds = 15) {
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $Binary
    $startInfo.Arguments = $Arguments
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) { Fail "Could not start $Binary" }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            try {
                $process.Kill()
            } catch {
                Fail "Command timed out and could not be terminated: $Binary $Arguments`n$($_.Exception.Message)"
            }
            if (-not $process.WaitForExit(2000)) {
                Fail "Command timed out and did not terminate after kill: $Binary $Arguments"
            }
            Fail "Command timed out after $TimeoutSeconds seconds: $Binary $Arguments"
        }
        if (-not $stdoutTask.Wait(2000) -or -not $stderrTask.Wait(2000)) {
            Fail "Command exited but inherited output streams remained open: $Binary $Arguments"
        }
        $stdout = $stdoutTask.Result.Trim()
        $stderr = $stderrTask.Result.Trim()
        if ($process.ExitCode -ne 0) {
            Fail "Command failed with exit code $($process.ExitCode): $Binary $Arguments`n$stderr"
        }
        return $stdout
    } finally {
        $process.Dispose()
    }
}

function Test-FsqliteBinary([string]$Binary) {
    $versionOutput = Invoke-BoundedFsqlite $Binary '--version'
    if ($versionOutput -ne "fsqlite $($Version.TrimStart('v'))") {
        Fail "Binary version mismatch: $versionOutput"
    }
    $sqlOutput = Invoke-BoundedFsqlite $Binary '--batch --command "SELECT 1 + 2;"'
    if ($sqlOutput -ne '3') { Fail "SQL smoke test failed: $sqlOutput" }
}

function Invoke-PostInstallVerification {
    Test-FsqliteBinary (Join-Path $Destination 'fsqlite.exe')
    Write-Ok 'version and SQL smoke tests passed'
}

try {
    if ($OfflineArchive -and $FromSource) { Fail '-OfflineArchive and -FromSource are mutually exclusive' }
    if ($Checksum -and -not $OfflineArchive) { Fail '-Checksum requires -OfflineArchive' }
    if (-not [Environment]::Is64BitOperatingSystem) { Fail 'A 64-bit Windows host is required' }
    $architecture = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
    if ($architecture -ne 'AMD64') {
        Fail 'Only windows/amd64 artifacts are currently published'
    }

    Resolve-Version
    if (-not $Quiet) {
        Write-Host ''
        Write-Host 'FrankenSQLite installer' -ForegroundColor Green
        Write-Host "Installing $Version for windows_amd64"
        Write-Host ''
    }

    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    $lockPath = Join-Path ([IO.Path]::GetTempPath()) 'fsqlite-install.lock'
    try {
        $LockStream = [IO.File]::Open($lockPath, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    } catch {
        Fail "Another installer is running or a stale lock needs inspection: $lockPath"
    }

    $TempDirectory = Join-Path ([IO.Path]::GetTempPath()) ("fsqlite-install.{0}" -f [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $TempDirectory | Out-Null

    if ($FromSource) {
        Install-FromSource
    } else {
        $archiveName = "fsqlite-$($Version.TrimStart('v'))-windows_amd64.zip"
        if ($OfflineArchive) {
            $archive = (Resolve-Path -LiteralPath $OfflineArchive).Path
            if (-not $NoVerify) {
                if ($Checksum -notmatch '^[0-9a-fA-F]{64}$') { Fail '-OfflineArchive requires -Checksum SHA256 (or explicitly -NoVerify)' }
                Test-Archive $archive $Checksum
            }
        } else {
            $base = "https://github.com/$Owner/$Repo/releases/download/$Version"
            $archive = Join-Path $TempDirectory $archiveName
            Write-Info "Downloading $archiveName"
            Invoke-Download "$base/$archiveName" $archive
            if (-not $NoVerify) {
                $manifest = Join-Path $TempDirectory 'SHA256SUMS.txt'
                $signature = Join-Path $TempDirectory 'SHA256SUMS.txt.minisig'
                Invoke-Download "$base/SHA256SUMS.txt" $manifest
                $minisign = Get-Command minisign -ErrorAction SilentlyContinue
                if ($minisign) {
                    Invoke-Download "$base/SHA256SUMS.txt.minisig" $signature
                    & $minisign.Source -Vm $manifest -x $signature -P $MinisignPublicKey | Out-Null
                    if ($LASTEXITCODE -ne 0) { Fail 'Release checksum signature verification failed' }
                    Write-Ok 'release manifest signature verified'
                } else {
                    if (-not $Quiet) { Write-Warning 'minisign is not installed; authenticity check skipped (SHA-256 still required)' }
                }
                $line = Get-Content -LiteralPath $manifest | Where-Object { $_ -match "^[0-9a-fA-F]{64}\s+\*?$([regex]::Escape($archiveName))$" } | Select-Object -First 1
                if (-not $line) { Fail "Checksum missing for $archiveName" }
                $expected = ($line -split '\s+')[0]
                Test-Archive $archive $expected
            }
        }

        Add-Type -AssemblyName System.IO.Compression.FileSystem
        $zip = [IO.Compression.ZipFile]::OpenRead($archive)
        try {
            if ($zip.Entries.Count -ne 1 -or $zip.Entries[0].FullName -ne 'fsqlite.exe' -or $zip.Entries[0].Length -eq 0) {
                Fail 'Release archive must contain exactly one root-level fsqlite.exe'
            }
        } finally {
            $zip.Dispose()
        }

        $extract = Join-Path $TempDirectory 'extract'
        Expand-Archive -LiteralPath $archive -DestinationPath $extract -Force
        $candidate = Join-Path $extract 'fsqlite.exe'
        if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) { Fail 'fsqlite.exe not found in archive' }
        Stage-Binary $candidate
        if (-not $SkipPostVerify) { Test-FsqliteBinary $script:StagedBinary }
        Promote-Binary
    }

    Write-Ok "installed fsqlite $Version to $(Join-Path $Destination 'fsqlite.exe')"
    if (-not $SkipPostVerify) { Invoke-PostInstallVerification }
    if (-not $Quiet) { Write-Host "Uninstall: remove $(Join-Path $Destination 'fsqlite.exe')" -ForegroundColor DarkGray }
} finally {
    if ($StagedBinary -and (Test-Path -LiteralPath $StagedBinary)) {
        if (-not $Quiet) { Write-Warning "Unpromoted candidate preserved for diagnostics: $StagedBinary" }
    }
    if ($LockStream) { $LockStream.Dispose() }
    if ($TempDirectory -and (Test-Path -LiteralPath $TempDirectory)) {
        if ($KeepTemp -or $env:FSQLITE_KEEP_TEMP -eq '1') {
            if (-not $Quiet) { Write-Warning "Temporary directory preserved: $TempDirectory" }
        } else {
            Remove-Item -LiteralPath $TempDirectory -Recurse -Force
        }
    }
    [Net.ServicePointManager]::SecurityProtocol = $OriginalSecurityProtocol
}
