param(
    [string]$Path = "logs/audit.jsonl",
    [string]$ArchiveDir = "logs/archive",
    [switch]$Compress
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Path)) {
    throw "Audit file not found: $Path"
}

$sourceItem = Get-Item -LiteralPath $Path
if ($sourceItem.PSIsContainer) {
    throw "Audit path must be a file: $Path"
}

if ($sourceItem.Length -le 0) {
    Write-Output "Audit file is empty, archive skipped: $Path"
    exit 0
}

New-Item -ItemType Directory -Path $ArchiveDir -Force | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$baseName = [System.IO.Path]::GetFileNameWithoutExtension($sourceItem.Name)
$extension = $sourceItem.Extension
$archivedFileName = "{0}-{1}{2}" -f $baseName, $timestamp, $extension
$archivedPath = Join-Path $ArchiveDir $archivedFileName

Move-Item -LiteralPath $sourceItem.FullName -Destination $archivedPath
New-Item -ItemType File -Path $Path -Force | Out-Null

$outputPath = $archivedPath
if ($Compress) {
    $zipPath = "$archivedPath.zip"
    Compress-Archive -LiteralPath $archivedPath -DestinationPath $zipPath -Force
    Remove-Item -LiteralPath $archivedPath -Force
    $outputPath = $zipPath
}

[pscustomobject]@{
    source_path = $sourceItem.FullName
    archived_path = (Resolve-Path -LiteralPath $outputPath).Path
    compressed = [bool]$Compress
    archived_at = (Get-Date).ToString("s")
} | ConvertTo-Json -Depth 4
