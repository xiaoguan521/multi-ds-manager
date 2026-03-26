param(
    [string]$Path = "logs/audit.jsonl",
    [string]$RequestId = "",
    [string]$CallerId = "",
    [string]$Jgbh = "",
    [string]$Datasource = "",
    [string]$OperationType = "",
    [ValidateSet("all", "true", "false")]
    [string]$Success = "all",
    [int]$Limit = 20
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path $Path)) {
    throw "Audit file not found: $Path"
}

$items = foreach ($line in Get-Content -Path $Path) {
    if ([string]::IsNullOrWhiteSpace($line)) {
        continue
    }

    $item = $line | ConvertFrom-Json

    if ($RequestId -and $item.request_id -ne $RequestId) {
        continue
    }

    if ($CallerId -and $item.caller_id -ne $CallerId) {
        continue
    }

    if ($Jgbh -and $item.jgbh -ne $Jgbh) {
        continue
    }

    if ($Datasource -and $item.datasource_name -ne $Datasource) {
        continue
    }

    if ($OperationType -and $item.operation_type -ne $OperationType) {
        continue
    }

    if ($Success -eq "true" -and -not [bool]$item.success) {
        continue
    }

    if ($Success -eq "false" -and [bool]$item.success) {
        continue
    }

    $item
}

$items |
    Select-Object -Last $Limit |
    ConvertTo-Json -Depth 8
