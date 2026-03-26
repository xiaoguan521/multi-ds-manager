param(
    [string]$Target = "127.0.0.1:50051",
    [string]$ImportPath = (Join-Path $PSScriptRoot "..\..\proto"),
    [string]$ProtoFile = "dynamic_ds.proto",
    [string]$Jgbh = "340100",
    [string]$Sql = "SELECT 1 AS test",
    [string]$CallerId = "bootstrap-client",
    [string]$AuthToken = "bootstrap-secret",
    [string]$Operator = "bootstrap",
    [string]$RequestId = ("grpc-smoke-" + [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$grpcurl = Get-Command grpcurl -ErrorAction SilentlyContinue
if (-not $grpcurl) {
    throw "grpcurl is required. Install grpcurl first, then rerun this script."
}

$importPathResolved = (Resolve-Path $ImportPath).Path
$executePayload = @{
    jgbh = $Jgbh
    operationType = "QUERY"
    sql = $Sql
    callerId = $CallerId
    authToken = $AuthToken
    requestId = $RequestId
    operator = $Operator
    maxRows = 10
} | ConvertTo-Json -Compress

Write-Host "[1/2] Ping $Target"
& $grpcurl.Source `
    -plaintext `
    -import-path $importPathResolved `
    -proto $ProtoFile `
    -d '{}' `
    $Target `
    "multi_ds.grpc.v1.DynamicDataSource/Ping"
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "[2/2] Execute query against jgbh $Jgbh"
& $grpcurl.Source `
    -plaintext `
    -import-path $importPathResolved `
    -proto $ProtoFile `
    -d $executePayload `
    $Target `
    "multi_ds.grpc.v1.DynamicDataSource/Execute"
exit $LASTEXITCODE
