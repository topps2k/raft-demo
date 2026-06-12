# Live three-node demo: elect, replicate, kill the leader, fail over.
# Usage: .\demo.ps1   (requires the shaded jar: mvn package)

$ErrorActionPreference = "Stop"
$jar = Join-Path $PSScriptRoot "target\toy-raft-0.1.0.jar"
if (-not (Test-Path $jar)) { throw "Build first: mvn package" }
$peers = "1=localhost:5001,2=localhost:5002,3=localhost:5003"

function Invoke-Client { & java -jar $jar @args }

Write-Host "`n=== starting 3 nodes ===" -ForegroundColor Cyan
$nodes = @{}
foreach ($id in 1..3) {
    $nodes[$id] = Start-Process -FilePath "java" -PassThru -WindowStyle Hidden `
        -ArgumentList "-jar", $jar, "node", $id, $peers
}
Start-Sleep -Seconds 2

try {
    Write-Host "`n=== cluster status (one leader, two followers) ===" -ForegroundColor Cyan
    foreach ($port in 5001..5003) { Invoke-Client status "localhost:$port" }

    Write-Host "`n=== writes via arbitrary nodes (redirected to the leader) ===" -ForegroundColor Cyan
    Invoke-Client set localhost:5002 color teal
    Invoke-Client set localhost:5003 answer 42

    Write-Host "`n=== reads from other replicas ===" -ForegroundColor Cyan
    Invoke-Client get localhost:5001 color
    Invoke-Client get localhost:5002 answer

    Write-Host "`n=== killing the leader ===" -ForegroundColor Cyan
    $leaderId = $null
    foreach ($port in 5001..5003) {
        $status = Invoke-Client status "localhost:$port" | ConvertFrom-Json
        if ($status.role -eq "LEADER") { $leaderId = $status.id }
    }
    Write-Host "leader is node $leaderId - stopping it"
    Stop-Process -Id $nodes[[int]$leaderId].Id -Force
    $nodes.Remove([int]$leaderId)
    Start-Sleep -Seconds 3

    Write-Host "`n=== survivors elected a new leader; data and writes still work ===" -ForegroundColor Cyan
    foreach ($id in $nodes.Keys) { Invoke-Client status "localhost:500$id" }
    Invoke-Client get "localhost:500$($nodes.Keys | Select-Object -First 1)" color
    Invoke-Client set "localhost:500$($nodes.Keys | Select-Object -First 1)" phoenix risen
    Invoke-Client get "localhost:500$($nodes.Keys | Select-Object -Last 1)" phoenix
}
finally {
    Write-Host "`n=== cleanup ===" -ForegroundColor Cyan
    foreach ($proc in $nodes.Values) {
        try { Stop-Process -Id $proc.Id -Force -Confirm:$false } catch {}
    }
}
