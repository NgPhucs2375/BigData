param(
    [int]$BatchSize = 5,
    [int]$IntervalSeconds = 5
)

$ErrorActionPreference = "Stop"
$runId = Get-Date -Format "yyyyMMddHHmmss"
$hostRelative = "stream_demo\$runId\incoming"
$hostPath = Join-Path "data" $hostRelative
$hdfsRoot = "/mua/stream/demo/$runId"
$hdfsIncoming = "$hdfsRoot/incoming"
$hdfsOutput = "$hdfsRoot/curated"
$hdfsCheckpoint = "$hdfsRoot/checkpoint"
$source = "data\silver\interactions_clean.jsonl"

Write-Host "[1/6] Starting the low-memory Hadoop/Spark cluster"
docker compose up -d
Start-Sleep -Seconds 30
docker exec namenode hdfs dfsadmin -safemode leave | Out-Host

Write-Host "[2/6] Creating isolated HDFS paths: $hdfsRoot"
docker exec namenode hdfs dfs -mkdir -p $hdfsIncoming $hdfsOutput $hdfsCheckpoint | Out-Host

Write-Host "[3/6] Starting Structured Streaming"
$streamCommand = "MUA_STREAM_SOURCE=hdfs://namenode:8020$hdfsIncoming MUA_STREAM_OUTPUT=hdfs://namenode:8020$hdfsOutput MUA_STREAM_CHECKPOINT=hdfs://namenode:8020$hdfsCheckpoint nohup /spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 512m --driver-memory 512m /jobs/stream_interactions.py > /tmp/stream-demo-$runId.log 2>&1 < /dev/null &"
docker exec spark-master sh -c $streamCommand

Write-Host "[4/6] Starting the event producer"
New-Item -ItemType Directory -Force -Path $hostPath | Out-Null
$producer = Start-Process -FilePath "python" -ArgumentList @(
    "tools/stream_demo_producer.py", $source, (Join-Path "data" $hostRelative),
    "--batch-size", $BatchSize, "--interval", $IntervalSeconds
) -PassThru -NoNewWindow

$copied = @{}
while (-not $producer.HasExited -or (Get-ChildItem -LiteralPath $hostPath -Filter "*.jsonl" -ErrorAction SilentlyContinue)) {
    $files = @(Get-ChildItem -LiteralPath $hostPath -Filter "*.jsonl" -ErrorAction SilentlyContinue)
    foreach ($file in $files) {
        if (-not $copied.ContainsKey($file.Name)) {
            Write-Host "[5/6] Landing $($file.Name) in HDFS"
            $containerFile = "/data_host/$($hostRelative.Replace('\', '/'))/$($file.Name)"
            docker exec namenode hdfs dfs -put -f $containerFile "$hdfsIncoming/" | Out-Host
            $copied[$file.Name] = $true
        }
    }
    if (-not $producer.HasExited) {
        Start-Sleep -Seconds 2
    } else {
        break
    }
}

Write-Host "[6/6] HDFS output and checkpoint"
Start-Sleep -Seconds 15
docker exec namenode hdfs dfs -ls -R $hdfsOutput | Out-Host
docker exec namenode hdfs dfs -ls -R $hdfsCheckpoint | Out-Host
Write-Host "Demo run $runId finished. Stream paths: $hdfsRoot"
