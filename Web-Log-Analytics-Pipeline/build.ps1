$ErrorActionPreference = "Stop"
$jars = (Get-ChildItem lib/*.jar | Select-Object -ExpandProperty FullName) -join ';'

if (-not (Test-Path target/classes)) { New-Item -ItemType Directory -Path target/classes -Force | Out-Null }
if (-not (Test-Path target/test-classes)) { New-Item -ItemType Directory -Path target/test-classes -Force | Out-Null }

Write-Host "Compiling main sources..."
$mainSources = (Get-ChildItem -Recurse src/main/java/*.java | Select-Object -ExpandProperty FullName)
& javac --release 11 -cp "$jars" -d target/classes $mainSources

Write-Host "Compiling test sources..."
$testSources = (Get-ChildItem -Recurse src/test/java/*.java | Select-Object -ExpandProperty FullName)
& javac --release 11 -cp "target/classes;$jars" -d target/test-classes $testSources

Write-Host "Packaging target/clickstream-mr-1.0.jar..."
& jar cf target/clickstream-mr-1.0.jar -C target/classes .

Write-Host "Build complete."
