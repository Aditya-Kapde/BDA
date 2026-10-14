$urls = @(
  'https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar',
  'https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.21/commons-compress-1.21.jar',
  'https://repo1.maven.org/maven2/commons-cli/commons-cli/1.5.0/commons-cli-1.5.0.jar'
)
foreach ($u in $urls) {
  $fname = Split-Path $u -Leaf
  $dest = "lib/$fname"
  if (-not (Test-Path $dest)) {
    Write-Host "Downloading $fname..."
    Invoke-WebRequest -Uri $u -OutFile $dest
  }
}
