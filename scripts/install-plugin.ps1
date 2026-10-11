param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$JMeterHome,
    [Parameter(Position = 1)]
    [string]$PluginJar
)

$ErrorActionPreference = "Stop"
$ProjectDirectory = Split-Path -Parent $PSScriptRoot
if (-not $PluginJar) {
    $PluginJar = Join-Path $ProjectDirectory "target/iotdb-jmeter-plugin.jar"
    if (-not (Test-Path -LiteralPath $PluginJar -PathType Leaf)) {
        $PluginJar = Join-Path $ProjectDirectory "lib/ext/iotdb-jmeter-plugin.jar"
    }
}

$ExtensionDirectory = Join-Path $JMeterHome "lib/ext"
if (-not (Test-Path -LiteralPath (Join-Path $JMeterHome "bin/ApacheJMeter.jar") -PathType Leaf) -or
    -not (Test-Path -LiteralPath $ExtensionDirectory -PathType Container)) {
    throw "Not an Apache JMeter installation: $JMeterHome"
}
if (-not (Test-Path -LiteralPath $PluginJar -PathType Leaf)) {
    throw "Plugin JAR not found: $PluginJar. Build with mvn clean package first."
}

$Destination = Join-Path $ExtensionDirectory "iotdb-jmeter-plugin.jar"
foreach ($OldPlugin in Get-ChildItem -LiteralPath $ExtensionDirectory -File) {
    if (($OldPlugin.Name -like "jmeter-connector*.jar" -or
         $OldPlugin.Name -like "iotdb-jmeter-plugin*.jar") -and
        $OldPlugin.Name -ne "iotdb-jmeter-plugin.jar") {
        throw "Remove the older plugin before installing: $($OldPlugin.FullName)"
    }
}

$TemporaryJar = Join-Path $ExtensionDirectory (".iotdb-jmeter-plugin." + [guid]::NewGuid())
try {
    Copy-Item -LiteralPath $PluginJar -Destination $TemporaryJar
    Move-Item -LiteralPath $TemporaryJar -Destination $Destination -Force
} finally {
    if (Test-Path -LiteralPath $TemporaryJar) {
        Remove-Item -LiteralPath $TemporaryJar -Force
    }
}
Write-Output "Installed $Destination"
Write-Output "Restart JMeter to load the plugin."
