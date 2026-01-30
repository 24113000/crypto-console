param(
    [string]$ConfigDir = "config_dev",
    [string]$JarPath = "build\\libs\\crypto-console-0.1.0.jar"
)

if (-not (Test-Path -Path $JarPath)) {
    Write-Error "Jar not found at '$JarPath'. Build it first (e.g., .\\gradlew.bat bootJar)."
    exit 1
}

& java @("-Dapp.config.dir=$ConfigDir", "-jar", $JarPath)
