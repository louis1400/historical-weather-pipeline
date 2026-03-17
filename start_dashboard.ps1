$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$streamlitPath = Join-Path $projectRoot ".venv\Scripts\streamlit.exe"
$appPath = Join-Path $projectRoot "dashboard\app.py"

if (-not (Test-Path $streamlitPath)) {
    Write-Error "Could not find Streamlit at $streamlitPath. Create the virtual environment and install dependencies first."
    exit 1
}

& $streamlitPath run $appPath
