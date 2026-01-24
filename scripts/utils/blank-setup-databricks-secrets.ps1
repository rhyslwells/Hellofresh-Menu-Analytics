# ============================================================
# Databricks Secrets Setup Script (Free Account)
# ============================================================
# This script:
# 1. Creates a Databricks-managed secret scope
# 2. Stores an API key securely in that scope
#
# REQUIREMENTS:
# - PowerShell 5.1+ or PowerShell 7+
# - Internet access
# - Valid Databricks Personal Access Token (PAT)
#
# DO NOT COMMIT THIS FILE WITH REAL VALUES
# ============================================================

# -----------------------------
# CONFIGURATION (REPLACE LOCALLY)
# -----------------------------

$DATABRICKS_HOST = "##"
$DATABRICKS_PAT  = "##"

$SECRET_SCOPE = "hfresh_secrets"
$SECRET_KEY   = "api_key"
$SECRET_VALUE = "##"

# -----------------------------
# COMMON HEADERS
# -----------------------------

$Headers = @{
    "Authorization" = "Bearer $DATABRICKS_PAT"
    "Content-Type"  = "application/json"
}

# -----------------------------
# STEP 1: CREATE SECRET SCOPE
# -----------------------------

Write-Host "Creating secret scope '$SECRET_SCOPE'..."

$CreateScopeBody = @{
    scope = $SECRET_SCOPE
} | ConvertTo-Json

try {
    Invoke-RestMethod `
        -Method Post `
        -Uri "$DATABRICKS_HOST/api/2.0/secrets/scopes/create" `
        -Headers $Headers `
        -Body $CreateScopeBody

    Write-Host "Secret scope created (or already exists)."
}
catch {
    if ($_.Exception.Response.StatusCode.Value__ -eq 400) {
        Write-Host "Scope already exists. Continuing..."
    }
    else {
        throw $_
    }
}

# -----------------------------
# STEP 2: STORE SECRET VALUE
# -----------------------------

Write-Host "Storing secret key '$SECRET_KEY' in scope '$SECRET_SCOPE'..."

$PutSecretBody = @{
    scope         = $SECRET_SCOPE
    key           = $SECRET_KEY
    string_value  = $SECRET_VALUE
} | ConvertTo-Json

Invoke-RestMethod `
    -Method Post `
    -Uri "$DATABRICKS_HOST/api/2.0/secrets/put" `
    -Headers $Headers `
    -Body $PutSecretBody

Write-Host "Secret stored successfully."

# -----------------------------
# STEP 3: VERIFY METADATA ONLY
# -----------------------------

Write-Host "Verifying secret metadata..."

$Secrets = Invoke-RestMethod `
    -Method Get `
    -Uri "$DATABRICKS_HOST/api/2.0/secrets/list?scope=$SECRET_SCOPE" `
    -Headers $Headers

$Secrets.secrets | ForEach-Object {
    Write-Host " - Found key:" $_.key
}

Write-Host "Done."


