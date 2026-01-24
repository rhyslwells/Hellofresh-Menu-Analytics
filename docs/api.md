Below is a **single, runnable PowerShell script** designed for **Windows**, using **placeholders only**, and compatible with a **Databricks Free account**.

This is intended to be saved as a `.ps1` file and run locally.


See: scripts\utils\blank-setup-databricks-secrets.ps1
---

#databricks #secrets #security #ml_process #powershell

## PowerShell Script: `setup-databricks-secrets.ps1`

```powershell
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

$DATABRICKS_HOST = "https://adb-XXXXXXXXXXXX.Y.azuredatabricks.net"
$DATABRICKS_PAT  = "dapiXXXXXXXXXXXXXXXXXXXXXXXX"

$SECRET_SCOPE = "hfresh_secrets"
$SECRET_KEY   = "api_key"
$SECRET_VALUE = "HELLOFRESH_API_KEY_VALUE"

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
```

---

## How to Run It

1. Save the file as:

   ```
   setup-databricks-secrets.ps1
   ```

2. Open **PowerShell** in the same directory.

3. If script execution is blocked (common on Windows):

   ```powershell
   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
   ```

4. Run the script:

   ```powershell
   .\setup-databricks-secrets.ps1
   ```

---

## After Running the Script

Use the secret in Databricks:

```python
api_key = dbutils.secrets.get(
    scope="hfresh_secrets",
    key="api_key"
)
```

This is the **only supported read path**.

---

## Notes Specific to Free Tier

* No Secrets UI
* No Key Vault integration
* No service principals
* Jobs must run as the same user who owns the secret

---

## Optional Hardening (Recommended)

After the script succeeds:

* Delete the script **or**
* Replace `$SECRET_VALUE` with a prompt:

```powershell
$SECRET_VALUE = Read-Host -AsSecureString "Enter API key"
```

(Requires small JSON handling change if you want this; I can provide that variant.)

---

If you want next:

* A **prompt-based version** (no secret in file)
* An **idempotent version** with existence checks only
* A **PowerShell + `.env` local fallback pattern** for development
