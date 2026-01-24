# Databricks Secrets Setup Guide

## Secure API Key Management

This guide shows how to store your HelloFresh API key in Databricks Secrets (encrypted vault).

## ⚠️ Never Commit API Keys!

**DO NOT** store API keys in:
- ✗ Code files
- ✗ Configuration files
- ✗ Environment variable declarations in scripts
- ✗ Git repository

**DO** store API keys in:
- ✅ **Databricks Secrets** (recommended)
- ✅ GitHub Secrets (for CI/CD)
- ✅ Environment variables at runtime (local dev only)

---

## Option 1: Store in Databricks Secrets (Recommended)

### Step 1: Create Secret Scope

In Databricks workspace, open a **Databricks CLI** terminal or use the Python API:

```bash
# Using Databricks CLI (if installed):
databricks secrets create-scope --scope hfresh_secrets

# Verify:
databricks secrets list-scopes
```

### Step 2: Store Your API Key

```bash
# Store the API key
databricks secrets put --scope hfresh_secrets --key api_key

# This will prompt you to enter your API key securely (won't echo to terminal)
# Paste your HelloFresh API key and press Ctrl+D (or Ctrl+Z on Windows)
```

### Step 3: Verify (Optional)

```bash
# List keys in the scope (won't show values):
databricks secrets list --scope hfresh_secrets

# Output:
# Key name: api_key
# Last updated timestamp: ...
```

### Step 4: Use in Your Scripts

Your scripts now automatically retrieve it:

```python
# In 1_bronze.py:
api_key = dbutils.secrets.get(scope="hfresh_secrets", key="api_key")
# ✅ Key is retrieved securely, never logged or exposed
```

---

## Option 2: Store in Databricks Notebook

If you don't have CLI access:

### Step 1: Create a Setup Notebook

In Databricks workspace, create a new notebook `00_setup_secrets.py`:

```python
# Databricks notebook source

# Create secret scope
dbutils.secrets.createScope("hfresh_secrets")

# Store API key (paste your key below)
dbutils.secrets.put(scope="hfresh_secrets", key="api_key", value="your_api_key_here")

print("✅ Secret stored in hfresh_secrets:api_key")
```

### Step 2: Run the Notebook Once

- Click "Run All"
- Replace `"your_api_key_here"` with your actual API key
- Delete the notebook after running (optional, it's safe to keep)

### Step 3: Verify

```python
# In a new cell:
secret = dbutils.secrets.get(scope="hfresh_secrets", key="api_key")
print(f"Secret stored: {len(secret)} characters")
```

---

## Option 3: Store in Azure Key Vault (Advanced)

If using Azure Databricks:

```python
# Store reference to Azure Key Vault
dbutils.secrets.createScope(
    scope="hfresh_secrets",
    initial_manage_principal="users"
)
```

Then map your API key from Azure Key Vault:

```bash
databricks secrets create-scope --scope hfresh_secrets --backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/xxx/resourcegroups/xxx/providers/microsoft.keyvault/vaults/xxx
```

---

## Using the API Key in Scripts

### Bronze Ingestion Script

No changes needed! The script automatically retrieves it:

```python
# scripts/1_bronze.py automatically does this:

def get_api_key() -> str:
    """Retrieve API key from Databricks Secrets (secure)."""
    api_key = dbutils.secrets.get(scope="hfresh_secrets", key="api_key")
    return api_key

# Then in create_session():
session = create_session()  # Automatically uses secure API key
```

### Running the Script

```python
# In Databricks notebook:
%run ./scripts/1_bronze

# It will:
# 1. Retrieve API key from secrets
# 2. Fetch data from HelloFresh API
# 3. Write to Bronze layer
# ✅ API key never exposed in logs or code
```

---

## Security Best Practices

### ✅ DO:
- Store API keys in Databricks Secrets
- Use separate scopes for different environments (dev/prod)
- Rotate API keys periodically
- Grant secret access to specific users only

### ✗ DON'T:
- Print API keys in logs
- Store in `.env` files committed to Git
- Hardcode in Python scripts
- Pass as notebook parameters
- Share in Slack/Email

---

## Troubleshooting

### Error: "Failed to retrieve API key from Databricks Secrets"

**Solution:** Verify the secret exists:

```python
# In Databricks cell:
try:
    secret = dbutils.secrets.get(scope="hfresh_secrets", key="api_key")
    print("✅ Secret found")
except Exception as e:
    print(f"❌ Error: {e}")
```

If not found, create it:

```python
dbutils.secrets.put(scope="hfresh_secrets", key="api_key", value="your_key_here")
```

### Error: "Secret scope does not exist"

**Solution:** Create the scope:

```python
dbutils.secrets.createScope("hfresh_secrets")
```

### Local Development (Without Databricks)

Use environment variables locally:

```bash
# Set environment variable before running locally:
export HELLOFRESH_API_KEY="your_api_key_here"

# Or in .env file (add to .gitignore):
HELLOFRESH_API_KEY=your_api_key_here
```

Script fallback handles this automatically:

```python
# 1_bronze.py tries:
# 1. Databricks Secrets (in Databricks) ✅
# 2. HELLOFRESH_API_KEY env var (local) ✅
```

---

## Summary

1. **Create scope:** `dbutils.secrets.createScope("hfresh_secrets")`
2. **Store key:** `dbutils.secrets.put(scope="hfresh_secrets", key="api_key", value="YOUR_KEY")`
3. **Use in code:** `dbutils.secrets.get(scope="hfresh_secrets", key="api_key")`
4. **Run pipeline:** `%run ./scripts/1_bronze`

✅ API key is now secure, encrypted, and never exposed!
