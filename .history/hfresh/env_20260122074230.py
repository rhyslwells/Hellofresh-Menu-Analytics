Step 3 — Correct Setup (Guaranteed Working)
Option A — Session-only (fastest, safest)

Run this before activating the venv:

$env:HFRESH_API_TOKEN=

Then:

What To Do Now (Required)
1. Close PowerShell completely

Do not reuse this window.

2. Open a brand-new PowerShell window
3. Verify the variable exists
echo $env:HFRESH_API_TOKEN


You should now see the token.

# import os

# print(os.environ["HFRESH_API_TOKEN"])
