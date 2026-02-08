In docs\index.html the charts are not showing correctly.

created by scripts\report_utils.py

and scripts\5_dashboard.py

---

Additionally i think that 
 scripts\report_utils.py
contributes to scripts\4_weekly_report.py and scripts\5_dashboard.py

it might be better to refactor this to scripts\4_report_utils.py and scripts\5_report_utils.py to separate the concerns of weekly report vs dashboard generation.

----
I beleive these issues may be related.

Or maybe its the way the charts are being embedded in the dashboard. in 5_dashboard.py?
