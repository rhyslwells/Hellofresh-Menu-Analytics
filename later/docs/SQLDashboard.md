
## 3. Delta Lake Time Travel and Data Quality

#delta #evaluation #data_quality

**Objective**
Explore why Delta matters beyond storage.

**Project**
Introduce an error into your data, then recover using time travel.

**Steps**

* Append incorrect rows.
* Query older versions using `VERSION AS OF`.
* Add simple validation rules (null checks, ranges).

**Key Concepts**
$Delta\ Time\ Travel$, ACID guarantees, data validation patterns.

**Deliverable**

* Notebook demonstrating rollback and version comparison

---

## 4. Exploratory Analysis in Databricks SQL

#analysis #evaluation

**Objective**
Bridge engineering output to analytical consumption.

**Project**
Build a small SQL dashboard on top of your Delta table.

**Steps**

* Create views using Databricks SQL.
* Write aggregations (daily totals, distributions).
* Build a dashboard with scheduled refresh.

**Key Concepts**
SQL Warehouses, views, dashboards, refresh cadence.

**Deliverable**

* One SQL dashboard backed by Delta

---
