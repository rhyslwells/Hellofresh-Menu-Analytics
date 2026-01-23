
## 5. Feature Table for Machine Learning

#ml #ml_process #feature_engineering

**Objective**
Prepare clean, reusable features for modeling.

**Project**
Convert an analytical table into a feature-style dataset.

**Steps**

* Aggregate raw data into entity-level features.
* Handle missing values.
* Store as a Delta table with documentation.

**Key Concepts**
Feature engineering, reproducibility, separation of raw vs derived data.

**Deliverable**

* Feature table
* Notebook describing feature definitions

---

## 6. First End-to-End $ML$ Model

#ml #evaluation #optimisation

**Objective**
Run a minimal modeling workflow inside Databricks.

**Project**
Train a simple regression or classification model.

**Steps**

* Load feature table.
* Train a baseline model (e.g. logistic regression).
* Evaluate with standard metrics.
* Log runs using MLflow.

**Key Concepts**
$MLflow$, train/test split, metrics, experiment tracking.

**Deliverable**

* Tracked experiment
* Saved model artifact

---
