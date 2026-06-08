# Enterprise FinOps & ITAM Lakehouse

## Objective

Build an end-to-end Databricks Lakehouse demonstrating:

- Medallion Architecture
- Delta Lake
- Unity Catalog
- Terraform
- Asset Bundles
- GitHub Actions
- FinOps Analytics

---

## Raw Files

### cloud_usage.json

Purpose:
Cloud resource utilization and spend.

Concepts Covered:
- Nested JSON
- Arrays
- Explode
- Schema Evolution
- Rescue Column
- Data Type Conversion
- Incremental Loads

---

### software_licenses.csv

Purpose:
Software license optimization.

Concepts Covered:
- Fact Table
- Broadcast Join
- Aggregations

---

### incidents.json

Purpose:
Downtime and operational cost analysis.

Concepts Covered:
- Nested JSON
- Deduplication
- Window Functions

---

### departments.csv

Purpose:
Department Dimension.

Concepts Covered:
- Dimension Modeling
- Broadcast Join

---

### department_changes.csv

Purpose:
SCD Type 2 Source.

Concepts Covered:
- SCD Type 2
- MERGE INTO
- Window Functions