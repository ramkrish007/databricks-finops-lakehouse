# Databricks FinOps Lakehouse

End-to-End FinOps Analytics Platform built on Databricks Lakehouse architecture using Delta Lake, Unity Catalog, Workflows, SQL Dashboards, Terraform, Asset Bundles and GitHub Actions.

<img width="871" height="472" alt="Databricks_Project drawio" src="https://github.com/user-attachments/assets/8e52ad33-25b7-42a6-8526-d4bc0a5ab943" />

## Project Overview

Cloud cost management (FinOps) has become a critical challenge for modern organizations operating at scale.

This project demonstrates an end-to-end Databricks Lakehouse implementation that ingests cloud usage data, applies business transformations, generates optimization insights, and exposes executive dashboards for FinOps reporting.

The solution follows the Medallion Architecture (Bronze → Silver → Gold) and leverages Delta Lake capabilities such as MERGE, OPTIMIZE, ACID Transactions and Time Travel.

The platform also incorporates Infrastructure as Code (Terraform), Databricks Asset Bundles, and GitHub Actions to demonstrate modern Data Engineering and DevOps practices.

## Project Highlights

| Feature | Implementation |
|----------|---------------|
| Architecture | Bronze, Silver, Gold Medallion Architecture |
| Storage Format | Delta Lake |
| Processing Engine | PySpark |
| Metadata Management | Unity Catalog |
| Workflow Orchestration | Databricks Workflows |
| Analytics Layer | Databricks SQL Dashboard |
| Infrastructure as Code | Terraform |
| Deployment | Databricks Asset Bundles |
| CI/CD | GitHub Actions |
| Delta Lake Features | MERGE, OPTIMIZE, ACID Transactions, Time Travel |

## Technology Stack

| Layer | Technology |
|---------|------------|
| Cloud Analytics Platform | Databricks |
| Data Processing | PySpark |
| Storage Layer | Delta Lake |
| Metadata Governance | Unity Catalog |
| Workflow Orchestration | Databricks Workflows |
| Dashboard & Reporting | Databricks SQL |
| Infrastructure Provisioning | Terraform |
| Deployment Automation | Databricks Asset Bundles |
| Version Control | GitHub |
| CI/CD | GitHub Actions |

## Data Architecture

The solution follows the Medallion Architecture pattern, which progressively refines data through Bronze, Silver, and Gold layers.

### Bronze Layer

The Bronze layer ingests raw cloud usage data into Delta tables without applying business transformations.

Key Responsibilities:

- Raw data ingestion
- Schema enforcement
- Historical data retention
- Delta table creation

### Silver Layer

The Silver layer enriches and transforms the raw data into business-ready datasets.

Key Responsibilities:

- Dimension modeling
- Resource enrichment
- Department mapping
- Fact table creation
- Business transformations

### Gold Layer

The Gold layer provides curated business-level datasets optimized for reporting and analytics.

Gold Tables:

- gold_department_spend_summary
- gold_department_waste_summary
- gold_environment_cost_summary
- gold_service_cost_summary
- gold_underutilized_resources

These tables serve as the foundation for executive reporting and FinOps dashboards.

