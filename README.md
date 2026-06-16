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

## Workflow Orchestration

The FinOps pipeline is orchestrated using Databricks Workflows. Each task represents a logical stage of the Medallion Architecture and executes sequentially to ensure data quality and dependency management.


<img width="1257" height="322" alt="Screenshot 2026-06-15 at 12 17 05 PM" src="https://github.com/user-attachments/assets/869b9b70-a2e7-4da3-ab20-c33c7f3f561b" />


Pipeline Execution Flow:

1. bronze_ingestion
   - Ingests raw cloud usage data into Bronze Delta tables.

2. silver_dimensions
   - Creates and enriches business dimensions.

3. silver_fact
   - Generates cloud usage fact tables.

4. gold_layer
   - Produces business-ready Gold summary tables for reporting and analytics.

This workflow provides automated orchestration, dependency management, monitoring, and operational visibility for the entire data pipeline.

## Delta Lake Features

The project leverages several Delta Lake capabilities to improve reliability, performance, and governance.

### MERGE Operations

MERGE statements are used to perform incremental updates and upserts into Delta tables while maintaining transactional consistency.


<img width="595" height="393" alt="Screenshot 2026-06-15 at 12 23 28 PM" src="https://github.com/user-attachments/assets/e60066aa-f57f-437d-9bc2-fc38ac2dd340" />


### OPTIMIZE

Delta tables are optimized to improve query performance and reduce file fragmentation.

### ACID Transactions

Delta Lake guarantees atomicity, consistency, isolation, and durability across all write operations.

### Time Travel

Historical versions of Delta tables can be queried and restored using Delta Lake Time Travel capabilities, enabling auditing and recovery scenarios.

## Terraform Infrastructure as Code

Terraform is used to provision and manage Databricks resources as code.

The Terraform configuration defines:

- Databricks Provider
- Unity Catalog Objects
- Catalog Configuration
- Schema Creation
- Environment Variables

<img width="598" height="232" alt="Screenshot 2026-06-15 at 12 20 04 PM" src="https://github.com/user-attachments/assets/0ee581c5-f662-430f-b509-82121453f866" />

Project Structure:

```text
terraform/
├── provider.tf
├── catalog.tf
├── schemas.tf
└── variables.tf
```

## Databricks Asset Bundles

Databricks Asset Bundles are used to package and deploy Databricks resources in a repeatable and version-controlled manner.

The bundle definition is maintained in `databricks.yml` and provides a declarative way to manage workflows, notebooks, deployment targets, and environment configurations.

Asset Bundle Validation:

<img width="613" height="116" alt="Screenshot 2026-06-15 at 12 19 01 PM" src="https://github.com/user-attachments/assets/db9065c2-ae1a-486c-a4a7-5d337fed7d2b" />

Example Commands:

```bash
databricks bundle validate
databricks bundle deploy -t dev
```

Benefits:

* Environment consistency
* Reproducible deployments
* Version-controlled configurations
* Simplified Databricks resource management

Asset Bundles provide a modern deployment framework that aligns with Infrastructure as Code and CI/CD best practices.

## GitHub Actions CI/CD

GitHub Actions is used to automate validation checks whenever code changes are pushed to the repository.

The workflow validates both infrastructure and deployment configurations before changes are promoted.

GitHub Actions Workflow:

<img width="1097" height="468" alt="Screenshot 2026-06-15 at 12 17 34 PM" src="https://github.com/user-attachments/assets/2a0ab2bb-f688-435b-a19f-5142cfddf591" />

CI/CD Flow:

```text
Developer Push
      ↓
GitHub Actions
      ↓
Terraform Validate
      ↓
Asset Bundle Validate
      ↓
Deployment Ready
```

Benefits:

* Automated validation
* Early error detection
* Consistent deployment standards
* Improved developer productivity

This approach ensures that infrastructure and deployment artifacts remain production-ready throughout the development lifecycle.

## Dashboard & Analytics

The final Gold Layer tables power a Databricks SQL Dashboard that provides business stakeholders with actionable FinOps insights.

### Executive Summary

Provides high-level visibility into cloud spending, optimization opportunities, and budget health.

<img width="1205" height="640" alt="Screenshot 2026-06-15 at 12 13 03 PM" src="https://github.com/user-attachments/assets/95a0a7c9-86df-4fe6-bd7f-3f17c872a9ae" />


Key Metrics:

* Total Cloud Spend
* Potential Annual Savings
* Total Resources
* Underutilized Resources
* Departments Over Budget

---

### Department Financials

Analyzes departmental spending patterns and budget utilization.

<img width="1205" height="640" alt="Screenshot 2026-06-15 at 12 13 40 PM" src="https://github.com/user-attachments/assets/c2656342-04c5-4fff-a984-8c8d966b6213" />

<img width="1257" height="421" alt="Screenshot 2026-06-15 at 12 14 04 PM" src="https://github.com/user-attachments/assets/478832c1-17de-49ef-bc0f-e19d9f527388" />

<img width="1257" height="226" alt="Screenshot 2026-06-15 at 12 14 27 PM" src="https://github.com/user-attachments/assets/b08490be-e1c1-493f-9545-5ec95e8e2624" />


Insights:

* Department Spend Rankings
* Budget Status Distribution
* Budget Utilization Analysis
* Budget Variance Tracking

---

### Resource Optimization

Identifies optimization opportunities and potential cost savings.

<img width="1257" height="469" alt="Screenshot 2026-06-15 at 12 15 07 PM" src="https://github.com/user-attachments/assets/05d77d32-2dd7-462c-9a09-30e66ab8247c" />


Insights:

* Underutilized Resources
* Resource Utilization Metrics
* Potential Annual Savings
* Optimization Candidates

---

### Cloud Service Analysis

Provides service-level visibility into cloud consumption patterns.

<img width="1257" height="622" alt="Screenshot 2026-06-15 at 12 15 31 PM" src="https://github.com/user-attachments/assets/2d22c916-b7db-46d9-ab4d-461bc76337bc" />

<img width="1257" height="378" alt="Screenshot 2026-06-15 at 12 15 47 PM" src="https://github.com/user-attachments/assets/87ba6d08-3b2d-423e-9b6f-2bca5d9965bb" />


Insights:

* Service Cost Distribution
* Cost Concentration Analysis
* Service Spend Rankings
* Resource Cost vs Utilization Analysis

The dashboard enables finance teams, engineering leaders, and FinOps practitioners to make data-driven cloud cost optimization decisions.


## Key Learnings

This project provided hands-on experience with:

- Databricks Lakehouse Architecture
- Delta Lake Optimization Techniques
- Unity Catalog Governance
- Workflow Orchestration
- Databricks SQL Dashboard Development
- Infrastructure as Code with Terraform
- Deployment Automation using Asset Bundles
- CI/CD Implementation using GitHub Actions

The solution demonstrates how modern data engineering and FinOps practices can be combined to build scalable, governed, and analytics-ready cloud cost management platforms.
