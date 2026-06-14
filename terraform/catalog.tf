resource "databricks_catalog" "finops" {

  name = var.catalog_name

  comment = "FinOps Lakehouse Catalog managed by Terraform"

}