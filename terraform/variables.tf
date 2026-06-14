variable "databricks_host" {
  description = "Databricks Workspace URL"
  type        = string
}

variable "databricks_token" {
  description = "Databricks Personal Access Token"
  type        = string
  sensitive   = true
}

variable "catalog_name" {
  description = "FinOps Catalog Name"
  type        = string
  default     = "finops"
}