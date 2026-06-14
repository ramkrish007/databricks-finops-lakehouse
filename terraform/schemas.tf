resource "databricks_schema" "bronze" {

  catalog_name = databricks_catalog.finops.name

  name = "bronze"

  comment = "Bronze layer schema"

}

resource "databricks_schema" "silver" {

  catalog_name = databricks_catalog.finops.name

  name = "silver"

  comment = "Silver layer schema"

}

resource "databricks_schema" "gold" {

  catalog_name = databricks_catalog.finops.name

  name = "gold"

  comment = "Gold layer schema"

}