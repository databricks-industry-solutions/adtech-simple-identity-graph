# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Setup for Digital Identity Graph
# MAGIC
# MAGIC This notebook sets up the foundational environment for building a digital identity graph using Databricks Unity Catalog. 
# MAGIC
# MAGIC ## What this notebook does:
# MAGIC 1. **Creates a Unity Catalog** - Establishes the main catalog to organize our identity data
# MAGIC 2. **Sets up Medallion Architecture** - Creates Bronze, Silver, and Gold schemas for data processing layers:
# MAGIC    - **Bronze**: Raw impression logs and source data
# MAGIC    - **Silver**: Cleaned, deduplicated, and enriched identity data  
# MAGIC    - **Gold**: Final identity graph ready for activation and analytics
# MAGIC
# MAGIC ## 🔧 Configuration Instructions
# MAGIC **IMPORTANT**: Update the `catalog_name` variable below to match your desired catalog name before running this notebook.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks workspace with Unity Catalog enabled
# MAGIC - Permissions to create catalogs and schemas
# MAGIC - Access to impression log data (will be generated in subsequent notebooks if using sample data)

# COMMAND ----------

# Load catalog name and schema prefix from configuration
import json
current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
current_dir = "/Workspace"+"/".join(current_path.split("/")[:-1])
print(f"{current_dir}/data/catalog_name.json")
with open(f"{current_dir}/data/catalog_name.json", "r") as f:
    config = json.load(f)
    catalog_name = config["catalog_name"]
    # Schema prefix is optional - if present, it will be prepended to bronze, silver, gold schemas
    # Example: prefix="adtech_" results in schemas like "adtech_bronze", "adtech_silver", "adtech_gold"
    schema_prefix = config.get("schema_prefix", "adtech")

print(f"✅ Loaded catalog name: {catalog_name}")
if schema_prefix:
    print(f"✅ Schema prefix: {schema_prefix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Medallion Architecture Schemas
# MAGIC
# MAGIC We'll create three schemas following the **Medallion Architecture** pattern:
# MAGIC - `bronze` - Raw data ingestion layer
# MAGIC - `silver` - Cleaned and enriched data layer  
# MAGIC - `gold` - Business-ready, aggregated data layer

# COMMAND ----------

required_schemas = ["bronze", "silver", "gold"]
create_catalog_stm = f"CREATE CATALOG IF NOT EXISTS {catalog_name}"

print(f"✅ Creating catalog: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create the Main Catalog

# COMMAND ----------

display(spark.sql(create_catalog_stm))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create Required Schemas
# MAGIC
# MAGIC This will create our three-tier data architecture for the identity graph workflow.

# COMMAND ----------

for schema_name in required_schemas:
    prefixed_schema_name = f"{schema_prefix}_{schema_name}"
    create_schema_stm = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{prefixed_schema_name}"
    print(f"✅ Creating schema: {catalog_name}.{prefixed_schema_name}")
    spark.sql(create_schema_stm)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create Empty Tables from Schema Definitions
# MAGIC
# MAGIC This step creates empty tables based on the JSON schema definitions in the schemas/ directory.
# MAGIC Each schema file name matches its corresponding table name, and table descriptions are extracted from the schema files.

# COMMAND ----------

import json
import os

def json_type_to_spark_type(json_type):
    """Convert JSON schema type to Spark SQL type"""
    # Type mapping dictionary
    type_mapping = {
        "string": "STRING",
        "long": "BIGINT", 
        "integer": "INT",
        "date": "DATE",
        "boolean": "BOOLEAN",
        "double": "DOUBLE"
    }
    
    if isinstance(json_type, dict):
        if json_type.get("type") == "array":
            element_type = json_type_to_spark_type(json_type["elementType"])
            return f"ARRAY<{element_type}>"
        else:
            return "STRING"  # fallback for complex types
    else:
        return type_mapping.get(json_type, "STRING")  # fallback to STRING if type not found

def create_table_from_schema(schema_file_path, table_name, catalog_name, schema_name, schema_prefix=""):
    """Create an empty table from a JSON schema definition"""
    try:
        with open(schema_file_path, "r") as f:
            schema_def = json.load(f)
        
        # Get table description from schema file
        table_description = schema_def.get("description", "")
        
        # Build column definitions
        columns = []
        for field in schema_def["fields"]:
            field_name = field["name"]
            field_type = json_type_to_spark_type(field["type"])
            nullable = "NULL" if field.get("nullable", True) else "NOT NULL"
            comment = field.get("metadata", {}).get("comment", "")
            
            if comment:
                column_def = f"`{field_name}` {field_type} {nullable} COMMENT '{comment}'"
            else:
                column_def = f"`{field_name}` {field_type} {nullable}"
            columns.append(column_def)
        
        # Create the DDL statement with table comment if available
        prefixed_schema_name = f"{schema_prefix}{schema_name}"
        full_table_name = f"{catalog_name}.{prefixed_schema_name}.{table_name}"
        table_comment = f" COMMENT '{table_description}'" if table_description else ""
        
        create_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {','.join(columns)}
        ) USING DELTA{table_comment}
        """
        
        print(f"✅ Creating table: {full_table_name}")
        if table_description:
            print(f"   📋 {table_description}")
        spark.sql(create_table_ddl)
        
    except Exception as e:
        print(f"❌ Error creating table {table_name}: {str(e)}")

# COMMAND ----------

# Define table names and their target schemas (file names now match table names)
table_definitions = [
    {
        "table_name": "email_ifa_pairs",
        "target_schema": "silver"
    },
    {
        "table_name": "email_ip_pairs", 
        "target_schema": "silver"
    },
    {
        "table_name": "identity_info_consolidated",
        "target_schema": "silver"
    },
    {
        "table_name": "identity_graph",
        "target_schema": "gold"
    }
]

# Create tables from schema definitions
schemas_dir = f"{current_dir}/schemas"
for table_def in table_definitions:
    # Schema file name matches table name
    schema_file_path = f"{schemas_dir}/{table_def['table_name']}.json"
    create_table_from_schema(
        schema_file_path, 
        table_def['table_name'],
        catalog_name,
        table_def['target_schema'],
        schema_prefix
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Environment Setup Complete!
# MAGIC
# MAGIC Your environment is now ready for building the digital identity graph. 
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Run `01_Create Consolidated Identity Info` to aggregate raw impression data
# MAGIC 2. Run `02a_Create Email IFA Paired Table` to link emails with device identifiers
# MAGIC 3. Run `02b_Create Email IP Paired Table` to link emails with IP addresses  
# MAGIC 4. Run `03_Create Identity Graph` to generate the final identity graph
# MAGIC
# MAGIC **Catalog Structure Created:**
# MAGIC ```
# MAGIC {catalog_name}/
# MAGIC ├── bronze/     # Raw impression logs
# MAGIC ├── silver/     # Cleaned identity pairings
# MAGIC │   ├── email_ifa_pairs
# MAGIC │   ├── email_ip_pairs  
# MAGIC │   └── identity_info_consolidated
# MAGIC └── gold/       # Final identity graph
# MAGIC     └── identity_graph
# MAGIC ```
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `{catalog_name}.silver.email_ifa_pairs` - Email to IFA device identifier mappings
# MAGIC - `{catalog_name}.silver.email_ip_pairs` - Email to IP address mappings  
# MAGIC - `{catalog_name}.silver.identity_info_consolidated` - Consolidated identity information
# MAGIC - `{catalog_name}.gold.identity_graph` - Final identity graph for activation
