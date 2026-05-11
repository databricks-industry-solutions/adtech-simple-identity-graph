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
# MAGIC - Access to the impression-logs source table via Delta Share (lands at `<catalog>.<prefix>bronze.impression_logs_prod`)

# COMMAND ----------

# Load catalog name and schema prefix.
# Two sources are supported (widget wins so DAB job parameters take precedence over the file):
#   1. Job parameters / notebook widgets `catalog_name` and `schema_prefix` (DAB flow)
#   2. ./data/catalog_name.json written by 01_Workflow_Orchestration/setup.py (Solution Launcher flow)
import json
import os

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_prefix", "")

current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
current_dir = "/Workspace" + "/".join(current_path.split("/")[:-1])
config_path = f"{current_dir}/data/catalog_name.json"

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_prefix = dbutils.widgets.get("schema_prefix").strip()

if not catalog_name and os.path.exists(config_path):
    with open(config_path, "r") as f:
        config = json.load(f)
    catalog_name = config["catalog_name"]
    if not schema_prefix:
        schema_prefix = config.get("schema_prefix", "")

if not catalog_name:
    raise ValueError(
        "catalog_name is empty. Pass --params catalog_name=<name> to `bundle run`, "
        "or run the Solution Launcher (01_Solution Launcher.py) so setup.py writes the config file."
    )

print(f"✅ Loaded catalog name: {catalog_name}")
if schema_prefix:
    print(f"✅ Schema prefix: {schema_prefix}")
    schema_prefix += "_"

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Ensure the Catalog Exists
# MAGIC
# MAGIC On metastores that use **Default Storage** (no metastore-level storage root),
# MAGIC a bare `CREATE CATALOG IF NOT EXISTS` will fail even when the catalog already
# MAGIC exists — Unity Catalog validates the create statement before checking existence.
# MAGIC We check `SHOW CATALOGS` first and only attempt to create when the catalog is missing.

# COMMAND ----------

existing_catalogs = {row.catalog for row in spark.sql("SHOW CATALOGS").collect()}
if catalog_name in existing_catalogs:
    print(f"✅ Catalog already exists, reusing: {catalog_name}")
else:
    print(f"✅ Creating catalog: {catalog_name}")
    try:
        spark.sql(f"CREATE CATALOG {catalog_name}")
    except Exception as e:
        raise RuntimeError(
            f"Could not create catalog '{catalog_name}'. If this workspace uses Default Storage, "
            f"either (a) create the catalog ahead of time in the UI with Default Storage enabled, "
            f"or (b) pre-create with an explicit storage location: "
            f"`CREATE CATALOG {catalog_name} MANAGED LOCATION '<s3://... or abfss://...>'`. "
            f"Underlying error: {e}"
        ) from e

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create Required Schemas
# MAGIC
# MAGIC This will create our three-tier data architecture for the identity graph workflow.

# COMMAND ----------

for schema_name in required_schemas:
    prefixed_schema_name = f"{schema_prefix}{schema_name}"
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
