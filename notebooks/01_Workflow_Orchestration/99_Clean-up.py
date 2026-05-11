# Databricks notebook source
# MAGIC %md
# MAGIC # 99_Clean-up: Digital Identity Graph Cleanup
# MAGIC
# MAGIC This notebook cleans up all resources created by the Digital Identity Graph workflow, including:
# MAGIC
# MAGIC ## 🗑️ What This Notebook Deletes:
# MAGIC
# MAGIC ### 📊 **Database Objects** (Smart Cleanup):
# MAGIC - **Tables**: Only the specific tables created by this workflow:
# MAGIC   - `{catalog_name}.silver.identity_info_consolidated`
# MAGIC   - `{catalog_name}.silver.email_ifa_pairs` 
# MAGIC   - `{catalog_name}.silver.email_ip_pairs`
# MAGIC   - `{catalog_name}.gold.identity_graph`
# MAGIC - **Schemas**: `bronze`, `silver`, `gold` (only if empty after table deletion)
# MAGIC - **Catalog**: `{catalog_name}` (only if empty after schema deletion)
# MAGIC
# MAGIC ### 📁 **Generated Files**:
# MAGIC - `Identity Graph Health.lvdash.json` (Lakeview dashboard)
# MAGIC - `job_info.json` (workflow job information)
# MAGIC - `catalog_name.json` (configuration file)
# MAGIC
# MAGIC ## ⚠️ **WARNING**: 
# MAGIC This operation is **IRREVERSIBLE**. Identity graph data and dashboards will be permanently deleted.
# MAGIC **SAFE**: Only deletes workflow-specific objects. Preserves other data in shared catalogs/schemas.
# MAGIC
# MAGIC ## 🔧 **Prerequisites**:
# MAGIC - Permissions to drop tables and schemas
# MAGIC - File system access to delete dashboard files
# MAGIC
# MAGIC ## 📋 **Usage**:
# MAGIC Run all cells to perform complete cleanup, or run individual sections for selective cleanup.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Configuration and Setup

# COMMAND ----------

import json
import os
from pathlib import Path

# Two config sources are supported (widget wins so DAB job parameters take precedence over the file):
#   1. Job parameters / notebook widgets `catalog_name` and `schema_prefix` (DAB flow)
#   2. data/catalog_name.json written by 01_Workflow_Orchestration/setup.py (Solution Launcher flow)
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_prefix", "")

current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
current_dir = "/Workspace" + "/".join(current_path.split("/")[:-1])
config_path = f"{current_dir}/data/catalog_name.json"

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_prefix = dbutils.widgets.get("schema_prefix").strip()

if not catalog_name and os.path.exists(config_path):
    print(f"🔍 Loading configuration from: {config_path}")
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        catalog_name = config["catalog_name"]
        if not schema_prefix:
            schema_prefix = config.get("schema_prefix", "")
    except Exception as e:
        print(f"❌ ERROR loading configuration: {e}")
        dbutils.notebook.exit("Failed to load configuration")

if not catalog_name:
    print("❌ ERROR: catalog_name not set. Pass --params catalog_name=<name> or run the Solution Launcher.")
    dbutils.notebook.exit("Configuration missing")

print(f"✅ Loaded catalog name: {catalog_name}")
if schema_prefix:
    print(f"✅ Schema prefix: {schema_prefix}")
    schema_prefix += "_"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Inventory Existing Database Objects
# MAGIC
# MAGIC Before attempting cleanup, let's inventory what exists and identify workflow-specific objects.

# COMMAND ----------

# Define the tables and schemas created by this workflow.
# Note: bronze.impression_logs_prod is NOT in this list — that table is supplied via
# Delta Share from an upstream producer and cleanup must never drop it.
workflow_tables = {
    "silver": ["identity_info_consolidated", "email_ifa_pairs", "email_ip_pairs"],
    "gold": ["identity_graph"]
}
workflow_schemas = ["bronze", "silver", "gold"]

print(f"🔍 Checking catalog '{catalog_name}' and workflow objects...")

catalog_exists = False
existing_schemas = []
existing_tables = {}
workflow_objects_found = {}

try:
    # Check if catalog exists
    catalogs_df = spark.sql("SHOW CATALOGS")
    existing_catalogs = [row.catalog for row in catalogs_df.collect()]
    
    catalog_exists = catalog_name in existing_catalogs
    
    if catalog_exists:
        print(f"✅ Catalog '{catalog_name}' found")
        
        # Get all schemas in the catalog
        try:
            schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
            existing_schemas = [row.databaseName for row in schemas_df.collect()]
            print(f"   📂 All schemas: {existing_schemas}")
            
            # Check each workflow schema for tables
            for schema in workflow_schemas:
                prefixed_schema = f"{schema_prefix}{schema}"
                if prefixed_schema in existing_schemas:
                    try:
                        tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{prefixed_schema}")
                        schema_tables = [row.tableName for row in tables_df.collect()]
                        existing_tables[schema] = schema_tables
                        
                        # Find workflow tables in this schema
                        workflow_tables_in_schema = workflow_tables.get(schema, [])
                        found_workflow_tables = [t for t in workflow_tables_in_schema if t in schema_tables]
                        other_tables = [t for t in schema_tables if t not in workflow_tables_in_schema]
                        
                        workflow_objects_found[schema] = found_workflow_tables
                        
                        print(f"   📋 {prefixed_schema} schema:")
                        if found_workflow_tables:
                            print(f"      🎯 Workflow tables: {found_workflow_tables}")
                        if other_tables:
                            print(f"      📦 Other tables: {other_tables}")
                        if not schema_tables:
                            print(f"      📭 Empty schema")
                            
                    except Exception as e:
                        print(f"   ⚠️  Could not list tables in {prefixed_schema}: {str(e)}")
                        existing_tables[schema] = []
                        workflow_objects_found[schema] = []
                else:
                    print(f"   📂 {prefixed_schema} schema: Not found")
                    
        except Exception as e:
            print(f"   ⚠️  Could not list schemas: {str(e)}")
    else:
        print(f"ℹ️  Catalog '{catalog_name}' not found - nothing to clean up in database")
        
except Exception as e:
    print(f"❌ ERROR checking catalog existence: {str(e)}")
    catalog_exists = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Smart Database Cleanup
# MAGIC
# MAGIC This performs selective cleanup - only removing workflow-specific objects and empty containers.

# COMMAND ----------

deleted_tables = []
deleted_schemas = []
catalog_deleted = False

if catalog_exists:
    print(f"🗑️  Starting smart cleanup of '{catalog_name}'...")
    
    # Step 3.1: Delete workflow-specific tables
    print("\n📋 Deleting workflow tables...")
    for schema_name in workflow_schemas:
        if schema_name in workflow_objects_found:
            prefixed_schema_name = f"{schema_prefix}{schema_name}"
            tables_to_delete = workflow_objects_found[schema_name]
            for table_name in tables_to_delete:
                try:
                    full_table_name = f"{catalog_name}.{prefixed_schema_name}.{table_name}"
                    print(f"   🗑️  Dropping table: {full_table_name}")
                    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                    deleted_tables.append(full_table_name)
                    print(f"   ✅ Deleted: {full_table_name}")
                except Exception as e:
                    print(f"   ❌ ERROR deleting table {full_table_name}: {str(e)}")
    
    # Step 3.2: Check and delete empty schemas
    print("\n📂 Checking for empty schemas to delete...")
    for schema_name in workflow_schemas:
        prefixed_schema_name = f"{schema_prefix}{schema_name}"
        if prefixed_schema_name in existing_schemas:
            try:
                # Re-check if schema is now empty
                tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{prefixed_schema_name}")
                remaining_tables = [row.tableName for row in tables_df.collect()]
                
                if not remaining_tables:  # Schema is empty
                    print(f"   🗑️  Dropping empty schema: {catalog_name}.{prefixed_schema_name}")
                    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{prefixed_schema_name}")
                    deleted_schemas.append(f"{catalog_name}.{prefixed_schema_name}")
                    print(f"   ✅ Deleted empty schema: {catalog_name}.{prefixed_schema_name}")
                else:
                    print(f"   📦 Keeping schema {catalog_name}.{prefixed_schema_name} (contains {len(remaining_tables)} other tables)")
            except Exception as e:
                print(f"   ❌ ERROR checking/deleting schema {prefixed_schema_name}: {str(e)}")
    
    # Step 3.3: Check and delete catalog if empty
    print(f"\n🗄️  Checking if catalog '{catalog_name}' is empty...")
    try:
        schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
        remaining_schemas = [row.databaseName for row in schemas_df.collect()]
        
        # Filter out system schemas that might exist
        user_schemas = [s for s in remaining_schemas if s not in ['information_schema', 'default']]
        
        if not user_schemas:  # Catalog is empty (except system schemas)
            print(f"   🗑️  Dropping empty catalog: {catalog_name}")
            spark.sql(f"DROP CATALOG IF EXISTS {catalog_name}")
            catalog_deleted = True
            print(f"   ✅ Deleted empty catalog: {catalog_name}")
        else:
            print(f"   📦 Keeping catalog '{catalog_name}' (contains {len(user_schemas)} other schemas: {user_schemas})")
    except Exception as e:
        print(f"   ❌ ERROR checking/deleting catalog: {str(e)}")
        
else:
    print(f"⏭️  Skipping database cleanup - catalog '{catalog_name}' does not exist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Delete Generated Dashboard Files
# MAGIC
# MAGIC Remove the Lakeview dashboard file and job information file created by the workflow.

# COMMAND ----------

# Define file paths for cleanup
dashboard_dir = current_dir.replace("/01_Workflow_Orchestration", "/02_Dashboard_Insights")
dashboard_file = f"{dashboard_dir}/Identity Graph Health.lvdash.json"
job_info_file = f"{current_dir}/data/job_info.json"
catalog_config_file = f"{current_dir}/data/catalog_name.json"

files_to_delete = [
    {
        "path": dashboard_file,
        "description": "Identity Graph Health Dashboard (Lakeview)"
    },
    {
        "path": job_info_file, 
        "description": "Workflow Job Information"
    },
    {
        "path": catalog_config_file,
        "description": "Catalog Configuration File"
    }
]

print("🗑️  Deleting generated files...")

deleted_files = []
skipped_files = []

for file_info in files_to_delete:
    file_path = file_info["path"]
    description = file_info["description"]
    
    print(f"   🔍 Checking: {description}")
    print(f"      Path: {file_path}")
    
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"   ✅ Deleted: {description}")
            deleted_files.append(description)
        else:
            print(f"   ⏭️  File not found: {description}")
            skipped_files.append(description)
    except Exception as e:
        print(f"   ❌ ERROR deleting {description}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Cleanup Summary
# MAGIC
# MAGIC Display a summary of all cleanup operations performed.

# COMMAND ----------

print("=" * 60)
print("🧹 CLEANUP SUMMARY")
print("=" * 60)

# Database cleanup summary
if catalog_exists:
    print(f"✅ Database Objects:")
    if deleted_tables:
        print(f"   • Deleted {len(deleted_tables)} workflow tables:")
        for table in deleted_tables:
            print(f"     - {table}")
    else:
        print(f"   • No workflow tables found to delete")
        
    if deleted_schemas:
        print(f"   • Deleted {len(deleted_schemas)} empty schemas:")
        for schema in deleted_schemas:
            print(f"     - {schema}")
    else:
        print(f"   • No empty schemas deleted (other objects present)")
        
    if catalog_deleted:
        print(f"   • Deleted empty catalog: {catalog_name}")
    else:
        print(f"   • Catalog preserved (contains other objects)")
else:
    print(f"ℹ️  Database Objects:")
    print(f"   • No catalog found to clean up")

print()

# File cleanup summary
if deleted_files:
    print(f"✅ Files Deleted ({len(deleted_files)}):")
    for file_desc in deleted_files:
        print(f"   • {file_desc}")
else:
    print(f"ℹ️  Files Deleted: None")

if skipped_files:
    print(f"⏭️  Files Skipped ({len(skipped_files)}):")
    for file_desc in skipped_files:
        print(f"   • {file_desc}")

print()
print("=" * 60)

# Final status
total_db_operations = len(deleted_tables) + len(deleted_schemas) + (1 if catalog_deleted else 0)
total_operations = total_db_operations + len(deleted_files)

if total_operations > 0:
    print("🎉 CLEANUP COMPLETE!")
    print(f"   Successfully cleaned up {total_operations} items")
    if total_db_operations > 0:
        print("   Digital Identity Graph workflow objects removed from database.")
    if len(deleted_files) > 0:
        print("   Generated files and configuration cleaned up.")
    print("   ✅ Smart cleanup preserved other data in shared resources.")
else:
    print("ℹ️  CLEANUP COMPLETE!")
    print("   No workflow items found to clean up - workspace was already clean.")

print()
print("💡 To rebuild the identity graph, run the workflow notebooks in order:")
print("   1. 00_Create_Catalog.py")
print("   2. 01_Create Consolidated Identity Info.py") 
print("   3. 02a_Create Email IFA Paired Table.py")
print("   4. 02b_Create Email IP Paired Table.py")
print("   5. 03_Create Identity Graph.py")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Cleanup Complete!
# MAGIC
# MAGIC All Digital Identity Graph resources have been cleaned up. The workspace is now ready for:
# MAGIC
# MAGIC ### ✅ What Was Removed:
# MAGIC - **Database Objects**: Only workflow-specific tables, empty schemas, and empty catalogs
# MAGIC - **Dashboard Files**: Lakeview dashboard configuration  
# MAGIC - **Configuration**: Catalog configuration and job metadata files
# MAGIC
# MAGIC ### 🔄 To Rebuild:
# MAGIC Simply re-run the workflow notebooks in sequence to recreate the identity graph.
# MAGIC
# MAGIC ### 💾 Data Recovery:
# MAGIC If you need to recover deleted data:
# MAGIC - Check if Delta table time travel is available (if deletion was recent)
# MAGIC - Restore from backups if available
# MAGIC - Re-run the data generation notebooks to recreate sample data
