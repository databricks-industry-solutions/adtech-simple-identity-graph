# Databricks notebook source
# MAGIC %md
# MAGIC ### Solution Launcher
# MAGIC
# MAGIC #### Overview
# MAGIC This solution uses **Databricks Workflows**, logs all tables in **Unity Catalog**, and surfaces the results in an **AI/BI Dashboard**. Each workflow step is backed by notebooks that contain annotated code and detailed explanations.
# MAGIC
# MAGIC
# MAGIC This is meant to be a *simple, but powerful* example—it demonstrates what's possible with Databricks while providing a clear path to extend the solution with additional data sources, transformation logic, or advanced identity resolution techniques.
# MAGIC
# MAGIC
# MAGIC As mentioned earlier, this Solution Launcher will:
# MAGIC 1. Generate the Identity Graph Workflow
# MAGIC 2.  Create the Identity Graph Health Dashboard
# MAGIC
# MAGIC ##### Products used in this solution:
# MAGIC - [Workflows](https://docs.databricks.com/aws/en/jobs/): Orchestrate the graph build end-to-end. Includes scheduling, alerting, and the ability to extend with upstream/downstream tasks (e.g., refreshing dashboards).
# MAGIC - [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/): Govern and explore data. Review sample data, trace ownership and lineage, and optionally enable quality monitoring and usage metrics.
# MAGIC - [AI/BI Dashboards](https://docs.databricks.com/aws/en/dashboards/):  Fast, flexible way to explore the identity graph and monitor its evolution over time.
# MAGIC
# MAGIC We recommend the following journey:
# MAGIC  
# MAGIC > (1) Launch Workflows → Review Tasks → Explore Unity Catalog Table Entries → (2) Launch the AI/BI Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prerequisites 
# MAGIC Before we begin, you will need to confirm you have the following permissions in your Databricks workspace: 
# MAGIC - Ability to create or access a catalog 
# MAGIC - Ability to create a schema 
# MAGIC - Ability to create tables in that schema
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Specify Catalog for Identity Graph
# MAGIC
# MAGIC First, specify the catalog where the example identity graph will be created. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Run Required Libraries
# MAGIC %pip install --quiet --upgrade databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,INPUT HERE
catalog_name = "media_advertising"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 1. Launching the Workflow
# MAGIC
# MAGIC #### Data Architecture 
# MAGIC As mentioned in the Introduction, this solution uses the **medallion architecture** , with data staged across bronze, silver, and gold layers. Breaking the pipeline into these stages makes debugging easier, simplifies logic isolation, and supports incremental enhancements as your identity resolution needs grow.
# MAGIC
# MAGIC The workflow processes the **impression logs** (bronze), by: 
# MAGIC
# MAGIC 1. Consolidating the identities into an **optimized identity-based table** (silver), 
# MAGIC 2. Then, generating proxies at the **individual and household levels** (silver), and 
# MAGIC 3. Finally, all the information is placed together for a quick and consumable **identity graph**.
# MAGIC <img src="../assets/img/medallion-data-arch-annotated.png" style="object-fit:cover; object-position:50% 30%; width:300px; height:300px; zoom:1.2;"></img>
# MAGIC
# MAGIC Tables in this workflow:
# MAGIC | Step |  Table                       | Layer   | Purpose                                        |
# MAGIC | --------------|---------------|--------------|--------------------|
# MAGIC | 0    | Impression Logs              | Bronze  | Raw campaign impression activity with device-level identifiers and metadata. |
# MAGIC | 1    | Intermediate Consolidated Identity Table  | Silver  | Optimized for operational efficiency; aggregates identifiers for multiple use cases.|
# MAGIC | 2    | Individual-Level Proxy Table | Silver  |Stores identity resolution results at the individual level (not queried directly).|
# MAGIC | 2    | Household-Level Proxy Table|Silver  |Stores resolved household identifiers.|
# MAGIC | 3    | Final Identity Graph | Gold |Combines individual and household proxies into a unified, query-ready graph.|
# MAGIC
# MAGIC Future enhancements may include ML-based householding, probabilistic identity resolution, or additional enrichment data.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Click Run Cell on `RUN THIS: Create + Launch Workflow` to launch job creation.
# MAGIC
# MAGIC The workflow will generate the tables required for an identity graph. Each task links to a notebook explaining the logic behind it.
# MAGIC
# MAGIC <img src="../assets/img/db-task-annotated.png" style="object-fit:cover; object-position:50% 30%; width:300px; height:300px; zoom:1.2;"></img>
# MAGIC <br></br>
# MAGIC - To dive deeper, **open the generated job page** and click on a task to open its notebook to review the code + rationale. If you are interested in design principles and things to consider when creating an identity graph, we highly encourage you to review each individual task to better understand key decision points.
# MAGIC
# MAGIC - You can also **explore the resulting tables** in Unity Catalog once the workflow has run at least once.

# COMMAND ----------

# DBTITLE 1,RUN THIS: Create + Launch Workflow
# MAGIC %run "./01_Workflow_Orchestration/setup"  

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 2. Launching the Dashboard
# MAGIC
# MAGIC Once the identity graph workflow has been created, you can generate a dashboard to quickly explore and monitor the results. This dashboard provides summary insights into the identity graph’s scale, coverage, and health, making it easy to validate pipeline success and communicate results to stakeholders.
# MAGIC
# MAGIC Click Run Cell on `RUN THIS: Launch AI/BI Dashboard` to launch dashboard creation.

# COMMAND ----------

# DBTITLE 1,RUN THIS: Launch AI/BI Dashboard
# MAGIC %run "./02_Dashboard_Insights/setup"
