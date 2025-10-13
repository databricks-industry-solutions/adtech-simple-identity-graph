# Databricks notebook source
# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --quiet --upgrade databricks-sdk
# %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
if "catalog_name" not in locals():
  catalog_name = dbutils.widgets.get("catalog_name")

catalog_info_html = f"<p><strong>Current Catalog:</strong> <code>{catalog_name}</code></p>"

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings as Job

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

host_name = w.config.hostname
workspace_name = host_name.split('.')[0]
current_user = w.current_user.me().user_name
current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
current_dir = "/Workspace"+"/".join(current_path.split("/")[:-1])
workflow_dir = current_dir.split("01_Workflow_Orchestration")[0] + "/01_Workflow_Orchestration"
current_workspace_txt = f"""
Current Workspace: {workspace_name}
Current User: {current_user}
Current Directory: {current_dir}
Workflow Directory: {workflow_dir}
"""

workspace_info_html = f"""
<div style='background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #007bff;'>
<h4 style='color: #007bff; margin-top: 0;'>Workspace Information</h4>
<pre style='margin: 0; font-family: monospace;'>{current_workspace_txt.strip()}</pre>
</div>
"""

# Combine catalog and workspace info
setup_info_html = catalog_info_html + workspace_info_html
displayHTML(setup_info_html)

# COMMAND ----------

catalog_info = {"catalog_name": catalog_name}
with open(f"{workflow_dir}/data/catalog_name.json", "w") as f:
    json.dump(catalog_info, f)

# COMMAND ----------

with open(f"{workflow_dir}/job_definition.json") as f:
  job_txt = f.read()

# COMMAND ----------

job_json = json.loads(job_txt)

# COMMAND ----------

current_settings = {
  "current_user": current_user,
  "workflow_notebooks": workflow_dir
}

# COMMAND ----------

for k, v in current_settings.items():
    job_txt = job_txt.replace(f"{{{{{k}}}}}", v)

# COMMAND ----------

job_json = json.loads(job_txt)

# COMMAND ----------

Create_Consolidated_Identity_Info = Job.from_dict(
  job_json
)


# w.jobs.reset(new_settings=Create_Consolidated_Identity_Info, job_id=995720661562833)
# or create a new job using: 
# 
new_job_settings = w.jobs.create(**Create_Consolidated_Identity_Info.as_shallow_dict())

# COMMAND ----------

job_id = new_job_settings.job_id

# COMMAND ----------

job_url = f"https://{host_name}/jobs/{job_id}"
job_confirm_html = f"""
<div style='background-color: #fff3cd; padding: 15px; border-radius: 8px; border-left: 4px solid #ffc107;'>
<h4 style='color: #856404; margin-top: 0;'>Job Created Successfully</h4>
<p>Your Identity Graph Job has been created.</p>
<p>Job ID: {job_id}</p>
<p>Click this link to view: <a href="{job_url}">{job_url}</a></p>
</div>
"""
displayHTML(job_confirm_html)

# COMMAND ----------

job_info = {"job_id": job_id}
with open(f"{workflow_dir}/data/job_info.json", "w") as f:
    json.dump(job_info, f)

# COMMAND ----------

# Run the job
run_response = w.jobs.run_now(job_id=job_id)
run_id = run_response.run_id

job_run_html = f"""
<div style='background-color: #e8f5e8; padding: 15px; border-radius: 8px; border-left: 4px solid #28a745;'>
<h4 style='color: #28a745; margin-top: 0;'>Job Started</h4>
<p>Job Run ID: {run_id}</p>
<p>Monitor the job progress at: <a href="{job_url}/runs/{run_id}">{job_url}/runs/{run_id}</a></p>
</div>
"""
displayHTML(job_run_html)
