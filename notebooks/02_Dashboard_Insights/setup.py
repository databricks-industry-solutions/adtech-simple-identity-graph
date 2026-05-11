# Databricks notebook source
displayHTML("<h1>Dashboard Setup</h1><p>This notebook renders the Identity Graph Health dashboard from <code>dashboard_template.json</code> using the catalog/schema configuration written by the workflow's setup notebook.</p>")

# COMMAND ----------

# DBTITLE 1,Resolve paths and load workflow configuration
import json
import os

current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
current_dir = "/Workspace" + "/".join(current_path.split("/")[:-1])
workflow_dir = current_dir.split("02_Dashboard_Insights")[0] + "01_Workflow_Orchestration"

template_path = f"{current_dir}/dashboard_template.json"
output_path = f"{current_dir}/Identity Graph Health.lvdash.json"
config_path = f"{workflow_dir}/data/catalog_name.json"
job_info_path = f"{workflow_dir}/data/job_info.json"

paths_html = f"""
<div style='background-color: #f8f9fa; padding: 12px; border-radius: 6px; border-left: 3px solid #6c757d;'>
    <p style='margin: 0 0 4px 0;'>Template:    <code>{template_path}</code></p>
    <p style='margin: 0 0 4px 0;'>Output:      <code>{output_path}</code></p>
    <p style='margin: 0 0 4px 0;'>Config:      <code>{config_path}</code></p>
</div>
"""
displayHTML(paths_html)

# COMMAND ----------

# DBTITLE 1,Load catalog + schema prefix from workflow config
if not os.path.exists(config_path):
    raise FileNotFoundError(
        f"Could not find {config_path}. Run the workflow setup notebook first "
        f"(notebooks/01_Workflow_Orchestration/setup) so it writes the catalog config."
    )

with open(config_path, "r") as f:
    config = json.load(f)

catalog_name = config["catalog_name"]
schema_prefix = config.get("schema_prefix", "")
if schema_prefix:
    schema_prefix += "_"

if not catalog_name:
    raise ValueError(
        f"catalog_name is empty in {config_path}. "
        f"Re-run the workflow setup notebook with a non-empty catalog name."
    )

config_html = f"""
<div style='background-color: #f8f9fa; padding: 12px; border-radius: 6px; border-left: 3px solid #007bff;'>
    <p style='margin: 0 0 4px 0;'>Catalog:       <strong>{catalog_name}</strong></p>
    <p style='margin: 0;'>Schema prefix: <strong>{schema_prefix or '(none)'}</strong></p>
</div>
"""
displayHTML(config_html)

# COMMAND ----------

# DBTITLE 1,Check Job Status (optional)
job_status_html = ""
if os.path.exists(job_info_path):
    with open(job_info_path, "r") as f:
        job_info = json.load(f)
    job_id = job_info.get("job_id")
    if job_id:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            job = w.jobs.get(job_id)
            job_status_html = f"<p style='color: green;'>✅ Identity Graph job exists: <strong>{job.settings.name}</strong> (ID: {job_id})</p>"
        except Exception as e:
            job_status_html = f"<p style='color: orange;'>⚠️ Could not verify job (ID: {job_id}): {e}. Proceeding with dashboard creation anyway.</p>"
    else:
        job_status_html = "<p style='color: orange;'>⚠️ No job_id found in job_info.json.</p>"
else:
    job_status_html = f"<p style='color: blue;'>ℹ️ No job info file at <code>{job_info_path}</code> yet. The dashboard can still be rendered.</p>"

displayHTML(job_status_html)

# COMMAND ----------

# DBTITLE 1,Render dashboard from template
with open(template_path, "r") as f:
    template_str = f.read()

rendered_str = template_str.replace("{{catalog_name}}", catalog_name).replace("{{schema_prefix}}", schema_prefix)

# Validate that all placeholders were substituted
remaining_placeholders = [tok for tok in ("{{catalog_name}}", "{{schema_prefix}}") if tok in rendered_str]
if remaining_placeholders:
    raise RuntimeError(
        f"Template placeholders not replaced: {remaining_placeholders}. "
        f"Check dashboard_template.json for typos."
    )

# Ensure the result is still valid JSON before writing
rendered_dashboard = json.loads(rendered_str)

with open(output_path, "w") as f:
    json.dump(rendered_dashboard, f, indent=2)

n_catalog = template_str.count("{{catalog_name}}")
n_prefix = template_str.count("{{schema_prefix}}")
render_html = f"""
<div style='background-color: #d4edda; padding: 12px; border-radius: 6px; border-left: 3px solid #28a745;'>
    <p style='margin: 0 0 4px 0;'>✅ Rendered dashboard to <code>{output_path}</code></p>
    <p style='margin: 0 0 4px 0;'>Substituted <code>{{{{catalog_name}}}}</code> ({n_catalog}x) → <strong>{catalog_name}</strong></p>
    <p style='margin: 0;'>Substituted <code>{{{{schema_prefix}}}}</code> ({n_prefix}x) → <strong>{schema_prefix or '(none)'}</strong></p>
</div>
"""
displayHTML(render_html)

# COMMAND ----------

# DBTITLE 1,Validate output
with open(output_path, "r") as f:
    validation_data = json.load(f)

errors = []
if "datasets" not in validation_data or "pages" not in validation_data:
    errors.append("Output dashboard JSON is missing required `datasets` or `pages` sections.")

if errors:
    raise RuntimeError("Dashboard validation failed:\n" + "\n".join(errors))

validation_html = f"""
<div style='background-color: #d4edda; padding: 12px; border-radius: 6px; border-left: 3px solid #28a745;'>
    <p style='color: green; margin: 0 0 8px 0;'>✅ Dashboard structure is valid</p>
    <ul style='margin: 0; padding-left: 20px;'>
        <li>Datasets: <strong>{len(validation_data['datasets'])}</strong></li>
        <li>Pages: <strong>{len(validation_data['pages'])}</strong></li>
    </ul>
</div>
"""
displayHTML(validation_html)
displayHTML("<div style='background-color: #e8f5e8; padding: 15px; border-radius: 10px; border-left: 5px solid #4caf50; margin-top: 20px;'><h3 style='color: #2e7d32; margin: 0;'>🎉 Dashboard rendering complete!</h3></div>")
