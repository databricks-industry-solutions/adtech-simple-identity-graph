# Databricks notebook source
displayHTML("<h1>Dashboard Setup</h1><p>This notebook processes the dashboard template and generates an Identity Graph Health dashboard with the specified catalog name.</p>")

# COMMAND ----------

displayHTML("<h2>Widget Setup</h2>")

# COMMAND ----------

# DBTITLE 1,Widget Setup
# Create widget for catalog name
# dbutils.widgets.text("catalog_name", "", "Catalog Name")

# Get the catalog name from the widget if not already defined
if "catalog_name" not in locals():
    catalog_name = dbutils.widgets.get("catalog_name")

displayHTML(f"<p><strong>Using catalog name:</strong> {catalog_name}</p>")

# COMMAND ----------

displayHTML("<h2>Process Dashboard Template</h2>")

# COMMAND ----------

# DBTITLE 1,Process Dashboard Template
import json
import os

# Define file paths
current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
current_dir = "/Workspace"+"/".join(current_path.split("/")[:-1])
workflow_dir = current_dir.split("02_Dashboard_Insights")[0] + "/01_Workflow_Orchestration"
dashboard_dir = current_dir.split("02_Dashboard_Insights")[0] + "/02_Dashboard_Insights"
template_path = f"{dashboard_dir}/dashboard_template.json"
output_path = f"{dashboard_dir}/Identity Graph Health.lvdash.json"
job_info_path = f"{workflow_dir}/data/job_info.json"

# COMMAND ----------

displayHTML("<h2>Check Job Status</h2>")

# COMMAND ----------

# Check if job_info.json exists and verify the job
job_status_html = ""

if os.path.exists(job_info_path):
    job_status_html += f"<p>Found job info file: <code>{job_info_path}</code></p>"
    
    # Extract job_id from job_info.json
    with open(job_info_path, 'r') as f:
        job_info = json.load(f)
    
    job_id = job_info.get("job_id")
    job_status_html += f"<p>Extracted job_id: <code>{job_id}</code></p>"
    
    if job_id:
        # Import Databricks SDK and check if job exists
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            
            # Try to get the job to verify it exists
            job = w.jobs.get(job_id)
            job_status_html += f"<p style='color: green;'>✅ Job exists: <strong>{job.settings.name}</strong> (ID: {job_id})</p>"
                     
        except Exception as e:
            job_status_html += f"<div style='color: orange;'><p>⚠️ WARNING: Could not verify job existence (ID: {job_id})</p><p>&nbsp;&nbsp;&nbsp;Error: {str(e)}</p><p>&nbsp;&nbsp;&nbsp;Proceeding with dashboard creation anyway...</p></div>"
    else:
        job_status_html += "<p style='color: orange;'>⚠️ WARNING: No job_id found in job_info.json</p>"
else:
    job_status_html = f"<p style='color: blue;'>ℹ️ No job info file found at: <code>{job_info_path}</code>. Please run the workflow first.</p>"

displayHTML(job_status_html)

# COMMAND ----------

displayHTML("<h2>Process Dashboard Template</h2>")

# COMMAND ----------

# Read the dashboard template
with open(template_path, 'r') as f:
    dashboard_data = json.load(f)

displayHTML(f"<p>Loaded dashboard template from: <code>{template_path}</code></p>")

# COMMAND ----------

displayHTML("<h2>Replace Catalog Name Placeholder</h2>")

# COMMAND ----------

# Convert to string for replacement
dashboard_json_str = json.dumps(dashboard_data, indent=2)

# Replace all instances of {{catalog_name}} with the actual catalog name
updated_dashboard_str = dashboard_json_str.replace("{{catalog_name}}", catalog_name)

# Convert back to JSON object
updated_dashboard = json.loads(updated_dashboard_str)

# Create replacement summary
replacement_summary = f"""
<div style='background-color: #f8f9fa; padding: 12px; border-radius: 6px; border-left: 3px solid #28a745;'>
    <p style='margin: 0 0 8px 0;'>Replaced <code>{{{{catalog_name}}}}</code> with: <strong>{catalog_name}</strong></p>
    <p style='margin: 0;'>Number of replacements made: <strong>{dashboard_json_str.count('{{catalog_name}}')}</strong></p>
</div>
"""
displayHTML(replacement_summary)

# COMMAND ----------

displayHTML("<h2>Write Output Dashboard</h2>")

# COMMAND ----------

# Write the updated dashboard to the output file
with open(output_path, 'w') as f:
    json.dump(updated_dashboard, f, indent=2)

file_output_html = f"<p>Dashboard written to: <code>{output_path}</code></p>"

# Verify the file was created and show some stats
if os.path.exists(output_path):
    file_size = os.path.getsize(output_path)
    file_output_html += f"<p>File size: <strong>{file_size}</strong> bytes</p>"
    
    # Show a preview of the generated content
    with open(output_path, 'r') as f:
        content = f.read()
        lines = content.split('\n')
        # Create file statistics and preview
        preview_lines = "\n".join([f"{i+1:2d}: {line}" for i, line in enumerate(lines[:10])])
        file_output_html += f"""
        <div style='background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #6c757d;'>
            <h5 style='color: #495057; margin-top: 0; margin-bottom: 12px;'>📊 File Statistics</h5>
            <p style='margin: 0 0 8px 0;'>Total lines: <strong>{len(lines)}</strong></p>
            <p style='margin: 0 0 12px 0;'><strong>First 10 lines of generated dashboard:</strong></p>
            <pre style='background-color: #f5f5f5; padding: 10px; border-radius: 5px; margin: 0; font-size: 12px;'>{preview_lines}</pre>
        </div>
        """
else:
    file_output_html += "<p style='color: red; font-weight: bold;'>❌ ERROR: Output file was not created!</p>"

displayHTML(file_output_html)

# COMMAND ----------

displayHTML("<h2>Validation</h2>")

# COMMAND ----------

# Validate that the output file is valid JSON and contains expected content
validation_html = ""

try:
    with open(output_path, 'r') as f:
        validation_data = json.load(f)
    
    validation_html += "<p style='color: green;'>✅ Output file is valid JSON</p>"
    
    # Check that no template placeholders remain
    validation_str = json.dumps(validation_data)
    if "{{catalog_name}}" in validation_str:
        validation_html += "<p style='color: orange;'>⚠️ WARNING: Template placeholders still found in output!</p>"
    else:
        validation_html += "<p style='color: green;'>✅ All template placeholders successfully replaced</p>"
    
    # Check for expected structure
    if "datasets" in validation_data and "pages" in validation_data:
        # Create dashboard structure validation summary
        validation_html += f"""
        <div style='background-color: #d4edda; padding: 12px; border-radius: 6px; border-left: 3px solid #28a745;'>
            <p style='color: green; margin: 0 0 8px 0;'>✅ Dashboard structure is valid</p>
            <ul style='margin: 0; padding-left: 20px;'>
                <li>Datasets: <strong>{len(validation_data['datasets'])}</strong></li>
                <li>Pages: <strong>{len(validation_data['pages'])}</strong></li>
            </ul>
        </div>
        """
    else:
        validation_html += "<p style='color: red;'>❌ Invalid dashboard structure</p>"
        
except json.JSONDecodeError as e:
    validation_html = f"<p style='color: red;'>❌ ERROR: Output file is not valid JSON: {e}</p>"
except Exception as e:
    validation_html = f"<p style='color: red;'>❌ ERROR during validation: {e}</p>"

displayHTML(validation_html)

completion_html = "<div style='background-color: #e8f5e8; padding: 15px; border-radius: 10px; border-left: 5px solid #4caf50; margin-top: 20px;'><h3 style='color: #2e7d32; margin: 0;'>🎉 Dashboard processing complete!</h3></div>"
displayHTML(completion_html)
