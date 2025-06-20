# Databricks Data Engineering Project

This project demonstrates how to deploy a simple data engineering job to Databricks Workflows using Databricks Asset Bundles.

## Structure
- `src/main.py`: Main data engineering script
- `conf/bundle.yml`: Databricks Asset Bundle configuration
- `conf/job.yml`: Databricks Job definition
- `requirements.txt`: Python dependencies

## Prerequisites
- Databricks CLI v0.205+ installed and configured
- Access to a Databricks workspace

## Deploy and Run

```sh
# Deploy the bundle
cd databricks_project

databricks bundle deploy

databricks bundle run run_main
```

For more details, see the [Databricks Asset Bundles documentation](https://docs.databricks.com/en/dev-tools/bundles/index.html). 