# Databricks Lakehouse Project

This project demonstrates a classic multi-hop Lakehouse architecture (Bronze → Silver → Gold) using Databricks Asset Bundles for orchestration.

## Architecture

- **Bronze Layer**: Ingests raw data from multiple sources (customers, orders, order_items, products, product_reviews) using Auto Loader and stores it in Delta format.
- **Silver Layer**: Cleans, validates, and enriches the data from the bronze layer.
- **Gold Layer**: Aggregates silver data into business-level tables for analytics and reporting.

## Project Structure

```
databricks_project_lakehouse/
├── databricks.yml              # Main Databricks Asset Bundle configuration
├── deploy.py                   # Deployment script
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── .gitignore                  # Git ignore rules
│
├── config/
│   └── project_config.yml      # Central configuration for all layers
│
├── bronze/
│   └── notebooks/
│       └── 01_ingest_raw_data.py # Parameterized Auto Loader ingestion script
│
├── silver/
│   └── notebooks/
│       └── 02_clean_and_transform.py # Cleaning/transformation script
│
├── gold/
│   └── notebooks/
│       └── 03_aggregate_and_model.py # Aggregation script
│
└── workflows/
    ├── bronze_ingestion_workflow.json # Workflow configuration
    └── README.md                      # Workflow documentation
```

## Configuration

- **`config/project_config.yml`**: Central YAML file for all configuration (data paths, transformation logic).
- **`workflows/bronze_ingestion_workflow.json`**: Complete workflow configuration with job settings, triggers, and task parameters.
- **`.env` file**: For secrets and environment variables like `DATABRICKS_HOST`. Not checked into Git. Use `.env.example` as a template.

## Bronze Layer Workflow

The bronze layer workflow (`lakehouse_bronze_wf`) includes:

- **5 Parallel Tasks**: Loading data for customers, orders, order_items, products, and product_reviews
- **Auto Loader**: Streaming ingestion with schema evolution
- **Serverless Compute**: Cost-effective execution
- **Scheduled Execution**: Daily at 9 PM UTC
- **Parameterized Notebooks**: Reusable ingestion logic

### Data Sources

| Task | Source Path | Target Path | Table Name |
|------|-------------|-------------|------------|
| Customers | `s3://dbx-data-source-files/lakehouse_project/customers/` | `s3://dbx-target-filesystem/bronze/customers` | `customers` |
| Orders | `s3://dbx-data-source-files/lakehouse_project/orders/` | `s3://dbx-target-filesystem/bronze/orders` | `orders` |
| Order Items | `s3://dbx-data-source-files/lakehouse_project/order_items/` | `s3://dbx-target-filesystem/bronze/order_items` | `order_items` |
| Products | `s3://dbx-data-source-files/lakehouse_project/products/` | `s3://dbx-target-filesystem/bronze/products` | `products` |
| Product Reviews | `s3://dbx-data-source-files/lakehouse_project/product_reviews/` | `s3://dbx-target-filesystem/bronze/product_reviews` | `product_reviews` |

## How to Deploy and Run

### Prerequisites

1. **Databricks CLI**: Install and configure
   ```bash
   pip install databricks-cli
   databricks configure
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up Environment**:
   Create a `.env` file from `.env.example` and set your `DATABRICKS_HOST`.

### Deployment Options

#### Option 1: Using the Deployment Script (Recommended)

```bash
# Run the automated deployment script
python deploy.py
```

The script will:
- Validate the bundle configuration
- Deploy to your Databricks workspace
- Optionally run the job immediately

#### Option 2: Manual Deployment

```bash
# Validate the bundle configuration
databricks bundle validate

# Deploy the bundle to your Databricks workspace
databricks bundle deploy

# Run the bronze ingestion job
databricks bundle run lakehouse_bronze_wf
```

### Monitoring

- **Job Runs**: Monitor in Databricks Jobs UI
- **Data Quality**: Check Delta table statistics
- **Performance**: Review cluster metrics and job execution times

## Customization

### Adding New Data Sources

1. **Update `databricks.yml`**: Add a new task with appropriate parameters
2. **Update `workflows/bronze_ingestion_workflow.json`**: Add the new task configuration
3. **Deploy**: Run the deployment script

### Modifying Schedules

Edit the `schedule` section in `databricks.yml`:

```yaml
schedule:
  quartz_cron_expression: "0 0 21 * * ?"  # Daily at 9 PM UTC
  timezone_id: "UTC"
  pause_status: "UNPAUSED"
```

### Environment-Specific Configurations

Create different targets in `databricks.yml`:

```yaml
targets:
  dev:
    default: true
    mode: development
  prod:
    mode: production
    workspace:
      host: "https://your-prod-workspace.cloud.databricks.com"
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Ensure Databricks CLI is properly configured
2. **Permission Errors**: Check workspace and S3 bucket permissions
3. **Schema Evolution**: Auto Loader will handle schema changes automatically
4. **Job Failures**: Check logs in Databricks Jobs UI

### Debug Commands

```bash
# Check bundle status
databricks bundle status

# View job details
databricks jobs list

# Check cluster status
databricks clusters list
```

## Best Practices

- **Version Control**: All configurations are in version control
- **Environment Separation**: Use different targets for dev/prod
- **Monitoring**: Set up alerts for job failures
- **Cost Optimization**: Use serverless compute and spot instances
- **Data Quality**: Implement validation checks in silver layer 