# Delta Lake Deep Dive

This project demonstrates and explores advanced Delta Lake features, including:
- Delta log functionality and structure
- Delta log commit and rollup
- Viewing statistics stored in Delta logs
- Deletion vectors
- Transaction log exploration

The project uses mocked data and performs multiple iterations of insert, update, and delete operations to generate a rich set of Delta transaction logs for analysis.

## Getting Started

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the main notebook or script to generate and manipulate Delta tables.

## Contents
- `notebooks/` — Example notebooks and scripts for Delta Lake deep dive
- `data/` — (Optional) Mocked data files if needed 