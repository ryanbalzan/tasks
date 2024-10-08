# Task 1 - Originations and Payments Processing

This repository contains a PySpark application that processes **originations** and **payments** data using **Delta Lake**. The application can run in **batch** or **streaming** modes and stores the processed data in Delta tables, while also logging ingestion audit information.

## Features

- **Batch and Streaming Modes**: Processes both batch and real-time data.
- **Schema Enforcement**: Uses predefined schemas for originations and payments data.

## Usage
The application can run in batch and streaming mode. To run the application in the desired mode update the docker-compose file. The pipeline will be executed automatically when the docker application is started.

```bash
spark-submit --master spark://spark-master:7077 --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /pipelines/payments.py --mode batch
```


### Directory Structure
The default directory structure for the input data and Delta tables is as follows:
```graphql
/data/
  ├── originations/       # Contains JSON files for originations
  ├── payments/           # Contains JSON files for payments
  ├── delta/              # Delta table storage for fact tables
  └── checkpoints/        # Stores checkpoints for streaming jobs
```

### Visualizing data

Data can be visualized using Jupyter notebeooks. Access the Jupyter application from the following URL:
```bash
http://localhost:8888/lab/
```

The notebook DataAnalysis.ipynb should be available in the work directory. A sample of the SQL that can be used to view and validate the data has been provided.


### Streaming files

To test streaming the files the script stream_files.py can be used to simulate files being saved in the originations and payments folders