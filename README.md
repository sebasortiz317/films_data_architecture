# films_data_architecture

# AWS Glue Films Database ETL Pipeline

## Overview

This repository contains an **AWS Glue** ETL (Extract, Transform, Load) pipeline designed to process and transform data from an Excel-based films database stored in Amazon S3. The pipeline ensures data quality through validation and transformation before loading the processed data back into S3 and cataloging it in AWS Glue Data Catalog.

## Features

- **Modular Architecture:** Organized into separate Python modules for easy maintenance and scalability.
- **Data Quality Checks:** Validates data types, trims whitespace, handles null values, and logs data quality issues.
- **Dynamic Transformations:** Uses abstract transformer classes to apply specific transformations to different data sheets.
- **Seamless AWS Integration:** Utilizes AWS Glue for distributed data processing and integrates with Amazon S3 for data storage.

## Project Structure

- **glue_job.py:** Main entry point for the AWS Glue job.
- **transformer.py:** Abstract base class for all data transformers.
- **data_quality_utils.py:** Utility functions for data validation and cleaning.
- **\*_transformer.py:** Specific transformer classes for each data sheet (`film`, `inventory`, `rental`, `customer`, `store`).
- **excel_sheet_loader.py:** Class responsible for loading Excel sheets from S3.
- **films_database_loader.py:** Orchestrates the loading, transformation, and saving of all data sheets.

## Setup

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/tu-usuario/aws-glue-films-etl.git
   cd aws-glue-films-etl