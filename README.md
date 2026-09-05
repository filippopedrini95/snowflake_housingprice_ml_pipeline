## 1. Project Overview

This project implements an end-to-end machine learning workflow for **residential property price prediction**. Given a set of property characteristics, the system processes the input data and uses a trained machine learning model to predict the property's price. The project covers the complete lifecycle of the data and the ML model, from raw data ingestion and quality checks to data preparation, model training, model versioning and inference.

The main goal of the project, however, is not to develop a solution to a specific real-world housing market problem, but to **design and implement a complete end-to-end data engineering and machine learning pipeline in snowflake**. The housing price prediction task is used as a representative ML use case through which the different components of the pipeline can be integrated and demonstrated in a realistic scenario. The focus is therefore on learning how to use Snowflake to build, automate and integrate the different stages of the workflow rather than on the housing dataset itself.

The pipeline automatically ingests data from **AWS S3 into Snowflake**, performs data quality checks and transformations, prepares the data for machine learning, performs hyperparameter optimization and **distributed model training to support scalable ML workloads**, registers the resulting models in the **Snowflake Model Registry**, and exposes the trained model through a **FastAPI inference API**.




## 2. Architecture

The project is structured as an end-to-end data engineering and machine learning pipeline built around Snowflake. Data is ingested from an external AWS S3 bucket, processed through a sequence of automated data and ML steps, and ultimately used to produce a registered machine learning model that can be accessed through a FastAPI endpoint.

The high-level flow is:

```text
AWS S3
   │
   ▼
Snowpipe
   │
   ▼
Ingestion
(RAW_DATA table)
   │
   ├──────────────────► Data Quality Check
   │                          │
   │                          ▼
   │                    (Quality Report)
   │
   ▼
Data Cleaning
(CLEAN_DATA table)
   │
   ▼
Train / Test Split
(TRAIN_SET and TEST_SET tables)
   │
   ├──────────────────────────────┐
   │                              │
   ▼                              ▼
Preprocessing Pipeline            Hyperparameter Optimization
(PREPROCESSING_PIPELINE           (Best Parameters)
 in Model Registry)                   │
   │                                  │
   └──────────────┬───────────────────┘
                  │
                  ▼
            Model Training
            (XGBOOST_MODEL
             in Model Registry)
                  │
                  ▼
             FastAPI API
                  │
                  ▼
              Prediction
```

The diagram represents the overall data and model flow rather than the underlying Snowflake task tree. Most processing steps are automated through Snowflake Tasks and Stored Procedures, while Hyperparameter Optimization (HPO) is an independent, periodically scheduled process. HPO uses the training data and the registered preprocessing pipeline to identify suitable model hyperparameters, which are subsequently used by the model training step.

### Snowflake Database Structure

The project uses a dedicated Snowflake database, `HOUSING_PRICE_PROJECT`, organized into four schemas with separate responsibilities:

```text
HOUSING_PRICE_PROJECT
│
├── STAGING_LAYER
│   └── Data ingestion and data quality
│
├── CLEANING_LAYER
│   └── Data cleaning and cleaned data
│
├── ML_LAYER
│   └── Train/test split, preprocessing,
│       HPO and model training
│
└── PUBLIC
    └── Tasks and shared infrastructure
```

* **`STAGING_LAYER`** contains the objects responsible for data ingestion and quality control, including the raw data table, Snowpipe-related objects and Stored Procedures for data quality checks.
* **`CLEANING_LAYER`** contains the objects used to clean incoming data and store the resulting cleaned dataset.
* **`ML_LAYER`** contains the machine learning components of the pipeline, including the train/test datasets, Stored Procedures for ML operations, HPO and training results, and the Model Registry artifacts used by the ML workflow.
* **`PUBLIC`** contains the Snowflake Tasks responsible for orchestrating the different stages of the pipeline. It also contains shared infrastructure such as file formats, the external stage pointing to the S3 bucket, and the internal stage containing the Python source files executed by the Stored Procedures.

This separation keeps **data ingestion, data preparation, machine learning and orchestration logically separated**, while allowing the different components to work together as a single automated pipeline.

The following sections describe the individual components and their implementation in more detail.








## 3. Technology Stack

The project combines cloud data engineering, machine learning and API technologies, with **Snowflake** serving as the central data and ML platform.

### Cloud & Data Platform

* **Snowflake** — central platform for data storage, processing, orchestration and machine learning workloads.
* **AWS S3** — external object storage used as the source of the incoming CSV data.
* **Snowpipe** — automated ingestion of newly arrived files from S3 into Snowflake.

### Data Processing & Machine Learning

* **Snowpark** — Python-based data processing framework used to perform transformations directly within Snowflake.
* **Snowpark ML** — machine learning framework used for preprocessing, hyperparameter optimization and model training.
* **XGBoost / `XGBEstimator`** — gradient boosting algorithm used for the housing price regression model. The project uses Snowpark ML's distributed `XGBEstimator`, allowing model training to be distributed across multiple workers.
* **Snowflake ML Jobs** — execution environment used for distributed machine learning workloads, including hyperparameter optimization and model training with `XGBEstimator`.
* **Snowflake Model Registry** — used to store and version both the preprocessing pipeline and the trained XGBoost model, together with their associated metadata and metrics.

### Orchestration & Data Quality

* **Snowflake Tasks** — automate the execution of the different stages of the pipeline.
* **Snowflake Streams** — detect newly inserted or changed data and enable incremental processing.
* **Snowflake Stored Procedures** — encapsulate the Python logic executed by the automated pipeline stages.

### Application Layer

* **FastAPI** — REST API used to expose the trained model for inference.
* **Pydantic** — validates and parses prediction requests received by the API.
* **Uvicorn** — ASGI server used to run the FastAPI application.

### Development & Deployment

* **Python** — primary programming language used for data processing, machine learning, Stored Procedures, deployment scripts and API development.
* **Snowflake CLI** — used to interact with and deploy Snowflake objects from the command line.





## 4. Repository Structure

The repository is organized into separate components for infrastructure deployment, pipeline implementation, machine learning, SQL definitions, experimentation and model inference.

```text
.
├── README.md
├── .env.template
├── data/
├── notebooks/
├── src/
├── sql/
├── deploy.py
├── bootstrap_pipeline.py
├── api.py
├── credentials_template.py
├── pyproject.toml
└── uv.lock
```

### Directories

* **`data/`** — contains the housing price dataset used by the project. The dataset was originally downloaded from Kaggle at https://www.kaggle.com/datasets/ameyac11/housing-price-prediction-dataset. However, the Kaggle dataset repository was removed after the project was completed, and the data are no longer available at that address. Therefore, the CSV file included in this repository is the copy downloaded at the beginning of the project, when the dataset was still available. It is used as the source data for development and experimentation.

* **`notebooks/`** — contains Jupyter notebooks used for exploratory data analysis, development, experimentation and testing of individual components of the pipeline. The notebooks cover topics such as data quality, preprocessing, train/test splitting, hyperparameter optimization, model training and FastAPI deployment.

* **`src/`** — contains the Python implementation of the data engineering and machine learning pipeline. The code is organized into separate modules for data cleaning and machine learning operations.

  * **`src/data_cleaning/`** — contains the logic for data quality checks and data cleaning, including the generation of data quality reports and the preparation of cleaned data.

  * **`src/ML/`** — contains the machine learning components of the pipeline, including train/test splitting, preprocessing, hyperparameter optimization and model training.

* **`sql/`** — contains the SQL scripts used to create and configure the Snowflake infrastructure and pipeline objects. The scripts cover database setup, data ingestion, data quality, cleaning, train/test splitting, preprocessing, hyperparameter optimization, model training and the Snowflake storage integration.

### Main Python Files

* **`deploy.py`** — creates and configures the Snowflake infrastructure required by the project, including the database, schemas, warehouse, tables and other required Snowflake objects.

* **`bootstrap_pipeline.py`** — performs the initial execution of the pipeline required to bootstrap the system. It runs the necessary initial steps to produce the first ML results, including the initial hyperparameter optimization required before model training. Once the initial execution is complete, it activates the Snowpipe, the main task tree and the scheduled HPO task, allowing the pipeline to continue operating automatically.

* **`api.py`** — contains the FastAPI application used to expose the trained model for inference. It handles incoming prediction requests, loads the required preprocessing pipeline and model from the Snowflake Model Registry, and returns the predicted property price.

* **`credentials_template.py`** — contains the configuration used to establish authenticated connections to Snowflake. Before using the project, this file must be completed with the required Snowflake connection details and renamed to `credentials.py` by removing the `_template` suffix.

### Configuration Files

* **`.env.template`** — contains the template for the cronfiguration required to access the AWS S3 bucket used by the project. Before using the project, this file must be completed with the required AWS credentials and configuration values and renamed to `.env` by removing the `.template` suffix.

* **`pyproject.toml`** — defines the Python project configuration and its dependencies.

* **`uv.lock`** — records the resolved versions of the Python dependencies used by the project, ensuring a reproducible development environment.

* **`README.md`** — provides an overview of the project, its architecture, technology stack, repository structure and implementation.






## 5. Prerequisites & Configuration

Before installing and running the project, the following services, tools and configurations are required.

### Snowflake

A **Snowflake account** is required to execute the project.

The user executing the project must have sufficient privileges to create and manage the required Snowflake objects.

### AWS S3

The project uses a **private AWS S3 bucket** as the source of the incoming housing price data.

The bucket is accessed by Snowflake through an **AWS IAM role** and a Snowflake **Storage Integration**. The required AWS configuration consists of:

1. Create an S3 bucket and copy its URL to `S3_BUCKET_URL` in `.env`.

2. Create an IAM policy that allows Snowflake to access the bucket. The policy should grant:

   * `s3:GetBucketLocation` — allows Snowflake to determine the bucket location.
   * `s3:ListBucket` — allows Snowflake to list objects in the bucket.
   * `s3:GetObject` — allows Snowflake to read objects from the bucket.
   * `s3:GetObjectVersion` — allows Snowflake to read specific object versions.

   The `Resource` of the first statement should contain the **ARN of the S3 bucket**, while the second statement should use the same bucket ARN followed by `/*` to refer to the objects stored inside it.

3. Create an **IAM role** and attach the policy created above. During the initial configuration, the role's trust policy can temporarily use the AWS account root principal:

```json
"Principal": {
    "AWS": "arn:aws:iam::<YOUR_ACCOUNT_ID>:root"
}
```

4. Copy the IAM role ARN to `AWS_ROLE_ARN` in `.env`.

During the execution of `deploy.py`, Snowflake creates the Storage Integration and outputs the following values in the terminal:

```text
STORAGE_AWS_IAM_USER_ARN
STORAGE_AWS_EXTERNAL_ID
```

These values are then used to complete the IAM role's **trust policy**, replacing the temporary principal configuration with the AWS IAM user ARN and external ID provided by Snowflake.

The resulting architecture is:

```text
AWS S3 Bucket
      │
      ▼
AWS IAM Role
      │
      ▼
Snowflake Storage Integration
      │
      ▼
Snowflake External Stage
      │
      ▼
Snowpipe
```

### Python Environment

The project requires **Python 3.12** and uses **uv** for environment and dependency management.

After cloning the repository, the required Python environment and dependencies can be installed and synchronized with:

```bash
uv sync
```

The dependencies are defined in `pyproject.toml`, while `uv.lock` records their resolved versions.

### Snowflake CLI

The **Snowflake CLI (****`snow`****)** is required for interacting with Snowflake from the terminal.

A Snowflake CLI connection must be configured with the credentials and connection details of the Snowflake account and set as the **default connection**. This allows the project's command-line operations to use the intended Snowflake account without requiring the connection to be specified for every command.

### Configuration Templates

The repository does not contain the actual credentials required to access Snowflake or AWS.

Instead, it provides configuration templates:

* **`.env.template`** — contains the configuration template for the AWS S3 connection. It must be completed with the appropriate `S3_BUCKET_URL` and `AWS_ROLE_ARN` values and renamed to `.env`.

* **`credentials_template.py`** — contains the template for the Snowflake connection configuration. It must be completed with the required Snowflake connection details and renamed to `credentials.py`.

These files contain environment-specific configuration and must not be replaced with real credentials in the public repository.

### Required Configuration Overview

Before proceeding with the installation and deployment steps, the following should therefore be available:

```text
Snowflake Account
       │
       ├── Snowflake CLI connection
       │
       └── Python connection configuration
       
AWS Account
       │
       ├── S3 Bucket
       └── IAM Role
              │
              └── AWS_ROLE_ARN
                   
.env.template ──► .env
credentials_template.py ──► credentials.py
```

The remaining Snowflake infrastructure, including the database objects, Storage Integration, external stage, Snowpipe and automated pipeline components, is created and configured during the deployment process described in the following sections.





## 6. Installation & Setup

After completing the prerequisites and external service configuration described above, the local environment can be prepared as follows.

### Clone the Repository

Clone the repository and move into the project directory:

```bash
git clone https://github.com/filippopedrini95/snowflake_housingprice_ml_pipeline.

cd snowflake-housingprice-ml-pipeline
```

### Set Up the Python Environment

The project uses **Python 3.12** and **uv** for dependency management. Synchronize the environment using:

```bash
uv sync
```

This creates the project environment and installs the dependencies defined in `pyproject.toml`, using the versions recorded in `uv.lock`.

### Configure the Environment

Create the local configuration files from the provided templates:

```text
.env.template              → .env
credentials_template.py    → credentials.py
```

Complete both files with the configuration values described in the **Prerequisites & Configuration** section.

### Configure Snowflake CLI

Make sure the **Snowflake CLI (`snow`)** is installed and configure a connection to the Snowflake account:

```bash
snow connection add
```

The connection should then be set as the default connection so that the project's CLI commands use it automatically.

At this point, the local environment is ready for the deployment and pipeline initialization steps described in the next section.



## 7. Deployment & Pipeline Initialization

Once the prerequisites have been completed, the Snowflake infrastructure and the initial pipeline execution can be deployed using the two Python scripts provided in the repository.

Before starting, make sure that both **`credentials.py`** and **`.env`** are complete with the required configuration described in the **Prerequisites & Configuration** section.

### deploy.py

`deploy.py` creates and configures the Snowflake infrastructure required by the project, including the database, schemas, warehouse, tables, stages, file formats, Storage Integration, Snowpipe, Streams, Stored Procedures and Tasks.

Before performing the deployment, the script asks a few questions to verify that the required  configuration is correctly set up.

Run the deployment with:

```bash
uv run python deploy.py
```

During the deployment, Snowflake creates the Storage Integration and outputs the `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` values. These values must then be used to complete the AWS IAM role trust policy as described in the **Prerequisites & Configuration** section.

### bootstrap_pipeline.py

After the infrastructure has been deployed and the AWS connection is correctly configured, upload `HousingPricePredictionDataset.csv` to the configured S3 bucket.

`bootstrap_pipeline.py` performs the initial execution required to bootstrap the pipeline. It runs the initial processing steps needed to produce the first machine learning results, including the first hyperparameter optimization required before model training. Once the bootstrap process is complete, it activates Snowpipe, the main Snowflake Task tree and the scheduled HPO process, allowing the pipeline to continue operating automatically.

Before starting, the script also performs a few checks to verify that the Snowflake credentials, S3 connection and initial dataset are ready.

Run the initialization with:

```bash
uv run python bootstrap_pipeline.py
```

### Execution Order

The complete setup process is therefore:

```text
Complete credentials.py and .env
              │
              ▼
       Run deploy.py
              │
              ▼
Complete AWS IAM trust policy
              │
              ▼
Upload dataset to S3
              │
              ▼
  Run bootstrap_pipeline.py
              │
              ▼
     Automated pipeline
```

Following these steps allows the Snowflake infrastructure and pipeline to be recreated from the repository, provided that the required external AWS and Snowflake configuration is available.






## 8. Data Pipeline

The data pipeline is responsible for ingesting incoming housing price data from AWS S3, validating its quality, and incrementally cleaning the data before making it available for the machine learning pipeline.

### Data Ingestion

The source data is stored as CSV files in a private **AWS S3 bucket**. Snowflake accesses the bucket through the configured Storage Integration and external stage.

**Snowpipe** is used to automatically ingest newly arrived files into the `RAW_DATA` table. The pipe uses `COPY INTO` with `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`, allowing the columns in the incoming CSV files to be matched to the corresponding columns in `RAW_DATA` independently of their capitalization.

The ingestion flow is therefore:

```text
AWS S3
   │
   │ new CSV file
   ▼
Snowflake External Stage
   │
   ▼
Snowpipe
   │
   ▼
RAW_DATA table
```

### Incremental Data Quality Checks

Once new data has been ingested into `RAW_DATA`, a **Snowflake Stream** detects the newly inserted records.

The stream is used as the trigger for the data quality process: a Snowflake **Task** executes a Stored Procedure whenever new data is available. The Stored Procedure executes the Python module `data_quality_report.py`, which performs the configured quality checks and stores their results in the `DATA_QUALITY_REPORT` table.

Currently, the data quality process performs a `NULL_COUNT` check. For each incoming file, the report records the number of missing values found in each dataset column. The report is designed to be extensible. `SOURCE_FILE` identifies the file being checked, while `CHECK_TYPE` identifies the type of quality check performed. If additional checks are introduced in the future, the same source file can therefore generate multiple report rows with different `CHECK_TYPE` values.

Conceptually:

```text
New data in RAW_DATA
        │
        ▼
      Stream
        │
        ▼
  Quality Check Task
        │
        ▼
Stored Procedure
        │
        ▼
data_quality_report.py
        │
        ▼
DATA_QUALITY_REPORT
```

The `DATA_QUALITY_REPORT` table therefore acts as a structured record of the quality checks performed on each incoming data batch. The current implementation only checks missing values, but additional validation rules can be added by extending the Python quality-check module.

### Incremental Data Cleaning

After the quality check Task completes, a dependent **cleaning Task** is triggered.

The cleaning process reads the newly ingested records from the `RAW_DATA` Stream rather than scanning the entire `RAW_DATA` table. This ensures that each execution processes only newly arrived data and avoids repeatedly cleaning records that have already been handled.

The cleaning logic performs the following operations:

* removes duplicate rows using `SELECT DISTINCT *`, meaning that rows are considered duplicates only when all feature values are identical;
* removes rows exceeding a configurable maximum number of missing values;
* by default, rows containing more than **10 missing values** are excluded. With 30 features, this corresponds to approximately one third of the available features;
* removes rows where the target variable `PRICE_IN_LAKHS` is missing, since these records cannot contribute to supervised model training.

The cleaned records are then appended to the `CLEAN_DATA` table.

```text
RAW_DATA
   │
   ▼
RAW_DATA Stream
   │
   ▼
Cleaning Task
   │
   ▼
clean_data.py
   │
   ├── Remove duplicates
   ├── Remove rows with too many missing values
   └── Remove rows with missing PRICE_IN_LAKHS
   │
   ▼
CLEAN_DATA
```

This incremental design allows the pipeline to process continuously arriving data without repeatedly scanning and transforming the complete historical dataset.

### Pipeline Orchestration

The ingestion, quality-check and cleaning stages are connected through **Snowflake Tasks**, with each stage depending on the successful completion of the previous one.

The resulting flow is:

```text
AWS S3
   │
   ▼
Snowpipe
   │
   ▼
RAW_DATA
   │
   ▼
Stream
   │
   ▼
Data Quality Task
   │
   ▼
DATA_QUALITY_REPORT
   │
   ▼
Cleaning Task
   │
   ▼
CLEAN_DATA
```

The subsequent **train/test split** operates on `CLEAN_DATA` and is described in the **Machine Learning Pipeline** section.

### Related Files

The main files involved in this stage of the project are:

```text
sql/
├── 1_data_ingestion.sql
├── 2_data_quality_check.sql
└── 3_clean_data.sql

src/
└── data_cleaning/
    ├── clean_data.py
    └── data_quality_report.py
```

The SQL scripts define and configure the corresponding Snowflake objects and orchestration logic, while the Python modules contain the data quality and cleaning logic executed by the Stored Procedures.



## 9. Machine Learning Pipeline

The machine learning pipeline transforms the cleaned data into a trained XGBoost model. The pipeline is designed to keep data processing and model training within Snowflake's managed execution environment wherever possible, avoiding unnecessary transfers of large datasets to Pandas or local memory.

The main stages are:

```text
CLEAN_DATA
     │
     ▼
Train / Test Split
     │
     ├──► TRAIN_SET
     │
     └──► TEST_SET
              │
              ▼
      Preprocessing Pipeline
              │
              ▼
       ┌──────┴──────┐
       │             │
       ▼             ▼
      HPO        Model Training
       │             │
       ▼             ▼
Best Parameters   XGBoost Model
       │             │
       └──────┬──────┘
              ▼
          Evaluation
              │
              ▼
       Model Registry
```

### Train / Test Split

The train/test split is performed incrementally after new data has been added to `CLEAN_DATA`.

A **Snowflake Stream** identifies newly cleaned records, and a dependent Task invokes the corresponding Stored Procedure. The split is performed only on these new records rather than on the entire historical dataset.

An **80/20 split** is applied while the test set is below its configured maximum size. By default, the test set is limited to **5,000 rows**. Once this capacity is reached, newly arriving records are added entirely to the training set.

This produces a stable test set while allowing the training set to continuously grow:

```text
                 New CLEAN_DATA
                       │
                       ▼
                     Stream
                       │
                       ▼
                 Train / Test Split
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
        TRAIN_SET             TEST_SET
       continuously          max 5,000
          grows                rows
```

This approach allows the model to be trained on an increasingly large amount of historical data while maintaining a fixed test set for consistent evaluation.

The maximum test-set size is configurable through the Stored Procedure.


### Preprocessing

The preprocessing stage does not create a permanently preprocessed table. Instead, it **trains a preprocessing pipeline** on the complete current `TRAIN_SET` and stores it in the **Snowflake Model Registry**.

The preprocessing pipeline consists of:

* `SimpleImputer` using the **median** for numerical features;
* `SimpleImputer` using the **most frequent value** for categorical features;
* `OneHotEncoder` for categorical features.

Conceptually:

```text
TRAIN_SET
    │
    ▼
Preprocessing Pipeline
    │
    ├── Numerical → median imputation
    │
    ├── Categorical → most-frequent imputation
    │
    └── Categorical → one-hot encoding
    │
    ▼
PREPROCESSING_PIPELINE
(Model Registry)
```

A new version of `PREPROCESSING_PIPELINE` is registered for each execution, using a timestamp-based version identifier:

```text
run_YYYYMMDD_HHMMSS
```

The same registered preprocessing pipeline is subsequently loaded and applied during both **hyperparameter tuning** and **model training**, ensuring that the same transformations are used consistently across the ML workflow.



### Hyperparameter Optimization

Hyperparameter Optimization (HPO) is executed independently from the regular model-training cycle.

While model training is triggered whenever the preprocessing stage completes, HPO is executed periodically through a separate Task. The current configuration schedules HPO **once per week**.

The tuning process uses Snowflake's **Tuner** together with an **ML Job**, because the model being optimized is `XGBEstimator`, which supports distributed XGBoost training.

The preprocessing pipeline is retrieved from the Model Registry and applied before the tuning process. In particular, the pipeline is trained on the complete training set used for HPO and is then applied to both the training and validation folds generated during cross-validation. This introduces a potential data leakage, because information from the validation folds is indirectly used when fitting the preprocessing transformations. This leakage is acknowledged as a limitation of the current HPO implementation. However, since this stage is used only to identify suitable hyperparameters rather than to produce the final model, and since the number of missing values in the dataset is limited, the resulting impact is considered negligible and acceptable for this phase.

Because HPO is computationally expensive, it does not use the complete training set. Instead, a configurable subset of the training data is used. The default size is **15,000 rows**.

A **3-fold cross-validation** strategy is used by default.

The tunable hyperparameters are:

```text
max_depth
learning_rate
n_estimators
min_child_weight
subsample
```

The Tuner evaluates the different configurations using RMSE, MAE and R². RMSE is used as the primary metric for selecting the best configuration.

In addition to model performance, the implementation explicitly considers the difference between training and validation RMSE. A configuration is excluded when the relative gap exceeds a configurable threshold, which is **10% by default**. Among the remaining configurations, the one with the lowest average validation RMSE is selected.

Conceptually:

```text
TRAIN_SET
    │
    ├── sample 15,000 rows
    │
    ▼
Fit Preprocessing Pipeline
(on the complete HPO training sample)
    │
    ▼
Apply the same pipeline to
training and validation folds
    │
    ▼
3-Fold Cross-Validation
    │
    ├── Trial 1 → hyperparameter set 1
    ├── Trial 2 → hyperparameter set 2
    ├── Trial 3 → hyperparameter set 3
    └── ...
    │
    ▼
Compare Train / Validation RMSE
    │
    ▼
Filter configurations with excessive RMSE gap
    │
    ▼
Select lowest Validation RMSE
    │
    ▼
XGBOOST_BEST_PARAMETERS
```

All tuning results are stored in:

```text
XGBOOST_TUNING_RESULTS
```

while the selected configuration and its metrics are stored in:

```text
XGBOOST_BEST_PARAMETERS
```

The tuning configuration is intentionally exposed near the beginning of `ML_hyperp_tune_entrypoint.py`, making parameters such as the tuning dataset size, number of CV folds, RMSE-gap threshold, search space, number of trials and number of XGBoost workers easy to modify.



### Distributed Model Training

Model training is triggered by the completion of the preprocessing Task and is therefore executed whenever a new preprocessing version is produced.

The training uses the **complete** **` TRAIN_SET`** and the most recent hyperparameter configuration stored in `XGBOOST_BEST_PARAMETERS`.

The preprocessing pipeline is first loaded from the Model Registry and applied to the training data. The resulting data is then passed to `XGBEstimator` for distributed XGBoost training inside a Snowflake **ML Job**.

```text
TRAIN_SET
    │
    ▼
Registered Preprocessing Pipeline
    │
    ▼
Preprocessed Training Data
    │
    ▼
XGBEstimator
    │
    ▼
Distributed Training
    │
    ▼
HOUSING_PRICE_XGBOOST
(Model Registry)
```

The number of workers used by the distributed XGBoost training is configurable and is set to **2 by default**.

The distinction between HPO and training is important: HPO determines **which hyperparameters should be used**, while `XGBEstimator` performs the actual distributed training of the model using those parameters.



### Model Evaluation

After training, the model is evaluated on both the complete test set and a limited sample of the training set.

The test set is used in its entirety to provide a consistent estimate of model performance. The training metrics are instead calculated on a configurable subset of the training data, with **15,000 rows by default**, rather than on the potentially much larger complete training set. This choice was made for scalability reasons, as explained in the **Scalable Execution Strategy** subsection below.

The evaluation includes:

* RMSE;
* MAE;
* R².

The resulting metrics are stored in:

```text
XGBOOST_TRAINING_RESULTS
```

The trained model is simultaneously registered in the Model Registry as:

```text
HOUSING_PRICE_XGBOOST
```

using a timestamp-based version identifier.



### Scalable Execution Strategy

A key design principle throughout the ML pipeline is to avoid loading large datasets into local Python memory.

Where possible, the pipeline uses **Snowpark, Snowflake-managed execution, ML Jobs and distributed training**:

```text
Large Snowflake Dataset
          │
          ▼
   Snowpark / Snowflake
          │
          ├── Train/Test Split
          ├── Preprocessing
          ├── HPO
          └── Distributed Training
```

Pandas is only used where required by specific ML or evaluation APIs, and intentionally limited subsets of data are used in those cases. For example, training-set evaluation uses a limited sample rather than the complete training dataset because it relies on scikit-learn APIs.

This design allows the pipeline to remain practical as the amount of available training data increases, without making the local Python environment the bottleneck.

### Related Files

The main files involved in this stage are:

```text
sql/
├── 4_ML_traintestsplit.sql
├── 5_ML_preprocessing.sql
├── 6_ML_hyperparam_tuning.sql
└── 7_ML_training.sql

src/
└── ML/
    ├── ML_traintest_split.py
    ├── ML_preproc.py
    ├── ML_hyperp_tune_entrypoint.py
    ├── ML_hyperp_tune_func1.py
    ├── ML_hyperp_tune_func2.py
    └── ML_training.py
```

The SQL scripts define the Snowflake Tasks and Stored Procedures that orchestrate the ML stages, while the Python modules contain the implementation of the corresponding processing, tuning and training logic.


## 10. Model Registry

The **Snowflake Model Registry** is used to store, version and retrieve the two main ML artifacts produced by the pipeline:

* **`PREPROCESSING_PIPELINE`** — the trained preprocessing pipeline used to transform the input features before model training and inference.
* **`HOUSING_PRICE_XGBOOST`** — the trained XGBoost model used for housing price prediction.

Both models are registered in the `ML_LAYER` schema of the `HOUSING_PRICE_PROJECT` database. Each execution that produces a new artifact creates a new **model version**, using a timestamp-based identifier:

```text
run_YYYYMMDD_HHMMSS
```

This provides a history of the different preprocessing and model versions produced by the pipeline.

For the final XGBoost model, performance metrics are also stored together with the model version, including RMSE, MAE and R² for both the training and test sets. This allows each registered model version to be associated with its corresponding training run and performance.

The model versions are therefore organized conceptually as:

```text
PREPROCESSING_PIPELINE
├── run_YYYYMMDD_HHMMSS
├── run_YYYYMMDD_HHMMSS
└── ...

HOUSING_PRICE_XGBOOST
├── run_YYYYMMDD_HHMMSS
├── run_YYYYMMDD_HHMMSS
└── ...
```

The implementation currently retrieves the **latest registered version** using the `LAST` version alias. This applies both to the preprocessing pipeline and to the XGBoost model:

```text
PREPROCESSING_PIPELINE → LAST
HOUSING_PRICE_XGBOOST  → LAST
```

`LAST` refers to the most recently registered version; it does **not** necessarily represent the best-performing version. Snowflake also supports a **default version**, which can instead be used when a specific version should be explicitly promoted for production use. The current implementation uses `LAST` for simplicity, but the version-selection logic can be modified to use the default or another specific version if a stricter model-promotion strategy is required.

During inference, the FastAPI application retrieves the required artifacts from the Model Registry rather than storing the trained models directly inside the API code. The registered preprocessing pipeline is applied to the incoming record before the registered XGBoost model generates the prediction.

```text
                    Model Registry
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
PREPROCESSING_PIPELINE          HOUSING_PRICE_XGBOOST
          │                             │
        LAST                          LAST
          │                             │
          └──────────────┬──────────────┘
                         ▼
                      FastAPI
                         │
                         ▼
                     Prediction
```

This separation allows the model lifecycle to remain independent from the API implementation: new versions can be trained and registered without requiring the model itself to be embedded in the application code.




## 11. Model Inference / API

The trained model is exposed through a lightweight **FastAPI** application that provides an HTTP interface for making predictions on new, unlabeled property records.

The API connects to Snowflake and retrieves the latest registered versions of both the preprocessing pipeline and the trained XGBoost model from the **Snowflake Model Registry**. This means that the API does not contain a copy of the model itself: the models remain managed in Snowflake and are retrieved from the registry when the application starts.

The API exposes two endpoints:

* **`POST /prediction`** — receives a property record, preprocesses it using the registered preprocessing pipeline, and returns the predicted housing price.
* **`GET /health`** — simple health-check endpoint used to verify that the API is running and reachable.

The prediction workflow is:

```text
HTTP Request
     │
     ▼
Request Parsing
     │
     ▼
Preprocessing
     │
     ▼
Registered Preprocessing Pipeline
     │
     ▼
XGBoost Model
     │
     ▼
Prediction
     │
     ▼
HTTP Response
```

The input record is validated using a **Pydantic model**, ensuring that the request contains the expected features and data types. The API then converts the validated record into a Pandas DataFrame, applies the preprocessing pipeline retrieved from the Model Registry, and passes the transformed data to the registered XGBoost model for inference.

The response contains the estimated property price:

```json
{
  "price_estim": 123.45
}
```

The API currently retrieves the **`LAST`** version of both the preprocessing pipeline and the XGBoost model. Therefore, whenever a new version is registered, it becomes the version used by the API the next time the application is started. This behavior can be changed in the API configuration if a specific model version or the default version should be used instead.

### Running the API locally

The API can be started locally with:

```bash
uv run uvicorn api:app --reload
```

Once running, the API is available locally and can be tested through its HTTP endpoints.

FastAPI also automatically provides an interactive **Swagger UI** for testing the API and inspecting its endpoints. It can be accessed at:

```text
http://127.0.0.1:8000/docs
```

This provides a convenient interface for submitting test requests to `/pred` and checking the returned predictions without requiring a separate API client.






## 12. Model Performance

The machine learning component of the project evaluates both the **hyperparameter tuning process** and the performance of the final XGBoost model.

It is important to note that **model performance itself was not the primary objective of this project**. The main goal was to design and implement a complete end-to-end data engineering and machine learning pipeline in Snowflake, including data ingestion, data quality, incremental processing, preprocessing, hyperparameter optimization, distributed training, model versioning and inference. The housing price prediction task was therefore used primarily as a realistic use case through which the different components of the pipeline could be integrated.

The hyperparameter values used in the example below should not be considered fixed, optimal or generally applicable. They were selected as a practical demonstration of the tuning workflow and are intentionally limited in scope. Anyone reproducing or adapting this pipeline should define the hyperparameter search space according to the characteristics of their dataset, model, computational resources and business requirements (for further details, see the section 9. Machine Learning Pipeline).

For this reason, the metrics below should be interpreted primarily as an evaluation of the implemented ML workflow rather than as evidence that the resulting model represents a highly optimized solution to the underlying housing-price prediction problem.


### Hyperparameter Optimization Results

The hyperparameter optimization process evaluates different configurations of the XGBoost model using cross-validation. For each configuration, the pipeline records training and validation metrics, the relative difference between training and validation RMSE, the preprocessing pipeline version used, and additional information about the trial.

The selected configuration is stored in `XGBOOST_BEST_PARAMETERS` and is subsequently used by the final model training process.

The hyperparameter values shown below belong to the specific example run documented in this section:

```text
MAX_DEPTH          = 5
LEARNING_RATE      = 0.3
N_ESTIMATORS       = 6
MIN_CHILD_WEIGHT   = 1
SUBSAMPLE          = 1.0
```

The corresponding HPO results were:

```text
TUNING_RUN_ID           20260831_165551
TIMESTAMP               2026-08-31 16:55:51.127145

RMSE_TRAIN_AVG          1590.38
RMSE_VALID_AVG          1688.19
RELATIVE_GAP            6.15%

MAE_TRAIN_AVG           29.60
MAE_VALID_AVG           30.24

R2_TRAIN_AVG            0.8552
R2_VALID_AVG            0.8480

PREPROCESSOR_VERSION    RUN_20260831_094722
TRIAL_ID                7816b_00001
TIME_TOTAL_S            126.45 s
```

The `RELATIVE_GAP` represents the relative difference between the average training and validation RMSE and is used by the HPO process as a simple control against excessive overfitting. Configurations exceeding the configured threshold are excluded from the final selection.

The selected configuration therefore represents the best configuration identified within the specific search space provided to the tuner. It should not be interpreted as the globally optimal configuration for XGBoost or as a fixed recommendation for other datasets.



### Final XGBoost Model

The selected hyperparameters are subsequently used by `XGBOOST_TRAINING_TASK` to train the final XGBoost model.

The resulting model is evaluated on both the training data and the held-out test set. The training metrics are calculated on a configurable sample of the training data, while the test metrics are calculated on the complete test set.

For the training run associated with the HPO configuration above, the results stored in `XGBOOST_TRAINING_RESULTS` were:

> **Note:** The following is a **transposed representation of a single row** from `XGBOOST_TRAINING_RESULTS`, used only to improve readability in the README. In the actual Snowflake table, each field below is a separate column and the values belong to a single horizontal table row.

```text
TRAIN_RUN_ID                         20260831_165921
TIMESTAMP                            2026-08-31 16:59:21.736522
HYPERPARAMETER_SET_ID                20260831_165551

RMSE_TRAIN                           1649.96
RMSE_TEST                            1649.68

MAE_TRAIN                              29.85
MAE_TEST                               29.84

R2_TRAIN                                0.8499
R2_TEST                                 0.8511

TRAIN_SET_SIZE                         30000
TRAIN_METRIC_SAMPLE_SIZE               15000
TEST_SET_SIZE                           5000

TARGET_COLUMN                         PRICE_IN_LAKHS

N_FEATURES_BEFORE_ENCODING                25
N_FEATURES_AFTER_ENCODING                 68

TRAINING_TIME_S                         75.46 s
PREDICTION_TIME_TESTSET_S                 0.006 s
```

The `HYPERPARAMETER_SET_ID` links this training run to the corresponding entry in `XGBOOST_BEST_PARAMETERS`, making it possible to identify which hyperparameter configuration was used to train the model.

The final model achieved an RMSE of approximately **1,650** on both the training sample and the test set, with an R² of approximately **0.85** on both datasets. The very similar training and test metrics indicate that, for this particular run, there is no obvious large discrepancy between training and test performance.

These results should nevertheless be interpreted in the context of the limited and illustrative hyperparameter search space used for this example. A different search space, preprocessing configuration, dataset split or feature engineering strategy could produce substantially different results.

The training process used **30,000 training records**, while the training metrics were calculated on a sample of **15,000 records**. The complete test set contained **5,000 records**. The preprocessing stage expanded the original **25 input features to 68 features** after categorical encoding.

The final model and its metrics are subsequently stored in the **Snowflake Model Registry** as a new version of `HOUSING_PRICE_XGBOOST`, allowing the exact trained model associated with this run to be retrieved and used by the inference API.


### Performance Interpretation

The reported metrics demonstrate that the complete ML workflow successfully performs the main steps required for model development.

The results also illustrate an important architectural characteristic of the project: model evaluation is integrated into the automated pipeline and its results are persisted in Snowflake tables rather than existing only as temporary values in a notebook.

The hyperparameter search space is deliberately configurable. The values passed to the tuner are not hard-coded as universally valid choices, and users reproducing the pipeline should adapt them to their own requirements. 

Consequently, the numerical performance shown in this section should not be interpreted as the main success criterion of the project. 

The primary outcome of the project is instead the **construction of a complete, automated and scalable ML pipeline in Snowflake**, in which model performance can be measured, tracked and associated with the exact preprocessing and hyperparameter configurations used to produce each model version.














## 13. Automation & Scheduling

The pipeline is largely automated through **Snowflake Tasks**, which orchestrate the execution of the different data engineering and machine learning stages.

The main pipeline follows a sequential **Task Tree**, where each task is triggered after the successful execution of the previous one:

```text
                    Snowpipe
                       │
                       ▼
                  RAW_DATA table
                       │
                       │  new data
                       ▼
             QUALITYCHECK_REPORT_TASK
                       │
                      AFTER
                       ▼
                CLEAN_DATA_TASK
                       │
                      AFTER
                       ▼
             TRAINTEST_SPLIT_TASK
                       │
                      AFTER
                       ▼
               PREPROCESSING_TASK
                       │
                      AFTER
                       ▼
             XGBOOST_TRAINING_TASK
```

Snowpipe is responsible for automatically ingesting newly arrived files from the external S3 stage into `RAW_DATA`. The subsequent processing stages are orchestrated through the Task Tree.

### Quality Check Task

The first Task in the processing chain is `QUALITYCHECK_REPORT_TASK`.

Snowflake Tasks cannot be directly triggered by the execution of a Snowpipe, so this Task uses a **scheduled trigger combined with a Stream condition**:

```sql
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA(
    'HOUSING_PRICE_PROJECT.STAGING_LAYER.RAWDATA_STREAM'
)
```

The Task is therefore evaluated every minute. When the `RAW_DATA` stream contains new data, the condition becomes true and the Task executes the Stored Procedure responsible for the data quality checks.

This provides an event-like behavior while still using Snowflake's Task scheduling mechanism: instead of polling the S3 bucket itself, the Task checks whether Snowflake has detected new records in the `RAW_DATA` stream.

### Data Cleaning Task

`CLEAN_DATA_TASK` is triggered with an `AFTER` dependency on `QUALITYCHECK_REPORT_TASK`.

Once the quality-check Task completes, the cleaning procedure processes the newly arrived data and updates the `CLEAN_DATA` table.

### Train/Test Split Task

`TRAINTEST_SPLIT_TASK` is triggered after `CLEAN_DATA_TASK`.

It takes the newly cleaned data and updates the training and test datasets according to the train/test splitting strategy described in the **Machine Learning Pipeline** section.

### Preprocessing Task

`PREPROCESSING_TASK` runs after `TRAINTEST_SPLIT_TASK`.

The Task trains the preprocessing pipeline using the available training data and stores the resulting pipeline as a new version in the **Snowflake Model Registry**.

### XGBoost Training Task

`XGBOOST_TRAINING_TASK` is triggered after `PREPROCESSING_TASK`.

It trains the final XGBoost model using the updated training data, the newly generated preprocessing pipeline and the latest hyperparameters available from the HPO results.

The trained model is then registered as a new version in the **Snowflake Model Registry**.

---

### Independent Hyperparameter Optimization

Hyperparameter optimization is intentionally separated from the main Task Tree.

The `HYPERPARAM_TUNING_TASK` is scheduled independently and runs automatically on a weekly basis. This means that the HPO process is executed once every seven days rather than being triggered every time new data arrives.

This separation is intentional because HPO is considerably more computationally expensive than the normal data-processing stages and does not need to be performed for every incoming data batch.

The HPO process evaluates different hyperparameter configurations using the available training data and updates `XGBOOST_BEST_PARAMETERS` with the selected configuration. The next execution of `XGBOOST_TRAINING_TASK` can then use these parameters when training a new model.

The main pipeline is therefore **data-driven**, reacting to newly ingested data, while HPO is **time-driven**, running independently on a weekly schedule.

Together, these two mechanisms provide an automated workflow in which new data can continuously flow through the pipeline, while model hyperparameters are periodically re-evaluated without unnecessarily running an expensive optimization process for every incoming data batch.





## 14. Future Improvements

The current project provides a complete end-to-end foundation, but several components could be further developed to make the system more production-ready.

* **Docker & SPCS deployment** — containerize the FastAPI application and deploy it through Snowpark Container Services instead of running it locally.
* **Monitoring & observability** — monitor Snowpipe, Tasks, ML Jobs, data quality checks and API performance, with automated error detection.
* **Model monitoring & drift detection** — monitor changes in incoming data and model performance over time, with the possibility of triggering automated retraining.
* **CI/CD & automated testing** — introduce automated testing and Git-based deployment workflows for the Python, SQL and Snowflake components.
* **Model lifecycle management** — introduce explicit model promotion, validation and rollback procedures using the Model Registry.
* **Extended HPO & data quality** — expand the hyperparameter search space and introduce additional data quality checks as the project evolves.

These improvements would extend the existing architecture toward a more **production-ready and fully managed ML system**, while the current project already demonstrates the complete integration of the main data engineering and machine learning components in Snowflake.



## 10. Contacts
Author: Filippo Pedrini\
Contact: filippopedrini95@gmail.com