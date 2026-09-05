-- CREATE THE REQUIRED TABLES ********************************************************************************
CREATE TABLE IF NOT EXISTS HOUSING_PRICE_PROJECT.ML_LAYER.XGBOOST_TUNING_RESULTS ( -- TABLE WITH ALL RESULTS
    tuning_run_id VARCHAR,
    timestamp TIMESTAMP_NTZ,
    
    rmse_train_avg DOUBLE,
    rmse_valid_avg DOUBLE,
    relative_gap DOUBLE, 

    max_depth BIGINT DEFAULT 6,
    learning_rate DOUBLE DEFAULT 0.3,
    n_estimators BIGINT DEFAULT 100,
    min_child_weight BIGINT DEFAULT 1,
    subsample DOUBLE DEFAULT 1.0,

    mae_train_avg DOUBLE,
    mae_valid_avg DOUBLE,
    r2_train_avg DOUBLE,
    r2_valid_avg DOUBLE,
    rmse_train VARIANT,
    rmse_valid VARIANT,

    preprocessor_version VARCHAR,
    should_checkpoint BOOLEAN,
    trial_id VARCHAR,
    time_total_s DOUBLE
);


CREATE TABLE IF NOT EXISTS HOUSING_PRICE_PROJECT.ML_LAYER.XGBOOST_BEST_PARAMETERS ( -- TABLE WITH BEST PARAMETERS
    tuning_run_id VARCHAR,
    timestamp TIMESTAMP_NTZ,
    
    rmse_train_avg DOUBLE,
    rmse_valid_avg DOUBLE,
    relative_gap DOUBLE, 

    max_depth BIGINT DEFAULT 6,
    learning_rate DOUBLE DEFAULT 0.3,
    n_estimators BIGINT DEFAULT 100,
    min_child_weight BIGINT DEFAULT 1,
    subsample DOUBLE DEFAULT 1.0,

    mae_train_avg DOUBLE,
    mae_valid_avg DOUBLE,
    r2_train_avg DOUBLE,
    r2_valid_avg DOUBLE,
    rmse_train VARIANT,
    rmse_valid VARIANT,

    preprocessor_version VARCHAR, 
    should_checkpoint BOOLEAN,
    trial_id VARCHAR,
    time_total_s DOUBLE
);




-- CREATE THE STORED PROCEDURE  **********************************************************************************************
CREATE OR REPLACE PROCEDURE HOUSING_PRICE_PROJECT.ML_LAYER.HYPERPARAM_TUNING_PROC()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION='3.12'
PACKAGES = (
    'snowflake-snowpark-python',
    'snowflake-ml-python'
)
HANDLER = 'main'
AS
$$
from snowflake.ml.jobs import submit_from_stage
def main(session):
    job = submit_from_stage(
        "@HOUSING_PRICE_PROJECT.PUBLIC.PROJECT_CODE/ML_CODE",
        "ml_job_computerpool",
        entrypoint = "ML_hyperp_tune_entrypoint.py",
        stage_name = "HOUSING_PRICE_PROJECT.ML_LAYER.MLJOBS_STAGE",
        target_instances = 6,
        session = session
    )
    job.wait()
    return job.status
$$;


-- CREATE THE TASK TO RUN THE STORED PROCEDURE ******************************************************************************************************
CREATE OR REPLACE TASK HOUSING_PRICE_PROJECT.PUBLIC.HYPERPARAM_TUNING_TASK
WAREHOUSE = HOUSEPRICEPROJECT_WH1
SCHEDULE = '168 hours'                     --168 hours = 1 week
AS 
CALL HOUSING_PRICE_PROJECT.ML_LAYER.HYPERPARAM_TUNING_PROC();