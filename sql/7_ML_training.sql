-- CREATE THE REQUIRED TABLES ********************************************************************************
CREATE TABLE IF NOT EXISTS HOUSING_PRICE_PROJECT.ML_LAYER.XGBOOST_TRAINING_RESULTS ( 
    train_run_id VARCHAR,
    timestamp TIMESTAMP_NTZ,
    hyperparameter_set_id VARCHAR,
    
    rmse_train DOUBLE,
    rmse_test DOUBLE,

    mae_train DOUBLE,
    mae_test DOUBLE,
    r2_train DOUBLE,
    r2_test DOUBLE,

    train_set_size BIGINT,
    train_metric_sample_size BIGINT,
    test_set_size BIGINT,
    
    target_column VARCHAR,
    n_features_before_encoding BIGINT,
    n_features_after_encoding BIGINT,
    
    training_time_s DOUBLE,
    prediction_time_testset_s DOUBLE
);



-- CREATE THE STORED PROCEDURE  **********************************************************************************************
CREATE OR REPLACE PROCEDURE HOUSING_PRICE_PROJECT.ML_LAYER.XGBOOST_TRAINING_PROC()
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
        entrypoint = "ML_training.py",
        stage_name = "HOUSING_PRICE_PROJECT.ML_LAYER.MLJOBS_STAGE",
        target_instances = 4,
        session = session
    )
    job.wait()
    return job.status
$$;


-- CREATE THE TASK TO RUN THE STORED PROCEDURE ******************************************************************************************************
CREATE OR REPLACE TASK HOUSING_PRICE_PROJECT.PUBLIC.XGBOOST_TRAINING_TASK
WAREHOUSE = HOUSEPRICEPROJECT_WH1
AFTER HOUSING_PRICE_PROJECT.PUBLIC.PREPROCESSING_TASK
AS 
CALL HOUSING_PRICE_PROJECT.ML_LAYER.XGBOOST_TRAINING_PROC();