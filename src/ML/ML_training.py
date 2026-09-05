from snowflake.ml.jobs import remote
from snowflake.snowpark.types import StringType
import re
import time
from snowflake.snowpark.context import get_active_session
from datetime import datetime
from snowflake.ml.data.data_connector import DataConnector
from snowflake.ml.modeling.distributors.xgboost.xgboost_estimator import XGBScalingConfig
from snowflake.ml.runtime_cluster import scale_cluster
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from snowflake.ml.modeling.distributors.xgboost.xgboost_estimator import XGBEstimator    
import pandas as pd
import numpy as np
from snowflake.ml.registry import Registry

def main():

    # ADJUSTABLE PARAMETERS ********************************************************************************************
    
    NUM_XGBOOST_WORKERS = 3  
    # number of workers used for distributed XGBoost training within each trial
    # [ Parameter of function XGBEstimator() ]

    TUNING_RUN_ID = "LAST"  
    # ID of the HPO tuning run from which to retrieve the hyperparameters for training; 
    # use "LAST" to select the most recent tuning run
    

    # SESSION, DATASET AND FEATURE LIST ************************************************************************
    job_session = get_active_session()
    
    
    # train tab ****************************************
    train_tab = job_session.table("HOUSING_PRICE_PROJECT.ML_LAYER.TRAIN_SET")
    
    # test tab ****************************************
    test_tab = job_session.table("HOUSING_PRICE_PROJECT.ML_LAYER.TEST_SET")


    # target col ****************************************
    target_col_name = 'PRICE_IN_LAKHS'
    
    # PREPROCESSING using the pipeline from model registry *******************************************************
    registry = Registry(
        session=job_session,
        database_name="HOUSING_PRICE_PROJECT",
        schema_name="ML_LAYER"
    )
    model_version = registry.get_model("PREPROCESSING_PIPELINE").version("LAST")    
    
    # train tab ****************************************
    train_tab_prepr = model_version.run(
        train_tab,
        function_name="transform"
    )
    
    # test tab ****************************************
    test_tab_prepr = model_version.run(
        test_tab,
        function_name="transform"
    )
    
    
    # ADJUSTING THE PREPROCESSED DATASET *************************************************************************
    def sanitize_column_name(column):
        column = column.replace('+', 'plus')
        column = re.sub(r'[()"]', '', column)
        column = re.sub(r'[\s-]', '_', column)                   
        column = re.sub(r'[^a-zA-Z0-9_]', '', column)
        return column

    # train tab ****************************************
    for column in train_tab_prepr.columns:
        train_tab_prepr = train_tab_prepr.with_column_renamed(column, sanitize_column_name(column))
    

    # test tab ****************************************
    for column in test_tab_prepr.columns:
        test_tab_prepr = test_tab_prepr.with_column_renamed(column, sanitize_column_name(column))
    
    
    cat_cols = [
        field.name
        for field in train_tab_prepr.schema.fields
        if isinstance(field.datatype, StringType)    
    ]
    
    train_tab_prepr = train_tab_prepr.drop(cat_cols)
    test_tab_prepr = test_tab_prepr.drop(cat_cols)

    #CREATING FEATURE LIST **************************************************************************************
    features = train_tab_prepr.columns
    features.remove(target_col_name)
    
    train_connector = DataConnector.from_dataframe(train_tab_prepr)

    # RETRIEVE BEST HYPERPARAM **************************************************************************************
    # retrieve the latest tuning run id

    if TUNING_RUN_ID == "LAST":
        TUNING_RUN_ID = job_session.sql("""
            SELECT TUNING_RUN_ID
            FROM XGBOOST_BEST_PARAMETERS
            ORDER BY TIMESTAMP DESC
            LIMIT 1
        """).to_pandas()["TUNING_RUN_ID"][0]


    # retrieve the best hyperparam using "TUNING_RUN_ID"
    params_df = job_session.sql(f"""
        SELECT MAX_DEPTH, LEARNING_RATE, N_ESTIMATORS, MIN_CHILD_WEIGHT, SUBSAMPLE
        FROM XGBOOST_BEST_PARAMETERS
        WHERE TUNING_RUN_ID = '{TUNING_RUN_ID}';
    """).to_pandas()


    estimator = XGBEstimator(
        params={
            "max_depth": int(params_df["MAX_DEPTH"][0]),
            "learning_rate": params_df["LEARNING_RATE"][0],
            "min_child_weight": int(params_df["MIN_CHILD_WEIGHT"][0]),
            "subsample": int(params_df["SUBSAMPLE"][0])
        },
        n_estimators = int(params_df["N_ESTIMATORS"][0]),
        
        objective = "reg:squarederror",
        scaling_config = XGBScalingConfig(
            num_workers = NUM_XGBOOST_WORKERS
        )
    )

    train_start = time.time()
    estimator.fit(
        dataset = train_connector,
        input_cols = features,
        label_col = target_col_name
    )
    train_end = time.time()


    # train tab ****************************************
    # Use a train subset for metrics to avoid materializing the full dataset in Pandas
    SIZE_TRAIN_SAMP = 15_000 
    
    X_train_metric_samp_pd = train_tab_prepr.sample(n=SIZE_TRAIN_SAMP).to_pandas()
    target_train_metric_samp = X_train_metric_samp_pd[target_col_name]
    X_train_metric_samp_pd = X_train_metric_samp_pd.drop([target_col_name], axis = 1)
    

    # test tab ****************************************
    X_test_pd = test_tab_prepr.to_pandas()
    target_test = X_test_pd[target_col_name]
    X_test_pd = X_test_pd.drop([target_col_name], axis = 1) 
    
            
    # train tab ****************************************            
    predictions_train_metric_samp = estimator.predict(X_train_metric_samp_pd)
       
    rmse_train = mean_squared_error(
        y_true = target_train_metric_samp,
        y_pred = predictions_train_metric_samp
    )

    mae_train = mean_absolute_error(
        y_true = target_train_metric_samp,
        y_pred = predictions_train_metric_samp
    )

    r2_train = r2_score(
        y_true = target_train_metric_samp,
        y_pred = predictions_train_metric_samp
    )
      

    # test tab ****************************************    
    pred_test_start = time.time()
    predictions_test = estimator.predict(X_test_pd)
    pred_test_end = time.time()
       
    rmse_test = mean_squared_error(
        y_true = target_test,
        y_pred = predictions_test
    )

    mae_test = mean_absolute_error(
        y_true = target_test,
        y_pred = predictions_test
    )

    r2_test = r2_score(
        y_true = target_test,
        y_pred = predictions_test
    )

    
    # RECORDING TIMESTAMP AT THE END OF THE PROCESS
    TIMESTAMP = datetime.now()
   
    # INSERT MODEL INTO THE SNOWFLAKE REGISTRY
    
    model_version = registry.log_model(
        model = estimator.get_booster(),
        model_name = "HOUSING_PRICE_XGBOOST",
        version_name = f"run_{TIMESTAMP.strftime('%Y%m%d_%H%M%S')}",
        #comment = "First version of the housing price model",
        metrics = {
            'RMSE_TRAIN' : rmse_train,
            'RMSE_TEST' : rmse_test,
            'MAE_TRAIN' : mae_train,
            'MAE_TEST' : mae_test,
            'R2_TRAIN' : r2_train,
            'R2_TEST' : r2_test,
            'TRAIN_RUN_ID' : TIMESTAMP.strftime("%Y%m%d_%H%M%S")
        },
        sample_input_data = X_test_pd.head(),
        target_platforms=[
            "WAREHOUSE",
            "SNOWPARK_CONTAINER_SERVICES"
        ]
    )  


    # INSERT DATA INTO XGBOOST_TRAINING_RESULTS TAB ******************************

    # results gathering
    results_dict = {
        'TRAIN_RUN_ID' : TIMESTAMP.strftime("%Y%m%d_%H%M%S"),
        'TIMESTAMP' : TIMESTAMP,
        'HYPERPARAMETER_SET_ID' : TUNING_RUN_ID,
        'RMSE_TRAIN' : rmse_train,
        'RMSE_TEST' : rmse_test,
        'MAE_TRAIN' : mae_train,
        'MAE_TEST' : mae_test,
        'R2_TRAIN' : r2_train,
        'R2_TEST' : r2_test,
        'TRAIN_SET_SIZE' : train_tab.count(),
        'TRAIN_METRIC_SAMPLE_SIZE' : SIZE_TRAIN_SAMP,
        'TEST_SET_SIZE' : test_tab.count(),
        'TARGET_COLUMN' : target_col_name,
        'N_FEATURES_BEFORE_ENCODING' : len(train_tab.columns), 
        'N_FEATURES_AFTER_ENCODING' : len(train_tab_prepr.columns),
        'TRAINING_TIME_S' : train_end - train_start,
        'PREDICTION_TIME_TESTSET_S' : pred_test_end - pred_test_start
    }


    # query creation
    cols_query = ""
    values_query = ""
    for key in results_dict:
        cols_query = cols_query + key + ", "
        if type(results_dict[key]) == int or type(results_dict[key]) == float:
            values_query = values_query + str(results_dict[key]) + ", "
        else:
            values_query = values_query + "'" + str(results_dict[key]) + "', "
    
    
    query = f"""INSERT INTO XGBOOST_TRAINING_RESULTS ({cols_query[:-2]})
    VALUES ({values_query[:-2]})"""
    
    # insertion
    job_session.sql(query).collect()



main()