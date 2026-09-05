from ML_hyperp_tune_func1 import dataset_map_creator
from ML_hyperp_tune_func2 import best_hyperparam
import time
from snowflake.snowpark.context import get_active_session
from ray import tune
from snowflake.ml.data.data_connector import DataConnector
from snowflake.ml.modeling.distributors.xgboost.xgboost_estimator import XGBScalingConfig
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from snowflake.ml.modeling.distributors.xgboost.xgboost_estimator import XGBEstimator   
from snowflake.ml.modeling.tune import (Tuner, TunerConfig, get_tuner_context) 
from snowflake.snowpark.types import StringType
import numpy as np
import pandas as pd


def main():

    # TUNING PARAMETERS ********************************************************************************************
    
    TUNING_DATA_SIZE = 15_000
    # Number of training-set rows used for hyperparameter tuning.
    # [ Parameter of function "dataset_map_creator()" ] 

    
    N_CV_FOLDS = 3
    # Number of cross-validation folds used during hyperparameter tuning.
    # Ideally, TUNING_DATA_SIZE should be divisible by N_CV_FOLDS.
    # [ Parameter of function "dataset_map_creator()" ]

    
    MAX_TRAIN_TEST_RMSE_GAP = 0.1
    # Maximum allowed relative difference between the RMSE calculated on the
    # training and test/validation predictions. Configurations exceeding this
    # threshold are excluded from the selection of the best hyperparameters.
    # For example, 1.0 corresponds to a maximum difference of 100%.
    # <0.1= very good | 0.1–0.2% = good | 0.2–0.3% = acceptable | 0.3–0.5% = possible overfitting | >0.5% = strong overfitting
    # [ Parameter of function "best_hyperparam()" ]
    

    # SEARCH SPACE
    # Defines the set of possible values for the hyperparameters explored by the Tuner.
    # These are the only hyperparameters made tunable by this implementation.
    SEARCH_SPACE = {
        "max_depth": tune.choice([4]),                   # Default: 6 
        "learning_rate": tune.choice([0.05]),             # Default: 0.3
        "n_estimators": tune.choice([200]),                # Default: 100
        "min_child_weight": tune.choice([1]),            # Default: 1 
        "subsample": tune.choice([0.9])                  # Default: 1.0
    }

    
    N_TRIALS = 3
    # Number of hyperparameter configurations (trials) evaluated by the Tuner.
    # [ Parameter of function TunerConfig() ]


    MAX_CONCURRENT_TRIALS = 3
    # Maximum number of trials that can be executed concurrently.
    # [ Parameter of function TunerConfig() ]


    NUM_XGBOOST_WORKERS = 2  
    # number of workers used for distributed XGBoost training within each trial
    # [ Parameter of function XGBEstimator() ]

    print(100*"*")
    print(SEARCH_SPACE)
    print(N_TRIALS)
    print(MAX_CONCURRENT_TRIALS)
    print(NUM_XGBOOST_WORKERS)

    
    # SESSION, DATASET AND FEATURE LIST ************************************************************************
    job_session = get_active_session()

    train_tab = job_session.table("HOUSING_PRICE_PROJECT.ML_LAYER.TRAIN_SET")

    dataset_map, features, preproc_model_version = dataset_map_creator(job_session, 
                                                                       train_tab, 
                                                                       size_tuning_df = TUNING_DATA_SIZE, 
                                                                       k_cv = N_CV_FOLDS)


    #TUNER CONFIG **********************************************************************************************
    tuner_config = TunerConfig(
        metric = "rmse_valid_avg",
        mode = "min",
        num_trials = N_TRIALS,
        uses_snowflake_trainer = True,
        max_concurrent_trials = MAX_CONCURRENT_TRIALS
    )
    

    #TRAIN FUNC DEFINITION **************************************************************************************
    def train_func():
    
        ctx = get_tuner_context()
    
        hyper_params = ctx.get_hyper_params()
        datasets = ctx.get_dataset_map()

        rmse_train_list, mae_train_list, r2_train_list = [], [], []
        rmse_valid_list, mae_valid_list, r2_valid_list = [], [], []
        
        for i in range(int(len(datasets)/2)):
            train_connector = datasets[f"train_{i}"]
            valid_connector = datasets[f"valid_{i}"]
        
            estimator = XGBEstimator(
                params={
                    "max_depth": hyper_params["max_depth"],                    
                    "learning_rate": hyper_params["learning_rate"],                
                    "min_child_weight": hyper_params["min_child_weight"],             
                    "subsample": hyper_params["subsample"]
                },
                n_estimators = hyper_params["n_estimators"],
                objective = "reg:squarederror",
                scaling_config = XGBScalingConfig(
                    num_workers = NUM_XGBOOST_WORKERS
                )
            )

            estimator.fit(
                dataset=train_connector,
                input_cols=features,
                label_col='PRICE_IN_LAKHS'
            )
    

            # DATASETS FOR PREDICTION
            train_conn_pd = train_connector.to_pandas()
            X_train_pd = train_conn_pd.drop(['PRICE_IN_LAKHS'], axis = 1)
            target_train = train_conn_pd['PRICE_IN_LAKHS']

            valid_conn_pd = valid_connector.to_pandas()
            X_valid_pd = valid_conn_pd.drop(['PRICE_IN_LAKHS'], axis = 1)
            target_valid = valid_conn_pd['PRICE_IN_LAKHS']
    
            # PREDICTIONS ON TRAIN SET
            predictions_train = estimator.predict(X_train_pd)
               
            rmse_train = mean_squared_error(
                y_true = target_train,
                y_pred = predictions_train
            )

            mae_train = mean_absolute_error(
                y_true = target_train,
                y_pred = predictions_train
            )
        
            r2_train = r2_score(
                y_true = target_train,
                y_pred = predictions_train
            )
            
            rmse_train_list.append(rmse_train)
            mae_train_list.append(mae_train)
            r2_train_list.append(r2_train)
            
            # PREDICTIONS ON VALIDATION SET
            predictions_valid = estimator.predict(X_valid_pd)
               
            rmse_valid = mean_squared_error(
                y_true = target_valid,
                y_pred = predictions_valid
            )

            mae_valid = mean_absolute_error(
                y_true = target_valid,
                y_pred = predictions_valid
            )
        
            r2_valid = r2_score(
                y_true = target_valid,
                y_pred = predictions_valid
            )

            rmse_valid_list.append(rmse_valid)
            mae_valid_list.append(mae_valid)
            r2_valid_list.append(r2_valid)
            

        ###################################################################################################################
        
        rmse_train_avg = np.mean(rmse_train_list)
        rmse_valid_avg = np.mean(rmse_valid_list)

        mae_train_avg = np.mean(mae_train_list)
        mae_valid_avg = np.mean(mae_valid_list)

        r2_train_avg = np.mean(r2_train_list)
        r2_valid_avg = np.mean(r2_valid_list)
        
        ctx.report(
            metrics={
                "rmse_valid": rmse_valid_list, 
                "rmse_train": rmse_train_list,
                "rmse_train_avg":  rmse_train_avg,
                "rmse_valid_avg": rmse_valid_avg,
                "mae_train_avg": mae_train_avg,
                "mae_valid_avg": mae_valid_avg,
                "r2_train_avg": r2_train_avg,
                "r2_valid_avg": r2_valid_avg
            },
            model=estimator
        )

    #TUNER DECLARATION ******************************************************************************************
    tuner = Tuner(
        train_func = train_func,
        search_space = SEARCH_SPACE,
        tuner_config = tuner_config
    )
    
    
    #RUN THE TUNER ******************************************************************************************     
    results = tuner.run(dataset_map = dataset_map)
    res = results.results
    best_hyperparam(job_session, 
                    res, 
                    preproc_model_version, 
                    gap_max = MAX_TRAIN_TEST_RMSE_GAP
                   )
    
main()