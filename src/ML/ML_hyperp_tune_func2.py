from datetime import datetime
import pandas as pd
import numpy as np


def best_hyperparam(job_session, res, preproc_model_version, gap_max = 0.2):
    
    GAP_MAX = gap_max
    
    # RENAME COLUMNS TO REMOVE "config/" ***************************************************************************
    res.rename(columns = { col : col.replace("config/", "") for col in res.columns }, inplace = True)

    # CREATE NEW COLUMNS AND CALCULATE AVERAGE METRICS **************************************************************
    timestamp = datetime.now()
    res["timestamp"] = timestamp
 
    timestamp_code = timestamp.strftime("%Y%m%d_%H%M%S")
    res["tuning_run_id"] = timestamp_code
    res["preprocessor_version"] = preproc_model_version
    res["relative_gap"] = (res["rmse_valid_avg"] - res["rmse_train_avg"])/res["rmse_train_avg"]

    
    # FIND THE RAW WITH BEST METRICS **************************************************************
    rmse_valid_avg_min = res.loc[res.relative_gap < GAP_MAX, "rmse_valid_avg"].min()
    best_raw = res[res["rmse_valid_avg"] == rmse_valid_avg_min]


    for data, tab in [(res, "XGBOOST_TUNING_RESULTS"), (best_raw, "XGBOOST_BEST_PARAMETERS")]:
        
        snowpark_df = job_session.create_dataframe(data)

        snowpark_df.write.mode("overwrite").save_as_table(
            "results_to_append",
            table_type="temporary"
        )
        
        
        query = f"""
            INSERT INTO {tab} ({", ".join(list(data.columns))})
            SELECT * FROM RESULTS_TO_APPEND
        """

        job_session.sql(query).collect()