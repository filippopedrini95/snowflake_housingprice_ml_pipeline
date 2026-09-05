from snowflake.snowpark import Session
from credentials import params
from pathlib import Path
import time
import os


def main():
    
    cred, conn, file = "", "", ""
    
    while cred != 'y':
        cred = input("Have you added your Snowflake account credentials to credentials.py? (y/n): ")
        if cred == 'n':
            print("\nPlease add your Snowflake account credentials to credentials.py before running the pipeline initialization.")
            return False
    
    while conn != 'y':
        conn = input("Is the connection to the S3 bucket active and working correctly? (y/n): ")
        if conn == 'n':
            print("\nPlease make sure the S3 connection is active and working correctly before running the pipeline initialization.")
            return False    

    while file != 'y':
        file = input("Have you uploaded HousingPricePredictionDataset.csv to the S3 bucket? (y/n): ")
        if file == 'n':
            print("\nPlease upload HousingPricePredictionDataset.csv to the S3 bucket before running the pipeline initialization.")
            return False
    
    session = Session.builder.configs(params).create()



    # FIRST EXECUTION OF TASK' STORED PROCEDURES ************************************************
    
    sql_ingest = """
        COPY INTO HOUSING_PRICE_PROJECT.STAGING_LAYER.RAW_DATA
        FROM @HOUSING_PRICE_PROJECT.PUBLIC.HOUSEPRICE_STAGE
        FILE_FORMAT = 'HOUSING_PRICE_PROJECT.PUBLIC.CSV_INFERSCHEMA_FF'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        INCLUDE_METADATA = (
            SOURCE_FILE = METADATA$FILENAME
        );"""

    actions_and_checks = [
        ("sql_ingest", "STAGING_LAYER.RAW_DATA"),
        ("STAGING_LAYER.QUALITY_CHECK_REPORT_PROC()", "STAGING_LAYER.DATA_QUALITY_REPORT"),
        ("CLEANING_LAYER.CLEAN_DATA_PROC(10)", "CLEANING_LAYER.CLEAN_DATA"),
        ("ML_LAYER.TRAINTEST_SPLIT_PROC(5000)", "ML_LAYER.TRAIN_SET"),
        ("ML_LAYER.PREPROCESSING_PROC()", "model_registry_check"),
        ("ML_LAYER.HYPERPARAM_TUNING_PROC()", "ML_LAYER.XGBOOST_BEST_PARAMETERS"),
        ("ML_LAYER.XGBOOST_TRAINING_PROC()", "ML_LAYER.XGBOOST_TRAINING_RESULTS")
    ]

    def TaskSuccessful(check):
        cicle_time_s = 10
        output = 0
        nr_cicle = 0 
        while output == 0:
            if nr_cicle == 3:
                print(f"{check} tab is either empty or not accessible!")
                return False
            try:    
                query = f"SELECT COUNT(1) FROM HOUSING_PRICE_PROJECT.{check};"
                output = PreprocModelRegistryCheck() if check == "model_registry_check" else session.sql(query).collect()[0][0]
                print(output)
            except:
                output = 0
            time.sleep(cicle_time_s)
            nr_cicle += 1
        return True


    def PreprocModelRegistryCheck():
        registry = Registry(
            session=session,
            database_name="HOUSING_PRICE_PROJECT",
            schema_name="ML_LAYER"
        )
        return registry.get_model("PREPROCESSING_PIPELINE").show_versions().shape[0]


    action_nr = 1
    for action, check in actions_and_checks:
        print(f"\n({action_nr}/7) {action} ********************")
        action_nr += 1
        match action:
            case "sql_ingest":
                try:          
                    print(session.sql(sql_ingest.strip()).collect())
                except:
                    print(f"{action} - failed")
                    break
                
                if not TaskSuccessful(check):
                    break
    
            case "ML_LAYER.PREPROCESSING_PROC()":
                try:
                    print(session.sql(f"CALL HOUSING_PRICE_PROJECT.{action};").collect())
                except:
                    print(f"{action} - failed")
                    break            
                
                if not TaskSuccessful(check):
                    break
    
            case _:
                try:
                    print(session.sql(f"CALL HOUSING_PRICE_PROJECT.{action};").collect())
                except:
                    print(f"{action} - failed")
                    break
                    
                if not TaskSuccessful(check):
                    break

    # TASKS AND PIPE ACTIVATION ******************************************************
    
    # Pipe Activation
    session.sql("""ALTER PIPE HOUSING_PRICE_PROJECT.STAGING_LAYER.S3_INGESTION_PIPE
        SET PIPE_EXECUTION_PAUSED = FALSE;""").collect()

    # Activate Tasks-Tree (i.e. all tasks except for "HYPERPARAM_TUNING_TASK")
    session.sql("""SELECT SYSTEM$TASK_DEPENDENTS_ENABLE(
        'HOUSING_PRICE_PROJECT.PUBLIC.QUALITYCHECK_REPORT_TASK'
    );""").collect()

    # Activate task "HYPERPARAM_TUNING_TASK"
    session.sql("""ALTER TASK HOUSING_PRICE_PROJECT.PUBLIC.HYPERPARAM_TUNING_TASK RESUME;""").collect()

    






if __name__ == "__main__":
    main()