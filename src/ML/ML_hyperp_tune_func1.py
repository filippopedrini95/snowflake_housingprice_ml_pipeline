from snowflake.snowpark.functions import row_number, random, col
from snowflake.snowpark.window import Window
from snowflake.ml.data.data_connector import DataConnector
from snowflake.ml.registry import Registry
from snowflake.snowpark.types import StringType


def dataset_map_creator(job_session, data_table, size_tuning_df = 15_000, k_cv = 3):

    SIZE_TUNING_DF = size_tuning_df
    K_CV = k_cv
    
    # import the dataset as snowpark dataframe
    df_sp = data_table 
    
    # take a random sample of SIZE_TUNING_DF rows
    tuning_df = df_sp.sample(n = SIZE_TUNING_DF)
    
    # updating SIZE_TUNING_DF in case df_sp.count() < SIZE_TUNING_DF
    SIZE_TUNING_DF = tuning_df.count()
    
    # save tuning_df as temporary table to avoid that any time tuning_df is called new random rows are taken
    tuning_df.write.mode("overwrite").save_as_table(
        "TEMP_TUNING_DATA",
        table_type="temporary"
    )
    
    # importing tuning_df from the temporary tab
    tuning_tab = job_session.table("TEMP_TUNING_DATA")


    # PREPROCESSING using the pipeline from model registry *******************************************************
    registry = Registry(
        session=job_session,
        database_name="HOUSING_PRICE_PROJECT",
        schema_name="ML_LAYER"
    )
    
    preprocessor_model = registry.get_model("PREPROCESSING_PIPELINE").version("LAST")    
    
    tuning_tab_prepr = preprocessor_model.run(
        tuning_tab,
        function_name="transform"
    )    
 
    # ADJUSTING THE PREPROCESSED DATASET *************************************************************************
    for column in tuning_tab_prepr.columns:
        tuning_tab_prepr = tuning_tab_prepr.with_column_renamed(column, column.replace('"', ''))
    
    cat_cols = [
        field.name
        for field in tuning_tab_prepr.schema.fields
        if isinstance(field.datatype, StringType)    
    ]
    
    tuning_tab_prepr = tuning_tab_prepr.drop(cat_cols)


    # CREATE FEATURE LIST
    features = tuning_tab_prepr.columns
    features.remove('PRICE_IN_LAKHS')

    # creating "random_numb" column to numerate each row
    window = Window.order_by(random())
    tuning_tab_prepr = tuning_tab_prepr.with_column(
        "RANDOM_NUMB",
        row_number().over(window)
    )
    
    # creating the ranges list, containing ranges on which to filter the dataframe; example: ranges = [(0, 100), (100, 200), ...]
    i = 0
    frac = SIZE_TUNING_DF / K_CV
    
    ranges = []
    for n in range(K_CV):
        j = (i, i + frac)
        ranges.append(j)
        i += frac
    
    # separating the snowpark dataframe in the k folds for the CV
    i = 0
    folds = []

    
    for low_limit, up_limit in ranges:
        fold = tuning_tab_prepr.filter((col("RANDOM_NUMB") > low_limit) & (col("RANDOM_NUMB") <= up_limit))
        folds.append(fold)
        i += 1
        

    # creating dataset_map with proper fold combinations
    dataset_map = {}

    for validation_idx in range(K_CV):
    
        validation_df = folds[validation_idx]
    
        train_df = None
    
        for i, fold in enumerate(folds):
            if i != validation_idx:
                if train_df is None:
                    train_df = fold
                else:
                    train_df = train_df.union(fold)
        
        dataset_map[f"train_{validation_idx}"] = DataConnector.from_dataframe(train_df.drop("RANDOM_NUMB"))
        dataset_map[f"valid_{validation_idx}"] = DataConnector.from_dataframe(validation_df.drop("RANDOM_NUMB"))
    
    return dataset_map, features, preprocessor_model.version_name