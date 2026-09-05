from snowflake.ml.modeling.impute import SimpleImputer
from snowflake.ml.modeling.preprocessing import OneHotEncoder
from snowflake.ml.modeling.pipeline import Pipeline
from snowflake.snowpark.types import (IntegerType, LongType, FloatType, DoubleType, DecimalType)
from snowflake.ml.registry import Registry
from datetime import datetime


def main(session):
    
    registry = Registry(
        session=session,
        database_name="HOUSING_PRICE_PROJECT",
        schema_name="ML_LAYER"
    )

    
    X_train = session.table("HOUSING_PRICE_PROJECT.ML_LAYER.TRAIN_SET")
    X_train = X_train.drop('PRICE_IN_LAKHS')
    
    
    
    #************************************************************************
    floating_types = (FloatType, DoubleType, DecimalType)
    integer_types = (IntegerType, LongType)
    
    integer_cols = [
        field.name
        for field in X_train.schema.fields
        if isinstance(field.datatype, integer_types)    
    ]
    
    floating_cols = [
        field.name
        for field in X_train.schema.fields
        if isinstance(field.datatype, floating_types)
    ]
    
    numeric_cols = integer_cols + floating_cols
    
    categorical_cols = [
        field.name
        for field in X_train.schema.fields
        if field.name not in numeric_cols    
    ]

    #************************************************************************
    imputer_num_int = SimpleImputer(
        input_cols = integer_cols,
        output_cols = integer_cols,
        strategy = 'median'
    )
    
    imputer_num_float = SimpleImputer(
        input_cols = floating_cols,
        output_cols = floating_cols,
        strategy = 'median'
    )
    
    imputer_cat = SimpleImputer(
        input_cols = categorical_cols,
        output_cols = categorical_cols,
        strategy = 'most_frequent'
    )
    
    encoder = OneHotEncoder(
        input_cols = categorical_cols,
        output_cols = categorical_cols,
        drop_input_cols = True,
        handle_unknown = 'ignore'
    )
    
    prepr_pip = Pipeline(
        steps = [
            ('imputer_num', imputer_num_int),
            ('imputer_num_float', imputer_num_float),
            ('imputer_cat', imputer_cat),
            ('encoder', encoder)
        ]
    )

    #************************************************************************
    prepr_pip.fit(X_train)
    
    TIMESTAMP = datetime.now()
    
    registry.log_model(
        model = prepr_pip,
        model_name = "PREPROCESSING_PIPELINE",
        version_name = f"run_{TIMESTAMP.strftime('%Y%m%d_%H%M%S')}",
        sample_input_data = X_train.limit(10),
        target_platforms=[
            "WAREHOUSE",
            "SNOWPARK_CONTAINER_SERVICES"
        ]
    )