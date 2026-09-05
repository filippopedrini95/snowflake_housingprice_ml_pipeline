import pandas as pd
import numpy as np
from snowflake.snowpark import Session
from credentials import params
from snowflake.ml.registry import Registry
from pydantic import BaseModel
import re
from fastapi import FastAPI
app = FastAPI()

session = Session.builder.configs(params).create()
session.use_database("HOUSING_PRICE_PROJECT")
session.use_schema("ML_LAYER")

class Property(BaseModel):
    CITY: str
    LOCALITY: str
    LOCALITY_TIER: str
    PROPERTY_TYPE: str
    BALCONIES: int
    CARPET_AREA: int
    FLOOR_NUMBER: int
    TOTAL_FLOORS: int
    FLOOR_CATEGORY: str
    FACING: str
    FURNISHING_STATUS: str
    PROPERTY_AGE: int
    PARKING_SPACES: int
    SECURITY_SCORE: float
    GYM_AVAILABLE: int
    SWIMMING_POOL: int
    POWER_BACKUP: int
    LIFT_AVAILABLE: int
    MAINTENANCE_FEE_MONTHLY: int
    DISTANCE_TO_CITY_CENTER_KM: float
    DISTANCE_TO_METRO_KM: float
    NEARBY_SCHOOLS: int
    NEARBY_HOSPITALS: int
    TRANSACTION_TYPE: str


registry = Registry(
    session = session,
    database_name = "HOUSING_PRICE_PROJECT",
    schema_name = "ML_LAYER"
)

preproc_model = registry \
    .get_model("PREPROCESSING_PIPELINE") \
    .version("LAST")

predict_model = registry \
    .get_model("HOUSING_PRICE_XGBOOST") \
    .version("LAST")


@app.post("/predict")
def endpoint(record_raw: Property):

    # Request parsing
    print("[1/3] Request parsing   ", end="\t")
    record_dict = record_raw.model_dump()
    record = pd.DataFrame([record_dict])
    print("COMPLETED")

    # Data preprocessing
    print("[2/3] Data preprocessing", end="\t")
    record_p = preprocessing(record)
    print("COMPLETED")

    # Model inference
    print("[3/3] Model inference   ", end="\t")
    pred = prediction(record_p)
    print("COMPLETED")

    return {"predicted_price" : float(pred)}


@app.get("/health")
def health():
    return {"status": "ok"}


def preprocessing(record: pd.DataFrame):
    
    record_p = preproc_model.run(
        record,
        function_name="transform"
    )

    return record_p.rename(columns = {col : sanitize_column_name(col) for col in record_p.columns})





def sanitize_column_name(column):
    
    column = column.replace('+', 'plus')
    column = re.sub(r'[()"]', '', column)
    column = re.sub(r'[\s-]', '_', column)                   
    column = re.sub(r'[^a-zA-Z0-9_]', '', column)
    return column.upper()





def prediction(record_p: pd.DataFrame):

    prediction = predict_model.run(
        record_p,
        function_name="predict"
    )

    return prediction.output_feature_0[0]