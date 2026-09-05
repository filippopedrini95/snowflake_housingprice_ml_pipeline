-- CREATE FILE FORMAT FOR TABLE INFER_SCHEMA ************************************************************************************************************************
CREATE OR REPLACE FILE FORMAT HOUSING_PRICE_PROJECT.PUBLIC.CSV_INFERSCHEMA_FF
TYPE = CSV
PARSE_HEADER = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
NULL_IF = ('NULL','N/A','NA');


-- CREATE THE RAW_DATA TABLE FROM THE FILE IN THE S3 BUCKET **********************************************************************************************
CREATE TABLE IF NOT EXISTS HOUSING_PRICE_PROJECT.STAGING_LAYER.RAW_DATA(
    PROPERTY_ID                  VARCHAR,
    CITY                         VARCHAR,
    LOCALITY                     VARCHAR,
    LOCALITY_TIER                VARCHAR,
    PROPERTY_TYPE                VARCHAR,
    BHK                          NUMBER,
    BATHROOMS                    NUMBER,
    BALCONIES                    NUMBER,
    BUILT_UP_AREA                NUMBER,
    CARPET_AREA                  NUMBER,
    FLOOR_NUMBER                 NUMBER,
    TOTAL_FLOORS                 NUMBER,
    FLOOR_CATEGORY               VARCHAR,
    FACING                       VARCHAR,
    FURNISHING_STATUS            VARCHAR,
    PROPERTY_AGE                 NUMBER,
    PARKING_SPACES               NUMBER,
    SECURITY_SCORE               FLOAT,
    GYM_AVAILABLE                NUMBER,
    SWIMMING_POOL                NUMBER,
    POWER_BACKUP                 NUMBER,
    LIFT_AVAILABLE               NUMBER,
    MAINTENANCE_FEE_MONTHLY      NUMBER,
    DISTANCE_TO_CITY_CENTER_KM   FLOAT,
    DISTANCE_TO_METRO_KM         FLOAT,
    NEARBY_SCHOOLS               NUMBER,
    NEARBY_HOSPITALS             NUMBER,
    TRANSACTION_TYPE             VARCHAR,
    PRICE_IN_LAKHS               FLOAT,
    PRICE_CATEGORY               VARCHAR,
    SOURCE_FILE                  VARCHAR
);


-- CREATE THE STREAM OVER THE RAW_DATA TABLE ************************************************************************************************************************
CREATE OR REPLACE STREAM HOUSING_PRICE_PROJECT.STAGING_LAYER.RAWDATA_STREAM
ON TABLE HOUSING_PRICE_PROJECT.STAGING_LAYER.RAW_DATA;


-- CREATE FILE FORMAT FOR INGESTION PIPELINE  ************************************************************************************************************************
CREATE OR REPLACE FILE FORMAT HOUSING_PRICE_PROJECT.PUBLIC.CSV_PIPE_FF
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL','N/A','NA');



-- CREATE PIPE ********************************************************************************************************************************************************
CREATE OR REPLACE PIPE HOUSING_PRICE_PROJECT.STAGING_LAYER.S3_INGESTION_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO HOUSING_PRICE_PROJECT.STAGING_LAYER.RAW_DATA
FROM @HOUSING_PRICE_PROJECT.PUBLIC.HOUSEPRICE_STAGE
FILE_FORMAT = 'HOUSING_PRICE_PROJECT.PUBLIC.CSV_INFERSCHEMA_FF'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (
    SOURCE_FILE = METADATA$FILENAME
); 