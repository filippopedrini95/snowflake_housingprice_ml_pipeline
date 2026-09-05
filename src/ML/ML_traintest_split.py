def main(session, testset_cap = 5000):

    len_testset = session.sql(
        "SELECT COUNT(1) FROM HOUSING_PRICE_PROJECT.ML_LAYER.TEST_SET"
    ).collect()[0][0]


    if len_testset >= testset_cap:
        
        query = f"""SELECT * EXCLUDE (BHK, BATHROOMS, BUILT_UP_AREA, PROPERTY_ID, PRICE_CATEGORY, SOURCE_FILE, "METADATA$ACTION", "METADATA$ISUPDATE", "METADATA$ROW_ID"),
        FROM HOUSING_PRICE_PROJECT.CLEANING_LAYER.CLEANDATA_STREAM
        WHERE METADATA$ACTION = 'INSERT'"""  

        dml_query = f"""INSERT INTO HOUSING_PRICE_PROJECT.ML_LAYER.TRAIN_SET ({query})"""

        session.sql(dml_query).collect()

    else:

        # CREATE THE TEMPORARY TABLE CONTAINING DATA + TRAIN/TEST SPLIT *************************************************************
        query = f"""SELECT * EXCLUDE (BHK, BATHROOMS, BUILT_UP_AREA, PROPERTY_ID, PRICE_CATEGORY, SOURCE_FILE, "METADATA$ACTION", "METADATA$ISUPDATE", "METADATA$ROW_ID"),
                    ROW_NUMBER() OVER (ORDER BY RANDOM()) AS RANDOM_NUMB
                    FROM HOUSING_PRICE_PROJECT.CLEANING_LAYER.CLEANDATA_STREAM
                    WHERE METADATA$ACTION = 'INSERT'"""
            
        ddl_query = f"""CREATE OR REPLACE TRANSIENT TABLE HOUSING_PRICE_PROJECT.ML_LAYER.CLEANDATA_SPLIT_TEMP 
            AS {query};"""
            
        session.sql(ddl_query).collect()


        # COUNT OF MISSING ROWS TO REACH TESTSET_CAP **********************************************************************************
        tot_rows = session.sql(
                "SELECT COUNT(1) FROM HOUSING_PRICE_PROJECT.ML_LAYER.CLEANDATA_SPLIT_TEMP"
        ).collect()[0][0]
        
        nr_test_rows = round(tot_rows * 0.2)
        
        miss_row_testset = testset_cap - len_testset

        thresh = min(nr_test_rows, miss_row_testset)
        

    # INSERTING THE ROWS EITHER IN TEST OR TRAIN SET *******************************************************************************
    for tab, cond in [("TEST_SET", "<="), ("TRAIN_SET",">")]:

        query = f"""SELECT * EXCLUDE RANDOM_NUMB
                    FROM HOUSING_PRICE_PROJECT.ML_LAYER.CLEANDATA_SPLIT_TEMP
                    WHERE RANDOM_NUMB {cond} {thresh}"""
        
        dml_query = f"""INSERT INTO HOUSING_PRICE_PROJECT.ML_LAYER.{tab} ({query})"""

        session.sql(dml_query).collect()     

    session.sql("DROP TABLE HOUSING_PRICE_PROJECT.ML_LAYER.CLEANDATA_SPLIT_TEMP").collect()


