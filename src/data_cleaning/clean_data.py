def main(session, max_nulls = 10):     # max number of null values per row: rows with more NULLS will be dropped

    df_snowpark = session.table("HOUSING_PRICE_PROJECT.STAGING_LAYER.RAW_DATA")

    cols = df_snowpark.columns
    iffs = """"""
    for i in cols:
        iffs += f""" + IFF({i} IS NULL, 1, 0)"""
    iffs = iffs[3:]


    sql_query = f"""SELECT DISTINCT * EXCLUDE ("METADATA$ACTION", "METADATA$ISUPDATE", "METADATA$ROW_ID") FROM HOUSING_PRICE_PROJECT.STAGING_LAYER.RAWDATA_STREAM WHERE {iffs} < {max_nulls} AND PRICE_IN_LAKHS IS NOT NULL AND METADATA$ACTION = 'INSERT' """


    dml_query = f"""INSERT INTO HOUSING_PRICE_PROJECT.CLEANING_LAYER.CLEAN_DATA ({sql_query})"""

    return(session.sql(dml_query).collect())