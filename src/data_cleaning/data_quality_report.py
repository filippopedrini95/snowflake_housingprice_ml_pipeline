from snowflake.snowpark.functions import col, count, when

def main(session):

    tab_name = "DATA_QUALITY_REPORT"
    sourcefile_list = []
    sourcefile_dqr_list = []
    table_exist = False


    df_snowpark = session.table("HOUSING_PRICE_PROJECT.STAGING_LAYER.RAW_DATA")

    #************************************************************************************************
    sourcefile_list_raw = df_snowpark.select(col('SOURCE_FILE')).distinct().collect()
    
    for i in sourcefile_list_raw:
        for j in i:
            sourcefile_list.append(j)

    try:
        dqr_df = session.table(f"HOUSING_PRICE_PROJECT.STAGING_LAYER.{tab_name}")
        sourcefile_dqr_raw = dqr_df.select(col('SOURCE_FILE')).distinct().collect()
        table_exist = True
    
        for i in sourcefile_dqr_raw:
            for j in i:
                sourcefile_dqr_list.append(j)
    except:
        pass
    #************************************************************************************************


    for sc in sourcefile_list:

        if sc in sourcefile_dqr_list:
            continue

        df_filtered = df_snowpark.filter(
            (col('SOURCE_FILE') == sc)
        )

        schema = ['SOURCE_FILE', 'CHECK_TYPE', 'OTHER']
        line = ["'" + sc + "'"]

        cols = df_filtered.columns
        cols.remove('SOURCE_FILE')
        cols

        # CREATE TAB HEADER (schema) AND ROW WITH MISSING VALUE (line) ****************************************************
        line, schema = null_count(df_filtered, schema, line, cols)

        if not table_exist:
            create_tab(session, tab_name, schema)

        inserting(session, tab_name, schema, line)




# FUNCTION FOR CHECKING FOR NULL VALUES *************************************************************************************
def null_count(df_filtered, schema, line, cols):
    for col_name in cols: 
        a = df_filtered.select(
            count(when(col(col_name).is_null(), 1)).alias(f"NULL_COUNT")
        ).collect()

        if len(line) == 1:
            line.append("'NULL_COUNT'")
            line.append("NULL")
        
        line.append(str(a[0][0]))
        schema.append(col_name)

    return line, schema



# FUNCTION FOR CREATING TABLE IF NOT EXISTS *************************************************************************************
def create_tab(session, tab_name, schema):
    col_dtypes = []
        
    for i in schema:
        if i == 'SOURCE_FILE' or i == 'CHECK_TYPE':
            col_dtypes.append(i + " STRING")
        else:
            col_dtypes.append(i + " NUMERIC")
        
    query_create = f"""CREATE TABLE IF NOT EXISTS HOUSING_PRICE_PROJECT.STAGING_LAYER.{tab_name} ({", ".join(col_dtypes)});"""   
    session.sql(query_create).collect()



# FUNCTION FOR INSERTING ROW WITH MISSING VALUE IF NOT EXISTS *****************************************************************
def inserting(session, tab_name, schema, line):
    
    query_insert = f"""INSERT INTO HOUSING_PRICE_PROJECT.STAGING_LAYER.{tab_name} 
    ({", ".join(schema)})
    VALUES
    ({", ".join(line)});"""
    
    session.sql(query_insert).collect()