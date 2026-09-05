from snowflake.snowpark import Session
from credentials import params
from pathlib import Path
import subprocess
import time
from snowflake.ml.registry import Registry
import os
from dotenv import load_dotenv
import subprocess



def main():
    
    cred, snow, env = "", "", ""
    
    while cred != 'y':
        cred = input("Have you added your Snowflake account credentials to credentials.py? (y/n): ")
        if cred == 'n':
            print("\nPlease add your Snowflake account credentials to credentials.py before running the deployment.")
            return False
    
    while snow != 'y':
        snow = input("Have you installed the Snowflake CLI (snow), created a connection, and set it as the default connection? (y/n): ")
        if snow == 'n':
            print("\nPlease install the Snowflake CLI, create a connection, and set it as the default connection before running the deployment.")
            return False
            
    while env != 'y':
        env = input("Have you added AWS_ROLE_ARN and S3_BUCKET_URL to the .env file? (y/n): ")
        if env == 'n':
            print("\nPlease add AWS_ROLE_ARN and S3_BUCKET_URL to the .env file before running the deployment.")
            return False


  

    session = Session.builder.configs(params).create()

    
    PROJECT_ROOT = Path(__file__).resolve().parent
    # Execute 0_setup.sql **************************************************************************************
    
    SetUp = Path('sql/0_setup.sql')
    
    subprocess.run(
        ["snow", "sql", "-f", f"{SetUp}"],
        check=True
    )
    
    
    # Upload Python Files in Snowflake Stage ********************************************************************
    
    path = Path("src/ML")
    
    files = list(path.glob("*.py"))
    
    for file in files:
        session.sql(f"""PUT file://{PROJECT_ROOT / file} 
            @HOUSING_PRICE_PROJECT.PUBLIC.PROJECT_CODE/ML_CODE
            AUTO_COMPRESS=FALSE
            OVERWRITE=TRUE;""").collect()
        print("IMPORTED: ", file)
    
    
    path = Path("src/data_cleaning")
    
    files = list(path.glob("*.py"))
    
    for file in files:
        session.sql(f"""PUT file://{PROJECT_ROOT / file} 
            @HOUSING_PRICE_PROJECT.PUBLIC.PROJECT_CODE
            AUTO_COMPRESS=FALSE
            OVERWRITE=TRUE;""").collect()
        print("IMPORTED: ", file)
    
    
    
    
    # Create all the remaining infrastructure *******************************************************************
    
    path = Path("sql")
    files = list(path.glob("[0-9]*.sql"))
    files.sort()
    
    files.remove(Path('sql/0_setup.sql')) # remove 0_setup.sql because it was already executed
    
    print("files that will be executed:\n")
    for file in files:
        print(file)
    print("")
    
    for file in files:
        subprocess.run(
            ["snow", "sql", "-f", f"{file}"],
            check=True
        )
    
    
    
    
    # STORAGE INTEGRATION ***************************************************************************************** 
    
    # CREATE STORAGE INTEGRATION AND EXTERNAL STAGE **********************
    # load the .env file
    load_dotenv()
    
    # read the file storage_integration.sql
    sql = Path("sql/storage_integration.sql").read_text()
    
    # replace placeholders with values from .env file
    sql = sql.replace("{{AWS_ROLE_ARN}}", os.environ["AWS_ROLE_ARN"])
    sql = sql.replace("{{S3_BUCKET_URL}}", os.environ["S3_BUCKET_URL"])
    
    # write the query in a temporary sql file
    Path("tmp.sql").write_text(sql)
    
    # execute the sql commands
    subprocess.run(
        ["snow", "sql", "-f", "tmp.sql"],
        check=True
    )
    
    # remove the file tmp.sql
    os.remove("tmp.sql")
    
    
    # OUTPUT CODES FOR TRUST RELATIONSHIP AND VERIFICATION OF CONNECTION **********************
    desc_integr = session.sql("DESCRIBE INTEGRATION HOUSEPRICE_S3_INTEGRATION").to_pandas()
    
    STORAGE_AWS_IAM_USER_ARN = desc_integr.loc[
        desc_integr['"property"'] == 'STORAGE_AWS_IAM_USER_ARN', '"property_value"'
        ].reset_index(drop=True)[0]
    
    STORAGE_AWS_EXTERNAL_ID = desc_integr.loc[
        desc_integr['"property"'] == 'STORAGE_AWS_EXTERNAL_ID', '"property_value"'
        ].reset_index(drop=True)[0]
    
    
    def TrustRelCodes(STORAGE_AWS_IAM_USER_ARN, STORAGE_AWS_EXTERNAL_ID):
        print(
            "\nPlease copy the following values and add them to the "
            "Trust Relationship policy of the AWS IAM role used by the S3 bucket:"
        )
        print(f"\nSTORAGE_AWS_IAM_USER_ARN = {STORAGE_AWS_IAM_USER_ARN}")
        print(f"STORAGE_AWS_EXTERNAL_ID = {STORAGE_AWS_EXTERNAL_ID}")
        
        copied = False
        while not copied:
            quest_input = input("\nEnter 'y' once you have copied both values: ")
            if quest_input.lower() == 'y':
                copied = True
    
    
    ConnectionWorks = False
    Attempt = 0
    
    while not ConnectionWorks:
        try: # check if the connection to the S3 bucket works
            
            ListStage = session.sql("LIST @HOUSING_PRICE_PROJECT.PUBLIC.HOUSEPRICE_STAGE;").to_pandas() 
            ConnectionWorks = True
            print(f"Attempt {Attempt}") if Attempt > 0 else 0
            print("connection works!")
            print("\nfiles in S3 bucket:")
            for row in range(ListStage['"name"'].count()):
                print(ListStage.iloc[row,0])
            print("\n" + 80 * "*")
        
        except: # in case the connection doesn't work and "try" failed
    
            # print it at the first attempt (i.e. when Attempt == 1)
            print(f"Attempt {Attempt}") if Attempt > 0 else 0           
            print("connection does NOT work") if Attempt > 0 else 0
    
            # runs function that prints the codes for S3-bucket trust relationship, only before any attempt (i.e. when Attempt == 0)
            TrustRelCodes(STORAGE_AWS_IAM_USER_ARN, STORAGE_AWS_EXTERNAL_ID) if Attempt == 0 else 0 
    
            # after 5 attempts it breaks out of the loop and interrupt the code
            if Attempt == 5:
                ConnectionWorks = True
                print("failed! no attempts left!\n" + 80 * "*")
                break
            
            
            
            Ready = False
    
            # print this only before first test attempt (when Attempt == 0)
            if Attempt == 0:  
                print("\n" + 80 * "*" + "\nCONNECTION TEST\nPlease, enter 'y' to test the connection, 'n' to interrupt it\n" + 80 * "*")
                re = ""
            else:
                re = "re-" 
       
            #  query user whether to start or not the test, which means let while-loop to run into the "try" statement above
            while not Ready:
                
                quest_input = input(f"\nProceed with connection {re}test: ") # query the user whether to start the test or not
                
                if quest_input.lower() == 'y': # verify whether the user says "yes"
                    Attempt += 1
                    Ready = True 
                
                elif quest_input.lower() == 'n': # verify whether the user says "no"
                    print("interrupt testing\n\n" + 80 * "*")
                    Ready = 7 # assign this to Ready, to trigger the coming if-clause 
    
            if Ready == 7: # interrupting main while-loop due to negative user answer on running connection test question
                break
    
    
    
    return True   
    
    
    
if __name__ == "__main__":
    main()