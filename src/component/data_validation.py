import pandas as pd  
from src.component.data_ingestion import load_data
from src.utils.logger import logger

# ============================ Checking columns =======================================
def check_columns(df:pd.DataFrame) :

    logger.info("Checking Requried Columns in data ... ")

    requred_columns = [ "CustomerID","Age","Gender","Tenure","Usage Frequency" ,"Support Calls","Payment Delay",
    "Subscription Type","Contract Length","Total Spend","Last Interaction","Churn"]

    for col in requred_columns : 
        if col not in df.columns   : 
            logger.warning(f'{col} Column is Missing in Data  ') 
            raise ValueError(f"{col} Column is missing. ")
        
    else : 
        logger.info("Column Validation Done !" ) 
        return "Column Validation Done !"

    return df 
# ============================ Checking data types ===================================

def check_dtype(df : pd.DataFrame) : 
    logger.info(" Checking Data types of columns in Data ")
    error = [ ]
    # customer id 

    if not pd.api.types.is_numeric_dtype(df['CustomerID']): 
        logger.warning('Customer Id should be int ') 
        error.append("Custumer id should be an integeter values")

    # age  
    if not pd.api.types.is_numeric_dtype(df['Age'])  : 
        logger.warning('Age should be int or float') 
        error.append("Age should be an integeter or flaot values") 
    
    # Gender 
    if not pd.api.types.is_object_dtype(df['Gender']) : 
        logger.warning('Gender Id should be string  ') 
        error.append("Gender id should be an string values")

    #Tenure
    if not pd.api.types.is_numeric_dtype(df['Tenure'])  : 
        logger.warning('Tenure should be Numaric Values') 
        error.append("Tenure should be an Numaric values") 

    # Usage Frequency
    if not pd.api.types.is_numeric_dtype(df['Usage Frequency'])  : 
        logger.warning('Usage Frequency should be Numaric Values') 
        error.append("Usage Frequency should be an Numaric Values") 

    # Support Calls 
    if not pd.api.types.is_numeric_dtype(df['Support Calls'])  : 
        logger.warning('Support Calls should be Numaric Values') 
        error.append("Support Calls should be an Numaric Values") 

    # Payment Delay
    if not pd.api.types.is_numeric_dtype(df['Payment Delay'])  : 
        logger.warning('Payment Delay should be Numaric Values') 
        error.append("Payment Delay should be an Numaric Values") 

    # Subscription Type
    if not pd.api.types.is_object_dtype(df['Subscription Type']) : 
        logger.warning('Subscription Type  should be string ') 
        error.append("Subscription Type  should be an string values")

    # Contract Length
    if not pd.api.types.is_object_dtype(df['Contract Length']) : 
        logger.warning('Contract Length  should be string  ') 
        error.append("Contract Length  should be an string values")

    # Total Spend 
    if not pd.api.types.is_numeric_dtype(df['Total Spend'])  : 
        logger.warning('Total Spend should be Numaric Values') 
        error.append("Total Spend should be an Numaric Values")

    # Last Interaction 
    if not pd.api.types.is_numeric_dtype(df['Last Interaction'])  : 
        logger.warning('Last Interaction  should be Numaric Values') 
        error.append("Last Interaction should be an Numaric Values")

    # Churn
    if not pd.api.types.is_numeric_dtype(df['Churn'])  : 
        logger.warning('Churn should be Numaric Values') 
        error.append("Churn should be an Numaric Values")
    
    if error : 
        return error 
    logger.info("Data Type Validation is Successfully Completed ! ")
    return "Data type  Validated Done  !"

# ========================== Checking for misssing values ============================
def check_null(df : pd.DataFrame ) : 
    
    logger.info("Checking for Missing And Duplicate values .")
    
    null_count = df.isnull().sum().sum()
    duplicate_count = df.duplicated().sum() 

    if null_count > 0 or  duplicate_count >0 : 
        logger.warning(f"Data have {null_count}  : null values  , {duplicate_count} : duplicate count ") 
        return { 
            'Null values' : null_count , 
            'duplicate values' : duplicate_count 
        }
    
    logger.info("No null and duplicate values in data ") 
    return f" Duplicate and Null validation is Done ! "
            

def Validation(df : pd.DataFrame) :  
    logger.info('Data Validation Started ... ')
    check_columns(df) 
    check_dtype(df)
    check_null(df)
    logger.info("Data Validation Done ! ")
    return df 

if __name__ == "__main__" : 
    data = load_data()  
    print(Validation(data))
