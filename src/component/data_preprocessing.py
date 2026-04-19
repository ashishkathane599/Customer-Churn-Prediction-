"""
Preprocessing file for : 
1. drop Null values 
2. remove duplicates 
3. encoding 
4. scalling 
""" 

import pandas as pd  
from src.component.data_validation import Validation 
from src.component.data_ingestion import load_data 
from src.utils.logger import logger
from sklearn.preprocessing import StandardScaler 
import joblib 
import os 


def clean_data(df):
    logger.info("Starting data cleaning...")

    df = df.copy()   

    
    # Remove null values
    null_count = df.isnull().sum().sum()
    logger.info(f"Null values found: {null_count}")

    if null_count > 0:
        before = len(df)
        df = df.dropna()
        after = len(df)
        logger.info(f"Removed {before - after} rows with null values")

    # Remove duplicates
    duplicate_count = df.duplicated().sum()
    logger.info(f"Duplicate rows found: {duplicate_count}")

    if duplicate_count > 0:
        before = len(df)
        df = df.drop_duplicates()
        after = len(df)
        logger.info(f"Removed {before - after} duplicate rows")

    
    # Remove unnecessary columns
    logger.info("Removing unnecessary columns...")

    unnecessary_columns = ["CustomerID"]

    existing_cols = [col for col in unnecessary_columns if col in df.columns]
    df = df.drop(columns=existing_cols)

    logger.info(f"Dropped columns: {existing_cols}")

    logger.info("Cleaning completed successfully")

    return df
  


def encode_data(df:pd.DataFrame) :  
    """ encode categorical columns here  
    like : ['Gender', 'Subscription Type', 'Contract Length'] """

    logger.info('Starting feature  encoding ....')

    # Gender
    logger.info("Converting Gender : categorical --> numarical ")
    df = pd.get_dummies(df, columns=["Gender"] , dtype=int)
    
    # Subscription Type
    logger.info("Converting subscription Type : categorical --> Numarical")
    df['Subscription Type'] = df['Subscription Type'].map({'Basic' : 0 , 'Standard' :1 ,'Premium':2})

    # Contract Length
    logger.info("Converting Contract Lenght : categorical --> Numarical")
    df['Contract Length'] = df['Contract Length'].map({'Monthly' :0 , 'Quarterly' :1 , 'Annual':2 })

    logger.info("Categorical feature Encoding Done ! ")

    return df 


def scale_data(df : pd.DataFrame ) : 
    logger.info("Starting Feature Scaling ") 

    # loading data to extract numaric features for scaling 
    copy_data  = load_data()
    num_columns  = [ col for col in copy_data.columns if copy_data[col].dtype != 'O' and col not in ['CustomerID'  , 'Churn'] ]
    
    # subscription type is binary columsn 
    if len(num_columns) != 7 : 
        logger.warning('Missing or Extra  Numaric values are there ...')
        raise ValueError("Numaric Columns number is missmached with staanderd columns.")

    logger.info('Spliting the Data ...')
    # spliting data here 
    X = df.drop(columns=['Churn'] , axis = 1 ) 
    y = df['Churn']

    ##  Applying Scaling to numaric features
    scaler = StandardScaler()  
    X[num_columns]  = scaler.fit_transform(df[num_columns])

    # saving sclaer in model folder if not exist 
    model_path = "Models"
    for file in os.listdir(model_path) : 
        if file != 'encoder.pkl' : 
            logger.info('Saving Encoder Model in the Model folder')
            joblib.dump(scaler , os.path.join(model_path , 'encoder.pkl'))
        else : 
            logger.info('Encoder Model file is  alredy is Present')
    logger.info('Feature Scaling and Scaler-Model Saving is Done ! ')

    return X , y 
    
    
def Preprocessing( df : pd.DataFrame ) : 
    data = load_data()
    data = Validation(data)
    data = clean_data(data)  
    data = encode_data(data)
    X ,y  = scale_data(data) 
    return X , y 



if __name__ == "__main__" : 
    # data = load_data()
    # data = Validation(data)
    # data = clean_data(data)  
    # data = encode_data(data)
    # print(scale_data(data) )

    print(Preprocessing(load_data()))