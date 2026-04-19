import pandas as pd  
import os 
from src.utils.logger import logger

def load_data(path = None ) : 
    if path : 
        path = path 
    else : 
        path = "Data\customer_churn_dataset-training-master.csv"

    try :     
        logger.info(" Data Loading Started ... ")

        # data loading 
        df = pd.read_csv(path)

        logger.info(f"Data Loaded Successfully with shape {df.shape}")
        
        return df 


    except Exception as e   : 
        logger.warning(f'No Data at the given file path {str(e)}')
        raise e 



if __name__ == '__main__' : 
    print(get_data("Data\customer_churn_dataset-testing-master.csv") )