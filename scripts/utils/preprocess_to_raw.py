'''
contains helper functions related to preprocessing of data to raw layer.
'''

# required libraries
# import all constants used in the note books
from utils.constants import *

# libraries required
from pyspark.sql import functions as F
import pandas as pd
import re
import os

from pyspark.sql import SparkSession

#os.env["SPARK_LOCAL_DIRS"] = os.getcwd()

# create a spark session 
spark = (
    SparkSession.builder.appName("MAST30034 Project 2")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config('spark.driver.memory', '4g')
    .config('spark.executor.memory', '2g')
    .getOrCreate()
)


def prepare_transaction_data(get_folder: str) -> None:    
    '''
    Helper function to clean the transaction data to raw 

    Arguments:
        get_folder: directory containing transaction data in pySpark format
    Ouput: None
    '''    
    
    # load the data
    data = spark.read.parquet(os.path.abspath(f'{TABLE_DATA}{get_folder}/'))  

    # round dollar value to 4 decimal places
    data = data.withColumn('dollar_value', F.round('dollar_value', 4))

    # save the data to the transactions sub-folder in the RAW layer
    data.write.mode('overwrite').parquet(os.path.abspath(f'{RAW_DATA}/transactions/{get_folder}'))

    return


def split_to_columns(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Helper function to split the features in consumer data.

    Arguments:
        - df: datafame with multiple features in a column
    Output:
        - df: modified dataframe with features split to diff columns
    '''

    df[['name', 'address', 'state', 'postcode', 'gender', 'consumer_id']] = df['info'].str.split('|', expand=True)
    df.drop(columns=['info'], inplace=True)
    return df

def preprocess_consumer_data() -> pd.DataFrame:
    '''
    Helper function to clean consumer data to raw.
        - Split features into separate columns
        - Clean postcode values for consistency
        - Save to raw

    Arguments: None
    Output: The modified dataframe
    '''

    # read consumer data
    tbl_consumer = pd.read_csv(f"{TABLE_DATA}tbl_consumer.csv")
    tbl_consumer.rename(columns={'name|address|state|postcode|gender|consumer_id': 'info'}, inplace=True)

    # split features into separate columns
    tbl_consumer = split_to_columns(tbl_consumer)

    # zero fill three digit postcodes
    tbl_consumer['postcode'] = tbl_consumer['postcode'].str.zfill(4)

    # udpate data type for consumer_id
    tbl_consumer['consumer_id'] = tbl_consumer['consumer_id'].astype('int')

    # save to raw 
    tbl_consumer.to_pickle(f"{RAW_DATA}tbl_consumer.pkl")

    return tbl_consumer
    

def extract_tags(tag: str) -> list[str, str, str]:
    '''
    Helper function to extract the products, revenue levels and take rate from a string
    using regular expressions.

    Arguments:
        - tag: a string consisting of info regarding products, revenue and take rate
    Output: 
        - 3-element list containing the separated product, revenue, and take rate
    '''

    tag = tag[1:-1]
    pattern = r'\([^()]*\)|\[[^\[\]]*\]'
    result = re.findall(pattern, tag)
    return result  


def preprocess_merchant_data() -> pd.DataFrame:
    '''
    Helper function to clean merchant data to raw.
        - Extract products, revenue, and take rate from the 'tags' column
        - Save to raw
    Arguments: None
    Output: the modified dataframe
    '''
    
    tbl_merchants = spark.read.parquet(os.path.abspath(f"{TABLE_DATA}tbl_merchants.parquet"))
    tbl_merchants_df = tbl_merchants.toPandas() #convert to pandas as it is a small df and is easier to work with
    
    # list to store products, revenue levels and take rates
    products_list = []
    revenue_levels_list = []
    take_rate_list = []

    # extract the products, revenue levels and take rates
    for i in range(len(tbl_merchants_df)):
        tag_str = tbl_merchants_df.at[i, 'tags']
        product, revenue_level, take_rate = extract_tags(tag_str)
        products_list.append(product[1:-1].lower()) # lower case all products for consistency
        revenue_levels_list.append(revenue_level[1:-1]) 
        take_rate_list.append(float(take_rate[12:-1])) # extract the rate and convert to float 
    
    # add the separate features to dataframe
    tbl_merchants_df["products"] = products_list
    tbl_merchants_df["revenue_levels"] = revenue_levels_list
    tbl_merchants_df["take_rate"] = take_rate_list

    # drop the tags column - no longer needed
    tbl_merchants_df = tbl_merchants_df.drop("tags", axis=1)

    # save to raw layer
    tbl_merchants_df.to_pickle(f"{RAW_DATA}tbl_merchants.pkl")

    return tbl_merchants_df


def preprocess_fp() -> None:
    '''
    Preprocessing fraud probability data for both consumers and merchants.
    Fix data type to date format, then save to pickle.

    Arguments: None
    Output: None
    '''

    # consumer fraud data
    consumer_fraud = pd.read_csv(f"{TABLE_DATA}consumer_fraud_probability.csv")
    consumer_fraud['order_datetime'] = pd.to_datetime(consumer_fraud['order_datetime'])
    consumer_fraud['order_datetime'] = consumer_fraud['order_datetime'].dt.date
    consumer_fraud.to_pickle(f"{RAW_DATA}consumer_fraud_probability.pkl")

    # merchant fraud data
    merchant_fraud = pd.read_csv(f"{TABLE_DATA}merchant_fraud_probability.csv")
    merchant_fraud['order_datetime'] = pd.to_datetime(merchant_fraud['order_datetime'])
    merchant_fraud['order_datetime'] = merchant_fraud['order_datetime'].dt.date
    merchant_fraud.to_pickle(f"{RAW_DATA}merchant_fraud_probability.pkl")

    return

def clean_abs(df: pd.DataFrame, rows_to_skip: int = 4, last_rows_to_skip: int = 3, last_cols_to_skip: int = 2) -> pd.DataFrame:
    '''
    Clean the ABS data - drop unrequired columns

    Arguments: 
        - df: dataFrame containing raw data
        - rows_to_skip: number of initial rows to skip (defaults to 4)
        - last_rows_to_skip: number of ending rows to skip (defaults to 3)
        - last_cols_to_skip: number of ending columns to skip (defaults to 2)
`    Output:
        - df: the processed dataframe
    '''

    # drop the first 'rows_to_skip' rows
    df = df.drop(df.index[:rows_to_skip])
    
    # set column names to the values of the first row
    df.columns = df.iloc[0]
    df = df[1:]
    
    # drop last 'last_rows_to_skip' rows 
    df = df.iloc[:-last_rows_to_skip]
    
    # drop all NaN value columns
    df = df.dropna(axis=1, how='all')
    
    # drop the last 'last_cols_to_skip' columns
    # df = df.iloc[:, :-last_cols_to_skip]
    
    return df


def preprocess_sa2() -> None:
    '''
    Helper function to preprocess SA2 to postcode mapping data.

    Arguments: None
    Output: None
    '''

    # read data and clean postcode values for consistency
    abs_sa2 = pd.read_csv(f"{LANDING_DATA}abs_sa2_lookup.csv")
    abs_sa2['postcode'] = abs_sa2['postcode'].astype(str)
    abs_sa2['postcode'] = abs_sa2['postcode'].str.zfill(4)

    # extract the desired columns
    abs_sa2 = abs_sa2[['postcode', 'state', 'SA2_MAINCODE_2016']]
    abs_sa2.rename(columns={'SA2_MAINCODE_2016': 'SA2_code'}, inplace=True)

    # remove duplicates, save to raw
    abs_sa2 = abs_sa2.drop_duplicates()
    abs_sa2.to_pickle(f"{RAW_DATA}SA2_code.pkl")

    # extract desired columns from correspondence file
    sa2_correspondence = pd.read_csv(f"{LANDING_DATA}sa2_correspondence.csv")
    sa2_correspondence = sa2_correspondence[["SA2_MAINCODE_2016", "SA2_CODE_2021"]]
    sa2_correspondence.to_pickle(f"{RAW_DATA}sa2_correspondence.pkl")

    return 

def preprocess_abs() -> None:
    '''
    Preprocess downloaded ABS data - renaming columns and fixing data types. Add SA2 code to each postcode.

    Arguments: None
    Output: None
    '''
    file_names = ["table_1"]
    file_path = f"{LANDING_DATA}{file_names}.csv"

    # process each CSV file and store the result in the dictionary
    for file_name in file_names:
        df = pd.read_csv(f"{LANDING_DATA}{file_name}_SA2.csv")

        processed_df = clean_abs(df)

        # Rename columns for clarity
        processed_df.columns = [
            'SA2_code',
            'SA2_name',
            'relative_SE_dis_score',
            'relative_SE_dis_decile',
            'relative_SE_ad_dis_score',
            'relative_SE_ad_dis_decile',
            'economic_resources_score',
            'economic_resources_decile',
            'education_occupation_score',
            'education_occupation_decile',
            'population'
        ]

        # change scores to float
        update_cols =  list(processed_df.columns[processed_df.columns.str.contains('score')])    
        # fills missing values with NaNs for now
        processed_df[update_cols] = processed_df[update_cols].apply(pd.to_numeric, errors='coerce')

        # change deciles and population to int
        update_cols =  list(processed_df.columns[processed_df.columns.str.contains('decile')]) + ['population']   
        # first converts to float, and fills with NaNs for now
        processed_df[update_cols] = processed_df[update_cols].apply(pd.to_numeric, errors='coerce')
        # then convert to int
        # processed_df[update_cols] = processed_df[update_cols].fillna(0).astype(int)
        processed_df[update_cols] = processed_df[update_cols].astype(pd.Int64Dtype())

        # save to pickle at raw layer
        processed_df.to_pickle(f"{RAW_DATA}abs_{file_name}_SA2.pkl")  

    return


def clean_old_abs(df: pd.DataFrame, rows_to_skip: int = 4, last_rows_to_skip: int = 3, last_cols_to_skip: int = 2):
    '''
    Clean the ABS data - drop unrequired columns
    Arguments: 
        - df: dataFrame containing raw data
        - rows_to_skip: number of initial rows to skip (defaults to 4)
        - last_rows_to_skip: number of ending rows to skip (defaults to 3)
        - last_cols_to_skip: number of ending columns to skip (defaults to 2)
`    Output:
        - df: the processed dataframe
    '''

    # drop the first 'rows_to_skip' rows
    df = df.drop(df.index[:rows_to_skip])

    # set column names to the values of the first row
    df.columns = df.iloc[0]
    df = df[1:]

    # drop last 'last_rows_to_skip' rows 
    df = df.iloc[:-last_rows_to_skip]

    # drop all NaN value columns
    df = df.dropna(axis=1, how='all')

    # drop the last 'last_cols_to_skip' columns
    df = df.iloc[:, :-last_cols_to_skip]

    return df


def preprocess_old_abs() -> None:
    '''
    Preprocess old postcode ABS data - renaming columns and fixing data types.

    Arguments: None
    Output: None
    '''

    file_names = ["table_1"]
    file_path = f"{LANDING_DATA}{file_names}.csv"

    # process each CSV file and store the result in the dictionary
    for file_name in file_names:
        df = pd.read_csv(f"{LANDING_DATA}{file_name}_postcode.csv")

        processed_df = clean_old_abs(df)

        # Rename columns for clarity
        processed_df.columns = [
            'POA_code',
            'relative_SE_dis_score',
            'relative_SE_dis_decile',
            'relative_SE_ad_dis_score',
            'relative_SE_ad_dis_decile',
            'economic_resources_score',
            'economic_resources_decile',
            'education_occupation_score',
            'education_occupation_decile',
            'population'
        ]

        # change scores to float
        update_cols =  list(processed_df.columns[processed_df.columns.str.contains('score')])    
        # fills missing values with NaNs for now
        processed_df[update_cols] = processed_df[update_cols].apply(pd.to_numeric, errors='coerce')

        # change deciles and population to int
        update_cols =  list(processed_df.columns[processed_df.columns.str.contains('decile')]) + ['population']   
        # first converts to float, and fills with NaNs for now
        processed_df[update_cols] = processed_df[update_cols].apply(pd.to_numeric, errors='coerce')
        # then convert to int
        processed_df[update_cols] = processed_df[update_cols].astype(pd.Int64Dtype())

        # save to pickle at raw layer
        processed_df.to_pickle(f"{RAW_DATA}abs_{file_name}_postcode.pkl")  

    return


def prepare_ATO_data(filename: str, keep_cols: dict[str, str]) -> None:    
    '''
    Helper function to preprocess and clean ATO data

    Arguments:
        - filename: name of CSV file to process and convert to pickle
        - keep_cols: dictionary with key = original column name in ATO data,
                    value = new column name for saving
    Ouput: None
    '''
    
    # load the data
    df = pd.read_csv(f"{LANDING_DATA}{filename}.csv", skiprows=1)     
    
    # get the columns to be saved, and rename
    get_cols = list(keep_cols.keys())
    df = df[get_cols]
    df.rename(columns=keep_cols, inplace=True)

    # change all columns other than postcode to int64
    update_cols =  list(df.columns)
    update_cols.remove('postcode')  
    # first converts to float, and fills with NaNs for now
    df[update_cols] = df[update_cols].apply(pd.to_numeric, errors='coerce')
    # then convert to int
    df[update_cols] = df[update_cols].astype(pd.Int64Dtype())

    # generate a new name for the processed DataFrame
    new_file_name = f"ato_{filename}"
    df.to_pickle(f"{RAW_DATA}{new_file_name}.pkl")  # Save the processed DataFrame to a pickle file in the RAW layer
    

def preprocess_to_raw() -> None:
    '''
    Helper function to perform all preprocessing to raw steps.
    
    Arguments: None
    Output: None
    '''
    
    # preprocess transactions data - work through all the transaction data and save to raw
    for data_folder in [x for x in os.listdir(TABLE_DATA) if \
                    os.path.isdir(os.path.join(TABLE_DATA, x))]:
        prepare_transaction_data(data_folder)

    # preprocess consumer data
    preprocess_consumer_data()

    # preprocess merchant data
    preprocess_merchant_data()

    # preprocess fraud probabilities
    preprocess_fp()

    # preprocess SA2 mapping data + correspondence file
    preprocess_sa2()

    # preprocess ABS data
    preprocess_abs()

    # preprocess ABS postcode data (unused, only for missing value analysis)
    preprocess_old_abs()

    # preprocess ATO data
    prepare_ATO_data('table_6b_ato', {'Postcode': 'postcode', 'Individuals\nno.': 'population', 
        'Taxable income or loss3\nno.': 'num_tax_payers', 'Taxable income or loss3\n$': 'total_taxable_income', 
        'Salary or wages\nno.': 'num_wage_earners', 'Salary or wages\n$': 'total_wages'})

    return
