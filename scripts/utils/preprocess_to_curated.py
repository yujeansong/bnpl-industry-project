'''
contains helper functions related to preprocessing of data to curated
'''

# required libraries
# import all constants used in the note books
from utils.constants import *
import os

# libraries required
import shutil
from pyspark.sql import functions as F
import pandas as pd
import re
import numpy as np
from sklearn.neighbors import KNeighborsRegressor

from pyspark.sql import SparkSession

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


def preprocess_consumer_to_curated() -> None:
    '''
    Helper function to clean consumer data to curated.
        - Create new feature: 'has_title'
        - Drop address column
        - Add user_id

    Arguments: None
    Output: None
    '''

    tbl_consumer = pd.read_pickle(f"{RAW_DATA}tbl_consumer.pkl")

    # feature engineer - 'has_title': whether or not the name contains a title eg. PhD
    tbl_consumer['has_title'] = tbl_consumer['name'].str.contains('MD|Dr.|DVM|PhD', na=False)

    # drop address since it is fake
    tbl_consumer = tbl_consumer.drop("address", axis=1)

    # add the user_id as this is the feature included in all other tables
    consumer_map = spark.read.parquet(os.path.abspath(f'{TABLE_DATA}consumer_user_details.parquet')).toPandas()

    # merge, then save to curated layer
    tbl_consumer.merge(consumer_map).to_pickle(f"{CURATED_DATA}tbl_consumer.pkl")

    return


def preprocess_merchant_to_curated() -> None:
    '''
    Helper function to clean the merchant data to curated. 
    The product descriptions have many different versions of the same descriptions with slight grammatical shifts. 
    This function creates a new feature 'category' which can be used to collapse these down to a common name

    Arguments: None        
    Ouput: None
    '''
    
    merchants = pd.read_pickle(f'{RAW_DATA}tbl_merchants.pkl')

    # add a new feature which takes only the first item, before the first comma, for the product listing
    # this should be used to identify what types of product the merchant sells
    merchants['category'] = [re.sub(' +', ' ', products.split(',')[0]) for products in merchants['products']]

    # save to curated layer
    merchants.to_pickle(f"{CURATED_DATA}tbl_merchants.pkl")
    return


def preprocess_fp_to_curated() -> None:
    '''
    For now, this is just a pass from raw layer to curated layer.

    Arguments: None
    Output: None
    '''

    # consumer fraud data
    consumer_fraud = pd.read_pickle(f"{RAW_DATA}consumer_fraud_probability.pkl")
    consumer_fraud.to_pickle(f"{CURATED_DATA}consumer_fraud_probability.pkl")

    # merchant fraud data
    merchant_fraud = pd.read_pickle(f"{RAW_DATA}merchant_fraud_probability.pkl")
    merchant_fraud.to_pickle(f"{CURATED_DATA}merchant_fraud_probability.pkl")
    return


def find_unmatched_transactions(transaction_data: pd.DataFrame, \
    reference_data: pd.DataFrame, join_col: str) -> list[pd.DataFrame, pd.DataFrame]:
    '''
    Helper function to check for any transactions that have unmatched data in a given reference dataframe,
    in the first instace this is merchant data and consumer data, but could be used on other references
    Returns the updated transaction data, with unmatched entries removed, and also the removed entries
    Arguments: 
        - transaction_data = pySpark dataframe of transactions
        - reference_data = pandas dataframe of reference data
        - join_col = the column being joined over
    Ouput:
        [updated transaction data, dataframe of unmatched transactions]
    '''

    # first check if we need to run a matching process at all
    # get ids from the reference data
    all_ids = reference_data[join_col].values.tolist()

    # get ids from the transaction data
    all_ids_transactions = transaction_data.select(join_col).distinct().toPandas()[join_col]
    missing_ids = list(set(all_ids_transactions)-set(all_ids))

    # if no missing ids then no need to do anything
    if len(missing_ids)==0:      
        return ([transaction_data, None])
    
    # extract unmatched transactions
    unmatched_data = transaction_data.join(
        F.broadcast(
            spark.createDataFrame(
                [(ID_,) for ID_ in all_ids],
                [join_col],
            )
        ),
        on=join_col, how='leftanti'
    )

    # remove the unmatched transaction data
    transaction_data = transaction_data.join(
        F.broadcast(
            spark.createDataFrame(
                [(ID_,) for ID_ in all_ids],
                [join_col],
            )
        ),
        on=join_col,
    )

    return ([transaction_data, unmatched_data])


def preprocess_transactions_to_curated() ->  list[pd.DataFrame, pd.DataFrame]:
    '''
    Helper function to preprocess transactions data to curated.
    Removes any transactions with unmatched reference data in the merchant and consumer tables.
    Arguments: None        
    Ouput:
        [missing abns, missing consumer ids]
    '''

    transaction_data = spark.read.parquet(os.path.abspath(f'{RAW_DATA}/transactions/*'))
    
    # check for merchant abns in the transaction data without corresponding entry in the merchant data
    merchant_data = pd.read_pickle(f"{CURATED_DATA}tbl_merchants.pkl")
    transaction_data, missing_abn = find_unmatched_transactions(transaction_data, merchant_data, 'merchant_abn')

    # check for consumer ids in the transaction data without corresponding entry in the consumer data
    # get all consumer ids
    consumer_data = pd.read_pickle(f"{CURATED_DATA}tbl_consumer.pkl")    
    transaction_data, missing_con_id = find_unmatched_transactions(transaction_data, consumer_data, 'user_id')
    
    transaction_data.write.mode('overwrite').parquet(os.path.abspath(f'{TRANSACTION_DATA}'))

    # we remove transactions > 30,000 in revenue
    transaction_data = transaction_data.filter(
        F.col('dollar_value') < 30000
    )

    # save to a temp folder
    transaction_data.write.mode('overwrite').parquet(os.path.abspath(f'{CURATED_DATA}/temp'))

    # now overwrite the original folder
    transaction_data = spark.read.parquet(os.path.abspath(f'{CURATED_DATA}/temp'))
    transaction_data.write.mode('overwrite').parquet(os.path.abspath(f'{TRANSACTION_DATA}'))

    # remove the temp folder
    shutil.rmtree(f'{CURATED_DATA}/temp')

    return([missing_abn, missing_con_id])


def preprocess_abs_to_curated() -> None:
    '''
    Helper function to preprocess ABS to curated.
    Requires postcode-SA2_2016 mapping and SA2_2016 and SA2_2021 correspondence.
    Joins SA2 (2021) ABS data with consumer data on postcode.
    Deal with missing values by imputation and removal.

    Arguments: None        
    Ouput: None
    '''

    # read all required files
    abs = pd.read_pickle(f'../data/raw/abs_table_1_SA2.pkl')
    sa2_correspondence = pd.read_pickle(f"{RAW_DATA}sa2_correspondence.pkl")
    sa2_postcode_map = (pd.read_pickle(f"{RAW_DATA}SA2_code.pkl")).drop("state", axis=1)
    tbl_consumer = pd.read_pickle(f"{CURATED_DATA}tbl_consumer.pkl")    

    # add SA2_2016 code to ABS data
    abs_new = pd.merge(abs, sa2_correspondence, left_on='SA2_code', right_on='SA2_CODE_2021', how='left')
    abs_new = abs_new.drop('SA2_CODE_2021', axis=1)
    abs_new = abs_new.rename(columns={'SA2_code': 'SA2_code_2021', 'SA2_MAINCODE_2016': 'SA2_code_2016'})

    # add SA2_2016 code to consumer data
    tbl_consumer_sa2 = pd.merge(tbl_consumer, sa2_postcode_map, on='postcode', how='left')
    tbl_consumer_sa2 = tbl_consumer_sa2.rename(columns={'SA2_code': 'SA2_code_2016'})

    # join consumer data and ABS through SA2_2016 code
    consumerxabs = pd.merge(tbl_consumer_sa2, abs_new, on='SA2_code_2016', how='left')

    # extract SA3 area from SA2 code
    consumerxabs['SA3_code'] = consumerxabs['SA2_code_2016'].apply(lambda x: str(x)[:5])

    # impute missing values with median from SA3 area
    missing_sa2 = (consumerxabs.loc[consumerxabs['relative_SE_dis_score'].isna() & 
        ~consumerxabs['SA2_code_2021'].isna(), 'SA2_code_2021'].unique().tolist())
    to_impute = ["relative_SE_dis_score", "relative_SE_dis_decile", "relative_SE_ad_dis_score", 
        "relative_SE_ad_dis_decile", "economic_resources_score", "economic_resources_decile"]

    for score in to_impute:
        # calculate the median score for each 'SA3_code' group
        median_scores = consumerxabs.groupby('SA3_code')[score].median().reset_index()
        median_scores.rename(columns={score: f'{score}_median'}, inplace=True)
        
        # merge the median_scores with original DataFrame for missing SA2s
        consumerxabs = consumerxabs.merge(median_scores, on='SA3_code', how='left')
        
        # update missing score values
        consumerxabs.loc[consumerxabs['SA2_code_2021'].isin(missing_sa2), score] = consumerxabs[f'{score}_median']
        
        # drop the temporary median column
        consumerxabs.drop(columns=f'{score}_median', inplace=True)

    # remove other missing values
    missing_postcodes = list(set(tbl_consumer_sa2.loc[tbl_consumer_sa2['SA2_code_2016'].isnull(), 'postcode'].tolist()))
    consumerxabs = consumerxabs[~consumerxabs['postcode'].isin(missing_postcodes)]
    consumerxabs_dropped = consumerxabs.dropna()

    # drop SA2 code 2016 and SA3 
    consumerxabs_dropped = consumerxabs_dropped.drop("SA2_code_2016", axis = 1)
    consumerxabs_dropped = consumerxabs_dropped.drop("SA3_code", axis = 1)
    consumerxabs_dropped = consumerxabs_dropped.drop_duplicates()

    # save to curated
    consumerxabs_dropped.to_pickle(f"{CURATED_DATA}consumer_abs_data.pkl")
    
    return


def preprocess_ato_to_curated() -> None:
    '''
    Helper function to preprocess ATO data to curated.
    Deals with missing values by imputing based on the scores of "nearest" postcodes.
    Uses K-Neighbours-Regressor to select 3 closest postcodes.

    Arguments: None        
    Ouput: None
    '''

    # read files
    ato = pd.read_pickle(f"{RAW_DATA}ato_table_6b_ato.pkl")
    consumer = pd.read_pickle(f"{CURATED_DATA}tbl_consumer.pkl")

    # join
    merged = pd.merge(consumer, ato, how="left", on ="postcode")

    # separate df with missing values and df with none
    missing_scores_df = merged[merged.isnull().any(axis=1)]
    valid_scores_df = merged[merged.notnull().all(axis=1)]

    # define the features (postcode) and target (multiple scores) variables for the imputation
    X = valid_scores_df[['postcode']]
    y = valid_scores_df[['population', 'num_tax_payers', 'total_taxable_income', 'num_wage_earners', 'total_wages']]

    # initialize and fit the KNeighborsRegressor on df with no missing values
    knn = KNeighborsRegressor(n_neighbors=3)
    knn.fit(X, y)

    # predict, and fill missing values
    imputed_scores = knn.predict(missing_scores_df[['postcode']])
    missing_scores_df[['population', 'num_tax_payers', 'total_taxable_income', 'num_wage_earners', 'total_wages']] = imputed_scores
    result_df = pd.concat([valid_scores_df, missing_scores_df])

    # save to curated
    result_df.to_pickle(f"{CURATED_DATA}consumer_ato_data.pkl")

    return



def preprocess_to_curated() -> None:
    '''
    Helper function to perform all preprocessing to curated steps.
    
    Arguments: None
    Output: None
    '''

    # preprocess consumer data
    preprocess_consumer_to_curated()

    # preprocess merchant data
    preprocess_merchant_to_curated()

    # preprocess fraud probability data
    preprocess_fp_to_curated()

    # preprocess transactions data
    preprocess_transactions_to_curated()

    # preprocess ABS data (join to consumer)
    preprocess_abs_to_curated()

    # preprocess ATO data
    preprocess_ato_to_curated()

    return