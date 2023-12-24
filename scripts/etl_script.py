'''
Complete ETL script
An automated ingestion pipeline to extract the datasets and output curated datasets for insights.
'''

# import required libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

from utils.constants import *
from utils.download_data import *
from utils.preprocess_to_raw import *
from utils.preprocess_to_curated import *

# make sure script is being run from scripts directory
if os.path.basename(os.path.normpath(os.getcwd())) != 'scripts':
    all_sub_dirs = [x[0] for x in os.walk(os.getcwd())]
    for sub_dir in all_sub_dirs:
        folder_name = os.path.basename(os.path.normpath(sub_dir))
        if folder_name =='scripts':
            os.chdir(sub_dir)
            break


# build file structure - create directory for landing, raw, and transactions data
print("stage 1 - build file structure")
for folder in ADD_FOLDERS:
    print(f'check directory: {folder}')
    if not os.path.exists(folder):
        os.makedirs(folder)
print("stage 1 completed\n")

# download external data
print("stage 2 - downloading external data")
download_all()
print("stage 2 completed\n")

# preprocessing to raw
print("stage 3 - preprocess dataset to raw layer")
preprocess_to_raw()
print("stage 3 completed\n")

# preprocessing from raw to curated
print("stage 4 - preprocess dataset to curated layer")
preprocess_to_curated()
print("stage 4 completed\n")