'''
helper functions to download external data
'''

# required libraries
from utils.constants import *
from urllib.request import urlretrieve
import requests, os, zipfile
import pandas as pd


def read_and_convert_excel_sheets_to_csv(filename: str, sheet_names_to_read: list, granularity: str) -> None:
    '''
    Helper function to read and convert xlsx files to csv.

    Arguments:
        - filename: directory of the xlsx file
        - sheet_names_to_read: list of sheets we wish to read and download
        - granularity: string of either SA2 or postcode
    Output: None
    '''

    for sheet_name in sheet_names_to_read:
        try:
            # read the specified sheet into a pandas DataFrame
            df = pd.read_excel(filename, sheet_name = sheet_name)

            # define the CSV filename 
            csv_filename = f"{LANDING_DATA}{'_'.join(sheet_name.split()).lower()}_{granularity}.csv"

            # save the DataFrame as a CSV file
            df.to_csv(csv_filename, index=False)

            print(f"Converted '{sheet_name}' to '{csv_filename}'")

        except Exception as e:
            print(f"An error occurred while processing {sheet_name}: {e}")

    return


def download_abs_data() -> None:
    '''
    Downloads relevant 2021 ABS data.

    Arguments: None
    Output: None
    '''

    # download xlsx file to landing layer
    url = "https://www.abs.gov.au/statistics/people/people-and-communities/socio-economic-indexes-areas-seifa-australia/2021/Statistical%20Area%20Level%202%2C%20Indexes%2C%20SEIFA%202021.xlsx"
    filename = f"{LANDING_DATA}SEIFA_SA2_data.xlsx"
    urlretrieve(url, filename)

    # define sheets to read, convert to csv, save
    sheet_names_to_read = ['Table 1']
    read_and_convert_excel_sheets_to_csv(filename, sheet_names_to_read, 'SA2')

    # download postcode to SA2 mapping file to landing layer
    url = f"https://www.matthewproctor.com/Content/postcodes/australian_postcodes.csv"
    filename = f"{LANDING_DATA}abs_sa2_lookup.csv"
    sa2 = requests.get(url)
    with open(filename, "w", encoding="utf-8") as fh:
        fh.write(sa2.text)

    # download SA2 2016 to SA2 2021 correspondence file
    sa2_correspondence_url = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/correspondences/CG_SA2_2016_SA2_2021.csv"
    urlretrieve(sa2_correspondence_url, f'{LANDING_DATA}sa2_correspondence.csv')

    return

def download_old_abs_data() -> None:
    '''
    Downloads postcode ABS data. This is only for missing value analysis, so the actual data won't be used.

    Arguments: None
    Output: None
    '''

     # download xlsx file to landing layer
    url = f"https://www.abs.gov.au/statistics/people/people-and-communities/socio-economic-indexes-areas-seifa-australia/2021/Postal%20Area%2C%20Indexes%2C%20SEIFA%202021.xlsx"
    filename = f"{LANDING_DATA}SEIFA_data.xlsx"
    urlretrieve(url, filename)

    # define sheets to read, convert to csv, save
    sheet_names_to_read = ['Table 1']
    read_and_convert_excel_sheets_to_csv(filename, sheet_names_to_read, 'postcode')

    return


def download_ato_data() -> None:
    '''
    Helper function to download ATO Data

    Arguments: None
    Output: None
    '''

    # download excel file into the landing layer
    url = f"https://data.gov.au/data/dataset/5fa69f19-ec44-4c46-88eb-0f6fd5c2f43b/resource/d2eb3863-78c6-4afe-a348-83043df5aeab/download/ts20individual06taxablestatusstateterritorypostcode.xlsx"
    filename = f"{LANDING_DATA}ATO_data.xlsx"
    urlretrieve(url, filename)

    # define the list of sheet names to read
    sheet_names_to_read = ['Table 6B']

    # read and convert Excel sheets to CSV
    read_and_convert_excel_sheets_to_csv(filename, sheet_names_to_read, 'ato')

    return


def download_shapefile_data() -> None:
    '''
    Helper function to download Australian SA2 shapefile data

    Arguments: None
    Output: None
    '''

    # download the zip file
    url = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip"
    urlretrieve(url,  "../data/landing/aussie_map_SA2.zip")

    #  extract the zip file
    zip_file_path = f"{LANDING_DATA}aussie_map_SA2.zip"
    extracted_dir = f"{LANDING_DATA}"
    if not os.path.exists(extracted_dir):
        os.makedirs(extracted_dir)
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_dir)

    return


def download_all() -> None:
    '''
    Downloads all external data. Saves all data to landing layer.
    External data includes:
        - ABS data
        - ATO data
        - Australia SA2 shapefile data
    
    Arguments: None
    Output: None
    '''
    
    # download ABS data
    download_abs_data()
    download_old_abs_data()
    download_ato_data()
    download_shapefile_data()

    return
