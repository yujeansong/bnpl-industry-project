[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/wNN69YNG)
# Generic Buy Now, Pay Later Project

This project aims to generate a robust ranking system and insights to assist a BNPL firm in finding which merchants they should form partnerships with, in order to maximise future profitability. Our final ranking system incorporates expected profitability per merchant based on:
  * Historic fraud-adjusted revenues, per merchant
  * Demographic exposure of the customer base, per merchant
  * Brand strength, per merchant

## Timeline
The datasets span Jan 2021 to Oct 2022

## Group 26:
  * Michael Pollard (36667)
  * Subin Seol (1086852)
  * Vanessa Famdanny (1297507)
  * Vania Tjanggra (1297661)
  * Yujean Song (1186495)

## How to Run?
A summary of the project can be found in the `summary.ipynb` notebook.  
All required modules are listed in the `requirements.txt file`.  


To run the pipeline, run the files in the following order:

1. ETL Process  
   Run `etl_script.py` under the `scripts` folder to complete the whole ETL process. This includes setting up file structure, downloading external data, and all preprocessing steps. After executing this file, you should be able to obtain a folder of curated dataset under `\data\curated`, which is ready for analysis.  

   For more detailed explanation of how we dealt with missing data when joining to the external dataset, please visit `0_missing_value_analysis.ipynb` notebook.

2. Ranking System - Revenue + Fraud  
   Run `1_fraud.ipynb` to see how missing fraud data is generated\
   Run `2_revenue.ipynb` to see how expected revenue after fraud adjustment is calculated for each merchant

3. Ranking System - Demographic Data  
   Run `3_abs_rank.ipynb` to obtain merchant ranking based on demographic data.

4. Ranking System - Brand Growth and Exposure  
   Run `4_brand_exposure_rank.ipynb` to obtain merchant ranking based on brand growth metrics.

5. Merchant Segmentation  
   Run `5_segmentation_merchant.ipynb` to segment merchants into different categories.

6. Final Ranking  
   Run `6_final_ranking.ipynb` to obtain final ranking of merchants.

7. Interesting Merchant Insights  
   Run `7_merchant_insights.ipynb` to obtain interesting merchant insights.
