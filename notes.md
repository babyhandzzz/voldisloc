# s&p constituents:
https://github.com/datasets/s-and-p-500-companies/blob/main/data/constituents.csv

# data aggregation problem:
options data is quite complex an requires normalization efforts. The plan is to figure aggreagation and normalization procedures using the sample data that was loaded.

# Two things:
 I passed FRM1, so I took two days off not solving FRM2 problems or coding this project.
 I realized there was no date in the table, so I didn't know which date any option chain belonged to. Fixed that.


# 01/07/2025
Figured out histroical data loading from alphaventage. running data collection as an app in GCP. 
Had some issue with the memory. I will need to get rid of pandas to go faster.