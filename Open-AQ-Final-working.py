#!/usr/bin/env python
# coding: utf-8

# script to download open-aq data 


#login node trial 
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
#import openaq
import warnings
import pandas as pd
import boto3
import botocore
import os
import time

#links for reference

#aws link - https://openaq-fetches.s3.amazonaws.com/index.html  

#https://patel-zeel.github.io/blog/data/openaq/2020/09/21/Programatically_download_OpenAQ_data.html 

# http://dhhagan.github.io/py-openaq/tutorial/api.html


s3 = boto3.client('s3', config=botocore.config.Config(signature_version=botocore.UNSIGNED))
bucket_name = 'openaq-fetches'
prefix = 'realtime-gzipped/'

# Path where you want to save the downloaded files
path = 'test/'

#change dates accordingly
start_date = '2018/10/01' # start date (inclusive)
end_date = '2018/11/30' # end date (inclusive)

for date in pd.date_range(start=start_date, end=end_date):
    from IPython.display import clear_output
    clear_output(wait=True)
    date = str(date).split(' ')[0]
    print('Downloading:', date)
    data_dict = s3.list_objects(Bucket = bucket_name)
    for file_obj in data_dict['Contents']:
        
        f_name = file_obj['Key']
        tmp_path = '/'.join((path+f_name).split('/')[:-1])
        if not os.path.exists(tmp_path):
            
             os.makedirs(tmp_path)

        s3.download_file(bucket_name, f_name, path+f_name)
    





