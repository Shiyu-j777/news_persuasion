## Convert from fst to parquet files

import os
import pandas as pd
import datetime
import numpy as np


## If Cluster


input_directory = "/scratch/sj4461/congressional_records/"

model_directory = "/scratch/sj4461/final_rhetoric_detection_model/"

output_file = "/scratch/sj4461/congressional_inference/inferred_congressional_record_{year}.parquet"

file_directory = os.path.join("/","scratch","sj4461","news_persuasion",\
    "workflow", "inference_code")

inference_file = os.path.join(file_directory, "inference_congressional.py")


year_list = np.arange(1981,1983)

###################################################
# Snakemake rules
###################################################

rule all:
    input:
        expand(output_file, year = year_list)

rule clean_comment:
    params:
        input_directory,
        "{year}",
        model_directory
    output:
        output_file
    script:
        inference_file
        
