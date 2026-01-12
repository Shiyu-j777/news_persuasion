## Convert from fst to parquet files

import os
import pandas as pd
import datetime
import numpy as np


## If Cluster


input_directory = "/scratch/sj4461/congressional_inference"

header = "inferred_congressional_record_"

output_file = "/scratch/sj4461/congressional_inference/aggregate_inferred_congressional_record_filter_words_{filter_count}.parquet"

file_directory = os.path.join("/","scratch","sj4461","news_persuasion",\
    "workflow", "aggregate_code")

inference_file = os.path.join(file_directory, "aggregate_congressional.R")

filter_count_list = [10, 20, 30, 50]

###################################################
# Snakemake rules
###################################################


rule all:
    input:
        expand(output_file, filter_count = filter_count)


rule clean_comment:
    params:
        input_directory,
        header,
        "{count}"
    output:
        output_file
    script:
        inference_file
        
