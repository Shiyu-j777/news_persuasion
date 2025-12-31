## Convert from fst to parquet files

import os
import pandas as pd
import datetime
import numpy as np


## If Cluster


input_directory = "/scratch/sj4461/congressional_inference"

header = "inferred_congressional_record_"

output_file = "/scratch/sj4461/congressional_inference/aggregate_inferred_congressional_record.parquet"

file_directory = os.path.join("/","scratch","sj4461","news_persuasion",\
    "workflow", "aggregate_code")

inference_file = os.path.join(file_directory, "aggregate_congressional.R")


###################################################
# Snakemake rules
###################################################


rule clean_comment:
    params:
        input_directory,
        header
    output:
        output_file
    script:
        inference_file
        
