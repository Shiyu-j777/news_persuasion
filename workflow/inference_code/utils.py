
import os
import datasets
import numpy as np
import pandas as pd

import torch
import torch.nn as nn
from transformers import Trainer



def assemble_prediction(original_data:pd.DataFrame, predict_output:list, prefix):
    """
    Assemble the predictions and return the df with predicted values
    Input:
        original_data(pd.DataFrame): Original dataset for inference
        predicted_values(list): List of dictionaries that contains the predictions
    Output:
        original_data(pd.DataFrame): A revised dataset of original_data
    """
    if (len(predict_output) !=  original_data.shape[0]):
        raise ValueError("The row count of the dataset doesn't match the prediction length.")

    predicted_scores = [single_prediction["score"] for single_prediction in predict_output]

    original_data[prefix + "_predicted_score"] = predicted_scores

    return original_data