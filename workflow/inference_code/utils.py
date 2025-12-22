
import os
import datasets
import numpy as np
from sklearn.metrics import f1_score, precision_score, recall_score, mean_squared_error, root_mean_squared_error
import pandas as pd

import torch
import torch.nn as nn
from transformers import Trainer


def load_and_tokenize_data(input_path:str, tokenizer, text_field:str, \
                           file_type:str, label_field:str):
    """
    Load and tokenize data wrapper function, and deal with labels
    Input:
        input_path(str): The input directory
        tokenizer: The tokenizer corresponding to the model class
        text_field(str): The text field that needs to be tokenized
        file_type(str): File suffix
        label_field(dict): label field
    Output:
        loaded_dataset(datasets): The loaded dataset

    """

    loaded_dataset = datasets.load_dataset(file_type, data_files = {"train":os.path.join(input_path, "train."+ file_type),
                                                                    "validation": os.path.join(input_path, "eval."+ file_type),
                                                                    "test": os.path.join(input_path, "test."+ file_type)},
                                                                    streaming = True)


    loaded_dataset["train"] = loaded_dataset["train"].map(tokenize_sentence, fn_kwargs= {"tokenizer" : tokenizer,
                                                                                         "text_field": text_field},
                                                                                         batched=True)

    loaded_dataset["validation"] = loaded_dataset["validation"].map(tokenize_sentence, fn_kwargs= {"tokenizer" : tokenizer,
                                                                                         "text_field": text_field},
                                                                                         batched=True)
    loaded_dataset["test"] = loaded_dataset["test"].map(tokenize_sentence, fn_kwargs= {"tokenizer" : tokenizer,
                                                                                       "text_field": text_field},
                                                                                         batched=True)

    loaded_dataset["train"] = loaded_dataset["train"].map(label_data, fn_kwargs={"label_field": label_field})
    loaded_dataset["validation"] = loaded_dataset["validation"].map(label_data, fn_kwargs={"label_field": label_field})
    loaded_dataset["test"] = loaded_dataset["test"].map(label_data, fn_kwargs={"label_field": label_field})

    return loaded_dataset


def load_and_tokenize_test_data(input_data:str, tokenizer, text_field:str, \
                           file_type:str):
    """
    Load and tokenize data wrapper function, and deal with labels
    Input:
        input_data(str): The input data for testing, need to contain labels
        tokenizer: The tokenizer corresponding to the model class
        text_field(str): The text field that needs to be tokenized
        file_type(str): File suffix
        label_dict(dict): label dict
    Output:
        loaded_dataset(datasets): The loaded dataset

    """

    loaded_dataset = datasets.load_dataset(file_type, data_files = {"test": os.path.join(input_data)})


    loaded_dataset["test"] = loaded_dataset["test"].map(tokenize_sentence, fn_kwargs= {"tokenizer" : tokenizer,
                                                                                         "text_field": text_field},
                                                                                         batched=True)

    return loaded_dataset

def tokenize_sentence(data_split, tokenizer, text_field):
    """
    Function to tokenize sentence, will be mapped on the dataset split
    """
    return tokenizer(data_split[text_field], padding='max_length', truncation=True, max_length=512)

def label_data(data_split, label_field):
    """
    Function to select correct label, will be mapped on the dataset split
    """
    data_split["label"] = data_split[label_field]
    return data_split


def compute_metrics_binary(eval_prediction_object):
    """
    Compute Metrics from Prediction Object:
    eval_prediction_object: Output for prediction

    """

    logits, gt = eval_prediction_object
    predictions = np.argmax(logits, axis=-1)

    metrics_dict = {"macro_f1": f1_score(y_true=gt, y_pred=predictions, average = "macro"),
                    "cognitive_f1": f1_score(y_true=gt, y_pred=predictions, average = "binary"),
                    "affective_f1": f1_score(y_true= 1- gt, y_pred= 1 - predictions, average = "binary"),
                    "macro_precision": precision_score(y_true=gt, y_pred=predictions, average = "macro"),
                    "cognitive_precision": precision_score(y_true=gt, y_pred=predictions, average = "binary"),
                    "affective_precision": precision_score(y_true= 1- gt, y_pred= 1 - predictions, average = "binary"),
                    "macro_recall": recall_score(y_true=gt, y_pred=predictions, average = "macro"),
                    "cognitive_recall": recall_score(y_true=gt, y_pred=predictions, average = "binary"),
                    "affective_recall": recall_score(y_true= 1- gt, y_pred= 1 - predictions, average = "binary")}

    return metrics_dict


def compute_metrics_continuous(eval_prediction_object):
    """
    Compute Metrics from Prediction Object:
    eval_prediction_object: Output for prediction

    """

    logits, gt = eval_prediction_object
    predictions = np.argmax(logits, axis=-1)

    metrics_dict = {"RMSE": root_mean_squared_error(y_true=gt, y_pred=logits) }

    return metrics_dict


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