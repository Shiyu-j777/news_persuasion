import pandas as pd
import datasets
import numpy as np

from utils import assemble_prediction

from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import TrainingArguments, Trainer, DefaultDataCollator, TextClassificationPipeline

if __name__ == "__main__":

    original_file_directory = snakemake.params[0]
    year = snakemake.params[1]
    model_directory = snakemake.params[2]

    output_file = snakemake.output[0]

    

    text_field = "Text"
    label_epoch_pair = ["causal", "moral", "emotional", "empirical"]
    current_data = pd.read_csv(original_file_directory + "".join(["congress_speeches_", str(year), ".csv"]))
    model_name = 'FacebookAI/roberta-base'

    for model_type in label_epoch_pair:
        print(model_type)
        current_strategy_model = model_directory + "_".join([model_type, "model"])
        tokenizer = AutoTokenizer.from_pretrained(model_name, padding='max_length', truncation=True, max_length=512, force_download= True)
        persuasion_classification_model =  AutoModelForSequenceClassification.from_pretrained(model_directory)
        prediction_pipeline = TextClassificationPipeline(model = persuasion_classification_model, \
                                                         tokenizer = tokenizer,torch_dtype='float16', device = "cuda")
        
        predicted_values = prediction_pipeline([str(item) for item in current_data[text_field].tolist()], truncation = True, batch_size= 256)
        current_data = assemble_prediction(current_data, predicted_values, model_type)
    
    current_data.to_parquet(output_file, index = False)