library(data.table)
library(ggplot2)
library(data.table)

data <- setDT(arrow::read_parquet("~/downloads/aggregate_inferred_congressional_record_filter_words_20.parquet"))


ggplot(data[Chamber == "All" & Partisan == "All"], aes(x = Year, 
                                                       y = Metrics,
                                                       color = PersuasionType, 
                                                       group = PersuasionType)) +
    geom_line()+theme_bw()
    