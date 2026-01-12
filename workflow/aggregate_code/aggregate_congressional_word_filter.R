library(data.table)



input_directory <- snakemake@params[[1]]
header <- snakemake@params[[2]]

filter_count <- as.integer(snakemake@params[[3]])

output_file <-  snakemake@output[[1]]

output_data <- data.table()

for (year in 1981:2016){
    input_file <- paste(input_directory, "/", header, as.character(year), ".parquet", sep = "")
    
    current_data <- setDT(arrow::read_parquet(input_file))
    current_data <- current_data[stringr::str_count(Text, "\\S+") >= filter_count]
    current_data <- current_data[Speaker != "Unknown" & chamber %in% c("S", "H")]
    senate_data <- current_data[chamber == "S"]
    house_data <- current_data[chamber == "H"]
    rep_data <- current_data[Party == "R"]
    dem_data <- current_data[Party == "D"]
    senate_rep_data <- senate_data[Party == "R"]
    senate_dem_data <- senate_data[Party == "D"]
    house_rep_data <- house_data[Party == "R"]
    house_dem_data <- house_data[Party == "D"]
    
    current_causal_metrics <- data.table("Year" = year,
                                         "Chamber" = c("All","All", "All",
                                                "Senate", "Senate","Senate",
                                                "House", "House", "House"),
                                  "Partisan" = c("All", "D", "R",
                                                 "All", "D", "R",
                                                 "All", "D", "R"),
                                  "PersuasionType" = "causal",
                                  "Metrics" = c(mean(current_data$causal_predicted_score),
                                                       mean(dem_data$causal_predicted_score),
                                                       mean(rep_data$causal_predicted_score),
                                                       mean(senate_data$causal_predicted_score),
                                                       mean(senate_dem_data$causal_predicted_score),
                                                       mean(senate_rep_data$causal_predicted_score),
                                                       mean(house_data$causal_predicted_score),
                                                       mean(house_dem_data$causal_predicted_score),
                                                       mean(house_rep_data$causal_predicted_score)),
                                  "SE" = c(sd(current_data$causal_predicted_score)/sqrt(length(current_data$causal_predicted_score)),
                                           sd(dem_data$causal_predicted_score)/sqrt(length(dem_data$causal_predicted_score)),
                                           sd(rep_data$causal_predicted_score)/sqrt(length(rep_data$causal_predicted_score)),
                                           sd(senate_data$causal_predicted_score)/sqrt(length(senate_data$causal_predicted_score)),
                                           sd(senate_dem_data$causal_predicted_score)/sqrt(length(senate_dem_data$causal_predicted_score)),
                                           sd(senate_rep_data$causal_predicted_score)/sqrt(length(senate_rep_data$causal_predicted_score)),
                                           sd(house_data$causal_predicted_score)/sqrt(length(house_data$causal_predicted_score)),
                                           sd(house_dem_data$causal_predicted_score)/sqrt(length(house_dem_data$causal_predicted_score)),
                                           sd(house_rep_data$causal_predicted_score)/sqrt(length(house_rep_data$causal_predicted_score)))
                                  )
    
    current_moral_metrics <- data.table("Year" = year,
                                        "Chamber" = c("All","All", "All",
                                                       "Senate", "Senate","Senate",
                                                       "House", "House", "House"),
                                         "Partisan" = c("All", "D", "R",
                                                        "All", "D", "R",
                                                        "All", "D", "R"),
                                         "PersuasionType" = "moral",
                                         "Metrics" = c(mean(current_data$moral_predicted_score),
                                                       mean(dem_data$moral_predicted_score),
                                                       mean(rep_data$moral_predicted_score),
                                                       mean(senate_data$moral_predicted_score),
                                                       mean(senate_dem_data$moral_predicted_score),
                                                       mean(senate_rep_data$moral_predicted_score),
                                                       mean(house_data$moral_predicted_score),
                                                       mean(house_dem_data$moral_predicted_score),
                                                       mean(house_rep_data$moral_predicted_score)),
                                         "SE" = c(sd(current_data$moral_predicted_score)/sqrt(length(current_data$moral_predicted_score)),
                                                  sd(dem_data$moral_predicted_score)/sqrt(length(dem_data$moral_predicted_score)),
                                                  sd(rep_data$moral_predicted_score)/sqrt(length(rep_data$moral_predicted_score)),
                                                  sd(senate_data$moral_predicted_score)/sqrt(length(senate_data$moral_predicted_score)),
                                                  sd(senate_dem_data$moral_predicted_score)/sqrt(length(senate_dem_data$moral_predicted_score)),
                                                  sd(senate_rep_data$moral_predicted_score)/sqrt(length(senate_rep_data$moral_predicted_score)),
                                                  sd(house_data$moral_predicted_score)/sqrt(length(house_data$moral_predicted_score)),
                                                  sd(house_dem_data$moral_predicted_score)/sqrt(length(house_dem_data$moral_predicted_score)),
                                                  sd(house_rep_data$moral_predicted_score)/sqrt(length(house_rep_data$moral_predicted_score)))
    )
    
    current_emotional_metrics <- data.table("Year" = year,
                                            "Chamber" = c("All","All", "All",
                                                      "Senate", "Senate","Senate",
                                                      "House", "House", "House"),
                                        "Partisan" = c("All", "D", "R",
                                                       "All", "D", "R",
                                                       "All", "D", "R"),
                                        "PersuasionType" = "emotional",
                                        "Metrics" = c(mean(current_data$emotional_predicted_score),
                                                      mean(dem_data$emotional_predicted_score),
                                                      mean(rep_data$emotional_predicted_score),
                                                      mean(senate_data$emotional_predicted_score),
                                                      mean(senate_dem_data$emotional_predicted_score),
                                                      mean(senate_rep_data$emotional_predicted_score),
                                                      mean(house_data$emotional_predicted_score),
                                                      mean(house_dem_data$emotional_predicted_score),
                                                      mean(house_rep_data$emotional_predicted_score)),
                                        "SE" = c(sd(current_data$emotional_predicted_score)/sqrt(length(current_data$emotional_predicted_score)),
                                                 sd(dem_data$emotional_predicted_score)/sqrt(length(dem_data$emotional_predicted_score)),
                                                 sd(rep_data$emotional_predicted_score)/sqrt(length(rep_data$emotional_predicted_score)),
                                                 sd(senate_data$emotional_predicted_score)/sqrt(length(senate_data$emotional_predicted_score)),
                                                 sd(senate_dem_data$emotional_predicted_score)/sqrt(length(senate_dem_data$emotional_predicted_score)),
                                                 sd(senate_rep_data$emotional_predicted_score)/sqrt(length(senate_rep_data$emotional_predicted_score)),
                                                 sd(house_data$emotional_predicted_score)/sqrt(length(house_data$emotional_predicted_score)),
                                                 sd(house_dem_data$emotional_predicted_score)/sqrt(length(house_dem_data$emotional_predicted_score)),
                                                 sd(house_rep_data$emotional_predicted_score)/sqrt(length(house_rep_data$emotional_predicted_score)))
    )
    
    
    current_empirical_metrics <- data.table("Year" = year,
                                            "Chamber" = c("All","All", "All",
                                                          "Senate", "Senate","Senate",
                                                          "House", "House", "House"),
                                            "Partisan" = c("All", "D", "R",
                                                           "All", "D", "R",
                                                           "All", "D", "R"),
                                            "PersuasionType" = "empirical",
                                            "Metrics" = c(mean(current_data$empirical_predicted_score),
                                                          mean(dem_data$empirical_predicted_score),
                                                          mean(rep_data$empirical_predicted_score),
                                                          mean(senate_data$empirical_predicted_score),
                                                          mean(senate_dem_data$empirical_predicted_score),
                                                          mean(senate_rep_data$empirical_predicted_score),
                                                          mean(house_data$empirical_predicted_score),
                                                          mean(house_dem_data$empirical_predicted_score),
                                                          mean(house_rep_data$empirical_predicted_score)),
                                            "SE" = c(sd(current_data$empirical_predicted_score)/sqrt(length(current_data$empirical_predicted_score)),
                                                     sd(dem_data$empirical_predicted_score)/sqrt(length(dem_data$empirical_predicted_score)),
                                                     sd(rep_data$empirical_predicted_score)/sqrt(length(rep_data$empirical_predicted_score)),
                                                     sd(senate_data$empirical_predicted_score)/sqrt(length(senate_data$empirical_predicted_score)),
                                                     sd(senate_dem_data$empirical_predicted_score)/sqrt(length(senate_dem_data$empirical_predicted_score)),
                                                     sd(senate_rep_data$empirical_predicted_score)/sqrt(length(senate_rep_data$empirical_predicted_score)),
                                                     sd(house_data$empirical_predicted_score)/sqrt(length(house_data$empirical_predicted_score)),
                                                     sd(house_dem_data$empirical_predicted_score)/sqrt(length(house_dem_data$empirical_predicted_score)),
                                                     sd(house_rep_data$empirical_predicted_score)/sqrt(length(house_rep_data$empirical_predicted_score)))
    )
    
    output_data <- rbindlist(list(output_data, 
                                  current_causal_metrics,
                                  current_empirical_metrics,
                                  current_moral_metrics,
                                  current_emotional_metrics))
    
}

arrow::write_parquet(output_data, sink = output_file)
