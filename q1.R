source("./R/utilities.R")
source("./R/opts.R")

library(parallel)

q1_answer <- function(){
  
  #Grab the full paths to those files within the date range we want.
  dates <- get_files(earliest = as.Date("2014-03-01"), latest = as.Date("2015-05-01"))
  
  #Split this task over multiple cores (4, to be precise), providing a subset of the filenames
  #to each core
  results <- mclapply(X = dates, mc.preschedule = FALSE, mc.cores = 3,
                      mc.allow.recursive = FALSE, mc.cleanup = FALSE,
                      FUN = function(file){
                        
                        #Read in the file and restrict it to just pageviews
                        data <- to_pageviews(read_sampled_log(file))
                        
                        #Sanitise the timestamps
                        data$timestamp <- format_timestamp(data$timestamp)
                        
                        #Filter to valid timestamps
                        data <- data[!is.na(data$timestamp),]
                        
                        #Aggregate to the one-day level
                        data$timestamp <- as.Date(data$timestamp)
                        data <- as.data.table(data)
                        data <- data[,j = list(pageviews = .N), by = "timestamp"]
                        
                        #Report and return
                        cat(".")
                        return(data)
                      })
  
  results <- do.call("rbind", results)
  results <- results[results$timestamp >= as.Date("2014-03-01"),]
  results <- results[,j=list(pageviews = sum(pageviews)), by = "timestamp"]
  results$pageviews <- results$pageviews*1000
  write.table(results, file = "./data/q1_results.tsv", row.names = FALSE, quote = TRUE, sep = "\t")
}

q1_baseline <- function(){
  
  
  baseline_data <- read.delim("./data/q1_baseline.tsv", as.is = TRUE, header = TRUE, quote = "")
  baseline_data$timestamp <- as.Date(paste(baseline_data$year, baseline_data$month, baseline_data$day, sep = "-"))
  baseline_data$type <- "Unsampled"
  baseline_data <- baseline_data[,c("timestamp","type", "pageviews")]
  
  sampled_data <- read.delim("./data/q1_results.tsv", as.is = TRUE, header = TRUE)
  sampled_data$timestamp <- as.Date(sampled_data$timestamp)
  sampled_data <- sampled_data[sampled_data$timestamp %in% baseline_data$timestamp,]
  sampled_data$type <- "Sampled"
  sampled_data <- sampled_data[,c("timestamp","type", "pageviews")]
  data <- rbind(sampled_data, baseline_data)
  
  ggsave(filename = "q1_benchmark.png",
         plot = ggplot(test, aes(timestamp, pageviews, group = type, colour = type, type = type)) + 
           geom_line() + labs(title = "Benchmark of sampled and unsampled logs for calculating pageview counts\n April 2015",
                              x = "Date",
                              y = "Pageviews"))
  
}