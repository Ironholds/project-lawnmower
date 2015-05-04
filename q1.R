source("./R/utilities.R")
source("./R/opts.R")

library(parallel)

q1_answer <- function(){
  
  #Grab the full paths to those files within the date range we want.
  dates <- get_files(earliest = as.Date("2014-03-01"), latest = as.Date("2015-04-01"))
  
  #Split this task over multiple cores (4, to be precise), providing a subset of the filenames
  #to each core
  results <- mclapply(X = dates, mc.preschedule = FALSE, mc.cores = 4,
                      mc.allow.recursive = FALSE, mc.cleanup = FALSE,
                      FUN = function(file){
                        
                        #Read in the file and restrict it to just pageviews
                        data <- to_pageviews(read_sampled_log(file))
                        
                        #Sanitise the timestamps
                        data$timestamp <- format_timestamp(data$timestamp)
                        
                        #Filter to valid timestamps
                        data <- data[complete.cases(data),]
                        
                        #Aggregate to the one-day level
                        data$timestamp <- as.Date(data$timestamp)
                        data <- as.data.table(data)
                        results <- data[,j = list(pageviews = .N), by = "timestamp"]
                        return(results)
                      })
}

q1_baseline <- function(){
  data <- read.delim("./data/q1_baseline.tsv", as.is = TRUE, header = TRUE, quote = "")
}