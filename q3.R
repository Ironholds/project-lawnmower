source("./R/utilities.R")
source("./R/opts.R")
library(rgeolocate)
library(pageviews)
library(parallel)
geo_path <- "/usr/local/share/GeoIP/GeoIP2-Country.mmdb"

dates <- get_files(earliest = as.Date("2015-02-01"), latest = as.Date("2015-05-01"))

#Split this task over multiple cores (4, to be precise), providing a subset of the filenames
#to each core
results <- mclapply(X = dates, mc.preschedule = FALSE, mc.cores = 3,
                    mc.allow.recursive = FALSE, mc.cleanup = FALSE,
                    FUN = function(file){
                      
                      #Read in the file and restrict it to just pageviews
                      data <- read_sampled_log(file)
                      
                      #Sanitise the timestamps
                      data$timestamp <- format_timestamp(data$timestamp)
                      
                      #Filter to valid timestamps
                      data <- data[!is.na(data$timestamp),c("timestamp", "ip_address", "x_forwarded")]
                      
                      #Geolocate
                      data$country <- maxmind(normalise_ips(data$ip_address, data$x_forwarded),
                                              geo_path)$country_code
                      data <- data[!data$country %in% c("US"),]
                      
                      #Aggregate to the one-day level
                      data$timestamp <- as.Date(data$timestamp)
                      data <- as.data.table(data)
                      data <- data[,j = list(pageviews = .N), by = "timestamp"]
                      
                      #Report and return
                      cat(".")
                      return(data)
                    })

results <- do.call("rbind", results)
results <- results[results$timestamp >= as.Date("2015-02-01") & results$timestamp < as.Date("2015-05-01"),]
results <- results[,j=list(pageviews = sum(pageviews)), by = "timestamp"]
results$pageviews <- results$pageviews*1000
write.table(results, file = "./data/q3_results.tsv", row.names = FALSE, quote = TRUE, sep = "\t")

baseline_data <- as.data.table(read.delim("./data/q3_baseline.tsv", as.is = TRUE, header = TRUE, quote = ""))
baseline_data$timestamp <- as.Date(paste(baseline_data$year, baseline_data$month, baseline_data$day, sep = "-"))
baseline_data <- baseline_data[,j=list(pageviews = sum(pageviews)), by = c("timestamp")]
baseline_data$type <- "Unsampled"
to_plot <- rbind(baseline_data,
                 results[results$timestamp %in% baseline_data$timestamp,])

ggsave(filename = "q3_benchmark.png",
       plot = ggplot(to_plot, aes(timestamp, pageviews, group = type, colour = type, type = type)) + 
         geom_line() + labs(title = "Benchmark of sampled and unsampled logs for calculating non-US\nHTTP/HTTPS requests \n April 2015",
                            x = "Date",
                            y = "Requests"))
q()