source("./R/utilities.R")
source("./R/opts.R")
library(parallel)
wikis <- c("fawiki","ruwiki","urwiki")
regex <- "(fa|ru|ur)\\.wikipedia\\.org"
results <- lapply(wikis, function(x){
  results <- sum(unlist(mysql_query("SELECT COUNT(*) AS edits FROM revision WHERE rev_timestamp > '20140523000000'", x)),
                 unlist(mysql_query("SELECT COUNT(*) AS edits FROM archive WHERE ar_timestamp > '20140523000000'", x)))
  results <- c(x, results)
  return(results)
})
results <- data.frame(matrix(unlist(results), nrow = length(results), 
                             byrow = TRUE), stringsAsFactors = FALSE)
names(results) <- c("project", "edits")

write.table(results, file = "./data/q7_edits.tsv", row.names = FALSE, quote = TRUE, sep = "\t")

dates <-get_files(earliest = Sys.Date()-360, latest = Sys.Date()-1)
results <- mclapply(X = dates, mc.preschedule = FALSE, mc.cores = 3,
                    mc.allow.recursive = FALSE, mc.cleanup = FALSE,
                    FUN = function(file){
                      
                      #Read in the file and restrict it to just pageviews
                      data <- to_pageviews(read_sampled_log(file))
                      cat(".")
                      #Sanitise the timestamps
                      data$timestamp <- format_timestamp(data$timestamp)
                      
                      #Filter to valid timestamps
                      data <- data[!is.na(data$timestamp),]
                      
                      #Extract the ones we care about
                      data <- data[grepl(x = data$url, pattern = regex, ignore.case=T, perl=T, useBytes=T),]
                      #Aggregate to the one-day level
                      data$timestamp <- as.Date(data$timestamp)
                      data <- as.data.table(data)
                      data <- data[,j = list(pageviews = .N), by = "timestamp"]
                      
                      #Report and return
                      return(data)
                    })