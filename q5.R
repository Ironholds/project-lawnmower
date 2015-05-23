data <- readr::read_delim("./data/q1_results.tsv", delim = "\t")
data$timestamp <- as.Date(data$timestamp)
data <- data[data$timestamp >= as.Date("2015-02-01") & data$timestamp < as.Date("2015-05-1"),]
sum(data$pageviews)