#Get sampled log files within a date range.
get_files <- function(earliest = NULL, latest = NULL){
  
  parse_date <- function(date){
    return(gsub("-","",date))
  }
  
  files <- list.files("/a/squid/archive/sampled", full.names= TRUE, pattern = "gz$")
  
  if(!is.null(earliest)){
    file_dates <- as.numeric(substring(files,47,55))
    if(!is.null(latest)){
      files <- files[file_dates >= as.numeric(parse_date(earliest)) & file_dates <= as.numeric(parse_date(latest))]
    } else {
      files <- files[file_dates >= as.numeric(parse_date(earliest))]
    }
  }
  
  return(files)
}

#Read a sampled log file
read_sampled_log <- function(file){
  output_file <- tempfile()
  system(paste("gunzip -c", file, ">", output_file))
  data <- read.delim(output_file, as.is = TRUE, quote = "",
                     col.names = c("squid","sequence_no",
                                   "timestamp", "servicetime",
                                   "ip_address", "status_code",
                                   "reply_size", "request_method",
                                   "url", "squid_status",
                                   "mime_type", "referer",
                                   "x_forwarded", "user_agent",
                                   "lang", "x_analytics"))
  file.remove(output_file)
  return(data)
}