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

#Calculate pageviews
to_pageviews <- function(data){
  
  fixed_grep <- function(field, pattern){
    grepl(x = field, pattern = pattern, fixed = TRUE, useBytes = TRUE)
  }
  
  fast_grep <- function(field, pattern){
    grepl(x = field, pattern = pattern, useBytes = TRUE, perl = TRUE)
  }
  
  is_app_pageview <- function(x){
    is_app <- fixed_grep(x$user_agent, "WikipediaApp")
    is_pv <- fixed_grep(x$url, "sections=0")
    is_ios_pv <- (fixed_grep(x$url, "sections=all") & fixed_grep(x$url, "iPhone") & is_app)
    return(x[(is_app & is_pv) | is_ios_pv,])
  }
  
  data <- data[data$mime_type %in% c("text/html; charset=iso-8859-1",
                                     "text/html; charset=ISO-8859-1",
                                     "text/html",
                                     "text/html; charset=utf-8",
                                     "text/html; charset=UTF-8",
                                     "application/json; charset=utf-8"),]
  
  data <- data[fast_grep(data$status_code, "(200|304)"),]
  data$url <- urltools::url_decode(data$url)
  data <- data[fast_grep(data$url, paste0("((commons|meta|incubator|species)\\.((m|mobile|wap|zero)\\.)?wikimedia|",
                                          "(wik(ibooks|idata|inews|ipedia|iquote|isource|tionary|iversity|ivoyage)))",
                                          "\\.org")),]
  data <- data[!fixed_grep(data$url, pattern = "donate.wikimedia.org"),]
  data <- data[fast_grep(data$url, "(/sr(-(ec|el))?|\\?((cur|old)id|title)=|/w(iki)?/|/zh(-(cn|hans|hant|hk|mo|my|sg|tw))?/)"),]
  data <- data[!fast_grep(data$url, "(BannerRandom|CentralAutoLogin|MobileEditor|Undefined|UserLogin|ZeroRatedMobileAccess)"),]
  
  is_api <- fixed_grep(data$url, "api.php")
  data <- rbind(data[!is_api,], is_app_pageview(data[is_api,]))
  return(data)
}

#Takes log-formatted timestamps and turns them into actual POSIX timestamps
format_timestamp <- function(x){
  x <- iconv(x, to = "UTF-8")
  x[nchar(x) > 19] <- substring(x[nchar(x) > 19],1,19)
  return(strptime(x, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC"))
}

#Query a single MediaWiki db
mysql_query <- function(query, db){
  
  #Open connection to the MySQL DB
  con <- dbConnect(drv = "MySQL",
                   host = "analytics-store.eqiad.wmnet",
                   dbname = db,
                   default.file = "/etc/mysql/conf.d/analytics-research-client.cnf")
  
  #Query and retrieve
  to_fetch <- dbSendQuery(con, query)
  data <- as.data.table(fetch(to_fetch, -1))
  
  #End and disconnect
  dbClearResult(dbListResults(con)[[1]])
  dbDisconnect(con)
  
  return(data)
}

#Query multiple MediaWiki dbs
global_query <- function(query, project_type = "all", dt = TRUE){
    
  #Get wikis
  wikis <- mysql_query(query = "SELECT wiki FROM wiki_info", db = "staging")$wiki
  
  #Instantiate progress bar and note environment
  env <- environment()
  progress <- txtProgressBar(min = 0, max = (length(wikis)), initial = 0, style = 3)
  
  #Retrieve data
  data <- lapply(wikis, function(x, query){
    
    #Retrieve the data
    data <- mysql_query(query = query, db = x)
    
    if(nrow(data) > 0){
      data$project <- x
    } else {
      data <- NULL
    }
    
    #Increment the progress bar and return
    setTxtProgressBar(get("progress",envir = env),(progress$getVal()+1))
    return(data)
    
  }, query = query)
  cat("\n")
  
  #Bind it into a single object and return
  data <- do.call(what = "rbind", args = data)
  return(data)
}