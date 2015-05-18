source("./R/utilities.R")
source("./R/opts.R")

data <- global_query("SELECT COUNT(*) FROM page WHERE page_namespace = 0 AND NOT page_is_redirect")
#60873624