source("./R/utilities.R")
source("./R/opts.R")

q2_answer <- function(){
  extant_edits <- global_query("SELECT COUNT(*) AS edits FROM revision")$edits
  deleted_edits <- global_query("SELECT COUNT(*) AS edits FROM archive")$edits
  result <- sum(extant_edits,deleted_edits)
}