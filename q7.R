source("./R/utilities.R")
source("./R/opts.R")

wikis <- c("fawiki","ruwiki","urwiki")

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
