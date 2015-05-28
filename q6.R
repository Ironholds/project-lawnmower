source("./R/utilities.R")
source("./R/opts.R")

library(openssl)
library(rgeolocate)
geo_country_path <- "/usr/local/share/GeoIP/GeoIP2-Country.mmdb"

data <- global_query("SELECT LEFT(cuc_timestamp, 8) AS timestamp,
                      cuc_ip as ip_address,
                      cuc_user AS user_id,
                      cuc_user_text AS user_name
                      FROM cu_changes
                      WHERE cuc_type IN(0,1)")

salt <- as.character(rand_num(1))
data$user <- sha512(paste0(data$user_id, data$user_name), salt = salt)
results <- data.table(date = timestamp, user = sha512(paste0(data$user_id, data$user_name),
                                                      salt = as.character(rand_num(1))),
                      location = maxmind(data$ip_address, geo_country_path)$country_code,
                      project = gsub(x = data$project, pattern = "wiki.*", replacement = ""))

save(results, file = "legal_data_please_preserve.RData")

write.table(as.data.frame(table(results$project)), "q6_output.tsv", row.names=F, sep = "\t", quote = T)